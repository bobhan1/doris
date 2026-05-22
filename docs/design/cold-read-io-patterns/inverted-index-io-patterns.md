# 倒排索引 `.idx` 文件读路径和 I/O Pattern

本文按倒排索引查询逻辑解释 `.idx` 读路径。V1/V2 compound 文件排布、ANN sub-file、以及 CLucene `readBytes()` 到 Doris `read_at` 的完整映射见 [文件格式排布和 `read_at` 清单](file-layout-and-read-at-inventory.md)。

## 文件和缓存边界

Doris 倒排索引文件由 `IndexFileReader` 和 CLucene 目录抽象读取。当前有两种存储格式：

- V1：每个 index id 和 suffix 对应单独的 `{rowset_id}_{segment_id}_{index_id}@{suffix}.idx`。
- V2：segment 级 `{rowset_id}_{segment_id}.idx`，文件内包含多个 index 的 sub-file directory entries。

倒排索引读同时使用三类缓存：

- `InvertedIndexSearcherCache`：缓存 fulltext searcher 或 BKD reader，避免重复打开和解析 `.idx`。
- `InvertedIndexQueryCache`：缓存某个 query 条件对应的 roaring bitmap，也缓存 null bitmap。
- `BlockFileCache`：底层 `.idx` 字节读仍然经 `CachedRemoteFileReader`，`IOContext::is_index_data` 会让这些字节进入 index cache queue。

`StoragePageCache` 是 `.dat` page 读用的缓存，倒排索引 CLucene 读不走 `PageIO`，因此不直接使用 `StoragePageCache`。

V2 compound `.idx` 内常见 sub-file 可以粗略分成两类：

- 元数据类：`null_bitmap`、`segments*`、`fnm`、`tii`、`bkd_meta`、`bkd_index`。
- posting/position 类：`tis`、`frq`、`prx` 等 term dictionary、doc frequency、position/proximity 数据。

这两类文件都会通过同一个 `DorisCompoundReader` 暴露给 CLucene 或 BKD reader。区别在于访问模式：元数据类通常在 searcher/BKD reader 构造或 null bitmap 读取时集中访问；posting/position 类通常在具体 query term、phrase、range 或 search DSL 已知后按查询内容访问。

## 初始化路径

普通 scan 在 `SegmentIterator::init_iterators()` 中同时初始化返回列迭代器和 index iterators。倒排索引入口是 `_init_index_iterators()`：

```text
SegmentIterator::_init_index_iterators
  -> create IndexQueryContext
  -> for each schema column:
       find TabletIndex metadata
       Segment::new_index_iterator(column, index_meta, read_options, &iterator)
          -> _create_column_meta_once
          -> get ColumnReader for the column
          -> _index_file_reader_open.call(_open_index_file_reader)
          -> ColumnReader::new_index_iterator
             -> ColumnReader::_load_index
                -> create FullTextIndexReader / StringTypeInvertedIndexReader / BkdIndexReader / AnnIndexReader
             -> index_reader->new_iterator
```

`Segment::_open_index_file_reader()` 只创建 `IndexFileReader` 对象。真正打开 `.idx` 和读取文件 header/directory entries 发生在 `IndexFileReader::init()`，通常由 `InvertedIndexReader::handle_searcher_cache()` 在 searcher cache miss 时触发。

## `.idx` 打开和目录读取

V2 格式下 `IndexFileReader::_init_from()` 的冷读顺序是：

```text
IndexFileReader::init
  -> get V2 index file path
  -> DorisFSDirectory::FSIndexInput::open
       -> fs->open_file(path, reader_options)
       -> reader_options.cache_type follows config::enable_file_cache
       -> is_doris_table = true
       -> tablet_id set
  -> _stream->setIoContext(io_ctx)
  -> _stream->setIndexFile(true)
  -> readInt(version)
  -> readInt(numIndices)
  -> for each index:
       readLong(indexId)
       read suffix
       read numFiles
       for each sub-file:
         read file name, offset, length
```

这些 `readInt/readLong/readBytes` 最终都会进入 `DorisFSDirectory::FSIndexInput::readInternal()`，然后调用 `_handle->_reader->read_at(_pos, result, &bytes_read, &_io_ctx)`。因此 `.idx` header 和 directory 的 cold I/O 仍然是 offset/size range read，只是由 CLucene buffered input 触发，读粒度由 CLucene buffer size 和调用模式决定。

V1 格式下，`IndexFileReader::_open()` 直接打开单个 V1 `.idx` 文件，并构造 `DorisCompoundReader(index_input, read_buffer_size)`。`DorisCompoundReader` 会读取 compound file directory，必要时把小 header sub-file 拷贝到 RAMDirectory。

依赖关系：

- 没有 `IndexFileReader::init()` 就无法知道 V2 `.idx` 中各 sub-file 的 offset 和 length。
- 没有 `DorisCompoundReader` 就无法让 CLucene 打开具体 term dictionary、posting、BKD 等 sub-file。
- searcher cache 命中时，上述打开和解析成本可以跳过。

## Searcher cache miss 的 I/O

`InvertedIndexReader::handle_searcher_cache()` 是倒排索引冷读的核心入口：

```text
handle_searcher_cache(context)
  -> build index_file_key
  -> InvertedIndexSearcherCache lookup
  -> on miss:
       IndexFileReader::init(read_buffer_size, io_ctx)
       IndexFileReader::open(index_meta, io_ctx)
       read_null_bitmap(context, dir)
       IndexSearcherBuilder::create_index_searcher_builder(type)
       create_index_searcher(builder, dir)
       insert InvertedIndexSearcherCache
```

`read_null_bitmap()` 会优先查 `InvertedIndexQueryCache`。cache miss 时，它打开 compound directory 内的 `null_bitmap` sub-file，把整个 null bitmap 文件读入内存并反序列化成 roaring bitmap。如果没有这个 sub-file，则缓存一个空 bitmap。

构造 fulltext searcher 或 BKD reader 时，CLucene 会读取所需的 dictionary、posting metadata、BKD metadata 等 sub-files。它们都是 `DorisCompoundReader::openInput(name)` 创建的 `CSIndexInput`，再由 `CSIndexInput::readInternal()` 转换成 base stream 的 `seek(fileOffset + start)` 和 `readBytes()`。base stream 最终还是 `FSIndexInput::readInternal()` 和 Doris `FileReader::read_at()`。

## Predicate 到倒排索引查询

`SegmentIterator::_get_row_ranges_by_column_conditions()` 在普通 `.dat` 数据页读取前调用 `_apply_inverted_index()`。列谓词路径是：

```text
SegmentIterator::_apply_inverted_index
  -> for each ColumnPredicate:
       _check_apply_by_inverted_index
       pred->evaluate(storage_name_and_type, index_iterator, num_rows, &_row_bitmap)
          -> construct InvertedIndexParam
          -> IndexIterator::read_from_index
             -> InvertedIndexIterator::select_best_reader
             -> reader->query(context, column_name, value, query_type, roaring, analyzer_ctx)
       _row_bitmap &= query result or adjusted result
```

常见 predicate 映射：

- `EQ`、`NE` 走 `EQUAL_QUERY`，`NE` 后续对 bitmap 取反或补集。
- `LT`、`LE`、`GT`、`GE` 走 BKD 或 string range query。
- `IN` 对每个 value 做 `EQUAL_QUERY` 并 OR 结果。
- `MATCH`、`MATCH_ANY`、`MATCH_ALL`、`MATCH_PHRASE`、`MATCH_PHRASE_PREFIX`、`MATCH_REGEXP` 走 fulltext query。
- `IS NULL`、`IS NOT NULL` 依赖 null bitmap。
- 部分 common expr 和 search DSL 会通过 `VExprContext::evaluate_inverted_index()` 走表达式级 index path。

如果 predicate 不能被当前 index 支持，或者出现可降级错误，`_downgrade_without_index()` 会保留 predicate，后续回到 `.dat` 数据列读取和表达式过滤。

## Fulltext 和 string index I/O

Fulltext reader 的 `query()` 先根据 query type 和 analyzer 生成 terms，然后构造 query cache key：

```text
FullTextIndexReader::query
  -> analyzer / parser produces term_infos
  -> InvertedIndexQueryCache lookup
  -> handle_searcher_cache
  -> QueryFactory::create(query_type)
  -> query->add(query_info)
  -> query->search(roaring)
  -> insert query cache
```

I/O pattern：

- 第一次 query 某个 index 时，searcher cache miss 读取 `.idx` header、directory、null bitmap、term dictionary 和 postings metadata。
- 查询执行阶段根据 term 和 query type 随机读取 term dictionary、posting list、position/proximity 等 sub-files。
- phrase、phrase prefix、regexp 等 query 可能读取更多 term enumeration 和 postings。
- phrase 和位置相关 query 会通过 `TermPositionsIterator` 触发 position/proximity 读取，`.prx` 类 sub-file 更容易成为冷读热点。
- query cache 命中时，可以直接得到 roaring bitmap，不再读取 `.idx`。

String type reader 的 exact/range query 也通过 CLucene searcher，但不做 full analyzer。`EQUAL_QUERY` 和 range query 会读 term dictionary 和 postings；range query 在 term 数过多时可能返回 `INVERTED_INDEX_BYPASS`，回退到 `.dat` 数据读取。

Search DSL 和 query-v2 path 也是倒排索引 I/O 的一部分。`FunctionSearch::evaluate_inverted_index_with_search_param()` 会把 search 参数转换成 query-v2 执行，随后由 `SegmentPostings`、`LoadedPostings` 等对象按 block 或全量 postings 方式读取 doc ids 和 position 信息。它与普通 predicate path 一样，最终产出 row bitmap 或匹配结果列，但微观 I/O 更接近“按 postings block 流式读取”，并且 phrase、score、top-k 场景会读取比普通 term equality 更多的 postings 数据。

## BKD index I/O

数值类型通常走 `BkdIndexReader`。路径是：

```text
BkdIndexReader::query
  -> get_bkd_reader(context)
       -> handle_searcher_cache
       -> BKDIndexSearcherBuilder builds lucene::util::bkd::bkd_reader
  -> encode query min/max
  -> InvertedIndexQueryCache lookup
  -> invoke_bkd_query
       -> bkd_reader->intersect(visitor)
       -> visitor fills roaring bitmap
  -> insert query cache
```

BKD 的微观 I/O 由 `bkd_reader->open()` 和 `intersect()` 控制：

- open 阶段读取 BKD tree metadata。
- range/equal 查询阶段按树结构访问 index blocks 和 leaf blocks。
- `try_query()` 会先用 `estimate_point_count()` 估算命中数量，超过 `inverted_index_skip_threshold` 时可 bypass，避免为低选择性谓词继续读大量 `.idx`。

BKD 的访问模式比普通 posting list 更像树上的随机访问。对冷读 prefetch 来说，只有在能提前知道 tree/leaf block 访问范围时才适合精确预取；否则只能在 `.idx` 文件级或 sub-file 级做较粗粒度预热。

## Null bitmap I/O

很多 predicate 在查询后还要读取 null bitmap。例如 comparison 和 in-list 需要把 null rows 从结果中剔除，因为 SQL 中 `NULL cmp VALUE` 在 `WHERE` 中视为 false。

路径：

```text
iterator->has_null()
  -> iterator->read_null_bitmap(cache_handle)
     -> InvertedIndexQueryCache lookup
     -> open null_bitmap sub-file on miss
     -> read entire null_bitmap file
     -> roaring::Roaring::read
```

这个 I/O 的特点是小文件全量读，通常和 searcher cache miss 绑定。`handle_searcher_cache()` 会在构造 searcher 前尝试读 null bitmap，以便复用已打开的 `DorisCompoundReader`。

## `.idx` 与 `.dat` 的依赖关系

倒排索引不是独立返回列数据，而是先产出 row bitmap。随后普通 scan 的 `.dat` 读取会使用这个 bitmap：

```text
.idx query -> roaring bitmap
  -> _row_bitmap &= bitmap
  -> SegmentIterator::_vec_init_lazy_materialization
  -> _read_columns_by_index(predicate columns)
  -> predicate/common expr fallback evaluation if needed
  -> _read_columns_by_rowids(non-predicate columns)
```

因此，`.idx` I/O 在 `.dat` 谓词列和数据列 I/O 之前。它的收益是减少后续 `.dat` 数据页读取；它的代价是额外 `.idx` 随机读和 searcher/query cache 构造。低选择性 predicate 可能通过 bypass 回退，避免 `.idx` 成本超过收益。

## 主要 I/O 类型矩阵

| I/O 类型 | 触发点 | 读什么 | 依赖 | 典型模式 |
| --- | --- | --- | --- | --- |
| V2 `.idx` header | `IndexFileReader::init()` | version、index 数、sub-file directory | searcher cache miss | sequential buffered reads |
| V1 compound directory | `DorisCompoundReader` ctor | compound file entries | open V1 index | sequential buffered reads |
| null bitmap | `read_null_bitmap()` | `null_bitmap` sub-file | nullable predicate/searcher miss | whole sub-file read |
| Fulltext searcher | `handle_searcher_cache()` | term dictionary/postings metadata | searcher cache miss | CLucene controlled random/sequential |
| Fulltext query | `FullTextIndexReader::query()` | terms、postings、positions | query cache miss | term-driven random reads |
| String exact/range | `StringTypeInvertedIndexReader::query()` | term dictionary/postings | query cache miss | term/range enumeration |
| BKD reader | `BkdIndexReader::get_bkd_reader()` | BKD metadata/tree | searcher cache miss | tree metadata reads |
| BKD query | `BkdIndexReader::invoke_bkd_query()` | BKD tree/leaf blocks | query cache miss | tree-guided random reads |
| Query-v2 postings | `SegmentPostings` / `LoadedPostings` | postings block、position block | search DSL / phrase / top-k | block streaming or materialized postings |

## 观测指标

倒排索引过滤在 scan profile 中有专门指标，常用来判断 `.idx` 冷读是否真的减少了 `.dat` 数据页读取：

- `RowsInvertedIndexFiltered`：倒排索引过滤掉的行数。
- `InvertedIndexFilterTime`：倒排索引过滤总耗时。
- query cache 和 searcher cache 的 hit/miss counters：区分“读取 `.idx` 文件”与“复用已有 bitmap/searcher”。
- searcher open/search timers：区分 searcher 构建开销和 query 执行开销。
- `InvertedIndexLookupTimer`：倒排索引 lookup 的细粒度耗时。

底层 file cache 还会区分 inverted-index local、remote、peer bytes，这些统计来自 `.idx` 读路径上的 `IOContext::is_inverted_index` 标记。冷查优化设计需要把 profile 层的 inverted-index filter 指标和 file-cache 层的 inverted-index bytes 结合看，单独看其中一层容易误判。

## 对 prefetch 的含义

倒排索引预取比 `.dat` 数据页预取更难，因为查询前通常只能知道“会用哪个 index”，很难知道 term dictionary、posting list 或 BKD leaf 的精确 offset。较实际的接口设计可以分层：

1. index file open prefetch：按 index id 预热 `.idx` header 和 directory entries。
2. searcher build prefetch：在知道要使用 fulltext/string/BKD reader 后，预热 searcher cache miss 需要的 metadata。
3. query-aware prefetch：在 query terms 或 BKD range 已知后，由 reader 暴露将要访问的 sub-file ranges。
4. result-driven `.dat` prefetch：倒排索引产出 bitmap 后，立即把 surviving rowids 转成 `.dat` page/cache-block ranges，预取后续谓词列和非谓词列数据。

当前代码没有第 3 层的 offset 暴露能力。短期更可行的收益点是第 4 层，即把 `.idx` 过滤后的 `_row_bitmap` 用于 `.dat` 预取；这也正是普通 scan 中 `SegmentPrefetcher::build_blocks_by_rowids(_row_bitmap, ...)` 的方向。
