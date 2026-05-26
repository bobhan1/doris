# 倒排索引 `.idx` 文件格式、读路径和 I/O Pattern

本文只描述 Doris 内表倒排索引文件，也就是 `.idx`。`.dat` 数据文件、普通列
data page、TopN 全局延迟物化二阶段 rowid fetch 见 `dat-file-io-patterns.md`。
按普通 scan、索引辅助 scan、TopN 二阶段、点查等执行场景串起 `.idx` 与 `.dat`
读模式的说明见 `scenario-read-patterns.md`。

倒排索引读路径和 `.dat` 最大的区别是：Doris 只实现 compound 文件封装、Doris
FileReader 桥接、cache context 传递和部分 query reader；term dictionary、
postings、position、BKD tree 的具体访问序列大多由 CLucene/BKD/FAISS 第三方代码
决定。

## 只看 Doris 代码是否足够

结论：不完全够。

| 问题 | 只看 Doris 代码能否确定 | 说明 |
| --- | --- | --- |
| `.idx` V1/V2 物理文件格式 | 能 | 写入和读取都在 `IndexStorageFormatV1/V2`、`IndexFileReader`、`DorisCompoundReader` 中 |
| `.idx` 打开时如何落到 Doris `read_at` | 能 | `FSIndexInput::readInternal()` 直接调用 `_reader->read_at(_pos, len, &_io_ctx)` |
| compound sub-file 如何映射 offset | 能 | `CSIndexInput::readInternal()` 把 sub-file local offset 转成 `fileOffset + start` |
| null bitmap、ANN IVF list 这类 Doris 显式逻辑 | 基本能 | Doris 代码显式 open sub-file 并 readBytes；ANN 再进入 FAISS reader |
| fulltext/string term dictionary 和 postings 的每一次 seek/read | 不能只靠 Doris | `IndexReader`、`TermQuery`、`TermDocs`、`TermPositions` 等内部访问计划在 CLucene |
| BKD range/equality 查询的每一次 tree/leaf block read | 不能只靠 Doris | Doris 调用 `bkd_reader->intersect()`，树遍历和 block 读取在 BKD 实现 |
| FAISS `read_index()` 内部读取序列 | 不能只靠 Doris | Doris 提供 `IndexInput`/`RandomAccessReader`，FAISS 决定读取顺序 |

当前 checkout 中能看到 Doris 对 CLucene/FAISS 的适配代码，但没有能枚举 CLucene
内部 `TermInfosReader`、`TermDocs`、`BufferedIndexInput` 等完整实现源码。因此本文
把 Doris 侧所有落到 `read_at` 的桥接点列全；对第三方内部微观序列，只能按 sub-file
类别和 query 形态描述。若要精确到 term/posting/BKD 的每次第三方读，需要读取实际
链接的 CLucene/BKD/FAISS 源码版本，或在 `FSIndexInput::readInternal()` 和
`CSIndexInput::readInternal()` 加 offset/len/sub-file/query context 观测。

## `.idx` 文件格式和物理排布

倒排索引路径前缀从 `.dat` 去掉 suffix 得到：

- `.dat`：`{prefix}.dat`
- V1 `.idx`：`{prefix}_{index_id}@{suffix}.idx`
- V2 `.idx`：`{prefix}.idx`

V1 是每个 tablet index 一个 compound 文件。写入格式来自
`IndexStorageFormatV1::write_header_and_data()`：

```text
V1 compound .idx =
  file_count: VInt
  repeat file_count:
    file_name: CLucene string
    data_offset: int64
    file_length: int64
    optional inline file bytes when data_offset == -1
  non-inline file bytes...
```

小 header 文件可能被内联进 compound header，此时 entry offset 是 `-1`。读端
`DorisCompoundReader` 构造时会把这些小文件复制到 `RAMDirectory`，后续 CLucene
打开该 sub-file 不再读远端 `.idx`。

V2 是 segment 级 compound 文件，一个 `.idx` 中包含多个 index id/suffix 的 sub-file
entry。写入格式来自 `IndexStorageFormatV2`：

```text
V2 compound .idx =
  version: int32
  num_indices: int32
  repeat num_indices:
    index_id: int64
    suffix_length: int32
    suffix bytes
    num_files: int32
    repeat num_files:
      file_name_length: int32
      file_name bytes
      file_offset: int64
      file_length: int64
  meta sub-file bytes...
  normal sub-file bytes...
```

V2 会把 meta sub-file 放在 normal sub-file 前面。当前 descriptor 中常见分类是：

| 分类 | sub-file | 典型用途 |
| --- | --- | --- |
| meta/index-data | `null_bitmap` | nullable row bitmap |
| meta/index-data | `segments.gen`、`segments_`、`fnm` | CLucene segment 和 field metadata |
| meta/index-data | `tii` | term dictionary index |
| meta/index-data | `bkd_meta`、`bkd_index` | BKD metadata / tree index |
| normal/posting | `tis` | term dictionary / term infos |
| normal/posting | `frq` | doc frequency / posting list |
| normal/posting | `prx` | position/proximity data |
| ANN | `ann.faiss`、`ann.ivfdata` 等 | FAISS index metadata 和 IVF on-disk lists |

这些 sub-file 都通过 `DorisCompoundReader::openInput(name)` 暴露给 CLucene、BKD 或
FAISS。V2 header 只负责告诉 Doris 每个 sub-file 的 compound-file offset 和 length；
具体读取哪些 sub-file、以什么顺序读取，取决于 searcher build 和 query 类型。

## 缓存和 reader 边界

倒排索引读同时涉及三类缓存：

- `InvertedIndexSearcherCache`：缓存 fulltext searcher、string searcher 或 BKD
  reader，命中后跳过 `.idx` open/searcher build 的大部分 I/O。
- `InvertedIndexQueryCache`：缓存 query 条件对应的 roaring bitmap，也缓存 null
  bitmap。命中后查询阶段不再读 `.idx`。
- `BlockFileCache`：底层 `.idx` 字节读仍由 `CachedRemoteFileReader` 处理，
  `IOContext::is_index_data` 会使这些字节进入 index cache queue。

`StoragePageCache` 是 `.dat` 的 `PageIO` 缓存。倒排索引 CLucene 读不走 `PageIO`，
因此不直接使用 `StoragePageCache`。

## `.idx` 打开和目录读取

V2 冷打开顺序：

```text
IndexFileReader::init
  -> get V2 index file path
  -> DorisFSDirectory::FSIndexInput::open
       -> fs->open_file(path, reader_options)
       -> cache_type follows config::enable_file_cache
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

这些 `readInt/readLong/readBytes` 是 CLucene `BufferedIndexInput` 上的读。它们不
一定一一对应远端 `read_at`，因为 buffer refill 会把多个小读合并到一次
`FSIndexInput::readInternal()`。但每次 `readInternal()` 最终都会调用 Doris
`FileReader::read_at(_pos, len)`。

V1 冷打开顺序：

```text
IndexFileReader::open
  -> open `{prefix}_{index_id}@{suffix}.idx`
  -> DorisCompoundReader(index_input, read_buffer_size)
       -> read V1 compound directory
       -> copy inline small header sub-files to RAMDirectory
```

V1 的非内联 sub-file 和 V2 的所有 sub-file 都经 `CSIndexInput` 映射：

```text
CSIndexInput::readInternal(b, len)
  -> start = getFilePointer()
  -> base->seek(fileOffset + start)
  -> base->readBytes(b, len)
  -> FSIndexInput::readInternal()
  -> FileReader::read_at(_pos, len)
```

## Doris 侧 `read_at` 清单

下表列的是当前 Doris 侧能精确枚举的 `.idx` 读点。第三方内部的小步
`readInt/readBytes` 会先被 CLucene buffer 合并，再进入 `FSIndexInput` 或
`CSIndexInput`；因此这里的“每次”是 Doris 能观测到的每次底层 `read_at` 场景。

| ID | 场景 | 代码入口 | 真实 `read_at` 位置 | offset/size 来源 | `IOContext` | 触发次数和依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| IDX-01 | V2 `.idx` header 和 directory | `IndexFileReader::_init_from()` 的 `readInt/readLong/readBytes` | `FSIndexInput::readInternal()` -> `_reader->read_at(_pos, len)` | CLucene buffer 当前 `_pos` 和 refill `len` | `_stream->setIndexFile(true)` 后 `is_index_data=true` | searcher cache miss 或显式 init；读取 version、num_indices、每个 index 的 suffix、sub-file offset/length |
| IDX-02 | V1 compound directory | `DorisCompoundReader(IndexInput*)` 的 `readVInt/readChars/readLong` | `FSIndexInput::readInternal()` -> `_reader->read_at(_pos, len)` | compound header 当前 `_pos` 和 CLucene buffer refill `len` | V1 open 时传入的 `io_ctx`，随后 `initialize()` 设置 context | 每个 V1 `{index_id}@{suffix}.idx` 打开时触发 |
| IDX-03 | V1 inline small header sub-file copy | `DorisCompoundReader::_copyFile()` | `_stream->readBytes()` -> `FSIndexInput::readInternal()` -> `read_at` | header 中 `offset=-1` 的 sub-file 内容 | 同 IDX-02 | 只对 V1 内联小文件触发；读后进入 `RAMDirectory` |
| IDX-04 | V1/V2 compound sub-file read | `CSIndexInput::readInternal()` | `base->seek(fileOffset + start)` -> `base->readBytes()` -> `FSIndexInput::readInternal()` -> `read_at` | sub-file entry 的 `fileOffset` + sub-file local offset | `CSIndexInput` 临时把 sub-file `io_ctx` 绑定到 base | 所有 term dict、posting、BKD、null bitmap、ANN sub-file 读都落到这里 |
| IDX-05 | null bitmap | `InvertedIndexReader::read_null_bitmap()` | `null_bitmap_in->readBytes(buf, null_bitmap_size)` -> IDX-04 | `null_bitmap` sub-file length | index-data | nullable predicate 或 searcher build 需要 null bitmap；query cache 命中则不读 |
| IDX-06 | fulltext/string searcher build | CLucene `IndexReader::open` / searcher builder | `segments*`、`fnm`、`tii`、`tis` 等 sub-file -> IDX-04 | CLucene 内部按 metadata/dictionary buffer refill | meta 文件一般 index-data，posting 文件按 normal 分类 | searcher cache miss 时触发；命中则跳过 |
| IDX-07 | term equality / IN / MATCH term query | CLucene `TermQuery` / `TermDocs` / term iterator | term dict/postings `readBytes()` -> IDX-04 | term 定位后的 dictionary/posting offset | 取决于 sub-file 类型 | query cache miss 时触发；IN 每个 value 可能触发一次 term 查询 |
| IDX-08 | phrase / phrase prefix / position query | CLucene `TermPositions` 或 query-v2 phrase postings | `.prx` / position postings `readBytes()` -> IDX-04 | position list offset | normal posting file | 只有需要 position 的 query 触发 |
| IDX-09 | string range query | `StringTypeInvertedIndexReader::query()` + CLucene range enumeration | term dictionary enumeration + postings -> IDX-04 | range term enumeration 控制 | 取决于 sub-file 类型 | term 过多可能 bypass，之后回到 `.dat` 数据读 |
| IDX-10 | BKD reader open | `BKDIndexSearcherBuilder::build()` / `bkd_reader::open` | `bkd_meta` / `bkd_index` / `bkd` sub-file -> IDX-04 | BKD metadata/tree offset | meta 文件 index-data；data 文件按 sub-file 分类 | BKD searcher cache miss 时触发 |
| IDX-11 | BKD range/equality query | `BkdIndexReader::invoke_bkd_query()` -> `bkd_reader->intersect()` | BKD tree/leaf block reads -> IDX-04 | tree traversal 决定 | 取决于 sub-file 类型 | query cache miss 时触发；`try_query()` 可先 estimate 并 bypass |
| IDX-12 | query-v2 postings block stream | `SegmentPostings` / `LoadedPostings` | postings block/position block `readBytes()` -> IDX-04 | query-v2 postings block offset | normal posting file | search DSL、phrase、top-k/score 等 query 触发 |
| IDX-13 | ANN index metadata | `AnnIndexReader::load_index()` -> `FaissVectorIndex::load()` -> `faiss::read_index()` | FAISS `IOReader` over `ann.faiss` -> `IndexInput::readBytes()` -> IDX-04 | FAISS reader sequential offset | ANN index file context | ANN searcher/load miss 时触发 |
| IDX-14 | ANN IVF on-disk list | `CachedRandomAccessReader::read_at()` / `borrow()` | `_input->seek(offset)` + `_input->readBytes()` -> IDX-04 | FAISS IVF list offset and length | per-read `g_current_io_ctx` | IVF_ON_DISK query 访问 list 时触发；命中 `AnnIndexIVFListCache` 则不读 |

## Predicate/query 到 I/O 的场景展开

普通 scan 在 `.dat` 数据页读取前调用倒排索引过滤：

```text
SegmentIterator::_apply_inverted_index
  -> ColumnPredicate::evaluate(...)
  -> IndexIterator::read_from_index
  -> InvertedIndexIterator::select_best_reader
  -> reader->query(...)
  -> produce roaring bitmap
  -> _row_bitmap &= bitmap
```

按 predicate/query 类型展开：

| 场景 | 主要代码路径 | `.idx` I/O | 后续 `.dat` 依赖 |
| --- | --- | --- | --- |
| `EQ` | `EQUAL_QUERY` -> fulltext/string term query 或 BKD equal | searcher miss: IDX-01/06 或 IDX-10；query miss: IDX-07 或 IDX-11；nullable 可能 IDX-05 | 结果 bitmap 收窄后再读谓词列/返回列 data page |
| `IN` | 每个 value 做 `EQUAL_QUERY` 并 OR | 每个 value 可能触发 IDX-07 或 IDX-11；query cache 可消除重复 | 同上 |
| `NE` | 先做 equality，再补集/取反 | equality I/O + null bitmap 语义 | 同上 |
| `LT/LE/GT/GE` 数值 | BKD range | IDX-10/11；低选择性可 bypass | bypass 时回到 `.dat` 谓词列过滤 |
| string range | CLucene range term enumeration | IDX-09；term 太多可 bypass | bypass 时回到 `.dat` 谓词列过滤 |
| `MATCH` / `MATCH_ANY` / `MATCH_ALL` | analyzer/parser -> fulltext query | searcher miss IDX-01/06；query miss IDX-07 | 产出 bitmap 后读 `.dat` |
| phrase / phrase prefix | fulltext + positions | IDX-07 + IDX-08，`.prx` 更容易成为冷读热点 | 产出 bitmap 后读 `.dat` |
| regexp | term enumeration + postings | 依赖 CLucene regexp query 的 term/posting 访问，归入 IDX-07/09 | 产出 bitmap 后读 `.dat` |
| `IS NULL` / `IS NOT NULL` | `read_null_bitmap()` | IDX-05 | bitmap 直接参与 `_row_bitmap` |
| search DSL / query-v2 | `VExprContext::evaluate_inverted_index()` / postings reader | IDX-12，可能伴随 IDX-06/07/08 | 产出 bitmap 或匹配结果列 |
| ANN | `AnnIndexReader` / `FaissVectorIndex` | load IDX-13；IVF_ON_DISK list IDX-14 | ANN topn predicate 产出 row ids 后继续 `.dat` 读取 |

如果 predicate 不能被当前 index 支持，或者出现可降级错误，
`_downgrade_without_index()` 会保留 predicate，后续回到 `.dat` 数据列读取和表达式
过滤。

## `.idx` I/O 模式矩阵

| I/O 类型 | 触发点 | 读什么 | 依赖 | 典型模式 |
| --- | --- | --- | --- | --- |
| V2 header/directory | `IndexFileReader::init()` | version、index 数、sub-file directory | searcher cache miss | sequential buffered reads |
| V1 compound directory | `DorisCompoundReader` ctor | compound file entries | V1 open | sequential buffered reads |
| V1 inline header copy | `DorisCompoundReader::_copyFile()` | 内联小 sub-file 内容 | V1 header entry `offset=-1` | sequential copy then RAMDirectory hit |
| null bitmap | `read_null_bitmap()` | `null_bitmap` sub-file | nullable predicate/searcher miss | whole sub-file read |
| Fulltext/string searcher | searcher builder / CLucene `IndexReader::open` | segment/field/term metadata | searcher cache miss | CLucene-controlled buffered reads |
| Term exact/IN/MATCH | CLucene term query | term dictionary、posting list | query cache miss | term-driven random/sequential reads |
| Phrase/position | CLucene positions / query-v2 phrase | postings + positions/proximity | phrase-like query | posting block and `.prx` reads |
| String range | string reader + CLucene range enumeration | term dictionary + postings | range predicate | term enumeration, may bypass |
| BKD reader | BKD searcher builder | BKD metadata/tree index | searcher cache miss | tree metadata reads |
| BKD query | `bkd_reader->intersect()` | BKD tree/leaf blocks | query cache miss | tree-guided random reads |
| Query-v2 postings | `SegmentPostings` / `LoadedPostings` | postings block、position block | search DSL / phrase / top-k | block streaming or materialized postings |
| ANN metadata | FAISS load | `ann.faiss` | ANN load/searcher miss | FAISS sequential reader |
| ANN IVF list | FAISS random access reader | `ann.ivfdata` list bytes | IVF_ON_DISK search | list-level random reads, cacheable |

## `.idx` 与 `.dat` 的依赖关系

倒排索引不直接返回列值。它先产出 row bitmap，然后普通 scan 用这个 bitmap 决定
`.dat` 谓词列和返回列读哪些 page：

```text
.idx query
  -> roaring bitmap
  -> SegmentIterator::_row_bitmap &= bitmap
  -> _vec_init_lazy_materialization
  -> _read_columns_by_index(predicate columns)
  -> predicate/common expr fallback evaluation if needed
  -> _read_columns_by_rowids(non-predicate columns)
```

因此 `.idx` I/O 在 `.dat` data page I/O 之前。它的收益是减少后续 `.dat` page
读取；代价是 searcher build、query cache miss、term/posting/BKD/ANN 的额外随机读。
低选择性 predicate 可能通过 bypass 避免继续读 `.idx`，回退到 `.dat` 数据过滤。

## file cache 层的二次 I/O

`.idx` 的底层 reader 同样可能是 `CachedRemoteFileReader`。`FSIndexInput` 和
`CSIndexInput` 发出的逻辑 `read_at` 进入 file cache 后，会按 block 对齐：

```text
FSIndexInput/CSIndexInput logical read
  -> CachedRemoteFileReader::read_at_impl(offset, size)
  -> BlockFileCache::get_or_set(aligned_offset, aligned_size)
  -> local cache file read, or remote read, or fallback remote read
```

CLucene buffer size、compound sub-file offset、file cache block size 三者会共同决定真实
远端 range read：

- 多个 CLucene 小读可能被 `BufferedIndexInput` 合并成一次 Doris `read_at`。
- 一个 Doris `read_at` 又可能被 file cache 放大到一个或多个 cache block。
- sub-file 本地 offset 先由 `CSIndexInput` 加上 compound `fileOffset`，再进入 base
  `.idx` reader。

## 观测指标

倒排索引过滤常用 profile/counter：

- `RowsInvertedIndexFiltered`：倒排索引过滤掉的行数。
- `InvertedIndexFilterTime`：倒排索引过滤总耗时。
- query cache 和 searcher cache hit/miss counters：区分“读取 `.idx` 文件”与“复用
  bitmap/searcher”。
- searcher open/search timers：区分 searcher 构建开销和 query 执行开销。
- `InvertedIndexLookupTimer`：倒排索引 lookup 的细粒度耗时。
- file cache 的 inverted-index local/remote/peer bytes：来自 `.idx` 路径上的
  `IOContext::is_index_data` / inverted-index context。

分析冷读时要把倒排索引 profile 和 file-cache bytes 一起看。只看 filter time
不知道是否真的发生远端读；只看 remote bytes 也不知道这些读是否换来了 `.dat` page
减少。

## 对 prefetch 接口的含义

倒排索引预取比 `.dat` data page 更难，因为 query 前通常只知道“会用哪个 index”，
不知道 term dictionary、posting list 或 BKD leaf 的精确 offset。较实际的接口需要
分层：

1. index file open prefetch：预热 V2 header/directory 或 V1 compound directory。
2. searcher build prefetch：在知道 fulltext/string/BKD/ANN reader 类型后，预热
   searcher cache miss 需要的 metadata。
3. query-aware prefetch：在 query term、range、phrase、ANN list 已知后，由 reader
   暴露将访问的 sub-file ranges。这个能力不能只靠当前 Doris 包装层，需要第三方
   reader 支持或 instrumentation。
4. result-driven `.dat` prefetch：`.idx` 产出 bitmap 后，立即把 surviving rowids
   转成 `.dat` page/cache-block ranges，预取后续谓词列和返回列。

短期最可控的是第 1、2、4 层。第 3 层涉及 CLucene/BKD/FAISS 内部访问计划，如果不
读第三方实现或加观测，Doris 包装层只能看到已经发生的 `read_at`，很难在 query
执行前准确预测 offset。
