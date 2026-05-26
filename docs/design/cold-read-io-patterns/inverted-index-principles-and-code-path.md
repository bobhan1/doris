# Doris 倒排索引原理、代码路径和 I/O Pattern

本文从倒排索引的基本概念开始，说明 Doris 当前代码中 string/fulltext/numeric inverted index 的写入组织、查询执行和冷读 I/O 形态。`.idx` compound 文件 header、sub-file offset 映射、底层 `read_at` 清单见 `inverted-index-io-patterns.md`；本文重点解释这些读为什么发生、读之间有什么依赖、以及不同 query 形态为什么会形成不同 I/O pattern。ANN 是独立的向量索引类型，虽然也复用 `.idx` compound 文件和部分 index-file 适配层，但不是本文讨论的经典倒排索引。

文中的代码块是当前关键路径节选，会省略 DBUG 分支、部分错误处理和与主题无关的上下文。

## 基本原理

倒排索引解决的问题是从“值找行”。列存文件 `.dat` 更适合按 rowid 顺序读列值，过滤一个条件通常要先定位并读取包含这些 rowid 的 data page，再解码列值做比较。倒排索引把某一列中出现过的 term、字符串值或数值范围结构提前写成索引，查询时先在 `.idx` 中得到满足条件的 segment-local rowid/docid 集合，然后把这个集合和 `_row_bitmap` 相交，后续 `.dat` 只读取 surviving rows 所在的 data pages。它不是最终数据存储，真正的列值仍在 `.dat`；`.idx` 的产物是“哪些 rowid 可能满足条件”。

CLucene 的基本抽象有四个：Document、Field、Term、Posting。Document 是索引中的最小编号单位，会被 CLucene 分配一个递增 docid；Field 是 document 中的一个可索引字段；Term 是 field value 经过 analyzer 后得到的词元，或 untokenized string 的完整值；Posting 是某个 term 对应的 docid 列表，必要时还包含 term frequency 和 position。倒排索引的核心结构就是 `field + term -> postings`。Doris 只用 CLucene 保存索引结构，不通过 CLucene 存储真实行数据；真实行数据仍由 segment `.dat` 中的列页保存。

在 Doris segment 内，string/fulltext 倒排索引把“一行”映射成一个 CLucene document，因此 CLucene docid 和 segment-local rowid 按写入顺序对齐。scalar string 列通常是一行一个 document、document 里一个 field；分词 fulltext field 会产生多个 term，所以同一个 docid 会出现在多个 term 的 postings 中；untokenized string field 只把完整字符串作为 term。ARRAY string 列仍是一行一个 document，但一行中的多个非空元素会作为多个 field/value 写进同一个 document，因此命中任意元素时返回该行 docid。写入空字符串、`ignore_above` 值或 string null document 的主要作用是保持 CLucene docid 和 Doris rowid 对齐；实际 SQL null 语义额外由 `null_bitmap` 维护。

numeric/date/decimal/bool/ip 等类型不使用 CLucene 的 term dictionary，而使用 BKD point tree。每个非空数值写成一个 sortable encoded value，再附带当前 rowid 作为 docid。scalar numeric 是一行一个 point；numeric ARRAY 可以是一行多个 point，这些 point 共享同一个 rowid。BKD 查询返回的也是 docid/rowid 集合，只是查找方式从 term dictionary + postings 变成 tree index + leaf block。

string/fulltext 索引的物理结构可以拆成三层。第一层是 term dictionary：`.tis` 保存完整 term 序列及其 `TermInfo`，`.tii` 保存每隔一段 term 的稀疏索引，用于快速定位 `.tis`；第二层是 postings：`.frq` 保存 term 命中的 docid delta、freq 和 skip 信息；第三层是 positions：当 field 保留 phrase support 时，`.prx` 保存每个命中文档中的 token position。查询一个 term 的依赖链是 `.tii` 内存二分 -> `.tis` seek/scan -> `TermInfo.freqPointer/proxPointer` -> `.frq` postings -> 可选 `.prx` positions。

BKD 索引的物理结构也可以拆成三层。`bkd_index` 保存 tree metadata、min/max、split values、leaf block file pointers 或 packed index；`bkd_meta` 保存 field type 和 root data pointer；`bkd_data` 保存叶子块中的 docids 和 encoded values。查询先在内存中的 `bkd_index` tree 上判断一个 subtree 和 query range 的关系，完全不相交的 subtree 不读 `bkd_data`，完全包含或部分相交的 leaf 才会读取 data leaf。它的依赖链是 `bkd_index` 全量加载 -> 内存 tree traversal -> 被选中的 `bkd_data` leaf block reads。

倒排索引有三个容易混淆的边界。第一，docid 是 segment 内局部编号，不是 tablet 全局 rowid，也不是主键；跨 segment 或跨 rowset 后，同一个数字 docid 表示不同 segment 的行。第二，Doris 的每个 inverted index 只负责一个 indexed column 或 ARRAY 元素列；下面例子为了并排说明会把多列放在一张表里，但实际 title、sku、tags、price 会分别有自己的 index writer、reader 和 `.idx` sub-file group，它们只是都用相同的 segment-local rowid/docid 编号。第三，倒排索引结果只说明“哪些 rowid 命中”，不携带输出列值；命中后仍要回到 `.dat` 读取 SELECT 需要的列。

最小例子如下，假设一个 segment 里有三行：

```text
rowid | title(fulltext) | sku(exact string) | tags(array string) | price(int)
----- | --------------- | ----------------- | ------------------ | ----------
0     | red apple       | A-1               | [fruit, red]       | 10
1     | green apple     | A-2               | [fruit, green]     | 12
2     | red phone       | P-1               | [device, red]      | 800
```

如果对 `title` 建 fulltext inverted index，Doris 写入 CLucene 时可以理解成下面的 document 和 term：

```text
title index documents

docid(rowid)=0
  field title -> term red   at position 0
              -> term apple at position 1

docid(rowid)=1
  field title -> term green at position 0
              -> term apple at position 1

docid(rowid)=2
  field title -> term red   at position 0
              -> term phone at position 1
```

对应的倒排表直观上是：

```text
term dictionary       postings(docids)        optional positions
---------------       ----------------        ------------------
title:apple     ->    [0, 1]                  0:[1], 1:[1]
title:green     ->    [1]                     1:[0]
title:phone     ->    [2]                     2:[1]
title:red       ->    [0, 2]                  0:[0], 2:[0]
```

因此 `title MATCH 'red'` 只需要查 `title:red`，得到 bitmap `{0,2}`。如果是 phrase 查询 `title MATCH_PHRASE 'red apple'`，只读 postings 还不够，因为 `red` 命中 `{0,2}`、`apple` 命中 `{0,1}`，docid 0 和 docid 2 是否满足 phrase 取决于 token position；读取 `.prx` 后能看到 docid 0 中 `red` 在 0、`apple` 在 1，二者相邻，所以 phrase 命中 `{0}`。

如果对 `sku` 建不分词 string index，每个完整字符串就是一个 term：

```text
sku:A-1 -> [0]
sku:A-2 -> [1]
sku:P-1 -> [2]
```

`sku = 'A-1'` 直接得到 bitmap `{0}`。如果对 `tags` 建 ARRAY string index，一行里的多个元素共享同一个 docid：

```text
tags:device -> [2]
tags:fruit  -> [0, 1]
tags:green  -> [1]
tags:red    -> [0, 2]
```

`array_contains(tags, 'red')` 或等价索引条件得到 `{0,2}`，表示 rowid 0 和 2 至少有一个数组元素是 `red`。这里不是把数组元素展开成多行；倒排索引中的多个 field/value 最终仍指向同一个 Doris rowid。

如果对 `price` 建 numeric inverted index，写入的是 BKD point：

```text
encoded price point -> rowid/docid
10                  -> 0
12                  -> 1
800                 -> 2
```

BKD tree 的形状可以简化理解为：

```text
             [10, 800]
             split: 12
             /       \
       leaf [10,12]  leaf [800]
        docs 0,1      doc 2
```

`price < 100` 在 tree 上判断右侧 `[800]` 完全不相交，不读右侧 leaf，只读左侧 leaf 得到 bitmap `{0,1}`。真实实现中的 tree 由 `bkd_index` 中的 split values、min/max 和 leaf block pointers 驱动，leaf 数据在 `bkd_data` 中，查询会读取命中的 leaf block。

多个索引条件会在 segment 内组合 bitmap，然后再决定 `.dat` 读哪些行。以上面数据为例：

```text
title MATCH 'red'     -> {0,2}
price < 100           -> {0,1}
bitmap intersection   -> {0}

surviving rowids {0}
  -> use .dat ordinal index to locate data pages
  -> read output columns' data pages that cover rowid 0
```

完整的数据流可以画成：

```text
SQL predicates
    |
    v
per-column inverted index readers
    |
    +-- title:red postings       -> bitmap {0,2}
    +-- price BKD range < 100    -> bitmap {0,1}
    |
    v
SegmentIterator row bitmap       -> bitmap {0}
    |
    v
.dat column readers
    |
    +-- ordinal index locates PagePointer
    +-- PageIO reads data page containing rowid 0
```

从 I/O 角度看，这个例子也说明了两个阶段的读。第一阶段读 `.idx`：fulltext term query 会按 `.tii -> .tis -> .frq` 找到 `red` 的 postings，BKD range 会按 `bkd_index -> bkd_data leaf` 找到 `< 100` 的 docids；第二阶段读 `.dat`：只对 bitmap `{0}` 覆盖到的 data page 做列读取。倒排索引越能把 bitmap 收窄，第二阶段读到的 data page 越少；但如果 bitmap 很稠密，第一阶段 `.idx` 读可能只是额外前置 I/O。

倒排索引和普通 page index 的根本差异是过滤粒度。ordinary zone map / bloom filter 仍然以 `.dat` data page 为单位过滤，结果是 row ranges；倒排索引直接以 rowid/docid 为单位返回 bitmap，可以表达稀疏命中。这个优势也带来 I/O 形态差异：`.idx` 会先读 dictionary/tree/posting/leaf，命中稀疏时 `.dat` 第二阶段通常变成按 rowid 的随机 data page 读取；命中稠密时 `.idx` 读可能成为额外前置成本。

## Doris 中的倒排索引类型

Doris 在 `ColumnReader::_load_index()` 根据列类型和 index properties 创建 reader。string-like 列如果配置 analyzer，就创建 `FullTextIndexReader`；string-like 列如果不分词，就创建 `StringTypeInvertedIndexReader`；numeric/date/decimal/bool/ip 等数值型列创建 `BkdIndexReader`。ARRAY 列会取元素类型建立索引：string array 是一行一个 CLucene doc、doc 中可有多个 field；numeric array 是多个 point 使用同一个 rowid。

三类 reader 对应三种查询模型：

| Reader | 适用列和查询 | 索引模型 | 主要 sub-file |
| --- | --- | --- | --- |
| `FullTextIndexReader` | 分词 string，`MATCH`、phrase、regexp 等 | analyzer 把文本切成 term，term dictionary 定位 postings，phrase 额外读 positions | `segments*`、`fnm`、`tii`、`tis`、`frq`、可选 `prx`、`null_bitmap` |
| `StringTypeInvertedIndexReader` | 不分词 string，`=`、`IN`、string range、部分 match query | 整个字符串或数组元素作为 untokenized term，查询仍用 CLucene term/range/posting | 同 fulltext |
| `BkdIndexReader` | numeric/date/decimal/bool/ip 的等值和范围 | 一维 BKD point tree，内部节点按 encoded value 分裂，叶子保存 docids 和 values | `bkd_meta`、`bkd_index`、`bkd_data`、`null_bitmap` |

`InvertedIndexIterator::select_best_reader()` 负责在同一个 index id 下选择最合适的 reader。文本 match 优先 fulltext，文本等值优先 string reader，数值范围优先 BKD；如果 analyzer 不匹配或 query 不能由当前 reader 安全处理，会返回 bypass 或 fallback，让执行继续走 `.dat` 列值过滤。

关键代码片段：`ColumnReader::_load_index()` 根据 index 类型、列类型和 analyzer 配置创建 reader。

```cpp
if (index_meta->index_type() == IndexType::ANN) {
    _index_readers[index_meta->index_id()] = std::make_shared<AnnIndexReader>(
            index_meta, index_file_reader, rowset_id, segment_id, rows_of_segment);
    return Status::OK();
}

if (is_string_type(type)) {
    if (should_analyzer) {
        index_reader = FullTextIndexReader::create_shared(index_meta, index_file_reader);
    } else {
        index_reader = StringTypeInvertedIndexReader::create_shared(index_meta, index_file_reader);
    }
} else if (is_numeric_type(type)) {
    index_reader = BkdIndexReader::create_shared(index_meta, index_file_reader);
}
_index_readers[index_meta->index_id()] = index_reader;
```

关键代码片段：`InvertedIndexIterator` 对文本和数值 reader 的选择规则。

```cpp
if (is_match_query(query_type)) {
    if (auto* best = pick_preferred(match.candidates, InvertedIndexReaderType::FULLTEXT)) {
        return best->reader;
    }
}
if (is_equal_query(query_type)) {
    if (auto* best = pick_preferred(match.candidates, InvertedIndexReaderType::STRING_TYPE)) {
        return best->reader;
    }
}

if (is_range_query(query_type)) {
    if (const auto* best = pick_preferred(match.candidates, InvertedIndexReaderType::BKD)) {
        return best->reader;
    }
}
```

## 写入组织

string/fulltext 写入由 `InvertedIndexColumnWriter::init_fulltext_index()` 初始化 CLucene `IndexWriter`。有 analyzer 时，`new_char_token_stream()` 把原始字符串交给 analyzer，tokenized field 写入 term；没有 analyzer 时，`new_field_char_value()` 把整段 bytes 作为 untokenized field。`create_field()` 中的 `setOmitTermFreqAndPositions()` 由 `parser_phrase_support` 控制：不需要 phrase 时省掉频次/位置信息；需要 phrase 时会写 `.prx` 位置流。写入结束后 CLucene 生成 term dictionary、term info index、postings、position 等 sub-files，并由 Doris 的 `IndexFileWriter` 收进 `.idx` compound 文件。

关键代码片段：string/fulltext 每写入一行就推进 `_rid`，正常值写成 CLucene document，不能索引的值写 null document 以保持 docid/rowid 对齐。

```cpp
const auto* v = (Slice*)values;
for (size_t i = 0; i < count; ++i) {
    if ((!_should_analyzer && v->get_size() > _ignore_above) ||
        (_should_analyzer && v->empty())) {
        RETURN_IF_ERROR(add_null_document());
    } else {
        RETURN_IF_ERROR(new_inverted_index_field(v->get_data(), v->get_size()));
        RETURN_IF_ERROR(add_document());
    }
    ++v;
    _rid++;
}
```

numeric 写入由 `InvertedIndexColumnWriter::init_bkd_index()` 创建 `lucene::util::bkd::bkd_writer`。Doris 先用 `KeyCoder::full_encode_ascending()` 把数值编码成按字节序可比较的 key，再调用 `_bkd_writer->add(encoded_value, rowid)`。writer 初始 leaf 上限是 `MAX_LEAF_COUNT=1024`，`finish()` 时还会结合 `max_depth_in_bkd_tree` 调整实际 tree depth/leaf size，并把最终参数写进 `bkd_index`。完成时写三个 BKD sub-file：`bkd_data` 保存 leaf block 的 docids 和 values，`bkd_index` 保存 tree metadata、min/max、split values 或 packed index，`bkd_meta` 保存 field type 和 `indexFP`。

关键代码片段：BKD writer 的初始化参数和每个 value 写入时携带的 `_rid`。

```cpp
_bkd_writer = std::make_shared<lucene::util::bkd::bkd_writer>(
        max_doc, DIMS, DIMS, value_length, MAX_LEAF_COUNT, MAXMBSortInHeap, total_point_count,
        true, config::max_depth_in_bkd_tree);

_value_key_coder->full_encode_ascending(&value, &new_value);
_bkd_writer->add((const uint8_t*)new_value.c_str(), value_length, _rid);
```

`null_bitmap` 对 string 和 numeric 都是独立 sub-file。`write_null_bitmap()` 把 writer 过程中收集的 null rowids 序列化为 roaring bitmap。查询 `IS NULL` / `IS NOT NULL`、nullable 谓词组合以及 searcher cache miss 时的 null 处理都可能读取这个 sub-file。它的 I/O 粒度是整个 `null_bitmap` sub-file 一次读入，而不是按 rowid 分块。

## 查询入口和执行链路

普通 scan 中倒排索引在 `SegmentIterator::_lazy_init()` 期间应用。整体路径是：

```text
SegmentIterator::_apply_inverted_index()
  -> ColumnPredicate::evaluate(...)
  -> ColumnReader::new_index_iterator(...)
  -> InvertedIndexIterator::read_from_index(...)
  -> select_best_reader(...)
  -> reader->query(...) or reader->try_query(...)
  -> roaring bitmap
  -> SegmentIterator::_row_bitmap &= bitmap
```

这意味着 `.idx` I/O 一定发生在该 segment 的 `.dat` 输出列读取之前。倒排索引产出的 bitmap 会改变后续 `.dat` 的读取形态：高选择性条件把连续 row range 切成稀疏 rowids，后续 data page 读可能从顺序推进变成按 surviving rowids 触碰少量 page；低选择性条件如果几乎不裁剪 rowids，则 `.idx` 读成为额外前置成本。

关键代码片段：`ColumnReader::new_index_iterator()` 先确保 reader 已创建，再把 reader 加到 `IndexIterator`。

```cpp
RETURN_IF_ERROR(
        _load_index(index_file_reader, index_meta, rowset_id, segment_id, rows_of_segment));
{
    std::shared_lock<std::shared_mutex> rlock(_load_index_lock);
    auto iter = _index_readers.find(index_meta->index_id());
    if (iter != _index_readers.end() && iter->second != nullptr) {
        RETURN_IF_ERROR(iter->second->new_iterator(iterator));
    }
}
```

查询执行有两个 cache 关口。`InvertedIndexQueryCache` 命中时，某个 query 条件的 roaring bitmap 已经存在，不需要打开 searcher，也不需要读 postings/BKD leaf。query cache miss 后会进入 `InvertedIndexSearcherCache`；searcher cache 命中时，CLucene `IndexSearcher` 或 BKD reader 已经建好，跳过 `.idx` header、CLucene metadata、`.tii` 或 `bkd_index` 的冷读；searcher cache miss 时才执行完整 open/build 路径。

关键代码片段：BKD 查询在真正读取 leaf 前可以先做命中数估算，超过阈值就 bypass。

```cpp
if (!i_param->skip_try && reader->type() == InvertedIndexReaderType::BKD) {
    if (runtime_state != nullptr &&
        runtime_state->query_options().inverted_index_skip_threshold > 0 &&
        runtime_state->query_options().inverted_index_skip_threshold < 100) {
        size_t hit_count = 0;
        RETURN_IF_ERROR(try_read_from_inverted_index(reader, i_param->column_name,
                                                     i_param->query_value, i_param->query_type,
                                                     &hit_count));
        if (hit_count > i_param->num_rows * query_bkd_limit_percent / 100) {
            return Status::Error<ErrorCode::INVERTED_INDEX_BYPASS>(
                    "hit count: {}, bkd inverted reached limit {}% , segment num rows:{}",
                    hit_count, query_bkd_limit_percent, i_param->num_rows);
        }
    }
}
```

## Fulltext/String 搜索器构建

fulltext 和 string reader 在 searcher cache miss 时调用 `InvertedIndexReader::handle_searcher_cache()`。Doris 先用 `IndexFileReader::init(config::inverted_index_read_buffer_size, io_ctx)` 打开 `.idx` 文件并读取 compound directory；`inverted_index_read_buffer_size` 默认是 4096 bytes。随后 `IndexFileReader::open()` 返回 `DorisCompoundReader`，CLucene 通过这个 directory 打开各个 sub-file。

关键代码片段：searcher cache miss 时，Doris 先打开 `.idx` compound directory，再读 null bitmap，最后构建 CLucene/BKD searcher 并插入 cache。

```cpp
auto st = _index_file_reader->init(config::inverted_index_read_buffer_size, context->io_ctx);
auto dir = DORIS_TRY(_index_file_reader->open(&_index_meta, context->io_ctx));

InvertedIndexQueryCacheHandle null_bitmap_cache_handle;
RETURN_IF_ERROR(read_null_bitmap(context, &null_bitmap_cache_handle, dir.get()));
size_t reader_size = 0;
auto index_searcher_builder =
        DORIS_TRY(IndexSearcherBuilder::create_index_searcher_builder(type()));
RETURN_IF_ERROR(create_index_searcher(index_searcher_builder.get(), dir.get(), &searcher,
                                      reader_size));
InvertedIndexSearcherCache::instance()->insert(searcher_cache_key, cache_value,
                                               inverted_index_cache_handle);
```

`FulltextIndexSearcherBuilder::build()` 调用 `lucene::index::IndexReader::open(directory, read_buffer_size, close_directory)`。在 CLucene 侧，`SegmentReader::initialize()` 会读取 field info，创建 `TermInfosReader`，打开 `.frq`，如果 field 有 positions 则打开 `.prx`。`TermInfosReader` 构造时同时打开 `.tii` 和 `.tis`，并立即调用 `ensureIndexIsRead()` 把 `.tii` 全量读入内存；`.tis` 保留为 term dictionary stream，后续按 term seek/scan。

关键代码片段：fulltext/string searcher build 进入 CLucene `IndexReader::open()`。

```cpp
reader = std::unique_ptr<lucene::index::IndexReader>(lucene::index::IndexReader::open(
        directory, config::inverted_index_read_buffer_size, close_directory));
reader_size = reader->getTermInfosRAMUsed();
auto index_searcher =
        std::make_shared<lucene::search::IndexSearcher>(reader.release(), close_reader);
```

`.tii` 是 term dictionary 的稀疏索引。CLucene writer 默认 `DEFAULT_TERM_INDEX_INTERVAL=128`，也就是每约 128 个 term 在 `.tii` 中保存一个入口。`TermInfosReader::get(term)` 查询时先在内存中的 `.tii` 数组做二分，得到不大于目标 term 的 dictionary 入口，再 `seekEnum(indexPointer)` 到 `.tis` 中对应位置，并顺序 scan 到目标 term。这个过程的依赖关系是 `.tii` 全量加载 -> `.tis` seek/scan -> 得到 `TermInfo{docFreq,freqPointer,proxPointer,skipOffset}` -> `.frq` / `.prx` postings 读取。

关键代码片段：CLucene `TermInfosReader::ensureIndexIsRead()` 把 `.tii` 全量加载到内存数组。

```cpp
indexTermsLength = (size_t)indexEnum->size;
indexTerms = new Term[indexTermsLength];
indexInfos = _CL_NEWARRAY(TermInfo, indexTermsLength);
indexPointers = _CL_NEWARRAY(int64_t, indexTermsLength);

for (int32_t i = 0; indexEnum->next(); ++i) {
    indexTerms[i].set(indexEnum->term(false), indexEnum->term(false)->text());
    indexEnum->getTermInfo(&indexInfos[i]);
    indexPointers[i] = indexEnum->indexPointer;
}
indexIsRead = true;
```

CLucene 的 `BufferedIndexInput` 会把小读合并成 buffer refill。小的 `readInt/readVInt/readString` 不会逐个变成远端读；当 buffer 不足时才调用 `DorisFSDirectory::FSIndexInput::readInternal()` 或 `CSIndexInput::readInternal()`，最终落到 Doris `FileReader::read_at(offset,len)`。默认小读 refill 是 4KB；如果一次 `readBytes(len)` 大于 buffer 或要求不用 buffer，则会按该 len 直接读。进入 file cache 后，逻辑 4KB 或更小读还会按 `file_cache_each_block_size` 对齐，当前默认是 1MB。

## Term/Postings 查询

`FullTextIndexReader::query()` 先把查询值解析成 `InvertedIndexQueryInfo`。普通 `MATCH` 会用 analyzer 得到一个或多个 term；`MATCH_PHRASE` 会解析 term sequence、slop 和 ordered；`MATCH_REGEXP` / prefix 类 query 会保留 pattern 或 prefix；`StringTypeInvertedIndexReader` 对 `=` / `IN` 则直接把字符串值作为 term。之后 `InvertedIndexReader::match_index_search()` 通过 `QueryFactory` 创建具体 query 对象。

`TermQuery` 是最小执行单元。`TermIterator::create()` 调用 CLucene `IndexReader` 创建 `TermDocs`，`SegmentTermDocs::seek(term)` 经 `TermInfosReader::get(term)` 找到 `TermInfo`，然后 `freqStream->seek(freqPointer)`。之后 `TermDocsBuffer::readRange()` 按 postings block 读取 docids。当前 contrib CLucene 的 `PFOR_BLOCK_SIZE=512`，新版本 postings 通过 `PforUtil::pfor_decode()` 解码，每个 block 先读 mode、arraySize、serializedSize，再读压缩 doc deltas；如果 field 保存 freq/positions，还会读 freq block。

关键代码片段：term query 从 Doris query object 进入 CLucene `TermDocs`，并通过 `readRange()` 取 postings block。

```cpp
auto t = make_term_ptr(field_name.c_str(), ws_term.c_str());
auto term_docs = make_term_doc_ptr(reader, t.get(), is_similarity, io_ctx);
return std::make_shared<TermIterator>(ws_term, std::move(term_docs));

MOCK_FUNCTION bool read_range(DocRange* docRange) const {
    return term_docs_->readRange(docRange);
}
```

关键代码片段：CLucene `TermInfosReader::get()` 先尝试顺序 scan，不能复用当前 enum 时再 seek 到 `.tii` 找到的 index offset。

```cpp
ensureIndexIsRead();
SegmentTermEnum* enumerator = getEnum();
if (enumerator) {
    enumerator->setIoContext(io_ctx);
}

if (enumerator->term(false) != NULL &&
    ((enumerator->prev != NULL && term->compareTo(enumerator->prev) > 0) ||
     term->compareTo(enumerator->term(false)) >= 0)) {
    int32_t _enumOffset = (int32_t)(enumerator->position / totalIndexInterval) + 1;
    if (indexTermsLength == _enumOffset ||
        term->compareTo(&indexTerms[_enumOffset]) < 0) {
        return scanEnum(term);
    }
}

seekEnum(getIndexOffset(term));
return scanEnum(term);
```

`MATCH_ANY` 对多个 term 创建多个 `TermIterator`，逐个读取 postings 后做 roaring OR。I/O 形态是多个 term 的 dictionary lookup 和 postings 顺序读，term 越多，`.tis` seek/scan 和 `.frq` posting block 读越多。`MATCH_ALL` / 多 term 等值语义会创建 `ConjunctionQuery`，先按 `doc_freq()` 从小到大排序 postings，再选择 bitmap intersection 或 skiplist intersection。skiplist intersection 会让较大的 postings list 通过 `skipTo(target)` 跳跃，CLucene 会懒加载 `DefaultSkipListReader` 并 seek 到新的 `.frq` pointer；这能减少解码的 docids，但会把原本顺序的 posting 读变成更多随机 seek。

phrase query 在 term postings 之外还依赖 positions。`PhraseQuery` 为每个 term 创建 `TermPositionsIterator`，先用 docid postings 找 candidate docs，再在 `nextPosition()` 时懒加载 `proxStream` 并按 `proxPointer` 读取 `.prx`。contrib CLucene 的 `TermPostingsBuffer` 对 index version >= V2 会用 `PforUtil::decodePos()` 成块解码 positions。phrase 的依赖链是 term dictionary -> `.frq` candidate docids/freq -> `.prx` positions -> phrase matcher；如果一个 query 需要多个 term 的 positions，`.prx` 会成为额外随机读来源。

关键代码片段：phrase path 创建 `TermPositionsIterator`，它和 `TermIterator` 的区别是底层使用 `TermPositions`，后续会读 `.prx` positions。

```cpp
auto t = make_term_ptr(field_name.c_str(), ws_term.c_str());
auto term_pos = make_term_positions_ptr(reader, t.get(), is_similarity, io_ctx);
return std::make_shared<TermPositionsIterator>(ws_term, std::move(term_pos));
```

string range 和 regexp 会先在 term dictionary 上枚举候选 terms，再读取这些 terms 的 postings。范围宽、regexp 展开 term 多时，I/O 会从“一个 term 的 dictionary+postings”放大为“多个 term 的 dictionary enumeration + 多个 postings”；`StringTypeInvertedIndexReader` 对 range 中 CLucene `TooManyClauses` 会返回 bypass，让上层继续用 `.dat` 列值过滤。

## BKD 查询

numeric/date/decimal/bool/ip 等类型使用 BKD。`BKDIndexSearcherBuilder::build()` 创建 `bkd_reader` 并调用 `open()`。`open()` 依次打开 `bkd_data`、`bkd_meta`、`bkd_index`：`bkd_meta` 只读 field type 和 `indexFP`；`bkd_index` 读 codec header、维度、leaf 参数、bytes_per_dim、num_leaves、min/max、point_count、doc_count，并把 packed index 或 split values / leaf block file pointers 读入内存；`bkd_data` 保留为后续 leaf block stream。

关键代码片段：BKD reader open 阶段读取 tree 结构和全局 min/max。

```cpp
num_data_dims_ = index_in->readVInt();
num_index_dims_ = version_ >= bkd_writer::VERSION_SELECTIVE_INDEXING
                          ? index_in->readVInt()
                          : num_data_dims_;
max_points_in_leaf_node_ = index_in->readVInt();
bytes_per_dim_ = index_in->readVInt();
num_leaves_ = index_in->readVInt();

min_packed_value_ = std::vector<uint8_t>(packed_index_bytes_length_);
max_packed_value_ = std::vector<uint8_t>(packed_index_bytes_length_);
index_in->readBytes(min_packed_value_.data(), packed_index_bytes_length_);
index_in->readBytes(max_packed_value_.data(), packed_index_bytes_length_);
```

`BkdIndexReader::query()` 先把查询值编码成和写入相同的 sortable bytes。等值会构造 `[value,value]`，范围查询构造 `[min,max]` 或开区间变体。query cache miss 后调用 `invoke_bkd_query()`，内部创建 `InvertedIndexVisitor` 并执行 `bkd_reader->intersect(visitor)`。

BKD 查询的核心 I/O 依赖是内存 tree -> data leaf。`intersect()` 在内存中的 tree index 上比较 query range 和 node cell：完全在 query 外的 subtree 不读；完全在 query 内的 subtree 调用 `add_all()` 读取其所有 leaf docids；部分相交的 leaf 会先 `read_doc_ids()` 再 `visit_doc_values()` 读取 common prefixes、min/max、compressedDim 和 packed values，逐值判断是否命中。每个 leaf 内部是顺序读，leaf 之间由 tree traversal 决定，整体表现为多个 `bkd_data` offset 的随机读。

关键代码片段：BKD `intersect()` 只对需要访问的 leaf 读取 `bkd_data`。

```cpp
relation r = s->visitor_->compare(cellMinPacked, cellMaxPacked);

if (r == relation::CELL_OUTSIDE_QUERY) {
} else if (r == relation::CELL_INSIDE_QUERY) {
    add_all(s, false);
} else if (s->index_->is_leaf_node()) {
    if (s->index_->node_exists()) {
        int32_t count = read_doc_ids(s->in_.get(), s->index_->get_leaf_blockFP(),
                                     s->docid_set_iterator.get());
        visit_doc_values(s->common_prefix_lengths_,
                         s->scratch_data_packed_value_,
                         s->scratch_min_index_packed_value_,
                         s->scratch_max_index_packed_value_,
                         s->in_.get(),
                         s->docid_set_iterator.get(),
                         count,
                         s->visitor_);
    }
} else {
    s->index_->push_left();
    intersect(s, cellMinPacked, splitPackedValue);
    s->index_->pop();
    s->index_->push_right();
    intersect(s, splitPackedValue, cellMaxPacked);
    s->index_->pop();
}
```

`InvertedIndexIterator::try_read_from_inverted_index()` 可在 BKD 上先调用 `try_query()`。它走 `estimate_point_count()`，只用内存 tree 估算命中点数，不读 leaf data blocks；如果估算命中比例超过 `inverted_index_skip_threshold`，就 bypass 当前 inverted index，避免低选择性范围查询继续读取大量 BKD leaf 和后续 `.dat` 稀疏页。

## Null Bitmap 查询

`InvertedIndexReader::read_null_bitmap()` 为 null 语义提供单独路径。它先查 `InvertedIndexQueryCache` 中 key 为 `null_bitmap` 的结果；miss 时会打开 index directory，`dir->openInput("null_bitmap")`，按 sub-file length 分配 buffer，一次 `readBytes()` 读完整 roaring bitmap，然后 `Roaring::read()` 并 `runOptimize()`。因此 null bitmap 的冷读是一个依赖 `.idx` header/directory 的整 sub-file read，大小约等于 roaring 序列化后的 null rowid 集合大小；全非空列可能只有 roaring header 或空 sub-file级别的读。

关键代码片段：null bitmap miss 时整 sub-file 一次读入并插入 query cache。

```cpp
if (dir->fileExists(null_bitmap_file_name)) {
    null_bitmap_in = dir->openInput(null_bitmap_file_name);
    auto null_bitmap_size = cast_set<int32_t>(null_bitmap_in->length());
    faststring buf;
    buf.resize(null_bitmap_size);
    null_bitmap_in->readBytes(buf.data(), null_bitmap_size);
    *null_bitmap = roaring::Roaring::read(reinterpret_cast<char*>(buf.data()), false);
    null_bitmap->runOptimize();
    cache->insert(cache_key, null_bitmap, cache_handle);
}
```

## 宏观 Query 场景和 I/O Pattern

| 场景 | `.idx` 读取 | 读之间的依赖 | 典型读大小 | 后续 `.dat` 影响 |
| --- | --- | --- | --- | --- |
| string 等值 / 单 term match | searcher miss 时读 `.idx` header、CLucene metadata、全量 `.tii`；query miss 时读 `.tis` term 附近区域和 `.frq` postings | `.tii` 内存二分 -> `.tis` seek/scan -> `freqPointer` -> `.frq` block | `.idx` header/metadata 4KB refill；`.tii` 全量；`.frq` 每 block 约 512 docids 的压缩 bytes | 高选择性时后续按少量 rowids 读 data page；低选择性时 `.dat` 仍接近顺序扫 |
| `IN` / `MATCH_ANY` | 多个 term 重复 dictionary lookup 和 postings read，结果 OR | 每个 term 独立，query 内部串行构造 bitmap | 约等于 term 数乘以单 term 成本，重复 term 可由 query cache 降低 | OR 后 bitmap 可能比单 term 更稠密 |
| `MATCH_ALL` / 多 term AND | 多个 term postings，可能使用 skiplist | 先取所有 term 的 doc_freq 并排序，再按最小 postings 推动其他 postings skip | 小 postings 顺序读，大 postings 可能随机 seek 到 skip pointer | 命中通常更稀疏，`.dat` 更容易变随机 rowid 读 |
| phrase / phrase prefix | term postings 加 `.prx` positions | term dictionary -> `.frq` candidate docs/freq -> `.prx` positions -> phrase matcher | `.frq` postings blocks + `.prx` position blocks，positions 数随 term frequency 放大 | 过滤能力强，但 `.idx` 自身 I/O 和 CPU 都比 term query 高 |
| string range / regexp | term dictionary 枚举多个 term，再读每个 term postings | range/pattern expansion -> per-term postings | 由 expanded term 数和每个 term df 决定，可能非常大 | 可能 bypass；不 bypass 时 bitmap 稠密程度取决于展开范围 |
| numeric 等值 / 小范围 | searcher miss 时读 `bkd_meta` 和全量 `bkd_index`；query miss 时读少量 `bkd_data` leaf blocks | encoded query -> memory tree traversal -> selected leaf block reads | `bkd_index` 全量；每个 leaf 通常最多约 leaf-size points 的 docids/values | 常产生稀疏 rowids，后续 data page 读按命中分布决定 |
| numeric 大范围 | 读很多 BKD leaf，或通过 `try_query()` bypass | tree estimate -> threshold 判断 -> intersect 或 bypass | 如果不 bypass，leaf block 数接近覆盖范围比例 | 不 bypass 时 bitmap 稠密，`.idx` 读可能成为额外成本 |
| `IS NULL` / nullable 组合 | 读完整 `null_bitmap` sub-file | `.idx` open -> `null_bitmap` read -> roaring parse | roaring serialized size，和 null rowid 数/分布相关 | null 稀疏时可强裁剪，null 稠密时后续仍读大量 data page |

## 微观 I/O 依赖和顺序性

fulltext/string 的 searcher build 是强依赖链：必须先读 compound directory 才知道 `.tii`、`.tis`、`.frq`、`.prx` 的 sub-file offset；必须先构建 CLucene reader 才能执行 term query；必须先通过 `.tii`/`.tis` 找到 `TermInfo` 才能读 postings；phrase 必须先得到 doc/freq 语义后才有意义地读 positions。这个链路中，metadata 和 `.tii` 冷读偏顺序，term lookup 的 `.tis` 是小范围随机 seek 后顺序 scan，postings 对单 term 是顺序 block 读，skiplist/多 term/phrase 会引入更多随机 seek。

BKD 的 searcher build 也是强依赖链：compound directory -> `bkd_meta`/`bkd_index` -> memory tree -> query leaf reads。`bkd_index` 冷读偏顺序且全量；query 阶段的 `bkd_data` 叶子块是随机读集合，叶子块内部顺序。`estimate_point_count()` 只依赖内存 tree，不读 leaf data，因此它是避免大范围 query 继续放大 I/O 的前置判断。

query cache 和 searcher cache 会直接改变这些依赖链。query cache 命中时，整条 `.idx` query 链路被 bitmap lookup 替代；searcher cache 命中但 query cache miss 时，metadata、`.tii` 或 `bkd_index` 不再冷读，但 postings/BKD leaf 仍按 query 读取；两个 cache 都 miss 时才出现完整冷读。

底层每次真正读远端对象前还有两层粒度转换。第一层是 CLucene/BKD 的 `IndexInput` buffer：默认小读按 4KB refill，大块 `readBytes()` 按实际请求长度。第二层是 Doris file cache：逻辑 `read_at(offset,len)` 被映射到 file-cache block，当前默认 block 是 1MB；同一个 block 内后续 `.idx` 或 `.dat` 小读可复用本地 cache，跨 block 的随机 leaf/posting read 会产生更多远端 round trip。

关键代码片段：CLucene `BufferedIndexInput` 对小读使用 buffer refill，对大读直接调用 `readInternal()`。

```cpp
if (useBuffer && len < bufferSize) {
    refill();
    memcpy(b + offset, buffer, len);
    bufferPosition = len;
} else {
    int64_t after = bufferStart + bufferPosition + len;
    readInternal(b + offset, len);
    bufferStart = after;
    bufferPosition = 0;
    bufferLength = 0;
}

void BufferedIndexInput::refill() {
    int64_t start = bufferStart + bufferPosition;
    int64_t end = start + bufferSize;
    if (end > length())
        end = length();
    bufferLength = (int32_t)(end - start);
    readInternal(buffer, bufferLength);
}
```

关键代码片段：Doris compound sub-file 先把 sub-file local offset 映射为 `.idx` 文件 offset，再经 `FSIndexInput` 落到 `FileReader::read_at()`。

```cpp
// CSIndexInput::readInternal
auto start = getFilePointer();
base->seek(fileOffset + start);
bool read_from_buffer = true;
base->readBytes(b, len, read_from_buffer);

// FSIndexInput::readInternal
int64_t position = getFilePointer();
Slice result {b, (size_t)len};
size_t bytes_read = 0;
Status st = _handle->_reader->read_at(_pos, result, &bytes_read, &_io_ctx);
```

## 与 `.dat` 读取的耦合

倒排索引不是最终数据读取，它只产出 row bitmap。后续 `.dat` 的 I/O 由 bitmap 分布决定：如果 bitmap 是连续大 range，`FileColumnIterator::seek_to_ordinal()` 后可以顺序 `next_batch()`，每列 data page 读接近顺序推进；如果 bitmap 是稀疏 rowids，`read_by_rowids()` 会按 rowid 找 distinct data pages，同一个 page 内多行复用，跨 page 则随机读；如果 predicate columns 和 output columns 分离，先读谓词列过滤，再按 surviving rowids 读非谓词列。

因此倒排索引场景的完整 I/O pattern 是两段式：第一段 `.idx` 负责 dictionary/tree/postings/bitmap，第二段 `.dat` 负责 surviving rows 的真实列值。第一段的低选择性会增加额外前置 I/O；第一段的高选择性会减少第二段 page 数，但可能把第二段变成随机页读。评估一个 inverted index 是否有收益时，需要同时看 `.idx` 冷读成本、bitmap 稀疏度、surviving rowids 覆盖的 `.dat` data page 数，以及 file-cache block 的重用情况。

## 代码定位

| 主题 | 主要代码 |
| --- | --- |
| scan 中应用倒排索引 | `be/src/storage/segment/segment_iterator.cpp` 的 `_apply_inverted_index()`、`_apply_inverted_index_on_column_predicate()`、`_apply_index_expr()` |
| 根据列类型创建 reader | `be/src/storage/segment/column_reader.cpp` 的 `_load_index()` |
| query 入口和 reader 选择 | `be/src/storage/index/inverted/inverted_index_iterator.cpp` |
| fulltext/string/BKD reader | `be/src/storage/index/inverted/inverted_index_reader.cpp` |
| searcher cache build | `be/src/storage/index/inverted/inverted_index_searcher.cpp` |
| `.idx` compound 文件打开 | `be/src/storage/index/index_file_reader.cpp`、`be/src/storage/index/inverted/inverted_index_compound_reader.cpp`、`be/src/storage/index/inverted/inverted_index_fs_directory.cpp` |
| string/fulltext/numeric 写入 | `be/src/storage/index/inverted/inverted_index_writer.cpp` |
| CLucene reader 和 term dictionary | `contrib/clucene/src/core/CLucene/index/SegmentReader.cpp`、`contrib/clucene/src/core/CLucene/index/TermInfosReader.cpp` |
| CLucene postings 和 positions | `contrib/clucene/src/core/CLucene/index/SegmentTermDocs.cpp`、`contrib/clucene/src/core/CLucene/index/SegmentTermPositions.cpp`、`contrib/clucene/src/core/CLucene/index/_SegmentHeader.h` |
| CLucene buffer 行为 | `contrib/clucene/src/core/CLucene/store/IndexInput.cpp` |
| BKD reader/writer | `contrib/clucene/src/core/CLucene/util/bkd/bkd_reader.cpp`、`contrib/clucene/src/core/CLucene/util/bkd/bkd_writer.cpp`、`contrib/clucene/src/core/CLucene/util/bkd/docids_writer.cpp` |
