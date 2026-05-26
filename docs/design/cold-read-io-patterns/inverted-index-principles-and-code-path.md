# Doris 倒排索引原理、代码路径和 I/O Pattern

本文从倒排索引的基本概念开始，说明 Doris 当前代码中 string/fulltext/numeric inverted index 的写入组织、查询执行和冷读 I/O 形态。`.idx` compound 文件 header、sub-file offset 映射、底层 `read_at` 清单见 `inverted-index-io-patterns.md`；本文重点解释这些读为什么发生、读之间有什么依赖、以及不同 query 形态为什么会形成不同 I/O pattern。ANN 是独立的向量索引类型，虽然也复用 `.idx` compound 文件和部分 index-file 适配层，但不是本文讨论的经典倒排索引。

## 基本原理

倒排索引解决的问题是从“值找行”。列存文件 `.dat` 更适合按 rowid 顺序读列值，过滤一个条件通常要先定位并读取包含这些 rowid 的 data page，再解码列值做比较。倒排索引把某一列中出现过的 term、字符串值或数值范围结构提前写成索引，查询时先在 `.idx` 中得到满足条件的 segment-local rowid/docid 集合，然后把这个集合和 `_row_bitmap` 相交，后续 `.dat` 只读取 surviving rows 所在的 data pages。

在 Doris segment 内，一个 CLucene doc 基本对应一行；docid 和 segment-local rowid 对齐。string/fulltext 索引把“term -> docid list”存成 term dictionary 和 postings；numeric 索引使用 BKD tree，把“数值范围 -> docid list”组织成 point tree；null 判断不走 term dictionary，而是单独的 `null_bitmap` roaring bitmap。查询结果也是 roaring bitmap，最后参与 `SegmentIterator` 的 row bitmap 裁剪。

倒排索引和普通 page index 的根本差异是：ordinary zone map / bloom filter 仍然以 `.dat` data page 为单位过滤，结果是 row ranges；倒排索引直接以 rowid/docid 为单位返回 bitmap，可以表达稀疏命中。这个优势也带来 I/O 形态差异：`.idx` 会先读小的 dictionary/tree/posting 结构，命中稀疏时 `.dat` 第二阶段通常变成按 rowid 的随机 data page 读取。

## Doris 中的倒排索引类型

Doris 在 `ColumnReader::_load_index()` 根据列类型和 index properties 创建 reader。string-like 列如果配置 analyzer，就创建 `FullTextIndexReader`；string-like 列如果不分词，就创建 `StringTypeInvertedIndexReader`；numeric/date/decimal/bool/ip 等数值型列创建 `BkdIndexReader`。ARRAY 列会取元素类型建立索引：string array 是一行一个 CLucene doc、doc 中可有多个 field；numeric array 是多个 point 使用同一个 rowid。

三类 reader 对应三种查询模型：

| Reader | 适用列和查询 | 索引模型 | 主要 sub-file |
| --- | --- | --- | --- |
| `FullTextIndexReader` | 分词 string，`MATCH`、phrase、regexp 等 | analyzer 把文本切成 term，term dictionary 定位 postings，phrase 额外读 positions | `segments*`、`fnm`、`tii`、`tis`、`frq`、可选 `prx`、`null_bitmap` |
| `StringTypeInvertedIndexReader` | 不分词 string，`=`、`IN`、string range、部分 match query | 整个字符串或数组元素作为 untokenized term，查询仍用 CLucene term/range/posting | 同 fulltext |
| `BkdIndexReader` | numeric/date/decimal/bool/ip 的等值和范围 | 一维 BKD point tree，内部节点按 encoded value 分裂，叶子保存 docids 和 values | `bkd_meta`、`bkd_index`、`bkd_data`、`null_bitmap` |

`InvertedIndexIterator::select_best_reader()` 负责在同一个 index id 下选择最合适的 reader。文本 match 优先 fulltext，文本等值优先 string reader，数值范围优先 BKD；如果 analyzer 不匹配或 query 不能由当前 reader 安全处理，会返回 bypass 或 fallback，让执行继续走 `.dat` 列值过滤。

## 写入组织

string/fulltext 写入由 `InvertedIndexColumnWriter::init_fulltext_index()` 初始化 CLucene `IndexWriter`。有 analyzer 时，`new_char_token_stream()` 把原始字符串交给 analyzer，tokenized field 写入 term；没有 analyzer 时，`new_field_char_value()` 把整段 bytes 作为 untokenized field。`create_field()` 中的 `setOmitTermFreqAndPositions()` 由 `parser_phrase_support` 控制：不需要 phrase 时省掉频次/位置信息；需要 phrase 时会写 `.prx` 位置流。写入结束后 CLucene 生成 term dictionary、term info index、postings、position 等 sub-files，并由 Doris 的 `IndexFileWriter` 收进 `.idx` compound 文件。

numeric 写入由 `InvertedIndexColumnWriter::init_bkd_index()` 创建 `lucene::util::bkd::bkd_writer`。Doris 先用 `KeyCoder::full_encode_ascending()` 把数值编码成按字节序可比较的 key，再调用 `_bkd_writer->add(encoded_value, rowid)`。writer 初始 leaf 上限是 `MAX_LEAF_COUNT=1024`，`finish()` 时还会结合 `max_depth_in_bkd_tree` 调整实际 tree depth/leaf size，并把最终参数写进 `bkd_index`。完成时写三个 BKD sub-file：`bkd_data` 保存 leaf block 的 docids 和 values，`bkd_index` 保存 tree metadata、min/max、split values 或 packed index，`bkd_meta` 保存 field type 和 `indexFP`。

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

查询执行有两个 cache 关口。`InvertedIndexQueryCache` 命中时，某个 query 条件的 roaring bitmap 已经存在，不需要打开 searcher，也不需要读 postings/BKD leaf。query cache miss 后会进入 `InvertedIndexSearcherCache`；searcher cache 命中时，CLucene `IndexSearcher` 或 BKD reader 已经建好，跳过 `.idx` header、CLucene metadata、`.tii` 或 `bkd_index` 的冷读；searcher cache miss 时才执行完整 open/build 路径。

## Fulltext/String 搜索器构建

fulltext 和 string reader 在 searcher cache miss 时调用 `InvertedIndexReader::handle_searcher_cache()`。Doris 先用 `IndexFileReader::init(config::inverted_index_read_buffer_size, io_ctx)` 打开 `.idx` 文件并读取 compound directory；`inverted_index_read_buffer_size` 默认是 4096 bytes。随后 `IndexFileReader::open()` 返回 `DorisCompoundReader`，CLucene 通过这个 directory 打开各个 sub-file。

`FulltextIndexSearcherBuilder::build()` 调用 `lucene::index::IndexReader::open(directory, read_buffer_size, close_directory)`。在 CLucene 侧，`SegmentReader::initialize()` 会读取 field info，创建 `TermInfosReader`，打开 `.frq`，如果 field 有 positions 则打开 `.prx`。`TermInfosReader` 构造时同时打开 `.tii` 和 `.tis`，并立即调用 `ensureIndexIsRead()` 把 `.tii` 全量读入内存；`.tis` 保留为 term dictionary stream，后续按 term seek/scan。

`.tii` 是 term dictionary 的稀疏索引。CLucene writer 默认 `DEFAULT_TERM_INDEX_INTERVAL=128`，也就是每约 128 个 term 在 `.tii` 中保存一个入口。`TermInfosReader::get(term)` 查询时先在内存中的 `.tii` 数组做二分，得到不大于目标 term 的 dictionary 入口，再 `seekEnum(indexPointer)` 到 `.tis` 中对应位置，并顺序 scan 到目标 term。这个过程的依赖关系是 `.tii` 全量加载 -> `.tis` seek/scan -> 得到 `TermInfo{docFreq,freqPointer,proxPointer,skipOffset}` -> `.frq` / `.prx` postings 读取。

CLucene 的 `BufferedIndexInput` 会把小读合并成 buffer refill。小的 `readInt/readVInt/readString` 不会逐个变成远端读；当 buffer 不足时才调用 `DorisFSDirectory::FSIndexInput::readInternal()` 或 `CSIndexInput::readInternal()`，最终落到 Doris `FileReader::read_at(offset,len)`。默认小读 refill 是 4KB；如果一次 `readBytes(len)` 大于 buffer 或要求不用 buffer，则会按该 len 直接读。进入 file cache 后，逻辑 4KB 或更小读还会按 `file_cache_each_block_size` 对齐，当前默认是 1MB。

## Term/Postings 查询

`FullTextIndexReader::query()` 先把查询值解析成 `InvertedIndexQueryInfo`。普通 `MATCH` 会用 analyzer 得到一个或多个 term；`MATCH_PHRASE` 会解析 term sequence、slop 和 ordered；`MATCH_REGEXP` / prefix 类 query 会保留 pattern 或 prefix；`StringTypeInvertedIndexReader` 对 `=` / `IN` 则直接把字符串值作为 term。之后 `InvertedIndexReader::match_index_search()` 通过 `QueryFactory` 创建具体 query 对象。

`TermQuery` 是最小执行单元。`TermIterator::create()` 调用 CLucene `IndexReader` 创建 `TermDocs`，`SegmentTermDocs::seek(term)` 经 `TermInfosReader::get(term)` 找到 `TermInfo`，然后 `freqStream->seek(freqPointer)`。之后 `TermDocsBuffer::readRange()` 按 postings block 读取 docids。当前 contrib CLucene 的 `PFOR_BLOCK_SIZE=512`，新版本 postings 通过 `PforUtil::pfor_decode()` 解码，每个 block 先读 mode、arraySize、serializedSize，再读压缩 doc deltas；如果 field 保存 freq/positions，还会读 freq block。

`MATCH_ANY` 对多个 term 创建多个 `TermIterator`，逐个读取 postings 后做 roaring OR。I/O 形态是多个 term 的 dictionary lookup 和 postings 顺序读，term 越多，`.tis` seek/scan 和 `.frq` posting block 读越多。`MATCH_ALL` / 多 term 等值语义会创建 `ConjunctionQuery`，先按 `doc_freq()` 从小到大排序 postings，再选择 bitmap intersection 或 skiplist intersection。skiplist intersection 会让较大的 postings list 通过 `skipTo(target)` 跳跃，CLucene 会懒加载 `DefaultSkipListReader` 并 seek 到新的 `.frq` pointer；这能减少解码的 docids，但会把原本顺序的 posting 读变成更多随机 seek。

phrase query 在 term postings 之外还依赖 positions。`PhraseQuery` 为每个 term 创建 `TermPositionsIterator`，先用 docid postings 找 candidate docs，再在 `nextPosition()` 时懒加载 `proxStream` 并按 `proxPointer` 读取 `.prx`。contrib CLucene 的 `TermPostingsBuffer` 对 index version >= V2 会用 `PforUtil::decodePos()` 成块解码 positions。phrase 的依赖链是 term dictionary -> `.frq` candidate docids/freq -> `.prx` positions -> phrase matcher；如果一个 query 需要多个 term 的 positions，`.prx` 会成为额外随机读来源。

string range 和 regexp 会先在 term dictionary 上枚举候选 terms，再读取这些 terms 的 postings。范围宽、regexp 展开 term 多时，I/O 会从“一个 term 的 dictionary+postings”放大为“多个 term 的 dictionary enumeration + 多个 postings”；`StringTypeInvertedIndexReader` 对 range 中 CLucene `TooManyClauses` 会返回 bypass，让上层继续用 `.dat` 列值过滤。

## BKD 查询

numeric/date/decimal/bool/ip 等类型使用 BKD。`BKDIndexSearcherBuilder::build()` 创建 `bkd_reader` 并调用 `open()`。`open()` 依次打开 `bkd_data`、`bkd_meta`、`bkd_index`：`bkd_meta` 只读 field type 和 `indexFP`；`bkd_index` 读 codec header、维度、leaf 参数、bytes_per_dim、num_leaves、min/max、point_count、doc_count，并把 packed index 或 split values / leaf block file pointers 读入内存；`bkd_data` 保留为后续 leaf block stream。

`BkdIndexReader::query()` 先把查询值编码成和写入相同的 sortable bytes。等值会构造 `[value,value]`，范围查询构造 `[min,max]` 或开区间变体。query cache miss 后调用 `invoke_bkd_query()`，内部创建 `InvertedIndexVisitor` 并执行 `bkd_reader->intersect(visitor)`。

BKD 查询的核心 I/O 依赖是内存 tree -> data leaf。`intersect()` 在内存中的 tree index 上比较 query range 和 node cell：完全在 query 外的 subtree 不读；完全在 query 内的 subtree 调用 `add_all()` 读取其所有 leaf docids；部分相交的 leaf 会先 `read_doc_ids()` 再 `visit_doc_values()` 读取 common prefixes、min/max、compressedDim 和 packed values，逐值判断是否命中。每个 leaf 内部是顺序读，leaf 之间由 tree traversal 决定，整体表现为多个 `bkd_data` offset 的随机读。

`InvertedIndexIterator::try_read_from_inverted_index()` 可在 BKD 上先调用 `try_query()`。它走 `estimate_point_count()`，只用内存 tree 估算命中点数，不读 leaf data blocks；如果估算命中比例超过 `inverted_index_skip_threshold`，就 bypass 当前 inverted index，避免低选择性范围查询继续读取大量 BKD leaf 和后续 `.dat` 稀疏页。

## Null Bitmap 查询

`InvertedIndexReader::read_null_bitmap()` 为 null 语义提供单独路径。它先查 `InvertedIndexQueryCache` 中 key 为 `null_bitmap` 的结果；miss 时会打开 index directory，`dir->openInput("null_bitmap")`，按 sub-file length 分配 buffer，一次 `readBytes()` 读完整 roaring bitmap，然后 `Roaring::read()` 并 `runOptimize()`。因此 null bitmap 的冷读是一个依赖 `.idx` header/directory 的整 sub-file read，大小约等于 roaring 序列化后的 null rowid 集合大小；全非空列可能只有 roaring header 或空 sub-file级别的读。

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
