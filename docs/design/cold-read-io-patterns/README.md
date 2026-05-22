# Doris 内表冷读 I/O Pattern 总览

## 背景和范围

本文档集面向存算分离场景下的内表冷查优化，目标是从当前代码实现出发，梳理读内表时 `.dat` 数据文件和 `.idx` 倒排索引文件的 I/O pattern、I/O 之间的依赖关系，以及这些模式对未来 prefetch 接口设计的约束。

范围只覆盖 Doris 内表读路径。外表 rowid fetch、写路径、compaction 写文件、远程对象存储 SDK 细节不作为重点。存算分离下的远程读主要落到两层缓存边界：上层是 `StoragePageCache`，缓存解压前后 page 对象；下层是 `CachedRemoteFileReader` 和 `BlockFileCache`，缓存远程文件的 cache block 字节。

## 文档索引

- [`.dat` 数据文件读路径和 I/O Pattern](dat-file-io-patterns.md)
- [倒排索引 `.idx` 文件读路径和 I/O Pattern](inverted-index-io-patterns.md)
- [TopN 全局延迟物化和 rowid 二阶段读取](global-lazy-materialization-io.md)

## 宏观读路径

普通内表扫描的主要链路是：

```text
OlapScanLocalState / ParallelScannerBuilder
  -> OlapScanner
  -> BlockReader / VCollectIterator
  -> BetaRowsetReader
  -> LazyInitSegmentIterator
  -> SegmentLoader / BetaRowset::load_segment
  -> Segment::open
  -> Segment::new_iterator
  -> SegmentIterator::next_batch
```

关键点：

- `BetaRowset::load_segment()` 打开 segment 文件，云模式启用 file cache 时使用 `FileReaderOptions.cache_type = FILE_BLOCK_CACHE`，并把 `is_doris_table`、`tablet_id` 带给 reader。
- `RemoteFileSystem::open_file_impl()` 打开原始远端 reader 后，经 `create_cached_file_reader()` 包装成 `CachedRemoteFileReader`。
- `Segment::open()` 只做文件打开和 footer 解析。footer 冷读先读文件尾部 12 字节，再按 footer 长度读取 `SegmentFooterPB`。
- `Segment::new_iterator()` 先根据 footer 创建 column meta，再做 segment-level zone map 剪枝，随后加载短 key 或主键索引，最后创建 `SegmentIterator`。
- `SegmentIterator::_lazy_init()` 在第一个 batch 才构造完整 row bitmap、应用 key range、倒排索引、zone map、bloom filter、delete bitmap、scanner row range，并初始化数据文件预取器。
- `SegmentIterator::_next_batch_internal()` 负责 batch 级读取。普通延迟物化场景先读谓词列和表达式列，过滤后再用 surviving rowids 读取非谓词列。

## 冷读 I/O 依赖图

普通 `.dat` 冷读大致依赖顺序如下：

```text
open .dat reader
  -> footer tail read: [file_size - 12, 12]
  -> footer protobuf read: [file_size - 12 - footer_len, footer_len]
  -> build ColumnMetaAccessor and ColumnReader objects from footer
  -> load short-key or primary-key index page when Segment::new_iterator runs
  -> first batch lazy init:
       key range pruning may read key-column pages
       inverted index pruning may read .idx
       bloom/zone-map/dict pruning may read index pages in .dat
  -> predicate/common-expression column data pages
  -> predicate evaluation
  -> non-predicate column data pages by selected rowids
```

倒排索引 `.idx` 冷读大致依赖顺序如下：

```text
SegmentIterator::_init_index_iterators
  -> Segment::_open_index_file_reader
  -> IndexFileReader::init
       V2: read .idx header and sub-file directory entries
       V1: open per-index .idx file when opening the directory
  -> InvertedIndexReader::handle_searcher_cache
       lookup searcher cache
       open DorisCompoundReader on miss
       read null_bitmap if present
       build CLucene searcher or BKD reader
  -> predicate evaluate / expression evaluate
       read term dictionary, postings, BKD tree, doc ids as needed
  -> produce roaring row bitmap
  -> SegmentIterator intersects _row_bitmap before .dat data page reads
```

TopN 全局延迟物化是另一条路径。第一阶段 scan 只输出 eager columns 和 `__DORIS_GLOBAL_ROWID_COL__...`，TopN 完成后 `MaterializationOperator` 按 backend 和 file id 发起 `multiget_data_v2`，远端 `RowIdStorageReader` 再按 segment 分组、排序、去重 rowids，并调用 `Segment::seek_and_read_by_rowid()` 读取 lazy columns。它不复用普通 `SegmentIterator::_init_segment_prefetchers()` 的 prefetch 入口。

## 两层缓存和 I/O 放大

`StoragePageCache` 位于 `PageIO` 之上，key 是 `(path, file_size, page_offset)`，缓存 page 的内存对象。它避免重复解压和重复 page 读，但只在 BE 进程内生效。

`BlockFileCache` 位于 `CachedRemoteFileReader` 之下，key 是文件 hash 和按 `file_cache_each_block_size` 对齐的 block offset。一次逻辑 page read 可能被 `CachedRemoteFileReader::s_align_size()` 扩大成一个或多个 cache block 的远端读。冷读优化必须同时考虑 page 粒度和 cache block 粒度，否则容易把预取接口设计成只覆盖逻辑 page，实际远端 I/O 却被 block 对齐放大。

`IOContext::is_index_data` 决定 file cache 的 cache type 走 `INDEX` 还是 `NORMAL` 等队列。footer、短 key、ordinal index、zone map、bloom filter、倒排索引元数据和倒排索引文件读都会倾向标记为 index data；普通数据页读继承查询 reader 的 `IOContext`，通常是 normal data。

## 当前 SegmentPrefetcher 的位置

当前代码中只有 `SegmentPrefetcher`，没有独立的 `CacheBlockAwarePrefetchRemoteReader`、`SegmentFileAccessRangeBuilder`、`FileAccessRange`、`CacheBlockReadPattern` 或 `CacheBlockPrefetchPolicy`。`CachedRemoteFileReader` 当前还是 `final`。

`SegmentPrefetcher` 的作用是把当前 scan 的 row bitmap 或全量 page 序列映射到 file-cache block 序列，在 `FileColumnIterator::seek_to_ordinal()` 触发 `CachedRemoteFileReader::prefetch_range()`。这个预取是 best-effort 的 dry-run `read_at_impl()`，目标是提前填充 file cache，不返回 completion handle，也不向上暴露错误、取消或命中统计。

这意味着当前 prefetch 能覆盖普通 scan 中的 `.dat` 数据 page，但不能直接覆盖 TopN 全局延迟物化二阶段 rowid fetch，也不能表达内存 buffer-only、remote-only、不写本地 cache、只 probe cache 等策略。

## Prefetch 接口设计约束

从当前 I/O pattern 看，未来 prefetch 接口不应该只暴露“按 rowid 预读列”或“按 offset 读文件”其中一种抽象，而应拆成以下层次：

1. `RowId -> PagePointer -> FileAccessRange` 的 planning 层。这个层需要利用 ordinal index、page pointer、列读模式、row bitmap、TopN 二阶段 rowid 列表，产出稳定的物理访问范围。
2. `FileAccessRange -> CacheBlockRange` 的 cache-aware 层。这个层负责按 file cache block 对齐，显式暴露读放大和 block 合并策略。
3. `PrefetchPolicy` 层。至少要区分写入 file cache、只进内存 buffer、remote-only、cache-probe-only、read-through/write-back、是否走 peer cache、是否允许跳过 cache。
4. `AsyncHandle` 层。当前 `prefetch_range()` 丢弃错误且无完成语义，后续接口应返回 completion、取消、错误和统计，供 scan pipeline 或 materialization operator 判断是否等待。
5. `IOContext` 生命周期层。异步任务不能持有 query stats 指针等短生命周期对象；当前 `prefetch_range()` 会复制并清空 query/stat 指针，这个约束应成为接口契约。

## 代码入口清单

`.dat` 数据文件：

- `be/src/storage/rowset/beta_rowset.cpp:260`，segment 文件打开和 file cache reader options。
- `be/src/storage/segment/segment.cpp:401`，footer 解析和 tail reads。
- `be/src/storage/segment/segment.cpp:251`，`Segment::new_iterator()`。
- `be/src/storage/segment/segment_iterator.cpp:609`，`SegmentIterator::_lazy_init()`。
- `be/src/storage/segment/segment_iterator.cpp:2396`，谓词列 first read。
- `be/src/storage/segment/segment_iterator.cpp:2779`，非谓词列 selected-rowid second read。
- `be/src/storage/segment/column_reader.cpp:2058`，`FileColumnIterator::seek_to_ordinal()`。
- `be/src/storage/segment/column_reader.cpp:2231`，`FileColumnIterator::read_by_rowids()`。
- `be/src/storage/segment/page_io.cpp:130`，page cache 和 page read/decompress。
- `be/src/io/cache/cached_remote_file_reader.cpp:274`，file cache block read path。

倒排索引文件：

- `be/src/storage/segment/segment_iterator.cpp:1689`，index iterator 初始化。
- `be/src/storage/segment/segment.cpp:793`，`Segment::new_index_iterator()`。
- `be/src/storage/index/index_file_reader.cpp:25`，`.idx` reader 初始化。
- `be/src/storage/index/inverted/inverted_index_reader.cpp:248`，searcher cache miss 时打开目录并构造 reader/searcher。
- `be/src/storage/index/inverted/inverted_index_fs_directory.cpp:84`，`.idx` 文件打开。
- `be/src/storage/index/inverted/inverted_index_fs_directory.cpp:220`，CLucene `IndexInput` 落到 Doris `FileReader::read_at()`。
- `be/src/storage/index/inverted/inverted_index_reader.cpp:371`，fulltext query。
- `be/src/storage/index/inverted/inverted_index_reader.cpp:490`，string type query。
- `be/src/storage/index/inverted/inverted_index_reader.cpp:624`，BKD query。

TopN 全局延迟物化：

- `fe/fe-core/src/main/java/org/apache/doris/nereids/processor/post/materialize/LazyMaterializeTopN.java:88`，FE rewrite。
- `fe/fe-core/src/main/java/org/apache/doris/planner/MaterializationNode.java:181`，thrift 输出。
- `be/src/storage/segment/segment_iterator.cpp:1631`，第一阶段 scan 生成 `GlobalRowLoacationV2`。
- `be/src/exec/operator/materialization_opertor.cpp:203`，rowid 分组和 multi-get 请求构造。
- `be/src/exec/rowid_fetcher.cpp:542`，`PMultiGetRequestV2` 服务端入口。
- `be/src/exec/rowid_fetcher.cpp:665`，按 segment 分组、排序、去重 rowids。
- `be/src/storage/segment/segment.cpp:947`，`Segment::seek_and_read_by_rowid()`。
