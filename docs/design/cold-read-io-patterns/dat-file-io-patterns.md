# `.dat` 数据文件读路径和 I/O Pattern

## 文件结构和读粒度

Doris beta rowset 的 segment 数据文件是 `{rowset_id}_{segment_id}.dat`。一个 segment 文件包含列数据页、列级索引页、短 key 或主键索引、footer protobuf、footer 长度、footer checksum 和 magic number。

读路径中同时存在三种粒度：

- Segment 粒度：`Segment::open()` 解析 footer，`Segment::new_iterator()` 创建行迭代器。
- Page 粒度：`PageIO::read_and_decompress_page()` 根据 `PagePointer{offset,size}` 读取一个 page。
- File cache block 粒度：`CachedRemoteFileReader` 把逻辑 `read_at(offset,size)` 对齐到 `file_cache_each_block_size` 的 block 范围。

冷读优化必须把这三种粒度区分开。业务逻辑通常以 rowid 或 page 为单位推导需要读的内容，实际远端 I/O 却可能以 cache block 为单位放大。

## 打开 segment 和 footer 冷读

`BetaRowset::load_segment()` 会为 `.dat` 文件构造 `FileReaderOptions`。启用 file cache 时，`cache_type` 是 `FILE_BLOCK_CACHE`，并标记 `is_doris_table`、`tablet_id` 和文件大小。远程文件系统最终通过 `create_cached_file_reader()` 把原始 S3/HDFS 等 reader 包装成 `CachedRemoteFileReader`。

`Segment::open()` 的核心 I/O 是 footer：

```text
Segment::open
  -> fs->open_file(path, &file_reader, reader_options)
  -> Segment::_open
      -> Segment::_get_segment_footer
          -> StoragePageCache lookup footer key
          -> Segment::_parse_footer on miss
              -> read_at(file_size - 12, 12)
              -> decode footer length/checksum/magic
              -> read_at(file_size - 12 - footer_len, footer_len)
              -> parse SegmentFooterPB
```

这两个 footer reads 使用 `IOContext{.is_index_data = true}`，因此在 file cache 中进入 index 类 cache queue。footer 还会放入 `StoragePageCache`，后续相同 segment footer 不需要再走 file reader。

依赖关系：

- 必须先读尾部 12 字节才能知道 footer protobuf 的长度。
- 必须解析 footer 后才能知道短 key、主键索引、列 meta、ordinal index、zone map、bloom filter、dict page 和 data page 的 `PagePointer`。
- footer 解析本身不读取列数据页。

## ColumnReader 创建和索引页加载

`Segment::new_iterator()` 首先调用 `_create_column_meta_once()`，从 footer 初始化 `ColumnMetaAccessor` 和 `ColumnReaderCache`。`ColumnReader::init()` 会从 `ColumnMetaPB` 中创建这些 reader 对象：

- `OrdinalIndexReader`
- `ZoneMapIndexReader`
- `BloomFilterIndexReader`
- `ColumnReader` 的 dict page pointer、data page meta、encoding meta

但这些对象的创建不等于 index page 已经加载。多数列级 index page 是 lazy load：

- ordinal index 在 `ColumnReader::_load_ordinal_index()` 或 `ColumnReader::seek_at_or_before()` 时加载。
- page zone map 在 `ColumnReader::_load_zone_map_index()` 时加载。
- bloom filter index 在 `ColumnReader::_load_bloom_filter_index()` 时加载。
- dict page 在 `FileColumnIterator::_read_dict_data()` 时加载，通常是首次遇到 dict-encoded data page 或字典过滤时。

`Segment::new_iterator()` 会同步加载短 key 或主键索引：

- UNIQUE KEY 且有主键索引时，`PrimaryKeyIndexReader::parse_index()` 解析主键索引。
- 其他场景读取 short key index page，`PageIO::read_and_decompress_page()` 以 `INDEX_PAGE` 读取 `_sk_index_page`。

依赖关系：

- key range 剪枝依赖短 key 或主键索引。
- rowid 到数据页的定位依赖 ordinal index。
- page-level zone map 和 bloom filter 剪枝依赖 ordinal index，因为 index 命中的是 page，需要映射成 row ranges。
- dict 过滤依赖 dict page，但只有全量 dict encoding 的列才有意义。

## 第一个 batch 的剪枝顺序

`SegmentIterator::next_batch()` 第一次进入 `_next_batch_internal()` 时会调用 `_lazy_init()`。主要顺序是：

```text
_lazy_init
  -> _row_bitmap = [0, num_rows)
  -> condition cache pruning
  -> _get_row_ranges_by_keys
  -> _get_row_ranges_by_column_conditions
       -> inverted index filtering
       -> expression-level index filtering
       -> dict filtering
       -> bloom filter filtering
       -> zone map filtering
  -> _vec_init_lazy_materialization
  -> delete bitmap filtering
  -> scanner row_ranges filtering
  -> ANN topn predicate
  -> create BitmapRangeIterator / BackwardBitmapRangeIterator
  -> init SegmentPrefetcher
```

这里有一个容易混淆的点：倒排索引过滤发生在普通 `.dat` 列数据读取之前，但倒排索引自身会读取 `.idx` 文件。zone map、bloom filter、dict 等剪枝读取的是 `.dat` 文件里的 index page 或 dictionary page。它们最终都把 `_row_bitmap` 或 `RowRanges` 收窄，之后数据页读取只针对剩余 rowids。

## 普通 scan 的列读取顺序

`_vec_init_lazy_materialization()` 根据 predicate、delete condition、common expr 和输出列把列分成几类：

- `_predicate_column_ids`：first read 读取，用于谓词和表达式计算。
- `_common_expr_column_ids`：如果表达式需要的列没有在 first read 中读到，过滤后第二阶段再读。
- `_non_predicate_columns`：普通返回列，只有 surviving rows 需要读。

`_next_batch_internal()` 的列 I/O 顺序是：

```text
_read_columns_by_index(nrows_read_limit)
  -> BitmapRangeIterator 取一批 rowids
  -> 对 _predicate_column_ids 逐列读取
       continuous rowids: seek_to_ordinal(first) + next_batch(n)
       non-continuous rowids: read_by_rowids(rowids)
  -> vectorized predicate
  -> short-circuit predicate
  -> optional common expr
  -> _read_columns_by_rowids(_non_predicate_columns, selected rowids)
  -> output columns
```

I/O 依赖关系：

- 非谓词列读取依赖谓词列和表达式过滤结果，因为它只读 surviving rowids。
- 谓词列 first read 依赖 `_row_bitmap`，而 `_row_bitmap` 已经被 key range、倒排索引、delete bitmap、zone map、bloom filter 等收窄。
- 如果没有谓词和 common expr，`_predicate_column_ids` 实际包含所有需要输出的列，scan 只做一次 first read，不进入非谓词列 second read。

## `seek_to_ordinal()` 和 `read_by_rowids()` 的微观 I/O

标量列的 `FileColumnIterator::seek_to_ordinal(ord)` 是 rowid 到 page 的关键转换：

```text
seek_to_ordinal(ord)
  -> optional SegmentPrefetcher trigger
  -> if current ParsedPage contains ord: only seek decoder position
  -> else ColumnReader::seek_at_or_before(ord)
       -> load ordinal index if needed
       -> find OrdinalPageIndexIterator
       -> _read_data_page(page_iter)
          -> ColumnReader::read_page
          -> PageIO::read_and_decompress_page
          -> ParsedPage::create
  -> _seek_to_pos_in_page(offset_in_page)
```

`FileColumnIterator::next_batch(n)` 在当前 page 内顺序解码，跨 page 时 `_load_next_page()` 继续读下一 data page。它适合连续 rowid。

`FileColumnIterator::read_by_rowids(rowids,count)` 针对稀疏 rowids：

```text
while remaining > 0:
  seek_to_ordinal(rowids[total_read_count])
  nrows_to_read = min(remaining, current_page.remaining())
  data_decoder->read_by_rowids(rowids + offset, page_first_ordinal, &nrows_to_read, dst)
```

这里的 page read 是按“请求 rowids 所在 page”触发的。多个 rowids 落在同一 page 时，只读取并解析一次 page，然后由 decoder 按 `rowid - page_first_ordinal` 提取值。rowids 很稀疏且分散到不同 page 时，会造成大量 seek 和 page read。

复杂类型的额外依赖：

- `ARRAY` 需要先读 offsets，再读 element row range，nullable 时还读 null map。
- `MAP` 需要 offsets、key、value 和可选 null map。
- `STRUCT` 会递归读取子列。
- `VARIANT` 可能通过 sparse column cache、binary column reader、路径列 reader 读取多个物理列。

这些复杂类型仍然复用底层 `FileColumnIterator` 的 page 和 rowid 机制，但一次逻辑列读取会展开成多个子列 I/O。

## PageIO 和两层缓存

`ColumnReader::read_page()` 统一进入 `PageIO::read_and_decompress_page()`：

```text
PageIO::read_and_decompress_page_
  -> StoragePageCache lookup(path, file_size, page_offset, page_type)
  -> file_reader->read_at(page_offset, page_size, io_ctx) on miss
  -> checksum verify
  -> parse PageFooterPB
  -> decompress if needed
  -> optional pre-decode
  -> insert StoragePageCache
```

`StoragePageCache` 命中后不会再触发 file cache 或远端 I/O。`StoragePageCache` miss 才进入 `CachedRemoteFileReader`。

`CachedRemoteFileReader::read_at_impl()` 的冷 miss 行为是：

```text
read_at(offset, size)
  -> align to file-cache block range
  -> BlockFileCache::get_or_set(hash, aligned_offset, aligned_size, CacheContext)
  -> DOWNLOADED: read local cache file
  -> EMPTY / SKIP_CACHE: current caller reads remote contiguous span
  -> DOWNLOADING: wait for downloader, timeout/failure fallback remote
  -> write back non-SKIP_CACHE blocks
  -> copy requested bytes to caller
```

在云模式内表读中 `_execute_remote_read()` 可能先尝试 peer cache，再 fallback 到 S3。真正 S3 range read 在 `S3FileReader::read_at_impl()`，参数仍然是 offset 和 size。

## `.dat` 内的主要 I/O 类型矩阵

| I/O 类型 | 触发点 | 读什么 | 依赖 | 典型粒度 |
| --- | --- | --- | --- | --- |
| Footer tail | `Segment::_parse_footer()` | 最后 12 字节 | 文件 size | 12 bytes |
| Footer PB | `Segment::_parse_footer()` | serialized `SegmentFooterPB` | footer length | footer length |
| Short key index | `Segment::load_index()` | short-key page | footer `_sk_index_page` | one index page |
| Primary key index | `Segment::load_index()` | indexed column pages | PK meta | index pages |
| Ordinal index | `seek_to_ordinal()` / zone map / bloom | ordinal -> `PagePointer` map | column meta | index page or root data page |
| Page zone map | zone map filtering | per-page min/max/null info | column predicate | indexed column pages |
| Bloom filter | bloom filtering | per-page bloom filter | bloom-capable predicate | indexed column pages |
| Dict page | dict encoding / dict filtering | dictionary values | dict-encoded column | dict page |
| Data page | first read or second read | column values | row bitmap or rowids | data page, then cache blocks |

## SegmentPrefetcher 当前行为

当前 `SegmentPrefetcher` 只在云模式下的 query/compaction reader 类型启用。普通 query 是否启用由 `enable_query_segment_file_cache_prefetch` 控制。

初始化顺序：

```text
SegmentIterator::_init_segment_prefetchers
  -> each ColumnIterator::init_prefetcher
       -> only works if underlying reader is CachedRemoteFileReader
       -> SegmentPrefetcher::init loads ordinal index
  -> build_all_data_blocks() for full scans/compaction
  -> build_blocks_by_rowids(_row_bitmap) for selective query scans
```

触发顺序：

```text
FileColumnIterator::seek_to_ordinal(ord)
  -> SegmentPrefetcher::need_prefetch(ord)
  -> returns next file-cache block ranges
  -> CachedRemoteFileReader::prefetch_range(offset, size, io_ctx)
  -> submit async dry-run read_at_impl
```

这个设计有几个限制：

- range planning、cache block 对齐和异步执行耦合在 `SegmentPrefetcher` 与 `CachedRemoteFileReader` 中。
- 预取只表达“填充 file cache”，不能表达“只读到内存 buffer 且不写 file cache”。
- `prefetch_range()` 不返回 future 或状态，scan 无法知道预取是否完成。
- TopN 全局延迟物化二阶段不走 `SegmentIterator::_init_segment_prefetchers()`，因此当前预取不能覆盖 rowid fetch。

## 对冷查优化的结论

普通 scan 中 `.dat` 数据文件的核心优化机会是把 `_row_bitmap` 或 selected rowids 提前转成 page/cache-block ranges，并在真正的 `seek_to_ordinal()` 前启动并发远端读。对于谓词列，range planning 可以在 `_lazy_init()` 后完成；对于非谓词列，必须等谓词和 common expr 过滤后才能知道 selected rowids。

TopN 全局延迟物化二阶段的优化机会不同。它天然已经拿到所有需要物化的 rowids，而且 `RowIdStorageReader` 会按 segment 排序和去重，因此更适合在 `read_doris_format_row()` 或 `Segment::seek_and_read_by_rowid()` 之前做批量 rowid-to-range planning，而不是依赖普通 scan 的逐 `seek_to_ordinal()` 触发式预取。
