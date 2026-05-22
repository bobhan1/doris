# 文件格式排布和 `read_at` 清单

本文从物理文件格式开始，按当前 checkout 的代码路径列出内表冷读过程中会落到 `FileReader::read_at()` 的位置。这里的“`read_at`”包含两类：

- 直接调用 Doris `io::FileReader::read_at()` 的代码点，例如 segment footer、page 读、external column meta 读。
- CLucene `IndexInput::readBytes()` 间接触发的 `DorisFSDirectory::FSIndexInput::readInternal()`，其底层同样调用 Doris `FileReader::read_at()`。这类场景中，代码里的 `readInt()`、`readLong()`、`readBytes()` 不一定一一对应远端 `read_at`，因为 `BufferedIndexInput` 会按 buffer refill 合并成一次或多次 `readInternal()`。

## `.dat` 文件格式

Beta rowset 的 segment 数据文件名是 `{rowset_id}_{segment_id}.dat`。读路径中所有 `.dat` 内部 page 都使用同一种 page 封装格式：

```text
Page =
  PageBody
  PageFooterPB
  FooterSize(4 bytes)
  Checksum(4 bytes)
```

对应代码是 `PageIO`：

- `PageFooterPB.type` 区分 `DATA_PAGE`、`INDEX_PAGE`、`DICTIONARY_PAGE`、`SHORT_KEY_PAGE`、`PRIMARY_KEY_INDEX_PAGE`。
- `PageFooterPB.uncompressed_size` 判断 page body 是否压缩。
- `DataPageFooterPB` 记录 `first_ordinal`、`num_values`、`nullmap_size`，array page 还记录 `next_array_item_ordinal`。
- `IndexPageFooterPB` 记录 index page entry 数和 leaf/internal 类型。
- `ShortKeyFooterPB` 记录 short-key item 数、key bytes、offset bytes、block rows 等。

整体 `.dat` 文件可以抽象为：

```text
0
  column data pages
  column index pages: ordinal / page zone map / bloom filter / dictionary page
  short-key page or primary-key indexed-column pages
  optional variant external-meta key indexed-column pages
  optional V3 external ColumnMetaPB region
  SegmentFooterPB
  FooterPBSize(4 bytes)
  FooterPBChecksum(4 bytes)
  MagicNumber(4 bytes)
file_size
```

这些区域不靠固定偏移解析，而是靠 footer 中的 `PagePointerPB{offset,size}` 定位：

- `SegmentFooterPB.short_key_index_page` 指向 short-key page。
- `SegmentFooterPB.primary_key_index_meta` 指向 primary-key indexed-column 和 PK bloom filter 的 meta。
- `SegmentFooterPB.columns` 在 V2 footer 中直接保存各列 `ColumnMetaPB`。
- `SegmentFooterPB.col_meta_region_start` 和 `column_meta_entries` 在 V3 footer 中描述外置 `ColumnMetaPB` region。
- `ColumnMetaPB.dict_page` 指向字典页。
- `ColumnMetaPB.indexes` 中的 `OrdinalIndexPB`、`ZoneMapIndexPB`、`BloomFilterIndexPB`、`NestedOffsetsIndexPB` 描述列级索引。
- `IndexedColumnMetaPB` 使用 `BTreeMetaPB.root_page` 定位 indexed-column 的 root。`is_root_data_page=true` 时 root 直接是 data page；否则 root 是 index page，index page 再映射到 data page。

## `.dat` 内部 indexed-column 格式

`.dat` 中很多索引不是单独一种文件格式，而是复用 `IndexedColumnReader`：

- primary-key index：`PrimaryKeyIndexMetaPB.primary_key_index` 是一个 indexed-column，通常有 ordinal index 和 value index。
- primary-key bloom filter：`PrimaryKeyIndexMetaPB.bloom_filter_index` 下的 bloom filter 是 indexed-column。
- page zone map：`ZoneMapIndexPB.page_zone_maps` 是 indexed-column，每个 value 是一个序列化 `ZoneMapPB`。
- bloom filter：`BloomFilterIndexPB.bloom_filter` 是 indexed-column，每个 value 是一个 serialized bloom filter。
- variant external meta key：`footer.file_meta_datas["variant_meta_keys.<root_uid>"]` 保存一个 indexed-column meta，key 是 variant subcolumn relative path。

`IndexedColumnReader::load()` 可能读取 ordinal index root page 和 value index root page。之后 `IndexedColumnIterator::seek_to_ordinal()` 或 `seek_at_or_after()` 可能读取 indexed-column 的 data page。它们最终都走 `IndexedColumnReader::read_page()` -> `PageIO::read_and_decompress_page()` -> `opts.file_reader->read_at(page_pointer.offset, page_pointer.size, ...)`。

## V3 external ColumnMetaPB region

当前分支支持 footer V3：大的 `ColumnMetaPB` 不都塞进 `SegmentFooterPB.columns`，而是把每个 top-level column 和部分 variant subcolumn 的 `ColumnMetaPB` 序列化后连续写在 `.dat` 中：

```text
external ColumnMetaPB region =
  ColumnMetaPB(col_id = 0)
  ColumnMetaPB(col_id = 1)
  ...
```

footer 只保存：

- `col_meta_region_start`：region 起始 offset。
- `column_meta_entries[i].length`：第 i 个 `ColumnMetaPB` 的长度。
- `column_meta_entries[i].unique_id`：top-level column 或 externalized subcolumn 的 unique id。

读路径有两种粒度：

- `ColumnMetaAccessorV3::traverse_metas()` 一次读取整个 region：`read_at(region_start, region_size)`。
- `ExternalColMetaUtil::read_col_meta()` 按 prefix-sum 定位某个 col_id，一次读取一个 `ColumnMetaPB` slice：`read_at(pos, length)`。

## `.idx` 文件格式

倒排索引文件由 `IndexFileReader`、`DorisFSDirectory::FSIndexInput` 和 `DorisCompoundReader` 读取。路径前缀从 `.dat` 去掉 suffix 得到：

- `.dat`：`{prefix}.dat`
- V1 `.idx`：`{prefix}_{index_id}@{suffix}.idx`
- V2 `.idx`：`{prefix}.idx`

### V1 `.idx`

V1 每个 tablet index 一个 compound 文件。写入格式来自 `IndexStorageFormatV1::write_header_and_data()`：

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

小 header 文件可能被内联进 header，此时 entry offset 是 `-1`，读端 `DorisCompoundReader` 会在构造时把这些小文件复制到 `RAMDirectory`。非内联文件由 `CSIndexInput` 把 sub-file local offset 映射成 compound 文件 offset。

### V2 `.idx`

V2 是 segment 级 compound 文件，一个 `.idx` 中包含多个 index id/suffix 的 sub-file entry。写入格式来自 `IndexStorageFormatV2`：

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

V2 会把 meta sub-file 放在 normal sub-file 前面。当前 `InvertedIndexDescriptor::index_file_info_map` 把这些文件当成 meta/index-data：

- `null_bitmap`
- `segments.gen`
- `segments_`
- `fnm`
- `tii`
- `bkd_meta`
- `bkd_index`

当前 `normal_file_info_map` 包括：

- `tis`
- `frq`
- `prx`

ANN 索引也通过 `.idx` compound directory 读写，但 sub-file 名是 FAISS 文件，例如 `ann.faiss` 和 `ann.ivfdata`。

## file cache 层的二次 `read_at`

存算分离下 `.dat` 和 `.idx` 的 `FileReader` 通常是 `CachedRemoteFileReader`。上层一次逻辑 `read_at(offset,size)` 进入它之后，内部还有几种 I/O：

```text
CachedRemoteFileReader::read_at_impl(offset, size)
  -> optional direct local cache-file read
  -> BlockFileCache::get_or_set(aligned_offset, aligned_size)
  -> EMPTY / SKIP_CACHE: _execute_remote_read(empty_start, empty_size)
  -> DOWNLOADED: FileBlock::read(...)
  -> DOWNLOADING timeout or missing cache file: remote_file_reader->read_at(current_offset, read_size)
```

对应的底层 `read_at`：

- 本地 cache 命中：`FileBlock::read()` -> `FSFileCacheStorage::read()` -> local cache file `read_at(value_offset,size)`。
- 冷 miss：`execute_s3_read()` -> remote reader `read_at(empty_start, empty_size)`；`empty_start/empty_size` 是按 file-cache block 对齐后的连续范围。
- 读到 `DOWNLOADED` 但 cache 文件缺失或等待 downloader 超时：fallback 到 `_remote_file_reader->read_at(current_offset, read_size)`。
- `prefetch_range()` 设置 `IOContext.is_dryrun=true`，会走同一套 block planning，但 dry-run 场景不把数据 copy 给 caller，部分分支会跳过实际本地 cache I/O。

因此，下面清单中的上层 `read_at` 是“逻辑文件读”。真实远端 I/O 可能被 file-cache block 对齐放大，或者被本地 cache 命中消除。

## `.dat` 直接 `read_at` 清单

| ID | 场景 | 代码入口 | `read_at` 位置 | offset/size 来源 | `IOContext` | 触发次数和依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| DAT-01 | footer fixed tail | `Segment::_parse_footer()` | `_file_reader->read_at(file_size - 12, 12)` | file size 的最后 12 字节 | `is_index_data=true` | 每个 segment footer cache miss 至少 1 次；先读它才能知道 footer PB length/checksum/magic |
| DAT-02 | footer protobuf | `Segment::_parse_footer()` | `_file_reader->read_at(file_size - 12 - footer_length, footer_length)` | DAT-01 解出的 `footer_length` | `is_index_data=true` | DAT-01 成功后 1 次；解析出 `SegmentFooterPB` |
| DAT-03 | footer corruption diagnostic | `Segment::_write_error_file()` | `cached_reader->get_remote_reader()->read_at(0, file_size)` | 整个远端 segment 文件 | 继承 footer read 的 `io_ctx` | 只在 footer/page corruption 诊断写 error file 时触发，不是正常读路径 |
| DAT-04 | V3 external meta whole-region scan | `ColumnMetaAccessorV3::traverse_metas()` | `_file_reader->read_at(region_start, region_size)` | `footer.col_meta_region_start` + sum(`column_meta_entries.length`) | `is_index_data=true` | 创建/遍历全部 column meta 时 1 次；只适用于 footer V3 |
| DAT-05 | V3 single ColumnMetaPB lookup | `ExternalColMetaUtil::read_col_meta()` | `file_reader->read_at(pos, length)` | `region_start + prefix_sum(length[0..col_id))` | `is_index_data=true` | 按需创建某列 reader 或 variant subcolumn meta 时触发；可多次 |
| DAT-06 | generic page read | `PageIO::read_and_decompress_page_()` | `opts.file_reader->read_at(page_pointer.offset, page_pointer.size)` | 调用方传入的 `PagePointer` | 来自 `PageReadOptions.io_ctx` | 所有 `.dat` data/index/dict/short-key/PK page 的唯一底层读点；`StoragePageCache` 命中时不触发 |
| DAT-07 | page corruption retry with file cache | `PageIO::read_and_decompress_page()` | 同 DAT-06，先清 file cache 重试，再用 `cached_file_reader->get_remote_reader()` 重试 | 同原 page pointer | 同原 options | 只在 cloud mode 且 DAT-06 返回 corruption 时触发；同一个 page 最多多读两轮 |

## `.dat` page 场景到 DAT-06 的展开

| 场景 | 上层代码 | Page type | PagePointer 来源 | 每次 `read_at` 覆盖什么 | 前置依赖 |
| --- | --- | --- | --- | --- | --- |
| short-key index | `Segment::load_index()` non-unique path | `INDEX_PAGE`，footer 校验为 `SHORT_KEY_PAGE` | `SegmentFooterPB.short_key_index_page` | short-key body + page footer + footer size + checksum | DAT-01/02 解析 footer 后才有 pointer |
| primary-key index root/index page | `PrimaryKeyIndexReader::parse_index()` -> `IndexedColumnReader::load()` | `PRIMARY_KEY_INDEX_PAGE` | `PrimaryKeyIndexMetaPB.primary_key_index` 中的 ordinal/value `BTreeMetaPB.root_page` | PK indexed-column root index page；如果 `is_root_data_page=true`，load 阶段不读，后续 iterator 读 data page | footer 中有 `primary_key_index_meta` |
| primary-key bloom root/index page | `PrimaryKeyIndexReader::parse_bf()` -> `BloomFilterIndexReader::load()` -> `IndexedColumnReader::load()` | `PRIMARY_KEY_INDEX_PAGE` 或 `INDEX_PAGE` | `PrimaryKeyIndexMetaPB.bloom_filter_index` | bloom indexed-column root index page | PK bloom 需要显式 `load_pk_index_and_bf()` 或点查路径触发 |
| primary-key bloom data page | `BloomFilterIndexIterator::read_bloom_filter(0)` -> `IndexedColumnIterator::seek_to_ordinal()` | `PRIMARY_KEY_INDEX_PAGE` 或 `DATA_PAGE` | bloom indexed-column ordinal index 定位出的 data page | 存放 bloom filter value 的 indexed-column data page | bloom root/index page 已加载或 root 是 sole data page |
| ordinary column ordinal index | `ColumnReader::_load_ordinal_index()` -> `OrdinalIndexReader::load()` | `INDEX_PAGE` | `OrdinalIndexPB.root_page.root_page` | ordinal -> data page pointer 的 index page | `is_root_data_page=false`；如果只有一个 data page，无 `read_at` |
| ordinary column data page | `FileColumnIterator::_read_data_page()` | `DATA_PAGE` | ordinal index iterator 当前 page pointer | 列值 page body；dict 编码时 page 内是字典 id 等编码数据 | `seek_to_ordinal()` 或 `next_batch()` 发现当前 page 不含目标 rowid |
| dictionary page | `FileColumnIterator::_read_dict_data()` | options 里设为 `INDEX_PAGE`，footer 是 dict page | `ColumnMetaPB.dict_page` | 字典值 page body | 第一次读到 dict-encoded data page 且 decoder 需要 dictionary |
| page zone map index root/index page | `ZoneMapIndexReader::load()` -> `IndexedColumnReader::load()` | `INDEX_PAGE` | `ZoneMapIndexPB.page_zone_maps` indexed-column meta | zone-map indexed-column 的 root index page | zone map predicate 触发 |
| page zone map data page | `ZoneMapIndexReader::_load()` -> `IndexedColumnIterator::seek_to_ordinal(i)` | `DATA_PAGE` | zone-map indexed-column ordinal 定位 | 一个或多个 serialized `ZoneMapPB` value 所在 data page | root/index page 已加载；循环每个 page zone map，多个 ordinal 可命中同一 data page |
| bloom filter index root/index page | `BloomFilterIndexReader::load()` -> `IndexedColumnReader::load()` | `INDEX_PAGE` | `BloomFilterIndexPB.bloom_filter` indexed-column meta | bloom indexed-column root index page | bloom predicate 触发 |
| bloom filter data page | `BloomFilterIndexIterator::read_bloom_filter(page_id)` | `DATA_PAGE` | bloom indexed-column ordinal 定位 | 某个 page_id 的 serialized bloom filter value | bloom row range 计算已得到 page ids |
| variant external-meta key root/index page | `VariantExternalMetaReader` lookup/load_all -> indexed-column reader load | `INDEX_PAGE` | `footer.file_meta_datas["variant_meta_keys.<root_uid>"]` 中的 `IndexedColumnMetaPB` | variant path key indexed-column 的 root/index page | 查询访问外置 variant subcolumn meta |
| variant external-meta key data page | `VariantExternalMetaReader::lookup_meta_by_path()` / `load_all()` | `DATA_PAGE` | key indexed-column ordinal/value index 定位 | relative path string value | 用于 path -> col_id，再走 DAT-05 读取 ColumnMetaPB |
| nested/array/map/struct child page | `ArrayFileColumnIterator` / `MapFileColumnIterator` / `StructFileColumnIterator` | `DATA_PAGE` 或子列索引页 | 子列自身 `ColumnMetaPB` / `PagePointer` | null map、offsets、items、key、value、struct child 等物理子列 page | 复杂类型的逻辑一次读会展开成多个子 iterator，各自按本表其它行触发 DAT-06 |

## 普通 scan 中按阶段发生的 `read_at`

普通 `SegmentIterator` 的冷路径可以精确拆成：

1. `Segment::open()`：DAT-01，DAT-02；如果 footer 已在 `StoragePageCache` 中，不触发。
2. `ColumnMetaAccessor::init()`：V2 footer inline meta 不读；V3 根据调用方式触发 DAT-04 或 DAT-05。
3. `Segment::new_iterator()`：需要 key index 时触发 short-key 或 PK index page 的 DAT-06。
4. `_lazy_init()` key range：short-key 已在上一步加载；主键或 key seek 若需要访问 indexed-column data page，会通过 PK indexed-column iterator 触发 DAT-06。
5. `_get_row_ranges_by_column_conditions()`：
   - 倒排索引先走 `.idx` 清单。
   - dict predicate 可能先触发 dictionary page DAT-06。
   - bloom predicate 先触发 ordinary column ordinal index DAT-06，再触发 bloom root/index page 和 bloom data page DAT-06。
   - zone map predicate 触发 zone-map root/index page 和 zone-map data page DAT-06。
6. `_read_columns_by_index()` first read：
   - 每个谓词/表达式列先按 row bitmap 取 rowids。
   - 连续 rowids：每列第一次 `seek_to_ordinal()` 可能读 ordinal index DAT-06 和 data page DAT-06，跨 page 的 `next_batch()` 继续读后续 data page。
   - 稀疏 rowids：`read_by_rowids()` 对每个新 page 触发一次 data page DAT-06，同页 rowids 复用内存 page。
7. 谓词和 common expr 过滤：无文件 `read_at`。
8. `_read_columns_by_rowids()` second read：对非谓词列重复第 6 步，但 rowids 是过滤后的 selected rowids。
9. `SegmentPrefetcher`：`FileColumnIterator::seek_to_ordinal()` 触发 `CachedRemoteFileReader::prefetch_range()`。它不是新的上层文件格式读点，但会进入 file cache 层的 dry-run block planning；是否产生远端读取决于 cache block 状态和 dry-run 分支。

## TopN 全局延迟物化二阶段的 `read_at`

TopN 二阶段不创建普通 `SegmentIterator`，因此没有 `_lazy_init()`、没有 `_init_segment_prefetchers()`，也没有普通 scan 内部 first/second read 的 batch 状态。它的文件读发生在服务端 `RowIdStorageReader`：

```text
PMultiGetRequestV2
  -> group by SegKey(tablet_id,rowset_id,segment_id)
  -> sort/dedup row_ids
  -> read_doris_format_row()
  -> Segment::seek_and_read_by_rowid()
  -> FileColumnIterator::read_by_rowids()
```

它会复用 `.dat` 清单中的这些读点：

- segment 首次加载时仍然触发 DAT-01/DAT-02 和必要的 column meta 读。
- 每个 lazy column 首次 `seek_to_ordinal()` 可能触发 ordinary column ordinal index DAT-06。
- 每个 rowid 所在的新 data page 触发 ordinary column data page DAT-06。
- dict-encoded lazy column 第一次读 data page 后可能额外触发 dictionary page DAT-06。
- 复杂类型 lazy column 仍会展开成 offsets/null/child/key/value 的子 iterator DAT-06。

二阶段的关键差异是 rowids 在进入 storage 前已经按 segment 排序去重，所以同一列内 `read_by_rowids()` 单调前进；dense rowids 会复用 page，sparse rowids 会接近“每个 rowid 一个 page read”。

## `.idx` `read_at` 清单

| ID | 场景 | 代码入口 | 真实 `read_at` 位置 | offset/size 来源 | `IOContext` | 触发次数和依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| IDX-01 | 打开 V2 `.idx` 读取 header 和 directory | `IndexFileReader::_init_from()` 的 `readInt/readLong/readBytes` | `DorisFSDirectory::FSIndexInput::readInternal()` -> `_reader->read_at(_pos, len)` | CLucene buffer 当前 `_pos` 和 refill `len` | `_stream->setIndexFile(true)` 后 `is_index_data=true` | searcher cache miss 或显式 init；读取 version、num_indices、每个 index 的 suffix、sub-file offset/length |
| IDX-02 | 打开 V1 compound directory | `DorisCompoundReader(IndexInput*)` 的 `readVInt/readChars/readLong` | `FSIndexInput::readInternal()` -> `_reader->read_at(_pos, len)` | compound header 当前 `_pos` 和 CLucene buffer size | V1 open 时传入的 `io_ctx`，随后 `initialize()` 设置 idx file cache | V1 每个 `{index_id}@{suffix}.idx` 打开时触发 |
| IDX-03 | V1 inline small header sub-file copy | `DorisCompoundReader::_copyFile()` | `_stream->readBytes()` -> `FSIndexInput::readInternal()` -> `read_at` | header 中 `offset=-1` 的 sub-file 内容顺序读取 | 同 IDX-02 | 只对 V1 中小于阈值并内联进 header 的 sub-file 触发，读后进入 RAMDirectory |
| IDX-04 | V1/V2 compound sub-file read | `CSIndexInput::readInternal()` | `base->seek(fileOffset + start)` -> `base->readBytes()` -> `FSIndexInput::readInternal()` -> `read_at` | sub-file entry 的 `fileOffset` + sub-file local offset | `CSIndexInput` 临时把 `_io_ctx` 绑定到 base；meta/normal 文件的 `is_index_data` 在 debug point 中校验 | 任何 term dict、posting、BKD、null bitmap、ANN file 读都落到这里 |
| IDX-05 | null bitmap | `InvertedIndexReader::read_null_bitmap()` | `null_bitmap_in->readBytes(buf, null_bitmap_size)` -> IDX-04 | `null_bitmap` sub-file length | 通常 index-data | nullable predicate 或 searcher cache miss；query cache 命中则不读 |
| IDX-06 | fulltext/string searcher build | `FulltextIndexSearcherBuilder::build()` / CLucene `IndexReader::open` | CLucene 打开的 `segments*`、`fnm`、`tii`、`tis` 等 sub-file -> IDX-04 | CLucene 内部按 dictionary/posting metadata refill | meta 文件一般 index-data，posting 文件可按 normal 文件分类 | searcher cache miss 时触发；命中则跳过 |
| IDX-07 | term equality / IN / MATCH term query | `TermQuery::search()` / `TermIterator` / `TermDocs::readRange` | term dict/postings `readBytes()` -> IDX-04 | term 定位后的 dictionary/posting offset | 取决于 sub-file 类型 | query cache miss 时触发；IN 每个 value 可能触发一次 term 查询 |
| IDX-08 | phrase / phrase prefix / position query | `TermPositionsIterator` 或 query-v2 phrase postings | `.prx` / position postings `readBytes()` -> IDX-04 | position list offset | normal posting file | 只有需要 position 的 query 触发 |
| IDX-09 | string range query | `StringTypeInvertedIndexReader::query()` | term dictionary enumeration + postings -> IDX-04 | range term enumeration 控制 | 取决于 sub-file 类型 | term 过多可能 bypass，之后回到 `.dat` 数据读 |
| IDX-10 | BKD reader open | `BKDIndexSearcherBuilder::build()` / `bkd_reader::open` | `bkd_meta` / `bkd_index` / `bkd` sub-file -> IDX-04 | BKD metadata/tree offset | meta 文件 index-data；data 文件按 sub-file 分类 | BKD searcher cache miss 时触发 |
| IDX-11 | BKD range/equality query | `BkdIndexReader::invoke_bkd_query()` -> `bkd_reader->intersect()` | BKD tree/leaf block reads -> IDX-04 | tree traversal 决定 | 取决于 sub-file 类型 | query cache miss 时触发；`try_query()` 可先 estimate 并 bypass |
| IDX-12 | query-v2 postings block stream | `SegmentPostings::skipToBlock/readBlock` / `LoadedPostings::load()` | postings block/position block `readBytes()` -> IDX-04 | query-v2 postings block offset | normal posting file | search DSL、phrase、top-k/score 等 query 触发 |
| IDX-13 | ANN index metadata | `AnnIndexReader::load_index()` -> `FaissVectorIndex::load()` -> `faiss::read_index()` | FAISS `IOReader` over `ann.faiss` -> `IndexInput::readBytes()` -> IDX-04 | FAISS reader sequential offset | ANN index file context | ANN searcher/load miss 时触发 |
| IDX-14 | ANN IVF on-disk list | `CachedRandomAccessReader::read_at()` / `borrow()` | `_input->seek(offset)` + `_input->readBytes()` -> IDX-04 | FAISS IVF list offset and length | per-read `g_current_io_ctx` | IVF_ON_DISK query 访问 list 时触发；命中 `AnnIndexIVFListCache` 则不读 |

## 倒排索引场景展开

倒排索引不是在 segment open 时必然读所有 `.idx`。普通 scan 会先创建 `IndexIterator` 对象，真正 I/O 延迟到 predicate/search 需要它：

```text
SegmentIterator::_init_index_iterators()
  -> Segment::new_index_iterator()
  -> ColumnReader::_load_index()
  -> reader object only

predicate/search evaluation
  -> InvertedIndexIterator::read_from_index()
  -> reader->query()
  -> handle_searcher_cache()
  -> IDX-01/02/04...
```

按 predicate 类型：

- `EQ` / `IN`：searcher cache miss 先 IDX-01/06；query cache miss 再 IDX-07；nullable 语义可能 IDX-05。
- `MATCH*`：searcher cache miss IDX-01/06；query cache miss IDX-07；phrase/position 类额外 IDX-08。
- string range：searcher cache miss IDX-01/06；query cache miss IDX-09；低选择性可能 bypass。
- numeric range/equality：BKD searcher miss IDX-01/10；query cache miss IDX-11；`try_query()` estimate 可能只读一部分 BKD metadata/tree。
- search DSL / query-v2：根据 query shape 触发 IDX-12，最终仍产出 bitmap 或命中结果。
- ANN：ANN reader load 触发 IDX-13；IVF_ON_DISK list 访问触发 IDX-14。

`.idx` 产出的 roaring bitmap 会先和 `SegmentIterator::_row_bitmap` 相交。之后 `.dat` 谓词列和非谓词列读才按过滤后的 rowids 走 DAT-06。也就是说，`.idx` read_at 与 `.dat` data page read_at 是串行依赖：先用 `.idx` 缩小 rowid 集合，再决定后续 `.dat` page 范围。

## 从 `read_at` 清单反推 prefetch 接口边界

从清单看，未来 prefetch 接口需要至少区分这些目标：

1. `.dat` footer/meta prefetch：固定 tail 和 footer PB 读，V3 还需要 external ColumnMetaPB region 或单列 meta。
2. `.dat` indexed-column prefetch：short-key、PK、ordinal、zone-map、bloom、variant meta key 都是 indexed-column/page 结构，接口需要支持 `IndexedColumnMetaPB -> PagePointer`。
3. `.dat` data page prefetch：普通 scan 依赖 `_row_bitmap`，TopN 二阶段依赖 sorted unique rowids。
4. `.idx` open/searcher prefetch：V2 header/directory、V1 compound directory、searcher build metadata。
5. `.idx` query-aware prefetch：term/posting/BKD/ANN 的精确 offset 只有 query term/range/list 已知后才能知道。
6. file-cache block prefetch：所有上层 `read_at` 最后都可能按 file-cache block 对齐，接口不能只暴露逻辑 page offset。

如果接口只表达“给列和 rowids 预取 data page”，会漏掉 footer、external meta、zone/bloom/dict indexed-column、倒排索引 metadata/posting、ANN IVF list，以及 file-cache block 对齐带来的远端读放大。

## 相关但不属于 segment/index 冷查主路径的 `read_at`

为了避免把代码里其它 `read_at` 和内表 scan 的数据/index 文件 I/O 混在一起，这里单独列出几个相关但不应作为本轮 prefetch 设计核心输入的读点：

| 场景 | 代码入口 | 文件格式 | `read_at` | 为什么不放入主清单 |
| --- | --- | --- | --- | --- |
| cooldown tablet meta | `Tablet::_read_cooldown_meta()` | 远端 `TabletMetaPB` 整文件 | `tablet_meta_reader->read_at(0, file_size)` | tablet/rowset 元数据加载路径，影响 tablet 冷启动，不是 scan 中 `.dat`/`.idx` page 访问 |
| local `FileHeader<MessageType, ExtraType>` | `FileHeader::deserialize()` | `FixedFileHeader` + optional old header + extra fixed header + protobuf body | 依次读 fixed header、兼容 old fixed header、extra fixed header、protobuf body | 本地旧式元数据/header 文件解析，不是 segment v2 `.dat` 的 footer/page 格式 |
| local cluster id | `DataDir::read_cluster_id()` | 文本 cluster id 文件 | `reader->read_at(0, fsize)` | BE data dir 初始化文件，不在查询扫描链路 |
| storage utils test file | `storage/utils.cpp` test helper | 测试文件 | `file_reader->read_at(0, TEST_FILE_BUF_SIZE)` | 测试工具路径，不参与查询 |

如果后续目标扩展为“BE 冷启动 + tablet 元数据加载 + 第一次查询”的端到端优化，cooldown tablet meta 可以单独建一个 meta-file prefetch 类别；但它和本文主线的 `.dat` page / `.idx` sub-file prefetch 接口不是同一层抽象。
