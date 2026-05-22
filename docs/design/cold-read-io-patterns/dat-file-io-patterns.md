# `.dat` 数据文件格式、读路径和 I/O Pattern

本文只描述 Doris 内表 beta rowset 的 segment 数据文件，也就是
`{rowset_id}_{segment_id}.dat`。倒排索引 `.idx` 文件单独见
`inverted-index-io-patterns.md`。

这里的 `read_at` 分两层：

- 上层逻辑读：Doris 代码直接调用 `io::FileReader::read_at()`，例如 footer、V3
  external column meta、PageIO。
- file cache 内部读：上层逻辑读进入 `CachedRemoteFileReader` 后，可能被拆成
  local cache-file `read_at()`、remote object-store `read_at()` 或 fallback remote
  `read_at()`。这层会按 file-cache block 对齐，不一定等于上层 offset/size。

## `.dat` 文件格式和物理排布

`.dat` 文件不是按固定 offset 解析的格式。除末尾 footer 外，所有 page 都依赖
footer 或 column meta 中的 `PagePointerPB{offset,size}` 定位。

文件整体可以抽象为：

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

其中所有 `.dat` 内部 page 都使用 `PageIO` 的统一封装：

```text
Page =
  PageBody
  PageFooterPB
  FooterSize(4 bytes)
  Checksum(4 bytes)
```

`PageFooterPB.type` 区分 `DATA_PAGE`、`INDEX_PAGE`、`DICTIONARY_PAGE`、
`SHORT_KEY_PAGE`、`PRIMARY_KEY_INDEX_PAGE`。`DataPageFooterPB` 记录
`first_ordinal`、`num_values`、`nullmap_size` 和 array page 的
`next_array_item_ordinal`；`IndexPageFooterPB` 记录 indexed-column BTree page
的 entry 数和 leaf/internal 类型；`ShortKeyFooterPB` 记录 short-key item、
key bytes、offset bytes 和 block rows。

footer 中直接或间接给出这些区域：

| 区域 | footer/meta 字段 | 读法 |
| --- | --- | --- |
| segment footer | 文件末尾 fixed tail 解出 length | 先读末尾 12 字节，再读 `SegmentFooterPB` |
| short-key page | `SegmentFooterPB.short_key_index_page` | `PageIO` 读一个 page |
| primary-key index | `SegmentFooterPB.primary_key_index_meta.primary_key_index` | `IndexedColumnReader` 读 root/index/data page |
| primary-key bloom filter | `SegmentFooterPB.primary_key_index_meta.bloom_filter_index` | `IndexedColumnReader` + bloom value data page |
| V2 column meta | `SegmentFooterPB.columns` | 已在 footer PB 内，无额外 `read_at` |
| V3 external column meta | `col_meta_region_start` + `column_meta_entries.length` | 整 region 读，或按 col id 读一个 slice |
| ordinary column data | `ColumnMetaPB` 中的 ordinal index 和 page pointer | `seek_to_ordinal()` 定位 page 后读 |
| dictionary page | `ColumnMetaPB.dict_page` | 首次需要字典时读 |
| page zone map | `ZoneMapIndexPB.page_zone_maps` | indexed-column root/index/data page |
| bloom filter | `BloomFilterIndexPB.bloom_filter` | indexed-column root/index/data page |
| variant external-meta key | `footer.file_meta_datas["variant_meta_keys.<root_uid>"]` | indexed-column key lookup 后再读 V3 meta slice |

`NestedOffsetsIndexPB` 在 proto 中定义了 `block_end_offsets` 和
`block_cumulative_offsets` 两个 indexed-column meta，但当前
`ColumnReader::init()` 对 `NESTED_OFFSETS_INDEX` 只识别类型并跳过，没有创建
reader，也没有读路径落到 `PageIO`。

## `.dat` 内部 indexed-column

`.dat` 中很多“索引”不是独立文件，而是复用 `IndexedColumnReader`：

- primary-key index：`PrimaryKeyIndexMetaPB.primary_key_index`。
- primary-key bloom filter：`PrimaryKeyIndexMetaPB.bloom_filter_index`。
- page zone map：`ZoneMapIndexPB.page_zone_maps`，每个 value 是 serialized
  `ZoneMapPB`。
- bloom filter：`BloomFilterIndexPB.bloom_filter`，每个 value 是 serialized
  bloom filter。
- variant external meta key：`variant_meta_keys.<root_uid>`，key 是 variant
  subcolumn relative path。

`IndexedColumnReader::load()` 会按 meta 读取 ordinal index root page 和 value
index root page；如果 `is_root_data_page=true`，root 本身就是 data page，load
阶段不读 index page，后续 iterator 读取 data page。`IndexedColumnIterator`
的 `seek_to_ordinal()`、`seek_at_or_after()` 和 `next_batch()` 最终都走
`IndexedColumnReader::read_page()` -> `PageIO::read_and_decompress_page()`。

## V3 external ColumnMetaPB region

footer V3 会把大的 `ColumnMetaPB` 外置到 `.dat` 文件中的连续 region：

```text
external ColumnMetaPB region =
  ColumnMetaPB(col_id = 0)
  ColumnMetaPB(col_id = 1)
  ...
```

footer 只保存 region 起点和每个 meta slice 的长度：

- `col_meta_region_start`：region 起始 offset。
- `column_meta_entries[i].length`：第 i 个 `ColumnMetaPB` 的长度。
- `column_meta_entries[i].unique_id`：top-level column 或 externalized
  subcolumn 的 unique id。

当前读路径有两个粒度：

- `ColumnMetaAccessorV3::traverse_metas()` 一次读取整个 region。
- `ExternalColMetaUtil::read_col_meta()` 通过 prefix-sum 定位一个 col id，只读
  这个 `ColumnMetaPB` slice。

## 直接 `read_at` 清单

下表是当前 `.dat` 文件正常读路径以及异常诊断路径中，Doris 代码直接落到
`FileReader::read_at()` 的位置。`DAT-06` 是所有 page 类型的统一底层读点；
下一节再展开它覆盖的 page 场景。

| ID | 场景 | 代码入口 | `read_at` 位置 | offset/size 来源 | `IOContext` | 触发次数和依赖 |
| --- | --- | --- | --- | --- | --- | --- |
| DAT-01 | footer fixed tail | `Segment::_parse_footer()` | `_file_reader->read_at(file_size - 12, 12)` | file size 最后 12 字节 | `is_index_data=true` | segment footer cache miss 时先读；得到 footer length/checksum/magic |
| DAT-02 | footer protobuf | `Segment::_parse_footer()` | `_file_reader->read_at(file_size - 12 - footer_length, footer_length)` | DAT-01 解出的 `footer_length` | `is_index_data=true` | DAT-01 后 1 次；解析 `SegmentFooterPB` |
| DAT-03 | footer/page corruption diagnostic | `Segment::_write_error_file()` | `cached_reader->get_remote_reader()->read_at(0, file_size)` | 整个远端 segment 文件 | 继承触发诊断的 `io_ctx` | 只在 corruption 诊断写 error file 时触发，不是正常扫描路径 |
| DAT-04 | V3 external meta whole-region scan | `ColumnMetaAccessorV3::traverse_metas()` | `_file_reader->read_at(region_start, region_size)` | `footer.col_meta_region_start` + sum(`column_meta_entries.length`) | `is_index_data=true` | 遍历全部 column meta 时 1 次；仅 footer V3 |
| DAT-05 | V3 single `ColumnMetaPB` lookup | `ExternalColMetaUtil::read_col_meta()` | `file_reader->read_at(pos, length)` | `region_start + prefix_sum(length[0..col_id))` | `is_index_data=true` | 按需创建某列 reader 或 variant subcolumn meta 时触发，可多次 |
| DAT-06 | generic page read | `PageIO::read_and_decompress_page_()` | `opts.file_reader->read_at(page_pointer.offset, page_pointer.size)` | 调用方传入的 `PagePointer` | 来自 `PageReadOptions.io_ctx` | 所有 `.dat` data/index/dict/short-key/PK page 的唯一底层读点；`StoragePageCache` 命中时不触发 |
| DAT-07 | page corruption retry | `PageIO::read_and_decompress_page()` | 同 DAT-06，先清 file cache 重试，再用 `cached_file_reader->get_remote_reader()` 重试 | 同原 page pointer | 同原 options | 只在 cloud mode 且 DAT-06 返回 corruption 时触发；同一个 page 最多额外读两轮 |

## DAT-06 的 page 场景展开

这些场景每次真正读 page 时都会落到 DAT-06。区别在于是谁给出
`PagePointer`、page type 是什么、以及一次逻辑读会重复多少次。

| 场景 | 上层代码 | Page type | PagePointer 来源 | 每次 `read_at` 覆盖什么 | 前置依赖 |
| --- | --- | --- | --- | --- | --- |
| short-key index | `Segment::load_index()` non-unique path | options 为 `INDEX_PAGE`，footer 校验为 `SHORT_KEY_PAGE` | `SegmentFooterPB.short_key_index_page` | short-key page body + footer + footer size + checksum | DAT-01/02 |
| primary-key indexed-column root/index page | `PrimaryKeyIndexReader::parse_index()` -> `IndexedColumnReader::load()` | `PRIMARY_KEY_INDEX_PAGE` | `PrimaryKeyIndexMetaPB.primary_key_index` 中的 ordinal/value `BTreeMetaPB.root_page` | PK indexed-column root/index page | footer 中存在 PK meta；`is_root_data_page=true` 时 load 阶段不读 |
| primary-key indexed-column data page | PK index iterator seek/read | `PRIMARY_KEY_INDEX_PAGE` 或 `DATA_PAGE` | PK indexed-column ordinal/value index 定位 | PK key value 所在 data page | PK root/index 已加载或 root 是 data page |
| primary-key bloom root/index page | `PrimaryKeyIndexReader::parse_bf()` -> `BloomFilterIndexReader::load()` | `PRIMARY_KEY_INDEX_PAGE` 或 `INDEX_PAGE` | `PrimaryKeyIndexMetaPB.bloom_filter_index` | bloom indexed-column root/index page | 显式加载 PK bloom 或点查路径触发 |
| primary-key bloom data page | `BloomFilterIndexIterator::read_bloom_filter(0)` | `PRIMARY_KEY_INDEX_PAGE` 或 `DATA_PAGE` | bloom indexed-column ordinal 定位 | serialized bloom filter value | bloom root/index 已加载或 root 是 data page |
| ordinary column ordinal index | `ColumnReader::_load_ordinal_index()` -> `OrdinalIndexReader::load()` | `INDEX_PAGE` | `OrdinalIndexPB.root_page.root_page` | ordinal -> data page pointer 的 BTree root/index page | `seek_to_ordinal()`、zone map、bloom 或 prefetch planning 需要 rowid->page；单 data page 时可能无读 |
| ordinary column data page | `FileColumnIterator::_read_data_page()` | `DATA_PAGE` | ordinal index iterator 当前 page pointer | 列值 data page | `seek_to_ordinal()` 或 `next_batch()` 发现当前 page 不含目标 rowid |
| dictionary page | `FileColumnIterator::_read_dict_data()` | options 设为 `INDEX_PAGE`，实际 footer 是 dict page | `ColumnMetaPB.dict_page` | 字典值 page | 首次读 dict-encoded data page 且 decoder 需要 dictionary，或 dict filtering |
| page zone map root/index page | `ZoneMapIndexReader::load()` -> `IndexedColumnReader::load()` | `INDEX_PAGE` | `ZoneMapIndexPB.page_zone_maps` indexed-column meta | zone-map indexed-column root/index page | zone map predicate 触发 |
| page zone map data page | `ZoneMapIndexReader::_load()` -> `IndexedColumnIterator::seek_to_ordinal(i)` | `DATA_PAGE` | zone-map indexed-column ordinal 定位 | 一个或多个 serialized `ZoneMapPB` | root/index 已加载；遍历每个 page zone map，同页 ordinal 复用 |
| bloom filter root/index page | `BloomFilterIndexReader::load()` -> `IndexedColumnReader::load()` | `INDEX_PAGE` | `BloomFilterIndexPB.bloom_filter` indexed-column meta | bloom indexed-column root/index page | bloom predicate 触发 |
| bloom filter data page | `BloomFilterIndexIterator::read_bloom_filter(page_id)` | `DATA_PAGE` | bloom indexed-column ordinal 定位 | 某个 page id 的 serialized bloom filter | bloom row range 计算得到 page ids |
| variant external-meta key root/index page | `VariantExternalMetaReader` lookup/load_all -> indexed-column reader load | `INDEX_PAGE` | `footer.file_meta_datas["variant_meta_keys.<root_uid>"]` | variant path key indexed-column root/index page | 查询访问外置 variant subcolumn meta |
| variant external-meta key data page | `VariantExternalMetaReader::lookup_meta_by_path()` / `load_all()` | `DATA_PAGE` | key indexed-column ordinal/value index 定位 | relative path string value | 用于 path -> col id，再走 DAT-05 读 ColumnMetaPB |
| array/map/struct null/offset/child page | `ArrayFileColumnIterator` / `MapFileColumnIterator` / `StructFileColumnIterator` | `DATA_PAGE` 或子列 index page | 子列自身 `ColumnMetaPB` / ordinal index | null map、offsets、items、key、value、struct child 等物理子列 page | 复杂类型一次逻辑列读会展开成多个子 iterator |
| variant root/binary/sparse subcolumn page | `VariantRootColumnIterator` / `HierarchicalDataIterator` / sparse column cache | `DATA_PAGE` 或子列 index page | root binary column、path column 或 sparse subcolumn meta | root binary value、extracted path value、sparse column value | 依赖 variant path 选择、外置 meta 和 sparse cache 状态 |

## 普通 scan 阶段和 `read_at` 顺序

普通 scan 的 `.dat` 冷路径可以拆成以下阶段：

1. `Segment::open()`：DAT-01、DAT-02。footer 已在 `StoragePageCache` 时跳过。
2. `ColumnMetaAccessor::init()`：V2 footer inline meta 不读；V3 触发 DAT-04
   或 DAT-05。
3. `Segment::new_iterator()`：需要 key index 时触发 short-key 或 PK index 的
   DAT-06。
4. `_lazy_init()` key range：short-key 已在上一步加载；PK 或 key seek 若需要访问
   indexed-column data page，会继续触发 DAT-06。
5. `_get_row_ranges_by_column_conditions()`：
   - 倒排索引先读 `.idx` 文件，并只产出 row bitmap。
   - dict predicate 可能触发 dictionary page DAT-06。
   - bloom predicate 先触发 ordinary column ordinal index，再读 bloom root/index
     page 和 bloom data page。
   - zone map predicate 触发 zone-map root/index page 和 zone-map data page。
6. `_read_columns_by_index()` first read：
   - 连续 rowids：`seek_to_ordinal(first)` 后 `next_batch(n)`，跨 page 时继续读
     data page。
   - 稀疏 rowids：`read_by_rowids()` 对每个新 page 触发一次 data page read，同
     page rowids 只复用内存 page。
7. 谓词、short-circuit predicate、common expr 过滤：不触发文件 `read_at`。
8. `_read_columns_by_rowids()` second read：非谓词列按过滤后的 selected rowids
   重复第 6 步。
9. `SegmentPrefetcher`：`FileColumnIterator::seek_to_ordinal()` 可能触发
   `CachedRemoteFileReader::prefetch_range()`。它不是新的文件格式读点，但会进入
   file cache 的 dry-run block planning；是否产生远端读取决于 cache block 状态。

## 普通 scan 列读取 I/O 模式矩阵

| I/O 类型 | 触发点 | 读什么 | 依赖 | 典型粒度 |
| --- | --- | --- | --- | --- |
| Footer tail | `Segment::_parse_footer()` | 最后 12 字节 | 文件 size | 12 bytes |
| Footer PB | `Segment::_parse_footer()` | serialized `SegmentFooterPB` | footer length | footer length |
| External column meta | `ColumnMetaAccessorV3` / `ExternalColMetaUtil` | whole region 或单列 meta slice | footer V3 pointers | region size 或 one `ColumnMetaPB` |
| Short key index | `Segment::load_index()` | short-key page | footer `_sk_index_page` | one page |
| Primary key index | `Segment::load_index()` / point lookup | PK indexed-column pages | PK meta | root/index/data pages |
| Ordinal index | `seek_to_ordinal()` / zone map / bloom / prefetch planning | ordinal -> `PagePointer` map | column meta | index page 或 root data page |
| Page zone map | zone map filtering | per-page min/max/null info | column predicate | indexed-column pages |
| Bloom filter | bloom filtering | per-page bloom filter | bloom-capable predicate | indexed-column pages |
| Dict page | dict encoding / dict filtering | dictionary values | dict-encoded column | one dict page |
| Predicate data page | first read | predicate/common expr column values | row bitmap after pruning | data page, then cache blocks |
| Return data page | second read | non-predicate output column values | selected rowids after predicate | data page, then cache blocks |
| Complex child page | array/map/struct/variant iterator | offsets/null/child/key/value/path data | logical type layout | multiple physical pages |

## `seek_to_ordinal()` 和 `read_by_rowids()`

标量列的 `FileColumnIterator::seek_to_ordinal(ord)` 是 rowid 到 page 的转换点：

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

`next_batch(n)` 适合连续 rowid，在当前 page 内顺序解码，跨 page 时读下一
data page。`read_by_rowids(rowids,count)` 适合稀疏 rowids：每次先 seek 到当前
rowid 所在 page，然后一次读取这个 page 内能覆盖的多个 rowids。rowids 如果很稀疏，
page read 会接近“每个 rowid 一个 page”；如果密集，则多个 rowids 共享一次 page
read。

复杂类型复用同一机制，但一次逻辑列读取会展开成多个物理子列：

- `ARRAY`：offsets、element row range、可选 null map。
- `MAP`：offsets、key、value、可选 null map。
- `STRUCT`：递归读取子列。
- `VARIANT`：root binary、path subcolumn、sparse column cache、外置 meta key。

## TopN 全局延迟物化二阶段

TopN 全局延迟物化不是普通 `SegmentIterator` 的 first/second read。第一阶段 scan
只输出 eager columns 和 `__DORIS_GLOBAL_ROWID_COL__...`，TopN 完成排序和 limit 后，
`MaterializationOperator` 再按 rowid 发起二阶段读取。

FE rewrite 的关键结构：

```text
PhysicalTopN
  -> LazyMaterializeTopN
       -> add synthetic __DORIS_GLOBAL_ROWID_COL__
       -> LazySlotPruning keeps eager columns
       -> wrap TopN with PhysicalLazyMaterialize
       -> MaterializationNode.toThrift()
```

第一阶段 scan 生成的 rowid 是 `GlobalRowLoacationV2`：

```text
GlobalRowLoacationV2 {
  version,
  backend_id,
  file_id,
  row_id
}
```

二阶段服务端路径：

```text
MaterializationOperator
  -> group rowids by backend_id and file_id
  -> PInternalService::multiget_data_v2()
  -> RowIdStorageReader::read_by_rowids(PMultiGetRequestV2)
  -> read_batch_doris_format_row()
       -> group by SegKey(tablet_id,rowset_id,segment_id)
       -> sort row_ids ascending
       -> deduplicate equal row_ids
       -> read_doris_format_row()
       -> Segment::seek_and_read_by_rowid()
       -> FileColumnIterator::read_by_rowids()
```

二阶段复用上面的 DAT 清单：

- segment 首次加载仍会触发 DAT-01/DAT-02 和 V3 meta DAT-04/DAT-05。
- 每个 lazy column 首次 `seek_to_ordinal()` 可能读 ordinal index DAT-06。
- 每个 lazy rowid 所在的新 data page 触发 ordinary column data page DAT-06。
- dict-encoded lazy column 可能额外触发 dictionary page DAT-06。
- complex/variant lazy column 仍会展开成子列 DAT-06。

它和普通 scan 的差异是：rowids 在进入 storage 前已经按 segment 排序去重，所以同
一列内 `read_by_rowids()` 单调前进；dense winners 会复用 page，sparse winners 会
接近 page-random。当前 `SegmentPrefetcher` 的入口在
`SegmentIterator::_init_segment_prefetchers()`，二阶段直接调用
`Segment::seek_and_read_by_rowid()`，因此不会复用普通 scan 的触发式预取。

## file cache 层的二次 I/O

云模式下 `.dat` 的 file reader 通常是 `CachedRemoteFileReader`。一次上层逻辑
`read_at(offset,size)` 进入它后，会被映射成 block cache 操作：

```text
CachedRemoteFileReader::read_at_impl(offset, size)
  -> align to file-cache block range
  -> BlockFileCache::get_or_set(aligned_offset, aligned_size)
  -> DOWNLOADED: FileBlock::read(...)
  -> EMPTY / SKIP_CACHE: _execute_remote_read(empty_start, empty_size)
  -> DOWNLOADING timeout or missing cache file: remote_file_reader->read_at(current_offset, read_size)
```

因此真实底层 I/O 有三类：

| file cache 状态 | 底层读 | offset/size |
| --- | --- | --- |
| local cache hit | `FileBlock::read()` -> local cache file `read_at(value_offset,size)` | caller request 在 cache block 内的 slice |
| cold miss / skip cache | remote reader `read_at(empty_start, empty_size)` | 按 file-cache block 对齐后的连续范围 |
| downloader timeout / cache file missing | fallback remote reader `read_at(current_offset, read_size)` | 当前未满足的 request 子区间 |

`prefetch_range()` 设置 `IOContext.is_dryrun=true`，复用同一套 block planning。它的
目标是提前填充 file cache，不返回 completion handle，也不向 scan 层暴露状态。

## 对 prefetch 接口的含义

`.dat` 优化不能只按“列 + rowids”设计接口。至少需要覆盖：

1. footer/meta：DAT-01、DAT-02、V3 DAT-04/DAT-05。
2. indexed-column：short-key、PK、ordinal、zone map、bloom、variant meta key。
3. data page：普通 scan 的 `_row_bitmap`，以及 TopN 二阶段的 sorted unique rowids。
4. complex/variant 子列：一次逻辑列读取会展开为多组物理 page。
5. file-cache block：上层 page offset/size 会被 block 对齐放大。
6. policy：是否写 file cache、是否只进内存 buffer、是否 remote-only、是否允许
   peer cache、是否需要 completion 语义。

TopN 二阶段天然已经拿到每个 segment 的 sorted unique rowids，更适合在
`read_doris_format_row()` 或 `Segment::seek_and_read_by_rowid()` 前一次性规划所有
lazy columns 的 page/cache-block ranges，而不是依赖普通 scan 的逐
`seek_to_ordinal()` 触发式预取。
