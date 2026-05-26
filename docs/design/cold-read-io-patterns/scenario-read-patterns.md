# Doris 存算分离冷读场景化 I/O Pattern

本文从执行场景出发描述 Doris 内表在存算分离下的 cold-read I/O pattern。`.dat` 和 `.idx` 的物理格式、`read_at` 清单、file cache block 放大规则分别见 `dat-file-io-patterns.md` 和 `inverted-index-io-patterns.md`；本文只在场景中引用这些底层读点。

## 场景总览

面向内表 cold read 的主要场景可以归为这些类：

| 场景 | 主要入口 | 宏观读模式 | 微观热点 |
| --- | --- | --- | --- |
| 普通 scan / SegmentIterator lazy materialization | `OlapScanner` -> `TabletReader` -> `BetaRowsetReader` -> `SegmentIterator` | rowset/segment 级并发扫描，先构造 row bitmap，再按列读取 | footer、short-key/PK、ordinal、zone map、bloom、data page、file cache block |
| 索引辅助 scan | `SegmentIterator::_apply_inverted_index()` / `_apply_index_expr()` / `_apply_ann_topn_predicate()` | `.idx` 或 `.dat` 内部索引先产生 row bitmap，再读 surviving rows | CLucene/BKD/ANN sub-file、null bitmap、page zone map、bloom filter、COUNT_ON_INDEX |
| 存储层聚合 / 元数据读 | `PhysicalStorageLayerAggregate` -> `TPushAggOp` -> `VStatisticsIterator` 或 `SegmentIterator` | 不读或少读 ordinary data page，用 segment/page metadata 生成上层聚合输入 | segment zone map、footer/meta、short-key/PK、COUNT_ON_INDEX 的 index bitmap |
| LIMIT / key TopN 下推 | `OlapScanner::_init_tablet_reader_params()` -> `VCollectIterator::_topn_next()` / `SegmentIterator::read_limit` | storage 内提前停止，key TopN 每个 rowset 取本地候选 | 首尾 data page、reverse rowid iteration、runtime TopN predicate |
| TopN 全局延迟物化 | FE `LazyMaterializeTopN` -> BE `MaterializationOperator` -> `RowIdStorageReader` | 第一阶段只读 eager 列和全局 rowid；第二阶段按 rowid 回读 lazy 列 | rowid group/sort/dedup、row-store single-row hidden column lookup、column-store `read_by_rowids()` |
| 点查 / short-circuit point query | FE `LogicalResultSinkToShortCircuitPointQuery` -> BE `PointQueryExecutor` | primary-key index 定位一行，优先 row cache / row store，必要时补列存列 | row cache、segment key bounds、PK bloom、PK indexed-column、row store data page |
| 维护类读者 | compaction / schema change / checksum / index build / delete bitmap calculation | 通常按 rowset/segment 全量读，reader type 决定是否 merge、是否 disposable cache | 全量 data page 读、delete bitmap、PK index scan、row store/column-store conflict read |
| File cache warm-up / 缓存预热 | `WARM UP SELECT` / `CloudWarmUpManager` / `FileCacheBlockDownloader` | query-shaped warm-up 复用 scan；block/file-shaped warm-up 按指定 `.dat`/`.idx` range 填充 file cache | file-cache block、`.dat` / `.idx` extent、`is_warmup` reader context |

这些场景共享同一批底层构件：

- `SegmentLoader` 和 `SegmentCache`：决定是否已经有 segment footer、column meta、PK meta 等内存对象。
- `StoragePageCache`：缓存 `PageIO` 解析后的 `.dat` page。
- `InvertedIndexSearcherCache`、`InvertedIndexQueryCache`、ANN IVF list cache：缓存 `.idx` reader/searcher/query result。
- `RowCache`：缓存点查 row-store 结果。
- `CachedRemoteFileReader`：最终把逻辑 `read_at(offset,size)` 转为本地 cache file 读、远端对象存储读、peer cache 读或 fallback remote 读，并按 file-cache block 对齐。
- `FileCacheBlockDownloader`：按 warm-up 任务给出的文件 offset/size 读取 `.dat` 或 `.idx` 区间，并把对应 file-cache blocks 填充到本地 cache。

## 文件 I/O 分析维度

本文后续每个场景都按同一组维度描述实际文件读取：

- 读对象：`.dat` footer、V3 column meta、`.dat` page、`.idx` compound header、`.idx` sub-file、file-cache block。
- 顺序性：连续 row range、同一列内 page 顺序推进、rowid 稀疏随机读、index tree / posting / IVF list 随机读、跨 rowset/segment 交错读。
- 依赖关系：footer -> column meta -> index/page pointer -> data page；`.idx` bitmap -> `.dat` data page；TopN winner rowid -> 二阶段 data page；PK index -> row-store / column-store row data。
- 读大小：上层逻辑 `read_at` 大小、PageIO page 大小、CLucene buffer 大小、file cache block 对齐后的远端读取大小。
- 可并发性：不同 scanner、tablet、rowset、segment 可以并发；同一个 `SegmentIterator` 内部仍受 row bitmap、predicate、common expr、输出列读取阶段约束。
- 放大来源：小的逻辑 page 或 index read 进入 file cache 后可能变成 block 对齐读取；row store 可减少列数但仍读取整页；复杂类型和 variant 会把一个逻辑列展开为多个物理子列。

### 基础读粒度

| 读点 | 顺序性 | 依赖 | 上层逻辑读大小 | 远端/cache 读大小 |
| --- | --- | --- | --- | --- |
| `.dat` footer tail | 文件尾部随机读 | 只依赖 file size | 固定 12 bytes | cold miss 时按 file-cache block 对齐 |
| `.dat` footer PB | 文件尾部随机读 | footer tail 解出 `footer_length` | `footer_length` | cold miss 时按 file-cache block 对齐，可能和 tail 落在同一 cache block |
| V3 external column meta whole region | 连续区间读 | footer 中的 region start 和各 meta length | region size | 按 file-cache block 对齐 |
| V3 single column meta | 小随机读 | footer 中的 prefix-sum offset | one `ColumnMetaPB` slice | 按 file-cache block 对齐 |
| `.dat` PageIO page | page 随机或顺序读 | `PagePointer{offset,size}` | `PagePointer.size`，包含 compressed body、page footer、footer size、checksum | cold miss 通常至少读覆盖该 page 的 cache block；默认 block 是 `file_cache_each_block_size=1MB` |
| `.idx` header/directory | buffered 顺序读 | 打开 index file | CLucene buffer refill，默认 `inverted_index_read_buffer_size=4096` | 按 file-cache block 对齐 |
| `.idx` sub-file | term/posting/tree/list 驱动，可能随机 | compound directory 给出 sub-file offset/length | CLucene/FAISS reader 本次请求长度 | `CSIndexInput` 映射到 compound file offset 后按 file-cache block 对齐 |
| warm-up file/range read | 任务给定的 `.dat` / `.idx` 文件区间 | 通常按 offset 递增分块读取 | 依赖 warm-up 任务中的 path、offset、size、cache type | 上层按 `s3_write_buffer_size` 分片读，底层仍按 file-cache block 处理 |
| local file cache hit | 本地 cache file slice | cache block 已下载 | caller request slice | 本地 `FileBlock::read()`，不访问远端 |

`.dat` page 的 `PagePointer.size` 是压缩后 page 在文件中的物理大小，不是解压后的列值大小。page 读完后 `PageIO` 才解析 page footer、校验 checksum、必要时解压。`StoragePageCache` 命中会跳过底层 file `read_at`，但不会改变逻辑上的 page 依赖关系。

file cache 层会把上层 `read_at(offset,size)` 映射到 block range。小 footer、index page、data page 在逻辑上可能只有几十字节到几十 KB，但 cold miss 下远端读取常由 cache block 决定；多个落在同一 block 的逻辑读可以复用同一次下载结果。

### 内部索引和 Data Page 组织

很多场景的 I/O 差异来自同一个 `.dat` 内部结构在不同触发点被读取。下面的表描述这些结构的 page 组织、首次读取方式和大小估算。这里的“page 数”指 Doris 逻辑 page；进入 file cache 后仍会被映射到 cache block。

| 结构 | 写入组织 | 首次读取行为 | page 个数和大小估算 |
| --- | --- | --- | --- |
| short-key index | 每个 segment 一个 `SHORT_KEY_PAGE`，body 是所有采样 short key 的连续 key bytes 加 varint offset bytes；采样 key 只编码 `num_short_key_columns` 个短键列；footer 记录 `num_items`、`key_bytes`、`offset_bytes`、`num_rows_per_block`、`num_segment_rows`；当前不压缩 | `Segment::load_index()` 在非 MoW PK 路径下一次 `PageIO::read_and_decompress_page()` 读整个 short-key page，然后 `ShortKeyIndexDecoder::parse()` 一次性解析到内存；`_load_index_once` 保证同一 `Segment` 只加载一次 | 逻辑 page 数固定为 1；采样项约为 `ceil(segment_rows / num_rows_per_block)`，默认 `num_rows_per_block=1024`；body 大小约为 `sum(encoded short-key for sampled rows) + sum(varint32 key offsets)`，内存约为 `key_bytes + (num_items + 1) * 4B + decoder/footer overhead` |
| PK primary-key index | 一个 `IndexedColumn`，每行一个 encoded VARCHAR key；key 来自完整主键，可能附带 sequence 标记和值，cluster-key 表还会附带 encoded rowid；写 ordinal index 和 value index；data page 使用 `primary_key_data_page_size`，默认 32KB，压缩为 ZSTD；多 data page 时 ordinal/value root 都是包含 `(first key or first ordinal, PagePointer)` 的 index page | `Segment::load_index()` 对 UNIQUE_KEYS + PK meta 只调用 `PrimaryKeyIndexReader::parse_index()`；它先构造 `IndexedColumnReader`，再串行加载 ordinal root/index、value root/index；不会一次性读取所有 PK data page | PK value 数等于 segment rows；如果 PK 值只落在一个 data page，root 指向 data page，`load_index()` 不额外读 index page；如果有多个 data page，`load_index()` 通常串行读 2 个 root/index page（ordinal + value）；PK data page 数约为 `ceil(total_encoded_pk_bytes / 32KB)` 的压缩后结果，真实以 `PagePointer.size` 为准；每个 root/index page 大小约为 `data_page_count * (encoded boundary key or ordinal + PagePointer encoding + length prefix)` |
| PK bloom filter | 一个单值 bloom-filter indexed-column；builder 把 segment 内所有 PK key 写进一个 bloom filter，目标 fpp 为 0.01，再作为 VARCHAR value 写入 indexed-column | `load_index()` 本身不读 PK bloom；点查、`read_key_by_rowid()` 等调用 `load_pk_index_and_bf()` 时，先确保 PK index 已加载，再加载 bloom indexed-column 并读取 ordinal 0 的 bloom value | bloom value 数固定为 1；logical read 包括 bloom indexed-column root/data page 和 bloom value 所在 data page；大小由 segment key 数和 fpp 决定，读入后整段 bloom 留在内存，后续 key miss 可在读 PK data page 前返回 |
| ordinary data page | 每个物理列按 page builder 累积到 `data_page_size` 后 flush；默认 `STORAGE_PAGE_SIZE_DEFAULT_VALUE=64KB` 是未压缩 page body 目标，实际写入 `PagePointer.size` 是压缩体、page footer、footer size 和 checksum | `FileColumnIterator::seek_to_ordinal()` 或 `read_by_rowids()` 先经 ordinal index 找到 `PagePointer`，然后一次 `PageIO` 读整页并解压；即使只需要一行也读整页 | page 数约为 `ceil(encoded_column_bytes / 64KB)`，受类型、encoding、nullmap、compression 和 row-size 影响；稀疏 rowid 的成本由触碰的 distinct data pages 决定，dense rowid 能共享当前 data page |
| ordinary ordinal index | 每个 data page 一个 entry，entry key 是 first ordinal，value 是 data page `PagePointer`；只有一个 data page 时 root 直接指向 data page，多 data page 时写一个 `INDEX_PAGE`；当前 ordinary ordinal index page 不压缩 | 首次 seek 或按 page id 计算 row ranges 时加载；如果 root 是 data page，只把 data page pointer 放进内存，不读 index page；否则一次读整个 ordinal index page 并解析所有 page entry | 每列 0 或 1 个 ordinal index page；entry 数等于 ordinary data page 数；index page body 约为 `data_page_count * (encoded ordinal + PagePointer encoding + length prefix)` |
| page zone map | 每个 ordinary data page 生成一个 serialized `ZoneMapPB` value，同时维护 segment zone map；page zone maps 存在一个 ordinal-only indexed-column 中，默认 indexed-column data page 目标 1MB，不压缩 | segment zone map 在 footer/column meta 中，整段过滤不读 page-zone-map page；page 级过滤第一次使用时，`ZoneMapIndexReader::load()` 加载 indexed-column root/index，然后按 ordinal 读取并缓存所有 page zone map values | zone-map value 数等于 ordinary data page 数；zone-map indexed-column data page 数约为 `ceil(total_serialized_zone_map_bytes / 1MB)`；首次 page 级过滤是全量加载该列所有 page zone maps，后续在内存中判断 |
| ordinary bloom filter | 每个 ordinary data page flush 一个 bloom filter value，存入 ordinal-only VARCHAR indexed-column，默认 indexed-column data page 目标 1MB | bloom 过滤先加载 ordinary ordinal index 以把当前 row ranges 转为 page ids，再加载 bloom indexed-column root/index；随后只读取候选 page ids 对应的 bloom value，page ids 稠密时可复用 bloom data page，离散时是 indexed-column data page 随机读 | bloom value 数等于 ordinary data page 数；bloom indexed-column data page 数取决于每个 bloom value 的序列化大小和 1MB 目标；低选择性或大 range 会读取很多 bloom values，高选择性的小 range 只读少量 value |
| dictionary page | dict encoding 列可有一个 `DICTIONARY_PAGE`，column meta 中保存 `dict_page` pointer | 首次需要解码该 dict-encoded 列时读取 dictionary page，然后再读 data page；dict page 与 data page 是独立 `PagePointer` | 每个 dict-encoded 列通常一个 dict page；大小是压缩后的 dictionary body 加 page footer/checksum；如果数据页很多，dict page 读是固定前置开销 |
| row-store hidden column | row store 是隐藏 string column，物理上仍按 ordinary scalar column 写 data page、ordinal index 和可选 cache | 点查和 TopN 二阶段 row-store path 用 rowid 读 hidden column；逻辑上一行或少量行，物理上仍是 hidden column data page | page 大小和普通 string column 相同，默认 64KB 未压缩目标；row store 把多列读取压到一个隐藏列，但不能避免整 data page / cache block 放大 |
| `.idx` compound sub-file | 倒排和 ANN 索引由 compound header/directory 管理多个 sub-file；Doris 侧 `CSIndexInput` 把 sub-file offset 映射回 index file offset | searcher/query cache miss 后，第三方 reader 触发 header、directory、term/posting/BKD/FAISS/IVF list 读取；CLucene buffer refill 默认 4KB | Doris 侧能确定的最小 buffer 是 `inverted_index_read_buffer_size=4096`；真实 sub-file read 次数和大小由 term 数、posting、BKD leaf、ANN probe list、IVF list 长度决定，进入 file cache 后按 block 对齐 |

这些结构的读依赖可以归纳为几条固定链路：

| 链路 | 实际含义 |
| --- | --- |
| `footer -> index meta -> short-key page` | 非 PK key range pruning 需要先有 footer 中的 short-key page pointer，再一次读完整 short-key page |
| `footer -> PK meta -> PK root/index page -> PK data page` | `load_index()` 到 root/index page 为止；点查或 rowid/key seek 才读取某个 PK data page |
| `column meta -> ordinal index -> ordinary data page` | 所有列值读取都先靠 ordinal index 定位 data page，root-data-page 场景可省掉 index page 读 |
| `column meta -> page-zone-map indexed-column -> all page zone map values -> row ranges` | page zone map 第一次使用是列级全量加载，随后只在内存中做 page 过滤 |
| `row ranges -> ordinary ordinal index -> bloom indexed-column -> selected bloom values -> row ranges` | bloom filter 的 value 读取依赖当前 row ranges 覆盖到哪些 data page |
| `PK bloom miss -> stop` / `PK bloom hit -> PK value seek -> row data` | 点查中 bloom miss 可避免 PK data page 读，hit 之后仍需 PK indexed-column seek 确认 key |

因此，“加载索引”不是一个统一动作：short-key 是单 page 全量读；PK `load_index()` 是 root/index page 读，不全量读 PK data pages；ordinary ordinal index 是每列 0/1 个 index page；page zone map 是首次全量读该列所有 page-zone-map values；ordinary bloom filter 是按候选 page ids 读 bloom values。估算冷读成本时，需要把这些读点拆开再叠加 file-cache block 对齐。

## 普通 Scan / SegmentIterator Lazy Materialization

### 宏观路径

普通 OLAP scan 的主体路径是：

```text
OlapScanOperator / OlapScanner
  -> TabletReader
  -> VCollectIterator
  -> BetaRowsetReader
  -> LazyInitSegmentIterator
  -> Segment::new_iterator()
  -> SegmentIterator::next_batch()
```

`TabletReader` 先决定是否需要跨 rowset merge：

- `DUP_KEYS`、`UNIQUE_KEYS` merge-on-write、direct mode 或 pre-aggregation 可以走 union / unordered read，不需要 storage 层按 key merge。
- `AGG_KEYS` 或 merge-on-read 等需要 merge 的情况，`VCollectIterator` 会用 heap 在多个 rowset/segment child 之间按 key 合并。I/O 仍由每个 child 的 `SegmentIterator` 触发，但宏观上会在多个 rowset 间交错读取。

`LazyInitSegmentIterator` 使 segment 真正用到时才加载，避免宽表、被过滤 segment 过早创建 column reader。segment 打开会解析 footer；`Segment::new_iterator()` 会先用 segment-level zone map 尝试整段过滤，再加载 short-key 或 primary-key index。这里的加载对 short-key 是一次读完整 single page；对 PK 是读 primary-key indexed-column 的 root/index page，不读全量 PK data pages。之后才创建 `SegmentIterator` 或 `VStatisticsIterator`。

`SegmentIterator::_lazy_init()` 的核心是构造 `_row_bitmap`：

```text
all segment rows
  -> key range pruning
  -> inverted/common-expr index pruning
  -> dict / bloom / page-zone-map pruning
  -> delete bitmap pruning
  -> caller-provided segment row ranges
  -> ANN TopN pruning
  -> BitmapRangeIterator / BackwardBitmapRangeIterator
```

只有 `_row_bitmap` 确定以后，后续列读取才知道应读连续 row ranges 还是稀疏 rowids。

### 谓词列先读，再读非谓词列

`SegmentIterator::_vec_init_lazy_materialization()` 把列拆成几组：

- `_predicate_column_ids`：需要先读来执行 column predicate、delete condition 或 common expr 的列。
- `_common_expr_column_ids`：没有在第一组中，但 common expr 还需要读取的列。
- `_non_predicate_columns`：只在过滤后输出的普通返回列。

`_next_batch_internal()` 的读顺序是：

```text
_read_columns_by_index(predicate/common-first columns)
  -> evaluate vectorized predicates
  -> evaluate short-circuit predicates and delete conditions
  -> read common-expr-only columns by selected rowids if needed
  -> evaluate common expr
  -> apply SegmentIterator read_limit if any
  -> _read_columns_by_rowids(non-predicate columns)
  -> output columns and materialize virtual columns
```

如果没有 predicate、delete condition、common expr，lazy materialization 不开启，`_predicate_column_ids` 会包含所有 schema columns，于是只有一次 `_read_columns_by_index()`。

如果有 predicate 且还有非谓词输出列，就会出现典型二段读：

```text
first read:
  predicate columns over row bitmap
  continuous rowids -> seek_to_ordinal(first) + next_batch(n)
  sparse rowids     -> chunked read_by_rowids()

filter:
  vectorized predicate / short-circuit predicate / delete condition / common expr

second read:
  non-predicate output columns by surviving rowids
```

如果 common expr 引用的列既不是谓词列也不是输出列，实际可能形成三组物理读取：谓词列 -> common-expr-only 列 -> 非谓词输出列。

### 微观读模式

`_read_columns_by_index()` 先从 `_range_iter` 拿一批 rowids：

- 连续 rowids：每列一次 `seek_to_ordinal(first)`，再 `next_batch(n)` 顺序解码；跨 page 时读下一 data page。
- 非连续 rowids：按 range iterator batch 切小批；小批连续则仍走 `seek_to_ordinal + next_batch`，否则走 `ColumnIterator::read_by_rowids()`。

`FileColumnIterator::seek_to_ordinal()` 是 rowid 到 page 的转换点：

```text
seek_to_ordinal(rowid)
  -> current page contains rowid: only move decoder position
  -> else ColumnReader::seek_at_or_before(rowid)
       -> load ordinal index if needed
       -> locate PagePointer
       -> _read_data_page()
       -> PageIO::read_and_decompress_page()
```

`FileColumnIterator::read_by_rowids()` 要求 rowids 单调。它会按 page 聚合能覆盖的 rowids：dense rowids 多个值共享一个 data page；sparse rowids 接近“一个 rowid 触发一个 page”。Nullable 列还会按 null run 跳过或读取 nested data。

复杂类型会把一次逻辑列读放大成多个物理子列读：

- `ARRAY` / `MAP`：offsets 先读，再根据 offsets 读 element、key、value。
- `STRUCT`：递归读每个 child。
- `VARIANT`：可能先读外置 path meta，再读 root binary、extracted subcolumn 或 sparse subcolumn cache。

### 文件 I/O Pattern

普通 scan 的实际文件读取可以按阶段拆开：

| 阶段 | 读文件内容 | 顺序性 | 依赖关系 | I/O 大小特征 |
| --- | --- | --- | --- | --- |
| segment open | `.dat` footer tail、footer PB、可能的 V3 column meta | 文件尾部随机读；V3 whole-region 是连续读，single meta 是小随机读 | 后续所有 `PagePointer` 都依赖 footer / column meta | 12 bytes + `footer_length`；V3 是 region size 或单列 meta slice |
| key range prune | short-key page 或 PK indexed-column root/index page | short-key 是单 page 随机读；PK root/index 是 0 或 2 个 index page 串行读 | 依赖 footer 中的 key index meta；决定初始 row ranges | short-key 固定 one page；PK `load_index()` 不读全量 data pages，后续 key seek 才读具体 PK data page |
| index / predicate prune | page zone map、bloom、dict、倒排索引 bitmap | page zone map 首次全量读该列 zone-map values；bloom 按候选 page ids 读；`.idx` 由 query 决定 | 必须先得到 `_row_bitmap`，才知道后续 data page 范围 | indexed-column root/index/data page、dict page、`.idx` buffer / sub-file read |
| predicate/common-first data read | 谓词列、delete condition 列、common expr 先读列 data page | rowids dense 时按 page 顺序推进；rowids sparse 时按 page 随机 | 依赖 `_row_bitmap`；后续过滤依赖这些列值 | 每次 PageIO 读 `PagePointer.size`；cold miss 按 cache block 放大 |
| non-predicate data read | 输出列 data page | 只访问 surviving rowids；通常比 first read 更稀疏 | 硬依赖 predicate / common expr 结果 | 每个输出列独立按 rowids 读 page；复杂类型展开成子列 page |

这个场景中，I/O 依赖链是：

```text
footer/meta
  -> key/index metadata
  -> row bitmap
  -> predicate/common-first data pages
  -> predicate/common expr result
  -> non-predicate output data pages
```

同一个 `SegmentIterator` 内，second read 不能越过 first read 和过滤结果提前确定。但不同 segment、rowset、tablet scanner 之间没有这个局部依赖，可以在 scan 调度层并发。

顺序性由 `_row_bitmap` 决定：

- 全 segment 或大连续 range：同一列内通常是 page 顺序读，page 间 offset 大体递增。
- page zone map / bloom / `.idx` 高选择性裁剪后：surviving rowids 可能离散，输出列更像 page-random。
- merge read：每个 child 内仍按自己的 page 访问，但 `VCollectIterator` 在多个 child 之间取当前候选行，宏观上会形成跨 rowset/segment 的交错 I/O。
- union / direct read：child 独立输出，宏观上更接近多个 segment scan 并行推进。

I/O 大小上的关键点是：逻辑 page read 以 `PagePointer.size` 为单位；如果一个 data page 只贡献少量 surviving rows，仍要读取并解压整个 page。进入 file cache 后，远端读取再按 cache block 对齐，所以稀疏 rowids 的真实成本经常由“触碰了多少 data page / cache block” 决定，而不是由返回行数决定。

## 索引辅助 Scan

索引辅助 scan 仍然落在 `SegmentIterator` 内，但宏观读法不同：先读索引得到 row bitmap，再决定 data page。

### `.dat` 内部索引

这些索引存放在 `.dat` 中，经 `PageIO` 读取：

- segment-level zone map：在 `Segment::new_iterator()` 直接用 footer/column meta 中的 segment zone map 判断整段是否可跳过，不读 page-zone-map data page。
- page zone map：`ColumnReader::_get_filtered_pages()` 先 load zone-map indexed-column，读取并缓存所有 page zone maps，然后把 page ids 转成 row ranges。
- bloom filter：先 load ordinal index 和 bloom indexed-column，再按当前 row ranges 覆盖到的 page ids 逐个读取 bloom filter。
- dictionary：`get_row_ranges_by_dict()` 或首次读 dict-encoded data page 时读取 dict page。
- short-key / PK index：用于 key range、点查或 unique-key lookup。

这些读通常发生在 data page 前面。选择性好时，它们用较少 index I/O 换取大量 data page 跳过；选择性差时，会表现为“先读一批 index page，再仍然读大部分 data page”。

### `.idx` 倒排索引和 ANN

`SegmentIterator::_init_index_iterators()` 为有 inverted index 或 ANN index 的列创建 `IndexIterator`，并把 `IOContext`、stats、runtime state 放进 `IndexQueryContext`。

column predicate 路径：

```text
_apply_inverted_index()
  -> ColumnPredicate::evaluate(...)
  -> InvertedIndexIterator::select_best_reader()
  -> Fulltext / String / BKD reader query
  -> update _row_bitmap
```

common expr / search expr 路径：

```text
_apply_index_expr()
  -> VExprContext::evaluate_inverted_index()
  -> IndexExecContext with segment-specific iterators
  -> update expression index result bitmap
```

ANN TopN / range 路径：

```text
_apply_ann_topn_predicate()
  -> AnnIndexIterator::try_load_index()
  -> AnnTopNRuntime::evaluate_vector_ann_search()
  -> update _row_bitmap
```

`.idx` 的微观 I/O 由第三方 reader 决定，但 Doris 侧能看到的边界是：

- searcher/cache miss 时读取 V2/V1 compound header 和 sub-file directory。
- fulltext/string 读 term metadata、term dictionary、posting、position。
- BKD 读 metadata、tree/index、leaf blocks。
- ANN 读 `ann.faiss` metadata；`IVF_ON_DISK` 还会按 list 随机读 `ann.ivfdata`，并受 ANN IVF list cache 影响。
- nullable predicate 可能读 `null_bitmap` sub-file。

索引执行成功后，`_check_all_conditions_passed_inverted_index_for_column()` 可能把 `_need_read_data_indices[cid]` 置为 false。随后 `_need_read_data()` / `_prune_column()` 会跳过该列 data page，直接填默认值或只保留 bitmap 语义。`COUNT_ON_INDEX` 就依赖这条路径：残余 scan conjunct 不允许存在，否则 BE 会拒绝，因为 count 必须在索引过滤后才安全。

如果索引不支持、bypass、missing 或允许 fallback，`_downgrade_without_index()` 会把 predicate 留在普通 scan，后续按 data page 读取再过滤。

### 文件 I/O Pattern

索引辅助 scan 的关键不是单独的 index read，而是 index read 和 data page read 的依赖：

```text
index open / index metadata
  -> query-specific index pages or sub-files
  -> roaring bitmap / row ranges
  -> _row_bitmap intersection
  -> predicate / output data page read
```

`.dat` 内部索引和 `.idx` 的 I/O 形态不同：

| 索引类型 | 读文件内容 | 顺序性 | 依赖关系 | I/O 大小特征 |
| --- | --- | --- | --- | --- |
| segment zone map | footer / column meta 内嵌信息 | 不额外读 page | 只依赖 segment open | 不产生 ordinary page read |
| page zone map | zone-map indexed-column root/index/data page | 第一次使用时按 ordinal 读取并缓存该列全部 page zone maps | 依赖 column meta；输出 page row ranges | value 数等于 ordinary data page 数；indexed-column data page 默认 1MB 目标，读大小以各 `PagePointer.size` 为准 |
| bloom filter | ordinary ordinal index + bloom indexed-column pages | 按当前 row ranges 覆盖的 page ids 读取，可能跳跃 | 先要知道待检查 page ids | value 数等于 ordinary data page 数；只读候选 page ids 对应的 bloom values |
| dictionary | dict page | 单 page 随机读 | 依赖 dict-encoded column meta | one dict page；后续 data page 可减少或照常读取 |
| short-key index | short-key page | 单 page 随机读，读后全量 parse 到内存 | 依赖 footer 中的 short-key page pointer | 固定 one page；item 数约 `ceil(segment_rows / 1024)` |
| PK index | PK indexed-column root/index/data page、PK bloom | root/index 串行加载；key seek 才读 data page；bloom 单值加载后内存判断 | PK bloom 可以先裁剪 miss，hit 后仍要 PK value seek | `load_index()` 是 0 或 2 个 root/index page；point seek 通常再读 1 个 PK data page |
| `.idx` fulltext/string | compound header、term dict、posting、position | header 顺序；term/posting 由 query 驱动，可能随机 | searcher/query cache miss 后才读；输出 bitmap | CLucene buffer 默认 4KB；远端按 cache block 对齐 |
| `.idx` BKD | BKD meta/tree/leaf block | tree-guided random | range/equality query 决定访问 leaf | sub-file block 读；可能被 query cache 消除 |
| `.idx` ANN | `ann.faiss`、`ann.ivfdata` | FAISS metadata 多为顺序；IVF list 是 list-level random | 先 load ANN index，再按 probe list 读 | list length 决定逻辑读大小；远端按 cache block 对齐 |

选择性决定后续 `.dat` I/O：

- 高选择性：索引 I/O 增加，但后续 data page / cache block 数显著减少，输出列读通常变成稀疏 page-random。
- 低选择性：索引会先读一批 metadata / posting / tree，再仍然读取大部分 data page；如果 reader 能 bypass 或 downgrade，就会减少无效 `.idx` I/O。
- 完全由索引判断的列：`_need_read_data_indices[cid] = false` 后，该列 ordinary data page 可以跳过；但其它输出列仍依赖 surviving rowids 继续读取。
- nullable、phrase、ANN 等路径可能多读额外 sub-file，例如 null bitmap、position 或 IVF list，这些读和普通列 data page 之间没有值依赖，但有 bitmap 结果依赖。

因此索引辅助 scan 的宏观顺序通常是“先 index，后 `.dat` data page”。它不是简单并行的两个读流：如果不知道 index bitmap，就不能准确知道后续应该读哪些 data page。

## 存储层聚合 / 元数据读

FE 的 `PhysicalStorageLayerAggregate` 会把无 group-by 的聚合翻译成 `TPushAggOp`：

- `COUNT`
- `COUNT_ON_INDEX`
- `MINMAX`
- `MIX`

在 BE，`Segment::new_iterator()` 对非 `COUNT_ON_INDEX` 的 pushdown aggregate 创建 `VStatisticsIterator`，前提是没有 delete condition。此时宏观上不走 `SegmentIterator` 的 data page scan。需要注意的是，`Segment::new_iterator()` 创建 iterator 前会先 `load_index()`：非 unique-key segment 会一次读取并解析 short-key page；unique-key 且存在 primary-key meta 的 segment 会串行加载 PK indexed-column 的 ordinal/value root/index page，但不会读取全量 PK data pages，也不会加载 PK bloom，除非后续路径显式调用 `load_pk_index_and_bf()`。

### COUNT

`VStatisticsIterator::next_batch()` 对 `COUNT` 只按 segment row count 插入默认值：

```text
target rows = segment->num_rows()
next_batch:
  insert_many_defaults(batch_size)
```

冷读 I/O 基本只剩 segment open、footer、column meta、short-key / PK root/index load 和 segment cache miss 相关读；不读 ordinary data page，也不因 `COUNT` 本身读取 PK data page。

### MINMAX / MIX

`MINMAX` 和 `MIX` 通过 `ColumnIterator::next_batch_of_zone_map()` 读取 segment-level zone map 里的 min/max，生成上层聚合输入。segment zone map 来自 column meta，不需要逐个 data page 解码，也不需要 page-zone-map indexed-column。CHAR 类型会做尾部 padding 处理。和 `COUNT` 一样，它仍会经过 `Segment::new_iterator()` 前置的 short-key / PK index load。

### COUNT_ON_INDEX

`COUNT_ON_INDEX` 不走 `VStatisticsIterator`。它仍创建 `SegmentIterator`，先用倒排索引或 ANN/index expression 得到 row bitmap，然后通过 `_need_read_data()` 跳过已经完全由索引判断的列 data read。宏观模式是“index bitmap count”，不是“footer row count”，因此它可以表达 `COUNT_ON_MATCH` 这类索引条件计数。

### 文件 I/O Pattern

存储层聚合的 I/O pattern 按 pushdown 类型分成三类：

| 聚合类型 | 读文件内容 | 顺序性 | 依赖关系 | I/O 大小特征 |
| --- | --- | --- | --- | --- |
| `COUNT` | segment footer / row count metadata + short-key page 或 PK root/index page | 文件尾部随机 footer 读；short-key 单 page；PK root/index 串行加载 | segment open 和 key index load 后即可产出 | 12 bytes + `footer_length` + one short-key page，或 PK 0/2 个 root/index page；不读 ordinary data page / PK data page |
| `MINMAX` / `MIX` | footer / column meta 中的 segment zone map + short-key page 或 PK root/index page | footer/meta 读；short-key 单 page；PK root/index 串行加载 | 依赖 column meta 和前置 key index load；不依赖 page zone map indexed-column | footer 或 V3 meta slice / region + one short-key page，或 PK 0/2 个 root/index page；不读 ordinary data page |
| `COUNT_ON_INDEX` | `.idx` 或 ANN/index expression 相关 sub-file，必要时 `.dat` internal index | index-driven | index bitmap 完成后才能 count | `.idx` buffer/sub-file read；通常不读 ordinary data page |

`COUNT` 和 `MINMAX` 的读路径是 metadata-first，并且不会读取 ordinary column data page。它们对 cold read 的特点是：扫描行数可以很大，但文件 I/O 与行数没有线性关系，主要取决于 segment 数、footer 是否命中 segment cache、V3 column meta 是否需要额外读取、以及 short-key page 或 PK root/index page 是否已经加载。对 PK segment，这里不能按“PK 总大小”估算 I/O，而应按 root/index page 和后续是否发生 key seek 分开估算。

`COUNT_ON_INDEX` 和普通 metadata 聚合不同。它的硬依赖是：

```text
index metadata / query sub-file
  -> bitmap
  -> bitmap cardinality
```

这个路径不读取普通列值，但可能比 `COUNT` 多很多 `.idx` 随机读。它的远端 I/O 大小由 query 涉及的 index sub-file、CLucene/FAISS buffer、file-cache block 对齐共同决定。

## LIMIT / Key TopN 下推

这类场景和 TopN 全局延迟物化不同：它不做 rowid 二阶段回读，而是在 storage scan 阶段尽早少读。

`OlapScanner::_init_tablet_reader_params()` 只有在满足这些条件时才下推：

- scan 有 limit；
- 没有 runtime filter；
- SegmentIterator 中没有 residual conjunct；
- session 允许 `enable_segment_limit_pushdown`；
- storage no-merge，即 storage 层不需要为了语义做跨 rowset merge。

普通 LIMIT 会设置 `general_read_limit`，由 `VCollectIterator::add_child()` 转为 rowset reader 的 `_topn_limit`，再进入 `SegmentIterator::_opts.read_limit`。`SegmentIterator` 在 predicate/common expr 之后应用 limit；当没有任何 SegmentIterator 侧过滤时，`_can_opt_limit_reads()` 还会直接缩小 first read 的 `nrows_read_limit`。

key TopN 会设置 `read_orderby_key_limit` 和 `read_orderby_key_reverse`。宏观路径是：

```text
VCollectIterator::_topn_next()
  -> for each RowSetSplits:
       rs_reader->set_topn_limit(limit)
       read up to local limit rows
       keep local candidates in sorted multiset
       optionally update runtime TopN predicate
```

如果是 `ORDER BY key DESC LIMIT n`，rowset/segment iterator 顺序会反向，底层 `BackwardBitmapRangeIterator` 从较大的 rowid 开始。微观上它仍使用普通 `SegmentIterator` 的 data page 读取，只是常见情况下只触碰每个 segment 的前几个或后几个 data page。如果还有必须在 SegmentIterator 内执行的 predicate，则为了得到 n 个 survivors 可能读超过 n 行。

runtime TopN filter 是另一个叠加项：上游 TopN 维护当前边界值，scan 侧把它当 predicate 下推。它会进入普通 scan 的 row bitmap 裁剪路径，可能触发 zone map、bloom 或倒排索引过滤，也可能回退到 data page 过滤。

### 文件 I/O Pattern

LIMIT / key TopN 下推改变的是“读到什么时候停止”，不是底层 page 格式：

| 场景 | 读文件内容 | 顺序性 | 依赖关系 | I/O 大小特征 |
| --- | --- | --- | --- | --- |
| 普通 LIMIT，无 SegmentIterator 侧过滤 | key / 输出列前若干 data page | 同一列从起始 rowid 顺序推进 | footer/meta -> first row range -> data page；满足 limit 后停止 | 通常只读每列前几个 page；仍以 `PagePointer.size` 为单位 |
| 普通 LIMIT，有谓词过滤 | 谓词列先读，输出列读 survivors | 谓词列可能要读超过 limit 行 | limit 依赖过滤结果，不能只按 n 行停止 | I/O 大小取决于为得到 n 个 survivors 需要扫描多少 page |
| `ORDER BY key ASC LIMIT n` | 每个 rowset/segment key 顺序前缀 | page 顺序读 | rowset 内本地候选集依赖 key 顺序输出 | 触碰每个参与 segment 的前缀 page |
| `ORDER BY key DESC LIMIT n` | 每个 rowset/segment key 反向前缀 | page 序反向推进；连续时不是随机，只是从尾部开始 | 依赖反向 row iterator | 触碰每个参与 segment 的尾部 page |
| runtime TopN filter | index / data page 过滤路径 | 取决于被转成的 predicate | 上游边界值更新后影响后续 scan | 可能多读 index page，也可能减少 data page |

这类读的依赖链仍在单次 scan 内完成：

```text
footer/meta
  -> optional key/index pruning
  -> ordered row range or row bitmap
  -> data page read
  -> local candidate / limit satisfied
```

与 TopN 全局延迟物化相比，它不会把 winners 的 rowid 再发起第二轮读取。I/O 收益主要来自少读后续 page；如果谓词选择性低或 storage 需要 merge，实际读 page 数会明显高于 `limit` 本身。

## TopN 全局延迟物化

TopN 全局延迟物化是二阶段 rowid fetch，不等同于上一节的 storage LIMIT/key TopN 下推。

### 第一阶段：只读 eager 列和全局 rowid

FE `LazyMaterializeTopN` 在 TopN 周围插入 `PhysicalLazyMaterialize`：

```text
PhysicalTopN
  -> LazySlotPruning keeps eager slots
  -> scan outputs eager slots + __DORIS_GLOBAL_ROWID_COL__
  -> TopN computes winners
  -> MaterializationNode fetches lazy slots by rowid
```

对内表，第一阶段 scan 的底层读仍然是普通 `SegmentIterator` 模式，但 lazy slots 不在 scan 输出中。`__DORIS_GLOBAL_ROWID_COL__` 由 rowid iterator 构造 `GlobalRowLoacationV2{backend_id,file_id,row_id}`，不对应一个普通 data page。

因此第一阶段的 I/O 形态取决于 eager 列：

- order-by 列、predicate 列、join/projection 仍按普通 scan 读取。
- lazy output 列不读 data page。
- 如果 eager predicate 能用索引解决，对应 data page 也可能被跳过。

### 第二阶段：按 rowid 回读 lazy 列

BE `MaterializationOperator` 对 TopN winners 执行 rowid expr，把请求按 backend 发到 `PInternalService::multiget_data_v2()`。服务端 `RowIdStorageReader` 的内表路径是：

```text
RowIdStorageReader::read_by_rowids(PMultiGetRequestV2)
  -> read_batch_doris_format_row()
     -> group by file_id -> tablet_id,rowset_id,segment_id
     -> per segment sort row_ids ascending
     -> deduplicate equal row_ids
     -> read_doris_format_row()
        -> load tablet / temp rowset / segment
        -> row store path or column-store path
```

column-store path：

```text
for each lazy slot:
  Segment::seek_and_read_by_rowid(schema, slot, sorted_unique_rowids)
    -> create/reuse ColumnIterator
    -> ColumnIterator::read_by_rowids()
```

row-store path：

```text
for each row_id:
  BaseTablet::lookup_row_data()
    -> hidden row-store column iterator
    -> read_by_rowids(one row)
    -> JsonbSerializeUtil::jsonb_to_block()
```

第二阶段的微观特征：

- rowids 在每个 segment 内会先排序、去重，后续再按原请求位置 scatter 回结果 block。
- column-store path 对每个 lazy slot 调用一次 `Segment::seek_and_read_by_rowid(..., sorted_unique_rowids, ...)`，column iterator 可以按 sorted rowids 单调前进。winners dense 时，多行共享 data page；winners sparse 时，lazy 列表现为随机 page 读。
- row-store path 当前对每个 rowid 逐行调用 `BaseTablet::lookup_row_data()`，每次读取 hidden row-store column 的一个 rowid。dense rowids 的 page 复用主要依赖 `StoragePageCache`、file cache 和底层 column reader/page cache 状态，而不是一次批量 `ColumnIterator::read_by_rowids()`。
- row store 把多列读取压到 hidden row-store string column，但每个 rowid 仍要落到其所在 data page；如果 row store 不覆盖输出列，还要补 column-store path。
- column-store path 对复杂/variant lazy slot 仍会展开为 offsets、child、path 等子列。

这个场景天然已经拿到每个 segment 的 sorted unique rowids。因此它的文件访问单位不是 “scan batch 中的连续 row range”，而是“每个 segment 内的一组 rowids 对应的 lazy column pages”。

### 文件 I/O Pattern

TopN 全局延迟物化有一个跨阶段硬依赖：

```text
first-stage scan data pages
  -> TopN winners
  -> global rowids grouped by backend/file/segment
  -> second-stage row-store or column-store pages
```

第一阶段和第二阶段的 I/O 形态不同：

| 阶段 | 读文件内容 | 顺序性 | 依赖关系 | I/O 大小特征 |
| --- | --- | --- | --- | --- |
| 第一阶段 scan | eager 列、predicate 列、rowid 虚拟列所需 metadata | 与普通 scan 相同；lazy output 列不读 | 依赖普通 scan 的 row bitmap / predicate | 少读 lazy 列 data page；rowid 虚拟列不对应 ordinary data page |
| rowid shuffle / RPC | 不直接读 segment 文件 | 按 backend / file / segment 分组 | 必须等 TopN winners 确定 | 网络请求大小由 winner 数、slot 数决定 |
| 第二阶段 row store | hidden row-store column data page | 每个 segment 内 rowids 已排序，但当前逐 rowid 调用 lookup；dense 复用依赖 page/cache 命中 | 依赖 rowid 分组、segment load、row-store hidden column | single-row lookup 也会触碰整 data page / cache block |
| 第二阶段 column store | 每个 lazy slot 的 data page / 子列 page | 每列独立按 sorted rowids 批量读；多列之间各自触碰 page | 依赖 lazy slot 列表和 rowids；复杂类型依赖 offsets / child | 列数越多，随机 page 数越容易放大 |
| second-stage segment open | footer、column meta、ordinal index | 小随机 metadata read | 首次访问某 segment / column 时触发 | 12 bytes + footer、V3 meta、ordinal index page |

第二阶段的局部顺序性在 column-store path 上最明确：同一 segment、同一 lazy column、排序后的 rowids 可以让 iterator 单调前进。row-store path 虽然也使用同一组 sorted rowids，但实现上是逐 rowid hidden-column lookup，因此更像批量单点读。跨 lazy columns、跨 segments、跨 backend RPC 的读取没有 page-level 顺序保证；宏观上常见形态是多路小随机 page read。row store 能把多列合并成 hidden column 的 page read，但如果 winners 分散在很多 data page 或 file-cache block 中，远端读取仍然会放大。

## 点查 / Short-Circuit Point Query

点查是和普通 scan 差异最大的 query 场景。FE `LogicalResultSinkToShortCircuitPointQuery` 只在较窄条件下标记 short-circuit：

- session 开启 short-circuit query；
- OLAP 表开启 light schema change、unique key merge-on-write、row column store；
- 不含 variant columns；
- predicate 覆盖所有 key columns。

云模式下，FE `PointQueryExecutor` 还会在 snapshot point query 打开时获取 partition snapshot visible version，再把请求发到候选 BE。

### BE 宏观路径

```text
PointQueryExecutor::lookup_up()
  -> _lookup_row_key()
     -> optional CloudTablet::sync_rowsets(snapshot version)
     -> RowCache lookup by tablet_id + encoded primary key
     -> BaseTablet::lookup_row_key()
  -> _lookup_row_data()
     -> row-store lookup
     -> optional missing column-store lookup
     -> delete-sign filtering
```

`BaseTablet::lookup_row_key()` 按 rowset end version 从新到旧搜索：

```text
specified rowsets sorted by end_version desc
  -> segment key bounds prune
  -> SegmentLoader::load_segments(..., need_load_pk_index_and_bf=true)
  -> Segment::lookup_row_key()
  -> delete bitmap contains_agg_with_cache_if_eligible()
```

一旦找到未删除的 key，就停止继续搜索旧 rowset。

### 微观读模式

row cache 命中时，不需要读 segment 文件。row cache miss 时：

1. segment load 读取 footer、PK meta，并加载 PK index / PK bloom。2. segment key bounds 在 rowset meta 中先排除不可能的 segment。3. `Segment::lookup_row_key()` 先用 PK bloom `check_present()`，不命中直接返回 `KEY_NOT_FOUND`。4. PK indexed-column iterator `seek_at_or_after(key)` 定位 ordinal。5. `next_batch(1)` 读出 encoded key，校验 sequence / rowid 语义，得到 `RowLocation{rowset_id,segment_id,row_id}`。6. delete bitmap 聚合缓存判断该 row 是否已被新版本删除。

这条链路里的 PK 文件读要拆开看：`load_pk_index_and_bf()` 先调用 `load_index()`，PK `load_index()` 只加载 primary-key indexed-column 的 ordinal/value root/index page；如果 PK 只有一个 data page，则 root 直接指向 data page，这一步不读 index page。随后 `_load_pk_bloom_filter()` 加载单值 PK bloom 并读出 ordinal 0 的 bloom value。真正的 PK key data page 发生在 `seek_at_or_after(key)` 时：value root/index 先给出候选 PK data page，然后一次 `PageIO` 读取该 PK data page，`next_batch(1)` 在同一页内读 encoded key 做精确校验。因此点查访问一个候选 segment 时，典型冷读是 footer/meta + PK root/index page + PK bloom value page + 1 个 PK data page；如果 bloom miss，则省掉 PK data page；如果 segment key bounds 先排除，则连 PK bloom/index 都不会读。

取值阶段优先 row store：

```text
BaseTablet::lookup_row_data()
  -> _get_segment_column_iterator(row-store hidden column)
  -> read_by_rowids(single row)
  -> JsonbSerializeUtil::jsonb_to_block()
  -> RowCache insert if enabled
```

如果查询输出列不完全在 row store 中，且 session 允许访问 column store，`PointQueryExecutor::_lookup_row_data()` 会对 missing columns 调用 `Segment::seek_and_read_by_rowid()`。此时模式和 TopN 二阶段的 column-store path 一样，只是 rowids 通常只有一个。

点查 cold read 通常是高随机、小范围 I/O：少量 PK root/index/bloom page、一个 PK data page、一个 row-store data page，以及可能的少量 missing column data pages。它不走普通 `SegmentIterator` 的谓词列/非谓词列二段读。PK root/index page 在同一 segment 内可复用，但 rowset version 从新到旧搜索会把这个小随机读模式重复到多个候选 rowset/segment 上。

### 文件 I/O Pattern

点查的文件依赖链是单行定位链：

```text
row cache miss
  -> segment footer / PK meta
  -> PK bloom
  -> PK indexed-column seek
  -> delete bitmap check
  -> row-store page
  -> optional missing column-store pages
```

具体 I/O 形态：

| 阶段 | 读文件内容 | 顺序性 | 依赖关系 | I/O 大小特征 |
| --- | --- | --- | --- | --- |
| rowset/segment search | footer、PK meta、PK root/index、PK bloom | 按 rowset version 从新到旧；每个 segment 内是小随机读 | 新版本未找到或命中 deleted row 才继续旧版本 | 每个候选 segment 可能读 footer、PK 0/2 个 root/index page、PK bloom value page |
| PK bloom | 单值 bloom indexed-column root/data page | 单值 read，读入后内存判断 | bloom miss 直接跳过 PK data page seek | 一个 segment 一个 bloom value；大小由 key 数和 fpp 决定，cold miss 按 cache block |
| PK indexed-column | PK root/index/data page | root/index 串行加载；key data page 随 key 随机 | bloom 可能先裁剪；定位 rowid 后才能读 row data | `load_index()` 不读全量 PK data pages；一次 point seek 通常读 1 个 PK data page |
| row store | hidden row-store column data page | 单 rowid 随机读 | 依赖 `RowLocation` | 逻辑上一行，物理上整 data page / cache block |
| missing column store | 普通列 data page | 单 rowid 随机读，多列分别读取 | 依赖 row store 不覆盖输出列且允许补列 | 每列一个或多个 data page；复杂类型展开子列 |

点查与 TopN 二阶段都使用 rowid 定位后读取行数据，但点查通常只有一个 key，并且搜索顺序受 rowset version 语义约束：找到最新未删除 row 后停止。它的远端 I/O 主要由“访问了多少候选 rowset/segment 的 PK metadata”和“最终 row data 落在哪些 cache block”决定。

## 维护类读者

以下不是普通用户 query 的主路径，但在存算分离下同样会产生 cold read。

### Compaction / Schema Change / Checksum

base/cumulative/full/cold-data compaction、schema change、checksum 等通过 `TabletReader` 和 rowset/segment iterator 读取旧数据。差异在 reader type：

- 非 query reader 默认 `is_disposable=true`，file cache 策略和 query 不完全相同。
- compaction 通常读取全列或 writer 需要的列，宏观上是全 segment 顺序读。
- base/cumulative/full compaction 通常按 segment 全量 data page 构造访问范围。
- query-only 路径不一定生效，例如低基数字典 row range 当前只在 `READER_QUERY` 使用。
- delete condition、delete sign、delete bitmap 的应用取决于 compaction 类型和配置。

这类读的核心 I/O pattern 是“多 rowset、多 segment、全量 data page 流式读取”，而不是 query 的“先过滤再读 surviving rowids”。

### Delete Bitmap Calculation / Partial Update Conflict Read

unique-key merge-on-write 的 delete bitmap 计算和 partial update 会大量使用 primary-key lookup：

```text
new segment PK index scan
  -> for each key, BaseTablet::lookup_row_key() in previous rowsets
  -> delete bitmap check
  -> conflict row may read row store or column store
```

宏观上这是写入/发布阶段的维护读；微观上像“顺序 PK 扫描 + 批量点查”：新 segment 侧已经按 key 排序，PK indexed-column 可从 root/index 定位后连续 `next_batch()` 读多个 PK data pages，顺序性明显强于单点查；历史 rowsets 侧仍按每个 key 做 segment bounds、PK bloom、PK value seek 和 row-store/column-store 读取，因此是批量小随机。

### Index Build

创建倒排索引或 ANN 索引时，index builder 会通过 `Segment::new_iterator()` 扫描已有 segment 的相关列，再写新 `.idx`。它的读侧是普通 segment scan，但输出不是 query block，而是 index writer。对 cold read 来说，它更接近“选定列全量读”。

### Binlog Read

`READER_BINLOG` 也复用 segment iterator，但会额外处理 LSN/TSO 隐藏列替换，并要求 binlog 行顺序。它的 data page 读取仍是普通 scan 模式；特殊点在于输出列和 reader type 改变了 merge/order 语义。

### 文件 I/O Pattern

维护类 reader 的共同点是：读侧多由“重写、校验、构建索引或发布版本”驱动，而不是由用户 query 的投影和谓词驱动。

| 场景 | 读文件内容 | 顺序性 | 依赖关系 | I/O 大小特征 |
| --- | --- | --- | --- | --- |
| compaction | 参与重写的 key/value/delete-sign 等列 data page | 每个 segment 内多为全量顺序读；多个 rowset merge 时交错 | 依赖 rowset merge / delete 语义 | 接近读取所需列的全部 data pages；file cache 以 block 计 |
| schema change | 源 schema 所需列 data page | 通常全量顺序读 | 依赖新 schema 表达式和默认值规则 | 读取源列全量 page；新列默认值不读旧文件 |
| checksum | 校验列 data page | 全量顺序读 | 依赖 checksum schema / reader type | 以 column data page 为主 |
| delete bitmap calculation | 新 segment PK root/index/data pages + 历史 rowset PK lookup | 新 segment PK data pages 近似顺序；历史 rowset 是批量随机点查 | 新 key -> 历史 PK lookup -> delete bitmap update | 新 PK indexed-column 大范围读按 32KB 目标 data page 推进；历史侧是 PK root/index/bloom/data page 小随机 |
| partial update conflict read | row store 或 column store 冲突列 | 按冲突 key 随机读 | 依赖 PK lookup 得到 rowid | 近似批量点查，row store 可减少列 page 数 |
| index build | 被建索引列 data page | 选定列全量顺序读 | 依赖 segment scan 输出列值后写 `.idx` | 读取目标列全部 page；复杂/variant 展开子列 |
| binlog read | binlog 输出列和隐藏列 data page | 受 binlog 行顺序约束 | 依赖 reader type 的顺序语义 | 接近普通 scan，但列集合和顺序固定 |

compaction / schema change / checksum 的主要 I/O 是大范围顺序 data page 读；它们对远端读取更接近“吞吐型”。delete bitmap calculation 和 partial update conflict read 则把 “扫描新数据”和“随机查旧数据”组合在一起：新 segment 的 PK data page 读取偏顺序，历史 rowset 的 PK root/index/bloom、PK data page、row store、column store 读取偏随机。

## File Cache Warm-up / 缓存预热

File cache warm-up 不是单一的 scan 形态，当前主要有两类 I/O：

- query-shaped warm-up：`WARM UP SELECT` 被改写成 blackhole sink，执行路径仍是普通 scan。它的 `.dat` / `.idx` 读取模式由实际 SELECT 的谓词、投影、索引和 TopN 等形态决定，只是最终不返回用户结果。
- block/file-shaped warm-up：job、event-driven rowset warm-up、rebalance warm-up 等路径直接给 BE 下发文件 path、offset、size、cache type。BE 通过 `FileCacheBlockDownloader` 读取 `.dat` 或 `.idx` 的指定区间并填充 file cache。

block/file-shaped warm-up 的依赖链是：

```text
warm-up task
  -> tablet / rowset / segment / index file metadata
  -> file path + offset + size + cache type
  -> FileCacheBlockDownloader::download_segment_file()
  -> FileReader::read_at(offset, size, IOContext{is_warmup=true})
  -> file-cache block state update
```

它不依赖 `_row_bitmap`、predicate result、TopN winners 或 PK row location，也不会读取列值后再交给表达式计算。它的 I/O pattern 由任务给出的文件区间决定：

| warm-up 形态 | 读文件内容 | 顺序性 | 依赖关系 | I/O 大小特征 |
| --- | --- | --- | --- | --- |
| `WARM UP SELECT` | SELECT 实际触碰的 `.dat` / `.idx` page 和 sub-file | 与对应 query scan 相同 | 依赖 scan 的 row bitmap、predicate、index 等路径 | 与普通 scan 相同，只是 sink 丢弃结果 |
| rowset / segment file warm-up | segment `.dat`、inverted index `.idx` 的整文件或大区间 | 按 offset 递增分片读 | 依赖 rowset file metadata 和任务范围 | 上层按 `s3_write_buffer_size` 分片；底层按 file-cache block 处理 |
| hot block warm-up | 来源 BE 反馈的 hot cache block ranges | block range 通常离散，按任务列表读取 | 依赖 source/destination BE 的 block meta 交换 | 每个 range 落到一个或多个 file-cache blocks |

这个场景和维护类读者的区别是：维护类读者通常通过 tablet/rowset iterator 读取并消费行数据；block/file-shaped warm-up 只消费文件字节和 cache block 状态，不需要解码 column page。

## 跨场景叠加维度

### Row Store vs Column Store

row store 是点查和 TopN 二阶段的重要分支。它把一组列编码到隐藏 row-store column 中：

- 优点：一次 hidden column data page read 后可反序列化多列，适合点查和 sparse rowid fetch。
- 代价：如果 row store 不覆盖查询输出，还要补 column-store `seek_and_read_by_rowid()`。
- 对 cold read：row store 把“多列随机 page 读”压缩成“单列随机 page 读”，但仍受 rowid 稀疏程度和 page/cache block 对齐影响。

### Merge vs Union

是否需要 storage merge 会改变宏观 I/O interleaving：

- union/direct：各 rowset/segment 独立输出，scanner 可更自由并发；I/O 更接近每个 segment 自己的顺序或稀疏读。
- merge：`VCollectIterator` 需要维护多个 child 当前行，按 key/sequence/version 输出；I/O 会在多个 child 之间交错，且每个 child 至少要读到当前候选行。

### Delete Bitmap 和 Delete Condition

delete bitmap 是内存/metadata 过滤，不直接读 `.dat` data page，但它改变 `_row_bitmap`，从而改变后续 data page 范围。delete condition 是表达式过滤，会把相关列加入 predicate columns；这会触发 data page 读取，并可能关闭某些元数据聚合路径。

### Cache 状态

同一个场景在 cold/warm 下表现差异很大：

- segment cache hit：跳过 footer、column meta、部分 index load。
- storage page cache hit：跳过 `PageIO` 对 `.dat` page 的底层 `read_at`。
- inverted index query/searcher cache hit：跳过 `.idx` searcher build 或 query reads。
- row cache hit：点查直接返回 row-store JSONB。
- file cache hit：逻辑 page read 仍发生，但底层是 local cache file read，不是远端对象存储。

因此分析 profile 时要同时看逻辑行数、page/cache 命中、file-cache local/remote/peer bytes、inverted-index counters 和 rowid fetch counters。

### I/O 大小和次数估算

不同场景最终都可以落到以下几个计数：

| 计数项 | 含义 | 影响因素 |
| --- | --- | --- |
| footer reads | 每个 cold segment 的 footer tail + footer PB | segment cache 命中率、参与 segment 数 |
| meta reads | V3 external meta region / slice、variant external meta key | 访问列数、variant path、column reader 创建时机 |
| index page reads | short-key single page、PK root/index/data page、ordinary ordinal page、zone map indexed-column page、bloom indexed-column page | 谓词类型、key range、是否首次 seek、indexed-column root 是否 data page、page zone map 是否首次全量加载 |
| `.idx` reads | compound header、metadata、posting、BKD、ANN list | searcher/query cache、predicate 类型、term 数、ANN probe list |
| ordinary data page reads | 实际触碰的 column data pages | row bitmap 密度、列数、row store 覆盖情况、complex/variant 展开 |
| cache block reads | 远端或本地 cache block 数 | file-cache block size、page offset 分布、多个 page 是否落在同一 block |
| warm-up extent reads | warm-up 任务显式要求读取的文件区间 | rowset/segment/index file 数、任务 offset/size、hot block ranges |

逻辑返回行数只影响其中一部分。对 cold read 更直接的是：

```text
remote bytes
  ~= unique cache blocks touched by footer/meta/index/data logical reads
     * file_cache_each_block_size
     adjusted by file tail, local hits, peer hits and skip-cache reads
```

`PagePointer.size` 越小，单次逻辑 page read 越小，但如果 page 分散在不同 cache block，远端读取仍按 block 放大。反过来，多个 page 即使属于不同列，只要落在同一个文件 cache block，cold miss 后也可以复用同一 block。

### 依赖关系分类

| 依赖类型 | 场景 | 说明 |
| --- | --- | --- |
| metadata hard dependency | 所有 `.dat` 场景 | 没有 footer / column meta，就不能定位 page offset 和 size |
| index-to-data dependency | 索引辅助 scan、`COUNT_ON_INDEX` | index bitmap 完成前，不能精确决定后续 data page 范围 |
| predicate-to-output dependency | 普通 scan lazy materialization | 非谓词列读取依赖谓词列和 common expr 过滤结果 |
| winner-to-rowid dependency | TopN 全局延迟物化 | 二阶段读取必须等待 TopN winners 和 global rowids |
| key-to-row dependency | 点查、delete bitmap calculation | PK bloom/index 定位 rowid 后才能读 row store / column store |
| merge-order dependency | merge reader、key TopN、binlog | 输出顺序或版本语义要求多个 child 交错推进 |
| task-to-range dependency | file cache warm-up | 任务中的 file path、offset、size、cache type 直接决定读取范围 |

这些硬依赖决定了哪些 I/O 可以提前知道，哪些必须等上一阶段结果出现。没有硬依赖的维度，例如不同 segment、不同 lazy column、不同 backend 的二阶段 rowid fetch，更多受调度和线程池并发控制影响。

## 最终场景划分

Doris 存算分离内表 cold read 的 I/O pattern 由以下场景共同构成：

- 普通 scan / SegmentIterator lazy materialization：先通过 key range、索引、谓词、delete bitmap 等构造 `_row_bitmap`，再按“谓词列 / common expr 列 / 非谓词输出列” 分阶段读取 data page。
- 索引辅助 scan：`.idx`、ANN、page zone map、bloom、dict 等先产生或收窄 row bitmap，surviving rows 再进入普通列读取。
- 存储层聚合 / 元数据读：`COUNT`、`MINMAX`、`MIX` 主要读取 footer、segment zone map 和 short-key page 或 PK root/index page；`COUNT_ON_INDEX` 依赖索引 bitmap，整体尽量避免 ordinary data page。
- LIMIT / key TopN 下推：storage scan 内提前停止、反向读取或维护 rowset 内候选集，不形成 TopN 全局延迟物化的二阶段 rowid fetch。
- TopN 全局延迟物化：第一阶段只读排序/输出所需 eager 列和全局 rowid，第二阶段按 rowid 回读 lazy 列；column-store path 是批量 rowid 读，row-store path 是逐 rowid hidden column lookup。
- 点查 / short-circuit point query：primary-key index 定位单行，按 row cache、row store、column store 补列的优先级读取。
- 维护类读者：compaction、schema change、checksum、index build、delete bitmap calculation 等非 query reader 主要体现为全量列读、主键/索引读或批量点查式读。
- File cache warm-up / 缓存预热：`WARM UP SELECT` 复用 query scan；job、event-driven rowset warm-up 和 rebalance warm-up 可按 `.dat` / `.idx` 文件区间或 hot block ranges 填充 file cache。

这些场景最终落到少数微观动作：footer/meta 读、indexed-column 读、`.idx` sub-file 读、ordinary data page 读、row-store hidden column 读，以及 file-cache block 对齐后的本地或远端 `read_at`。
