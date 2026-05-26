# Doris 存算分离冷读场景化 I/O Pattern

本文从执行场景出发描述 Doris 内表在存算分离下的 cold-read I/O pattern。`.dat`
和 `.idx` 的物理格式、`read_at` 清单、file cache block 放大规则分别见
`dat-file-io-patterns.md` 和 `inverted-index-io-patterns.md`；本文只在场景中引用
这些底层读点。

## 场景总览

面向内表 cold read 的主要场景可以归为这些类：

| 场景 | 主要入口 | 宏观读模式 | 微观热点 |
| --- | --- | --- | --- |
| 普通 scan / SegmentIterator lazy materialization | `OlapScanner` -> `TabletReader` -> `BetaRowsetReader` -> `SegmentIterator` | rowset/segment 级并发扫描，先构造 row bitmap，再按列读取 | footer、short-key/PK、ordinal、zone map、bloom、data page、file cache block |
| 索引辅助 scan | `SegmentIterator::_apply_inverted_index()` / `_apply_index_expr()` / `_apply_ann_topn_predicate()` | `.idx` 或 `.dat` 内部索引先产生 row bitmap，再读 surviving rows | CLucene/BKD/ANN sub-file、null bitmap、page zone map、bloom filter、COUNT_ON_INDEX |
| 存储层聚合 / 元数据读 | `PhysicalStorageLayerAggregate` -> `TPushAggOp` -> `VStatisticsIterator` 或 `SegmentIterator` | 不读或少读 data page，用 segment/page metadata 生成上层聚合输入 | segment zone map、footer/meta、COUNT_ON_INDEX 的 index bitmap |
| LIMIT / key TopN 下推 | `OlapScanner::_init_tablet_reader_params()` -> `VCollectIterator::_topn_next()` / `SegmentIterator::read_limit` | storage 内提前停止，key TopN 每个 rowset 取本地候选 | 首尾 data page、reverse rowid iteration、runtime TopN predicate |
| TopN 全局延迟物化 | FE `LazyMaterializeTopN` -> BE `MaterializationOperator` -> `RowIdStorageReader` | 第一阶段只读 eager 列和全局 rowid；第二阶段按 rowid 批量回读 lazy 列 | rowid group/sort/dedup、row store hidden column、column-store `read_by_rowids()` |
| 点查 / short-circuit point query | FE `LogicalResultSinkToShortCircuitPointQuery` -> BE `PointQueryExecutor` | primary-key index 定位一行，优先 row cache / row store，必要时补列存列 | row cache、segment key bounds、PK bloom、PK indexed-column、row store data page |
| 维护类读者 | compaction / schema change / checksum / index build / delete bitmap calculation | 通常按 rowset/segment 全量读，reader type 决定是否 merge、是否 disposable cache | 全量 data page 读、delete bitmap、PK index scan、row store/column-store conflict read |

这些场景共享同一批底层构件：

- `SegmentLoader` 和 `SegmentCache`：决定是否已经有 segment footer、column meta、PK
  meta 等内存对象。
- `StoragePageCache`：缓存 `PageIO` 解析后的 `.dat` page。
- `InvertedIndexSearcherCache`、`InvertedIndexQueryCache`、ANN IVF list cache：缓存
  `.idx` reader/searcher/query result。
- `RowCache`：缓存点查 row-store 结果。
- `CachedRemoteFileReader`：最终把逻辑 `read_at(offset,size)` 转为本地 cache file 读、
  远端对象存储读、peer cache 读或 fallback remote 读，并按 file-cache block 对齐。

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

- `DUP_KEYS`、`UNIQUE_KEYS` merge-on-write、direct mode 或 pre-aggregation 可以走
  union / unordered read，不需要 storage 层按 key merge。
- `AGG_KEYS` 或 merge-on-read 等需要 merge 的情况，`VCollectIterator` 会用 heap
  在多个 rowset/segment child 之间按 key 合并。I/O 仍由每个 child 的
  `SegmentIterator` 触发，但宏观上会在多个 rowset 间交错读取。

`LazyInitSegmentIterator` 使 segment 真正用到时才加载，避免宽表、被过滤 segment
过早创建 column reader。segment 打开会解析 footer；`Segment::new_iterator()` 会先用
segment-level zone map 尝试整段过滤，再加载 short-key 或 primary-key index，最后创建
`SegmentIterator` 或 `VStatisticsIterator`。

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

- `_predicate_column_ids`：需要先读来执行 column predicate、delete condition 或 common
  expr 的列。
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

如果没有 predicate、delete condition、common expr，lazy materialization 不开启，
`_predicate_column_ids` 会包含所有 schema columns，于是只有一次 `_read_columns_by_index()`。

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

如果 common expr 引用的列既不是谓词列也不是输出列，实际可能形成三组物理读取：
谓词列 -> common-expr-only 列 -> 非谓词输出列。

### 微观读模式

`_read_columns_by_index()` 先从 `_range_iter` 拿一批 rowids：

- 连续 rowids：每列一次 `seek_to_ordinal(first)`，再 `next_batch(n)` 顺序解码；跨 page
  时读下一 data page。
- 非连续 rowids：按 range iterator batch 切小批；小批连续则仍走
  `seek_to_ordinal + next_batch`，否则走 `ColumnIterator::read_by_rowids()`。

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

`FileColumnIterator::read_by_rowids()` 要求 rowids 单调。它会按 page 聚合能覆盖的
rowids：dense rowids 多个值共享一个 data page；sparse rowids 接近“一个 rowid
触发一个 page”。Nullable 列还会按 null run 跳过或读取 nested data。

复杂类型会把一次逻辑列读放大成多个物理子列读：

- `ARRAY` / `MAP`：offsets 先读，再根据 offsets 读 element、key、value。
- `STRUCT`：递归读每个 child。
- `VARIANT`：可能先读外置 path meta，再读 root binary、extracted subcolumn 或 sparse
  subcolumn cache。

## 索引辅助 Scan

索引辅助 scan 仍然落在 `SegmentIterator` 内，但宏观读法不同：先读索引得到 row
bitmap，再决定 data page。

### `.dat` 内部索引

这些索引存放在 `.dat` 中，经 `PageIO` 读取：

- segment-level zone map：在 `Segment::new_iterator()` 直接用 footer/column meta 中的
  segment zone map 判断整段是否可跳过，不读 page-zone-map data page。
- page zone map：`ColumnReader::_get_filtered_pages()` 先 load zone-map indexed-column，
  读取并缓存所有 page zone maps，然后把 page ids 转成 row ranges。
- bloom filter：先 load ordinal index 和 bloom indexed-column，再按当前 row ranges
  覆盖到的 page ids 逐个读取 bloom filter。
- dictionary：`get_row_ranges_by_dict()` 或首次读 dict-encoded data page 时读取 dict
  page。
- short-key / PK index：用于 key range、点查或 unique-key lookup。

这些读通常发生在 data page 前面。选择性好时，它们用较少 index I/O 换取大量 data
page 跳过；选择性差时，会表现为“先读一批 index page，再仍然读大部分 data page”。

### `.idx` 倒排索引和 ANN

`SegmentIterator::_init_index_iterators()` 为有 inverted index 或 ANN index 的列创建
`IndexIterator`，并把 `IOContext`、stats、runtime state 放进 `IndexQueryContext`。

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
- ANN 读 `ann.faiss` metadata；`IVF_ON_DISK` 还会按 list 随机读 `ann.ivfdata`，并受
  ANN IVF list cache 影响。
- nullable predicate 可能读 `null_bitmap` sub-file。

索引执行成功后，`_check_all_conditions_passed_inverted_index_for_column()` 可能把
`_need_read_data_indices[cid]` 置为 false。随后 `_need_read_data()` / `_prune_column()`
会跳过该列 data page，直接填默认值或只保留 bitmap 语义。`COUNT_ON_INDEX` 就依赖这条
路径：残余 scan conjunct 不允许存在，否则 BE 会拒绝，因为 count 必须在索引过滤后才
安全。

如果索引不支持、bypass、missing 或允许 fallback，`_downgrade_without_index()` 会把
predicate 留在普通 scan，后续按 data page 读取再过滤。

## 存储层聚合 / 元数据读

FE 的 `PhysicalStorageLayerAggregate` 会把无 group-by 的聚合翻译成 `TPushAggOp`：

- `COUNT`
- `COUNT_ON_INDEX`
- `MINMAX`
- `MIX`

在 BE，`Segment::new_iterator()` 对非 `COUNT_ON_INDEX` 的 pushdown aggregate 创建
`VStatisticsIterator`，前提是没有 delete condition。此时宏观上不走
`SegmentIterator` 的 data page scan。

### COUNT

`VStatisticsIterator::next_batch()` 对 `COUNT` 只按 segment row count 插入默认值：

```text
target rows = segment->num_rows()
next_batch:
  insert_many_defaults(batch_size)
```

冷读 I/O 基本只剩 segment open、footer、column meta 和 segment cache miss 相关读；
不读 ordinary data page。

### MINMAX / MIX

`MINMAX` 和 `MIX` 通过 `ColumnIterator::next_batch_of_zone_map()` 读取 segment-level
zone map 里的 min/max，生成上层聚合输入。segment zone map 来自 column meta，不需要
逐个 data page 解码，也不需要 page-zone-map indexed-column。CHAR 类型会做尾部 padding
处理。

### COUNT_ON_INDEX

`COUNT_ON_INDEX` 不走 `VStatisticsIterator`。它仍创建 `SegmentIterator`，先用倒排索引
或 ANN/index expression 得到 row bitmap，然后通过 `_need_read_data()` 跳过已经完全由
索引判断的 key column data read。宏观模式是“index bitmap count”，不是“footer row
count”，因此它可以表达 `COUNT_ON_MATCH` 这类索引条件计数。

## LIMIT / Key TopN 下推

这类场景和 TopN 全局延迟物化不同：它不做 rowid 二阶段回读，而是在 storage scan
阶段尽早少读。

`OlapScanner::_init_tablet_reader_params()` 只有在满足这些条件时才下推：

- scan 有 limit；
- 没有 runtime filter；
- SegmentIterator 中没有 residual conjunct；
- session 允许 `enable_segment_limit_pushdown`；
- storage no-merge，即 storage 层不需要为了语义做跨 rowset merge。

普通 LIMIT 会设置 `general_read_limit`，由 `VCollectIterator::add_child()` 转为
rowset reader 的 `_topn_limit`，再进入 `SegmentIterator::_opts.read_limit`。
`SegmentIterator` 在 predicate/common expr 之后应用 limit；当没有任何 SegmentIterator
侧过滤时，`_can_opt_limit_reads()` 还会直接缩小 first read 的 `nrows_read_limit`。

key TopN 会设置 `read_orderby_key_limit` 和 `read_orderby_key_reverse`。宏观路径是：

```text
VCollectIterator::_topn_next()
  -> for each RowSetSplits:
       rs_reader->set_topn_limit(limit)
       read up to local limit rows
       keep local candidates in sorted multiset
       optionally update runtime TopN predicate
```

如果是 `ORDER BY key DESC LIMIT n`，rowset/segment iterator 顺序会反向，底层
`BackwardBitmapRangeIterator` 从较大的 rowid 开始。微观上它仍使用普通
`SegmentIterator` 的 data page 读取，只是常见情况下只触碰每个 segment 的前几个或后
几个 data page。如果还有必须在 SegmentIterator 内执行的 predicate，则为了得到 n 个
survivors 可能读超过 n 行。

runtime TopN filter 是另一个叠加项：上游 TopN 维护当前边界值，scan 侧把它当 predicate
下推。它会进入普通 scan 的 row bitmap 裁剪路径，可能触发 zone map、bloom 或倒排索引
过滤，也可能回退到 data page 过滤。

## TopN 全局延迟物化

TopN 全局延迟物化是二阶段 rowid fetch，不等同于上一节的 storage LIMIT/key TopN
下推。

### 第一阶段：只读 eager 列和全局 rowid

FE `LazyMaterializeTopN` 在 TopN 周围插入 `PhysicalLazyMaterialize`：

```text
PhysicalTopN
  -> LazySlotPruning keeps eager slots
  -> scan outputs eager slots + __DORIS_GLOBAL_ROWID_COL__
  -> TopN computes winners
  -> MaterializationNode fetches lazy slots by rowid
```

对内表，第一阶段 scan 的底层读仍然是普通 `SegmentIterator` 模式，但 lazy slots 不在
scan 输出中。`__DORIS_GLOBAL_ROWID_COL__` 由 rowid iterator 构造
`GlobalRowLoacationV2{backend_id,file_id,row_id}`，不对应一个普通 data page。

因此第一阶段的 I/O 形态取决于 eager 列：

- order-by 列、predicate 列、join/projection 仍按普通 scan 读取。
- lazy output 列不读 data page。
- 如果 eager predicate 能用索引解决，对应 data page 也可能被跳过。

### 第二阶段：按 rowid 回读 lazy 列

BE `MaterializationOperator` 对 TopN winners 执行 rowid expr，把请求按 backend
发到 `PInternalService::multiget_data_v2()`。服务端 `RowIdStorageReader` 的内表路径是：

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

- rowids 在每个 segment 内已排序去重，因此 column iterator 可以单调前进。
- winners dense 时，多行共享 data page；winners sparse 时，lazy 列表现为随机 page
  读。
- row store 只读隐藏 row-store string column，但每个 rowid 仍要落到其所在 data page。
- column-store 对每个 lazy slot 分别读 data page；复杂/variant lazy slot 会展开为子列。
- 当前路径直接调用 `Segment::seek_and_read_by_rowid()`，核心成本由 rowid 分布、data
  page 粒度和 file-cache block 对齐共同决定。

这个场景天然已经拿到每个 segment 的 sorted unique rowids。若要优化 cold read，最适合
在 `read_doris_format_row()` 或 `Segment::seek_and_read_by_rowid()` 前统一规划所有 lazy
columns 的 page range / file-cache block range。

## 点查 / Short-Circuit Point Query

点查是和普通 scan 差异最大的 query 场景。FE `LogicalResultSinkToShortCircuitPointQuery`
只在较窄条件下标记 short-circuit：

- session 开启 short-circuit query；
- OLAP 表开启 light schema change、unique key merge-on-write、row column store；
- 不含 variant columns；
- predicate 覆盖所有 key columns。

云模式下，FE `PointQueryExecutor` 还会在 snapshot point query 打开时获取 partition
snapshot visible version，再把请求发到候选 BE。

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

1. segment load 读取 footer、PK meta，并加载 PK index / PK bloom。
2. segment key bounds 在 rowset meta 中先排除不可能的 segment。
3. `Segment::lookup_row_key()` 先用 PK bloom `check_present()`，不命中直接返回
   `KEY_NOT_FOUND`。
4. PK indexed-column iterator `seek_at_or_after(key)` 定位 ordinal。
5. `next_batch(1)` 读出 encoded key，校验 sequence / rowid 语义，得到
   `RowLocation{rowset_id,segment_id,row_id}`。
6. delete bitmap 聚合缓存判断该 row 是否已被新版本删除。

取值阶段优先 row store：

```text
BaseTablet::lookup_row_data()
  -> _get_segment_column_iterator(row-store hidden column)
  -> read_by_rowids(single row)
  -> JsonbSerializeUtil::jsonb_to_block()
  -> RowCache insert if enabled
```

如果查询输出列不完全在 row store 中，且 session 允许访问 column store，
`PointQueryExecutor::_lookup_row_data()` 会对 missing columns 调用
`Segment::seek_and_read_by_rowid()`。此时模式和 TopN 二阶段的 column-store path 一样，
只是 rowids 通常只有一个。

点查 cold read 通常是高随机、小范围 I/O：少量 PK index page、一个 row-store data page，
以及可能的少量 missing column data pages。它不走普通 `SegmentIterator` 的谓词列/非谓词列
二段读。

## 维护类读者

以下不是普通用户 query 的主路径，但在存算分离下同样会产生 cold read。

### Compaction / Schema Change / Checksum

base/cumulative/full/cold-data compaction、schema change、checksum 等通过
`TabletReader` 和 rowset/segment iterator 读取旧数据。差异在 reader type：

- 非 query reader 默认 `is_disposable=true`，file cache 策略和 query 不完全相同。
- compaction 通常读取全列或 writer 需要的列，宏观上是全 segment 顺序读。
- base/cumulative/full compaction 通常按 segment 全量 data page 构造访问范围。
- query-only 优化不一定生效，例如低基数字典 row range 当前只在 `READER_QUERY` 使用。
- delete condition、delete sign、delete bitmap 的应用取决于 compaction 类型和配置。

这类读的核心 I/O pattern 是“多 rowset、多 segment、全量 data page 流式读取”，而不是
query 的“先过滤再读 surviving rowids”。

### Delete Bitmap Calculation / Partial Update Conflict Read

unique-key merge-on-write 的 delete bitmap 计算和 partial update 会大量使用
primary-key lookup：

```text
new segment PK index scan
  -> for each key, BaseTablet::lookup_row_key() in previous rowsets
  -> delete bitmap check
  -> conflict row may read row store or column store
```

宏观上这是写入/发布阶段的维护读；微观上像“批量点查”：连续扫描新 segment 的 PK
indexed-column，同时对历史 rowsets 做随机 PK index 和 row-store/column-store 读取。

### Index Build

创建倒排索引或 ANN 索引时，index builder 会通过 `Segment::new_iterator()` 扫描已有
segment 的相关列，再写新 `.idx`。它的读侧是普通 segment scan，但输出不是 query block，
而是 index writer。对 cold read 来说，它更接近“选定列全量读”。

### Binlog Read

`READER_BINLOG` 也复用 segment iterator，但会额外处理 LSN/TSO 隐藏列替换，并要求
binlog 行顺序。它的 data page 读取仍是普通 scan 模式；特殊点在于输出列和 reader type
改变了 merge/order 语义。

## 跨场景叠加维度

### Row Store vs Column Store

row store 是点查和 TopN 二阶段的重要分支。它把一组列编码到隐藏 row-store column 中：

- 优点：一次 hidden column data page read 后可反序列化多列，适合点查和 sparse rowid
  fetch。
- 代价：如果 row store 不覆盖查询输出，还要补 column-store `seek_and_read_by_rowid()`。
- 对 cold read：row store 把“多列随机 page 读”压缩成“单列随机 page 读”，但仍受 rowid
  稀疏程度和 page/cache block 对齐影响。

### Merge vs Union

是否需要 storage merge 会改变宏观 I/O interleaving：

- union/direct：各 rowset/segment 独立输出，scanner 可更自由并发；I/O 更接近每个
  segment 自己的顺序或稀疏读。
- merge：`VCollectIterator` 需要维护多个 child 当前行，按 key/sequence/version 输出；
  I/O 会在多个 child 之间交错，且每个 child 至少要读到当前候选行。

### Delete Bitmap 和 Delete Condition

delete bitmap 是内存/metadata 过滤，不直接读 `.dat` data page，但它改变 `_row_bitmap`，
从而改变后续 data page 范围。delete condition 是表达式过滤，会把相关列加入
predicate columns；这会触发 data page 读取，并可能关闭某些元数据聚合路径。

### Cache 状态

同一个场景在 cold/warm 下表现差异很大：

- segment cache hit：跳过 footer、column meta、部分 index load。
- storage page cache hit：跳过 `PageIO` 对 `.dat` page 的底层 `read_at`。
- inverted index query/searcher cache hit：跳过 `.idx` searcher build 或 query reads。
- row cache hit：点查直接返回 row-store JSONB。
- file cache hit：逻辑 page read 仍发生，但底层是 local cache file read，不是远端对象存储。

因此分析 profile 时要同时看逻辑行数、page/cache 命中、file-cache local/remote/peer
bytes、inverted-index counters 和 rowid fetch counters。

## 最终场景划分

Doris 存算分离内表 cold read 的 I/O pattern 由以下场景共同构成：

- 普通 scan / SegmentIterator lazy materialization：先通过 key range、索引、谓词、
  delete bitmap 等构造 `_row_bitmap`，再按“谓词列 / common expr 列 / 非谓词输出列”
  分阶段读取 data page。
- 索引辅助 scan：`.idx`、ANN、page zone map、bloom、dict 等先产生或收窄 row bitmap，
  surviving rows 再进入普通列读取。
- 存储层聚合 / 元数据读：`COUNT`、`MINMAX`、`MIX` 主要读取 footer、segment zone map
  或索引 bitmap，尽量避免 ordinary data page。
- LIMIT / key TopN 下推：storage scan 内提前停止、反向读取或维护 rowset 内候选集，
  不形成 TopN 全局延迟物化的二阶段 rowid fetch。
- TopN 全局延迟物化：第一阶段只读排序/输出所需 eager 列和全局 rowid，第二阶段按
  rowid 对 lazy 列做批量回读。
- 点查 / short-circuit point query：primary-key index 定位单行，按 row cache、row
  store、column store 补列的优先级读取。
- 维护类读者：compaction、schema change、checksum、index build、delete bitmap
  calculation 等非 query reader 主要体现为全量列读、主键/索引读或批量点查式读。

这些场景最终落到少数微观动作：footer/meta 读、indexed-column 读、`.idx` sub-file 读、
ordinary data page 读、row-store hidden column 读，以及 file-cache block 对齐后的本地或
远端 `read_at`。
