# TopN 全局延迟物化和 Rowid 二阶段读取

本文解释 TopN 全局延迟物化的二阶段 rowid 读取逻辑。二阶段复用的 `.dat` footer、column meta、ordinal index、data page、dict page 等 `read_at` 明细见 [文件格式排布和 `read_at` 清单](file-layout-and-read-at-inventory.md)。

## 与普通 scan 延迟物化的区别

普通 `SegmentIterator` 延迟物化发生在同一个 scan pipeline 内。它先读谓词列，过滤后用 selected rowids 读取非谓词列，所有逻辑都在 `SegmentIterator::_next_batch_internal()` 内完成。

TopN 全局延迟物化是另一条路径。FE 在 TopN 之下保留 eager columns 和全局 rowid，TopN 先在小 tuple 上完成排序和 limit，之后 BE 的 `MaterializationOperator` 再按 rowid 发起二阶段读取。这个二阶段可能跨 BE，因为 rowid 中包含拥有数据的 backend id。

因此，两者的主要差异是：

| 维度 | 普通 scan 延迟物化 | TopN 全局延迟物化 |
| --- | --- | --- |
| rowid 来源 | `SegmentIterator` 内部 `_block_rowids` | 第一阶段 scan 输出的 `GlobalRowLoacationV2` 字符串 |
| 过滤边界 | scan batch 内谓词过滤后 | TopN 排序 limit 后 |
| 读取位置 | 当前 scanner 所在 BE | rowid 指向的 owning BE |
| 入口 | `_read_columns_by_rowids()` | `MaterializationOperator` -> `multiget_data_v2` -> `RowIdStorageReader` |
| 当前 SegmentPrefetcher | 可覆盖普通 scan `.dat` page | 不覆盖二阶段 rowid fetch |

## FE rewrite

`LazyMaterializeTopN` 是 Nereids post processor。启用条件包括 session variable `topn_lazy_materialization_threshold`，并且 TopN limit 不超过 threshold。

核心流程：

```text
PhysicalTopN
  -> LazyMaterializeTopN.computeTopN
       -> find slots that can be lazy materialized
       -> group lazy slots by relation
       -> create synthetic __DORIS_GLOBAL_ROWID_COL__ slot per relation
       -> LazySlotPruning rewrites scan output
       -> wrap TopN with PhysicalLazyMaterialize
       -> final PhysicalProject restores original output
```

`MaterializeProbeVisitor` 会排除不支持的场景，例如部分 set operation、非 base OLAP index scan、`AGG_KEYS`、当前算子已经使用的 slot、部分表类型等。

`PhysicalLazyMaterialize` 记录 BE 需要的信息：

- rowid slots，用于从 child block 取出全局 rowid。
- lazy columns，用于告诉远端要读取哪些列。
- slot locations，用于把远端返回列放回 intermediate tuple。
- table column indices，用于远端 storage layer 定位物理列。

`MaterializationNode.toThrift()` 把这些信息写入 `TMaterializationNode`。

## 第一阶段 scan 如何产生全局 rowid

当 scan 输出中包含 `__DORIS_GLOBAL_ROWID_COL__...` 时，BE scan operator 会创建 query 级 `IdFileMap`。内部 Doris segment scan 在 `SegmentIterator::_init_return_column_iterators()` 中识别 global rowid column：

```text
if column name starts with GLOBAL_ROWID_COL:
  file_id = runtime_state->get_id_file_map()->get_file_mapping_id(
      FileMapping(tablet_id, rowset_id, segment_id))
  column iterator = RowIdColumnIteratorV2(version, backend_id, file_id)
```

`RowIdColumnIteratorV2::next_batch()` 和 `read_by_rowids()` 把下面结构序列化成 string column：

```text
GlobalRowLoacationV2 {
  version,
  backend_id,
  file_id,
  row_id
}
```

这里 `file_id` 是 query 内的 compact id，用来在二阶段反查 `(tablet_id,rowset_id,segment_id)`。`backend_id` 决定二阶段 RPC 发到哪个 BE。

## MaterializationOperator 构造 multi-get

`MaterializationOperator` 收到 TopN 后的 rowid block 后，会：

```text
create_muiltget_result(rowid columns, child_eos)
  -> for each rowid column and row:
       decode GlobalRowLoacationV2
       find rpc_struct by backend_id
       append row_id and file_id into request_block_desc
       remember block_order_results for merge

init_multi_requests(TMaterializationNode, RuntimeState)
  -> build PMultiGetRequestV2 template
  -> fill column_descs, slot descriptors, slot_locs, column_idxs
  -> create brpc stub per backend

push()
  -> send multiget_data_v2 requests
  -> receive PMultiGetResponseV2 blocks
  -> merge_multi_response() scatters rows back to original TopN order
```

I/O 依赖关系：

- 二阶段必须等 TopN 输出 rowid batch 后才能知道要读哪些 lazy rows。
- 请求必须按 `backend_id` 分组，因为数据由对应 BE 的 local rowset/segment mapping 管理。
- 返回结果必须按 `block_order_results` 合并回原始 TopN 输出顺序。

## 服务端 rowid fetch

`PInternalService::multiget_data_v2()` 调用 `RowIdStorageReader::read_by_rowids(PMultiGetRequestV2)`。服务端先从 query 的 `IdFileMap` 中解析 file id：

```text
RowIdStorageReader::read_by_rowids(PMultiGetRequestV2)
  -> for each request_block_desc:
       first_file_id -> FileMapping
       if INTERNAL:
          read_batch_doris_format_row(...)
       else:
          read_batch_external_row(...)
  -> serialize one block per request_block_desc
```

内表路径 `read_batch_doris_format_row()` 是重点：

```text
read_batch_doris_format_row
  -> build full_read_schema from request column_descs
  -> group all (file_id,row_id) by SegKey(tablet_id,rowset_id,segment_id)
  -> for each segment group:
       sort row_ids ascending
       deduplicate equal row_ids
       read_doris_format_row(..., sorted_unique_row_ids, ...)
  -> scatter_scan_blocks_to_result_block
```

这里的排序和去重非常重要。`Segment::seek_and_read_by_rowid()` 对 rowids 有明确前置条件：必须 sorted 且 unique。这样 `FileColumnIterator::read_by_rowids()` 能单调前进，避免回退读取。

## 内表列读取微观路径

`read_doris_format_row()` 对每个 lazy slot 调用：

```text
segment->seek_and_read_by_rowid(
    full_read_schema,
    slot,
    row_ids,
    result_column,
    storage_read_options,
    iterator_hint)
```

`Segment::seek_and_read_by_rowid()` 的普通列路径：

```text
if iterator_hint == nullptr:
  new_column_iterator(schema.column(index), &iterator_hint, &storage_read_options)
  iterator_hint->init(ColumnIteratorOptions)

iterator_hint->read_by_rowids(row_ids.data(), row_ids.size(), result)
```

`FileColumnIterator::read_by_rowids()` 再执行 rowid 到 page 的转换：

```text
while remaining > 0:
  seek_to_ordinal(rowids[total_read_count])
     -> load ordinal index if needed
     -> find data page pointer
     -> read data page if current page does not contain rowid
  read as many requested rowids as fit in this page
  decoder->read_by_rowids(rowids, page_first_ordinal, ...)
```

因此 TopN 二阶段的 `.dat` I/O pattern 是 segment-grouped、rowid-sorted、page-random：

- 如果 TopN winners 在 segment 内很密集，多个 rowids 会落到同一 data page，page read 数较少。
- 如果 winners 很稀疏，几乎每个 rowid 都可能触发不同 page 的 seek 和 read。
- 多个 lazy columns 会分别读取自己的 ordinal index 和 data pages，除非 page cache 或 file cache 命中。
- 同一个 slot 的 `iterator_hint` 会被复用，避免每个 rowid 重建 iterator。

## Row store 分支

如果 request 标记 `fetch_row_store()`，服务端会通过 `tablet->lookup_row_data()` 读取行存数据，再把 jsonb row store 转成 block。这条路径不是列存 `.dat` data page 的主要优化对象，但它与列存路径共享 rowid 分组和 segment 定位。

## 与普通 SegmentPrefetcher 的关系

当前 `SegmentPrefetcher` 的初始化入口在 `SegmentIterator::_init_segment_prefetchers()`，二阶段 `RowIdStorageReader` 不创建 `SegmentIterator`，而是直接调用 `Segment::seek_and_read_by_rowid()` 和列级 iterator。因此，当前 TopN 二阶段不会提前构建 file-cache block 序列，也不会在正式 `read_by_rowids()` 前启动 prefetch。

二阶段如果要做 prefetch，适合插入在这些位置之一：

1. `read_batch_doris_format_row()` 完成 segment grouping、sort、dedup 后。此时已知每个 segment 的完整 sorted rowids。
2. `read_doris_format_row()` 对某个 segment 和所有 slots 准备读取前。此时已知 lazy columns 和 sorted rowids。
3. `Segment::seek_and_read_by_rowid()` 创建或复用 column iterator 后。此时已知单列 reader 和 rowids，可以通过 ordinal index 映射 page pointers。

推荐边界是第 2 或第 3 层：第 2 层能跨列合并和调度，第 3 层最接近 column reader 和 ordinal index，容易准确产出 page/file ranges。

## 二阶段 prefetch 接口需求

TopN 二阶段已经具备比普通 scan 更完整的 rowid 信息，因此 prefetch 接口应支持批量输入：

```text
plan_prefetch(
  segment,
  lazy_column_ids,
  sorted_unique_rowids,
  policy
) -> vector<FileAccessRange>
```

它需要显式处理：

- rowid 到每个列的 ordinal index 和 page pointer 的映射。
- 多列、多 page、多 cache block 的合并策略。
- 是否写 file cache。TopN 二阶段在本地 cache 容量受限且倒排索引很大时，可能希望 lazy column 读只进入内存 buffer，不污染 file cache 中的大索引文件。
- completion 语义。`MaterializationOperator` 可以选择在 RPC 内等待一部分 prefetch，也可以让 storage reader pipeline 自然消费已完成的 buffer。
- stats 归属。二阶段运行在远端 BE，query profile 需要区分 rowid fetch 的 remote/file-cache/local-cache I/O。

## 对冷查优化的结论

TopN 全局延迟物化已经把“需要读哪些 lazy rows”收敛到了很小集合，因此优化重点不是再做谓词剪枝，而是把二阶段 sorted rowids 转成并发远端 I/O。与当前触发式 `SegmentPrefetcher` 相比，二阶段更适合一次性规划所有 lazy columns 的物理 page/cache-block ranges，然后根据 cache 容量和索引保护策略选择 file-cache 或 memory-buffer 的落点。
