// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

#include "vec/exec/scan/file_scanner.h"

#include <fmt/format.h>
#include <gen_cpp/Exprs_types.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/Opcodes_types.h>
#include <gen_cpp/PaloInternalService_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <glog/logging.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <map>
#include <ranges>
#include <tuple>
#include <unordered_map>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "exec/rowid_fetcher.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/runtime_profile.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_number.h"
#include "vec/data_types/data_type_string.h"
#include "vec/exec/format/arrow/arrow_stream_reader.h"
#include "vec/exec/format/avro/avro_jni_reader.h"
#include "vec/exec/format/csv/csv_reader.h"
#include "vec/exec/format/json/new_json_reader.h"
#include "vec/exec/format/orc/vorc_reader.h"
#include "vec/exec/format/parquet/vparquet_reader.h"
#include "vec/exec/format/table/hive_reader.h"
#include "vec/exec/format/table/hudi_jni_reader.h"
#include "vec/exec/format/table/hudi_reader.h"
#include "vec/exec/format/table/iceberg_reader.h"
#include "vec/exec/format/table/lakesoul_jni_reader.h"
#include "vec/exec/format/table/max_compute_jni_reader.h"
#include "vec/exec/format/table/paimon_jni_reader.h"
#include "vec/exec/format/table/paimon_reader.h"
#include "vec/exec/format/table/transactional_hive_reader.h"
#include "vec/exec/format/table/trino_connector_jni_reader.h"
#include "vec/exec/format/text/text_reader.h"
#include "vec/exec/format/wal/wal_reader.h"
#include "vec/exec/scan/scan_node.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/exprs/vexpr_fwd.h"
#include "vec/exprs/vslot_ref.h"
#include "vec/functions/function.h"
#include "vec/functions/function_string.h"
#include "vec/functions/simple_function_factory.h"
#include "vec/utils/stringop_substring.h"

namespace cctz {
class time_zone;
} // namespace cctz
namespace doris {
namespace vectorized {
class ShardedKVCache;
} // namespace vectorized
} // namespace doris

namespace doris::vectorized {
using namespace ErrorCode;

const std::string FileScanner::FileReadBytesProfile = "FileReadBytes";
const std::string FileScanner::FileReadTimeProfile = "FileReadTime";

FileScanner::FileScanner(
        RuntimeState* state, pipeline::FileScanLocalState* local_state, int64_t limit,
        std::shared_ptr<vectorized::SplitSourceConnector> split_source, RuntimeProfile* profile,
        ShardedKVCache* kv_cache,
        const std::unordered_map<std::string, ColumnValueRangeType>* colname_to_value_range,
        const std::unordered_map<std::string, int>* colname_to_slot_id)
        : Scanner(state, local_state, limit, profile),
          _split_source(split_source),
          _cur_reader(nullptr),
          _cur_reader_eof(false),
          _colname_to_value_range(colname_to_value_range),
          _kv_cache(kv_cache),
          _strict_mode(false),
          _col_name_to_slot_id(colname_to_slot_id) {
    if (state->get_query_ctx() != nullptr &&
        state->get_query_ctx()->file_scan_range_params_map.count(local_state->parent_id()) > 0) {
        _params = &(state->get_query_ctx()->file_scan_range_params_map[local_state->parent_id()]);
    } else {
        // old fe thrift protocol
        _params = _split_source->get_params();
    }
    if (_params->__isset.strict_mode) {
        _strict_mode = _params->strict_mode;
    }

    // For load scanner, there are input and output tuple.
    // For query scanner, there is only output tuple
    _input_tuple_desc = state->desc_tbl().get_tuple_descriptor(_params->src_tuple_id);
    _real_tuple_desc = _input_tuple_desc == nullptr ? _output_tuple_desc : _input_tuple_desc;
    _is_load = (_input_tuple_desc != nullptr);
}

Status FileScanner::prepare(RuntimeState* state, const VExprContextSPtrs& conjuncts) {
    RETURN_IF_ERROR(Scanner::prepare(state, conjuncts));
    _get_block_timer =
            ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(), "FileScannerGetBlockTime", 1);
    _cast_to_input_block_timer = ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(),
                                                      "FileScannerCastInputBlockTime", 1);
    _fill_missing_columns_timer = ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(),
                                                       "FileScannerFillMissingColumnTime", 1);
    _pre_filter_timer =
            ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(), "FileScannerPreFilterTimer", 1);
    _convert_to_output_block_timer = ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(),
                                                          "FileScannerConvertOuputBlockTime", 1);
    _runtime_filter_partition_prune_timer = ADD_TIMER_WITH_LEVEL(
            _local_state->scanner_profile(), "FileScannerRuntimeFilterPartitionPruningTime", 1);
    _empty_file_counter =
            ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(), "EmptyFileNum", TUnit::UNIT, 1);
    _not_found_file_counter = ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(),
                                                     "NotFoundFileNum", TUnit::UNIT, 1);
    _file_counter =
            ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(), "FileNumber", TUnit::UNIT, 1);

    _file_read_bytes_counter = ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(),
                                                      "FileReadBytes", TUnit::BYTES, 1);
    _file_read_calls_counter = ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(),
                                                      "FileReadCalls", TUnit::UNIT, 1);
    _file_read_time_counter =
            ADD_TIMER_WITH_LEVEL(_local_state->scanner_profile(), "FileReadTime", 1);

    _runtime_filter_partition_pruned_range_counter =
            ADD_COUNTER_WITH_LEVEL(_local_state->scanner_profile(),
                                   "RuntimeFilterPartitionPrunedRangeNum", TUnit::UNIT, 1);

    _file_cache_statistics.reset(new io::FileCacheStatistics());
    _file_reader_stats.reset(new io::FileReaderStats());

    RETURN_IF_ERROR(_init_io_ctx());
    _io_ctx->file_cache_stats = _file_cache_statistics.get();
    _io_ctx->file_reader_stats = _file_reader_stats.get();

    if (_is_load) {
        _src_row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                              std::vector<TupleId>({_input_tuple_desc->id()}),
                                              std::vector<bool>({false})));
        // prepare pre filters
        if (_params->__isset.pre_filter_exprs_list) {
            RETURN_IF_ERROR(doris::vectorized::VExpr::create_expr_trees(
                    _params->pre_filter_exprs_list, _pre_conjunct_ctxs));
        } else if (_params->__isset.pre_filter_exprs) {
            VExprContextSPtr context;
            RETURN_IF_ERROR(
                    doris::vectorized::VExpr::create_expr_tree(_params->pre_filter_exprs, context));
            _pre_conjunct_ctxs.emplace_back(context);
        }

        for (auto& conjunct : _pre_conjunct_ctxs) {
            RETURN_IF_ERROR(conjunct->prepare(_state, *_src_row_desc));
            RETURN_IF_ERROR(conjunct->open(_state));
        }

        _dest_row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                               std::vector<TupleId>({_output_tuple_desc->id()}),
                                               std::vector<bool>({false})));
    }

    _default_val_row_desc.reset(new RowDescriptor(_state->desc_tbl(),
                                                  std::vector<TupleId>({_real_tuple_desc->id()}),
                                                  std::vector<bool>({false})));

    return Status::OK();
}

// check if the expr is a partition pruning expr
bool FileScanner::_check_partition_prune_expr(const VExprSPtr& expr) {
    if (expr->is_slot_ref()) {
        auto* slot_ref = static_cast<VSlotRef*>(expr.get());
        return _partition_slot_index_map.find(slot_ref->slot_id()) !=
               _partition_slot_index_map.end();
    }
    if (expr->is_literal()) {
        return true;
    }
    return std::ranges::all_of(expr->children(), [this](const auto& child) {
        return _check_partition_prune_expr(child);
    });
}

void FileScanner::_init_runtime_filter_partition_prune_ctxs() {
    _runtime_filter_partition_prune_ctxs.clear();
    for (auto& conjunct : _conjuncts) {
        auto impl = conjunct->root()->get_impl();
        // If impl is not null, which means this a conjuncts from runtime filter.
        auto expr = impl ? impl : conjunct->root();
        if (_check_partition_prune_expr(expr)) {
            _runtime_filter_partition_prune_ctxs.emplace_back(conjunct);
        }
    }
}

void FileScanner::_init_runtime_filter_partition_prune_block() {
    // init block with empty column
    for (auto const* slot_desc : _real_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            // should be ignored from reading
            continue;
        }
        _runtime_filter_partition_prune_block.insert(
                ColumnWithTypeAndName(slot_desc->get_empty_mutable_column(),
                                      slot_desc->get_data_type_ptr(), slot_desc->col_name()));
    }
}

Status FileScanner::_process_runtime_filters_partition_prune(bool& can_filter_all) {
    SCOPED_TIMER(_runtime_filter_partition_prune_timer);
    if (_runtime_filter_partition_prune_ctxs.empty() || _partition_col_descs.empty()) {
        return Status::OK();
    }
    size_t partition_value_column_size = 1;

    // 1. Get partition key values to string columns.
    std::unordered_map<SlotId, MutableColumnPtr> parititon_slot_id_to_column;
    for (auto const& partition_col_desc : _partition_col_descs) {
        const auto& [partition_value, partition_slot_desc] = partition_col_desc.second;
        auto test_serde = partition_slot_desc->get_data_type_ptr()->get_serde();
        auto partition_value_column = partition_slot_desc->get_data_type_ptr()->create_column();
        auto* col_ptr = static_cast<IColumn*>(partition_value_column.get());
        Slice slice(partition_value.data(), partition_value.size());
        uint64_t num_deserialized = 0;
        RETURN_IF_ERROR(test_serde->deserialize_column_from_fixed_json(
                *col_ptr, slice, partition_value_column_size, &num_deserialized, {}));
        parititon_slot_id_to_column[partition_slot_desc->id()] = std::move(partition_value_column);
    }

    // 2. Fill _runtime_filter_partition_prune_block from the partition column, then execute conjuncts and filter block.
    // 2.1 Fill _runtime_filter_partition_prune_block from the partition column to match the conjuncts executing.
    size_t index = 0;
    bool first_column_filled = false;
    for (auto const* slot_desc : _real_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            // should be ignored from reading
            continue;
        }
        if (parititon_slot_id_to_column.find(slot_desc->id()) !=
            parititon_slot_id_to_column.end()) {
            auto data_type = slot_desc->get_data_type_ptr();
            auto partition_value_column = std::move(parititon_slot_id_to_column[slot_desc->id()]);
            if (data_type->is_nullable()) {
                _runtime_filter_partition_prune_block.insert(
                        index, ColumnWithTypeAndName(
                                       ColumnNullable::create(
                                               std::move(partition_value_column),
                                               ColumnUInt8::create(partition_value_column_size, 0)),
                                       data_type, slot_desc->col_name()));
            } else {
                _runtime_filter_partition_prune_block.insert(
                        index, ColumnWithTypeAndName(std::move(partition_value_column), data_type,
                                                     slot_desc->col_name()));
            }
            if (index == 0) {
                first_column_filled = true;
            }
        }
        index++;
    }

    // 2.2 Execute conjuncts.
    if (!first_column_filled) {
        // VExprContext.execute has an optimization, the filtering is executed when block->rows() > 0
        // The following process may be tricky and time-consuming, but we have no other way.
        _runtime_filter_partition_prune_block.get_by_position(0).column->assume_mutable()->resize(
                partition_value_column_size);
    }
    IColumn::Filter result_filter(_runtime_filter_partition_prune_block.rows(), 1);
    RETURN_IF_ERROR(VExprContext::execute_conjuncts(_runtime_filter_partition_prune_ctxs, nullptr,
                                                    &_runtime_filter_partition_prune_block,
                                                    &result_filter, &can_filter_all));
    return Status::OK();
}

Status FileScanner::_process_conjuncts_for_dict_filter() {
    _slot_id_to_filter_conjuncts.clear();
    _not_single_slot_filter_conjuncts.clear();
    for (auto& conjunct : _push_down_conjuncts) {
        auto impl = conjunct->root()->get_impl();
        // If impl is not null, which means this a conjuncts from runtime filter.
        auto cur_expr = impl ? impl : conjunct->root();

        std::vector<int> slot_ids;
        _get_slot_ids(cur_expr.get(), &slot_ids);
        if (slot_ids.size() == 0) {
            _not_single_slot_filter_conjuncts.emplace_back(conjunct);
            return Status::OK();
        }
        bool single_slot = true;
        for (int i = 1; i < slot_ids.size(); i++) {
            if (slot_ids[i] != slot_ids[0]) {
                single_slot = false;
                break;
            }
        }
        if (single_slot) {
            SlotId slot_id = slot_ids[0];
            _slot_id_to_filter_conjuncts[slot_id].emplace_back(conjunct);
        } else {
            _not_single_slot_filter_conjuncts.emplace_back(conjunct);
        }
    }
    return Status::OK();
}

Status FileScanner::_process_late_arrival_conjuncts() {
    if (_push_down_conjuncts.size() < _conjuncts.size()) {
        _push_down_conjuncts.clear();
        _push_down_conjuncts.resize(_conjuncts.size());
        for (size_t i = 0; i != _conjuncts.size(); ++i) {
            RETURN_IF_ERROR(_conjuncts[i]->clone(_state, _push_down_conjuncts[i]));
        }
        RETURN_IF_ERROR(_process_conjuncts_for_dict_filter());
        _discard_conjuncts();
    }
    if (_applied_rf_num == _total_rf_num) {
        _local_state->scanner_profile()->add_info_string("ApplyAllRuntimeFilters", "True");
    }
    return Status::OK();
}

void FileScanner::_get_slot_ids(VExpr* expr, std::vector<int>* slot_ids) {
    for (auto& child_expr : expr->children()) {
        if (child_expr->is_slot_ref()) {
            VSlotRef* slot_ref = reinterpret_cast<VSlotRef*>(child_expr.get());
            slot_ids->emplace_back(slot_ref->slot_id());
        } else {
            _get_slot_ids(child_expr.get(), slot_ids);
        }
    }
}

Status FileScanner::open(RuntimeState* state) {
    RETURN_IF_CANCELLED(state);
    RETURN_IF_ERROR(Scanner::open(state));
    RETURN_IF_ERROR(_split_source->get_next(&_first_scan_range, &_current_range));
    if (_first_scan_range) {
        RETURN_IF_ERROR(_init_expr_ctxes());
        if (_state->query_options().enable_runtime_filter_partition_prune &&
            !_partition_slot_index_map.empty()) {
            _init_runtime_filter_partition_prune_ctxs();
            _init_runtime_filter_partition_prune_block();
        }
    } else {
        // there's no scan range in split source. stop scanner directly.
        _scanner_eof = true;
    }

    return Status::OK();
}

Status FileScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    Status st = _get_block_wrapped(state, block, eof);

    if (!st.ok()) {
        // add cur path in error msg for easy debugging
        return std::move(st.append(". cur path: " + get_current_scan_range_name()));
    }
    return st;
}

// For query:
//                              [exist cols]  [non-exist cols]  [col from path]  input  output
//                              A     B    C  D                 E
// _init_src_block              x     x    x  x                 x                -      x
// get_next_block               x     x    x  -                 -                -      x
// _cast_to_input_block         -     -    -  -                 -                -      -
// _fill_columns_from_path      -     -    -  -                 x                -      x
// _fill_missing_columns        -     -    -  x                 -                -      x
// _convert_to_output_block     -     -    -  -                 -                -      -
//
// For load:
//                              [exist cols]  [non-exist cols]  [col from path]  input  output
//                              A     B    C  D                 E
// _init_src_block              x     x    x  x                 x                x      -
// get_next_block               x     x    x  -                 -                x      -
// _cast_to_input_block         x     x    x  -                 -                x      -
// _fill_columns_from_path      -     -    -  -                 x                x      -
// _fill_missing_columns        -     -    -  x                 -                x      -
// _convert_to_output_block     -     -    -  -                 -                -      x
Status FileScanner::_get_block_wrapped(RuntimeState* state, Block* block, bool* eof) {
    do {
        RETURN_IF_CANCELLED(state);
        if (_cur_reader == nullptr || _cur_reader_eof) {
            // The file may not exist because the file list is got from meta cache,
            // And the file may already be removed from storage.
            // Just ignore not found files.
            Status st = _get_next_reader();
            if (st.is<ErrorCode::NOT_FOUND>() && config::ignore_not_found_file_in_external_table) {
                _cur_reader_eof = true;
                COUNTER_UPDATE(_not_found_file_counter, 1);
                continue;
            } else if (!st) {
                return st;
            }
        }

        if (_scanner_eof) {
            *eof = true;
            return Status::OK();
        }

        // Init src block for load job based on the data file schema (e.g. parquet)
        // For query job, simply set _src_block_ptr to block.
        size_t read_rows = 0;
        RETURN_IF_ERROR(_init_src_block(block));
        {
            SCOPED_TIMER(_get_block_timer);

            // Read next block.
            // Some of column in block may not be filled (column not exist in file)
            RETURN_IF_ERROR(
                    _cur_reader->get_next_block(_src_block_ptr, &read_rows, &_cur_reader_eof));
        }
        // use read_rows instead of _src_block_ptr->rows(), because the first column of _src_block_ptr
        // may not be filled after calling `get_next_block()`, so _src_block_ptr->rows() may return wrong result.
        if (read_rows > 0) {
            if ((!_cur_reader->count_read_rows()) && _io_ctx) {
                _io_ctx->file_reader_stats->read_rows += read_rows;
            }
            // If the push_down_agg_type is COUNT, no need to do the rest,
            // because we only save a number in block.
            if (_get_push_down_agg_type() != TPushAggOp::type::COUNT) {
                // Convert the src block columns type to string in-place.
                RETURN_IF_ERROR(_cast_to_input_block(block));
                // FileReader can fill partition and missing columns itself
                if (!_cur_reader->fill_all_columns()) {
                    // Fill rows in src block with partition columns from path. (e.g. Hive partition columns)
                    RETURN_IF_ERROR(_fill_columns_from_path(read_rows));
                    // Fill columns not exist in file with null or default value
                    RETURN_IF_ERROR(_fill_missing_columns(read_rows));
                }
                // Apply _pre_conjunct_ctxs to filter src block.
                RETURN_IF_ERROR(_pre_filter_src_block());
                // Convert src block to output block (dest block), string to dest data type and apply filters.
                RETURN_IF_ERROR(_convert_to_output_block(block));
                // Truncate char columns or varchar columns if size is smaller than file columns
                // or not found in the file column schema.
                RETURN_IF_ERROR(_truncate_char_or_varchar_columns(block));
            }
        }
        break;
    } while (true);

    // Update filtered rows and unselected rows for load, reset counter.
    // {
    //     state->update_num_rows_load_filtered(_counter.num_rows_filtered);
    //     state->update_num_rows_load_unselected(_counter.num_rows_unselected);
    //     _reset_counter();
    // }
    return Status::OK();
}

/**
 * Check whether there are complex types in parquet/orc reader in broker/stream load.
 * Broker/stream load will cast any type as string type, and complex types will be casted wrong.
 * This is a temporary method, and will be replaced by tvf.
 */
Status FileScanner::_check_output_block_types() {
    if (_is_load) {
        TFileFormatType::type format_type = _params->format_type;
        if (format_type == TFileFormatType::FORMAT_PARQUET ||
            format_type == TFileFormatType::FORMAT_ORC) {
            for (auto slot : _output_tuple_desc->slots()) {
                if (is_complex_type(slot->type()->get_primitive_type())) {
                    return Status::InternalError(
                            "Parquet/orc doesn't support complex types in broker/stream load, "
                            "please use tvf(table value function) to insert complex types.");
                }
            }
        }
    }
    return Status::OK();
}

Status FileScanner::_init_src_block(Block* block) {
    if (!_is_load) {
        _src_block_ptr = block;
        return Status::OK();
    }
    RETURN_IF_ERROR(_check_output_block_types());

    // if (_src_block_init) {
    //     _src_block.clear_column_data();
    //     _src_block_ptr = &_src_block;
    //     return Status::OK();
    // }

    _src_block.clear();
    size_t idx = 0;
    // slots in _input_tuple_desc contains all slots describe in load statement, eg:
    // -H "columns: k1, k2, tmp1, k3 = tmp1 + 1"
    // _input_tuple_desc will contains: k1, k2, tmp1
    // and some of them are from file, such as k1 and k2, and some of them may not exist in file, such as tmp1
    // _input_tuple_desc also contains columns from path
    for (auto& slot : _input_tuple_desc->slots()) {
        DataTypePtr data_type;
        auto it = _slot_lower_name_to_col_type.find(slot->col_name());
        if (slot->is_skip_bitmap_col()) {
            _skip_bitmap_col_idx = idx;
        }
        if (_params->__isset.sequence_map_col) {
            if (_params->sequence_map_col == slot->col_name()) {
                _sequence_map_col_uid = slot->col_unique_id();
            }
        }
        data_type =
                it == _slot_lower_name_to_col_type.end() ? slot->type() : make_nullable(it->second);
        MutableColumnPtr data_column = data_type->create_column();
        _src_block.insert(
                ColumnWithTypeAndName(std::move(data_column), data_type, slot->col_name()));
        _src_block_name_to_idx.emplace(slot->col_name(), idx++);
    }
    if (_params->__isset.sequence_map_col) {
        for (const auto& slot : _output_tuple_desc->slots()) {
            // When the target table has seqeunce map column, _input_tuple_desc will not contains __DORIS_SEQUENCE_COL__,
            // so we should get its column unique id from _output_tuple_desc
            if (slot->is_sequence_col()) {
                _sequence_col_uid = slot->col_unique_id();
            }
        }
    }
    _src_block_ptr = &_src_block;
    _src_block_init = true;
    return Status::OK();
}

Status FileScanner::_cast_to_input_block(Block* block) {
    if (!_is_load) {
        return Status::OK();
    }
    SCOPED_TIMER(_cast_to_input_block_timer);
    // cast primitive type(PT0) to primitive type(PT1)
    uint32_t idx = 0;
    for (auto& slot_desc : _input_tuple_desc->slots()) {
        if (_slot_lower_name_to_col_type.find(slot_desc->col_name()) ==
            _slot_lower_name_to_col_type.end()) {
            // skip columns which does not exist in file
            continue;
        }
        if (slot_desc->type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
            // skip variant type
            continue;
        }
        auto& arg = _src_block_ptr->get_by_name(slot_desc->col_name());
        auto return_type = slot_desc->get_data_type_ptr();
        // remove nullable here, let the get_function decide whether nullable
        auto data_type = get_data_type_with_default_argument(remove_nullable(return_type));
        ColumnsWithTypeAndName arguments {
                arg, {data_type->create_column(), data_type, slot_desc->col_name()}};
        auto func_cast = SimpleFunctionFactory::instance().get_function(
                "CAST", arguments, return_type,
                {.enable_decimal256 = runtime_state()->enable_decimal256()});
        if (!func_cast) {
            return Status::InternalError("Function CAST[arg={}, col name={}, return={}] not found!",
                                         arg.type->get_name(), slot_desc->col_name(),
                                         return_type->get_name());
        }
        idx = _src_block_name_to_idx[slot_desc->col_name()];
        RETURN_IF_ERROR(
                func_cast->execute(nullptr, *_src_block_ptr, {idx}, idx, arg.column->size()));
        _src_block_ptr->get_by_position(idx).type = std::move(return_type);
    }
    return Status::OK();
}

Status FileScanner::_fill_columns_from_path(size_t rows) {
    DataTypeSerDe::FormatOptions _text_formatOptions;
    for (auto& kv : _partition_col_descs) {
        auto doris_column = _src_block_ptr->get_by_name(kv.first).column;
        IColumn* col_ptr = const_cast<IColumn*>(doris_column.get());
        auto& [value, slot_desc] = kv.second;
        auto _text_serde = slot_desc->get_data_type_ptr()->get_serde();
        Slice slice(value.data(), value.size());
        uint64_t num_deserialized = 0;
        if (_text_serde->deserialize_column_from_fixed_json(*col_ptr, slice, rows,
                                                            &num_deserialized,
                                                            _text_formatOptions) != Status::OK()) {
            return Status::InternalError("Failed to fill partition column: {}={}",
                                         slot_desc->col_name(), value);
        }
        if (num_deserialized != rows) {
            return Status::InternalError(
                    "Failed to fill partition column: {}={} ."
                    "Number of rows expected to be written : {}, number of rows actually written : "
                    "{}",
                    slot_desc->col_name(), value, num_deserialized, rows);
        }
    }
    return Status::OK();
}

Status FileScanner::_fill_missing_columns(size_t rows) {
    if (_missing_cols.empty()) {
        return Status::OK();
    }

    SCOPED_TIMER(_fill_missing_columns_timer);
    for (auto& kv : _missing_col_descs) {
        if (kv.second == nullptr) {
            // no default column, fill with null
            auto mutable_column = _src_block_ptr->get_by_name(kv.first).column->assume_mutable();
            auto* nullable_column = static_cast<vectorized::ColumnNullable*>(mutable_column.get());
            nullable_column->insert_many_defaults(rows);
        } else {
            // fill with default value
            auto& ctx = kv.second;
            auto origin_column_num = _src_block_ptr->columns();
            int result_column_id = -1;
            // PT1 => dest primitive type
            RETURN_IF_ERROR(ctx->execute(_src_block_ptr, &result_column_id));
            bool is_origin_column = result_column_id < origin_column_num;
            if (!is_origin_column) {
                // call resize because the first column of _src_block_ptr may not be filled by reader,
                // so _src_block_ptr->rows() may return wrong result, cause the column created by `ctx->execute()`
                // has only one row.
                auto result_column_ptr = _src_block_ptr->get_by_position(result_column_id).column;
                auto mutable_column = result_column_ptr->assume_mutable();
                mutable_column->resize(rows);
                // result_column_ptr maybe a ColumnConst, convert it to a normal column
                result_column_ptr = result_column_ptr->convert_to_full_column_if_const();
                auto origin_column_type = _src_block_ptr->get_by_name(kv.first).type;
                bool is_nullable = origin_column_type->is_nullable();
                _src_block_ptr->replace_by_position(
                        _src_block_ptr->get_position_by_name(kv.first),
                        is_nullable ? make_nullable(result_column_ptr) : result_column_ptr);
                _src_block_ptr->erase(result_column_id);
            }
        }
    }
    return Status::OK();
}

Status FileScanner::_pre_filter_src_block() {
    if (!_is_load) {
        return Status::OK();
    }
    if (!_pre_conjunct_ctxs.empty()) {
        SCOPED_TIMER(_pre_filter_timer);
        auto origin_column_num = _src_block_ptr->columns();
        auto old_rows = _src_block_ptr->rows();
        RETURN_IF_ERROR(vectorized::VExprContext::filter_block(_pre_conjunct_ctxs, _src_block_ptr,
                                                               origin_column_num));
        _counter.num_rows_unselected += old_rows - _src_block_ptr->rows();
    }
    return Status::OK();
}

Status FileScanner::_convert_to_output_block(Block* block) {
    if (!_is_load) {
        return Status::OK();
    }
    SCOPED_TIMER(_convert_to_output_block_timer);
    // The block is passed from scanner context's free blocks,
    // which is initialized by output columns
    // so no need to clear it
    // block->clear();

    int ctx_idx = 0;
    size_t rows = _src_block_ptr->rows();
    auto filter_column = vectorized::ColumnUInt8::create(rows, 1);
    auto& filter_map = filter_column->get_data();

    // After convert, the column_ptr should be copied into output block.
    // Can not use block->insert() because it may cause use_count() non-zero bug
    MutableBlock mutable_output_block =
            VectorizedUtils::build_mutable_mem_reuse_block(block, *_dest_row_desc);
    auto& mutable_output_columns = mutable_output_block.mutable_columns();

    std::vector<BitmapValue>* skip_bitmaps {nullptr};
    if (_should_process_skip_bitmap_col()) {
        auto* skip_bitmap_nullable_col_ptr =
                assert_cast<ColumnNullable*>(_src_block_ptr->get_by_position(_skip_bitmap_col_idx)
                                                     .column->assume_mutable()
                                                     .get());
        skip_bitmaps = &(assert_cast<ColumnBitmap*>(
                                 skip_bitmap_nullable_col_ptr->get_nested_column_ptr().get())
                                 ->get_data());
        // NOTE:
        // - If the table has sequence type column, __DORIS_SEQUENCE_COL__ will be put in _input_tuple_desc, so whether
        //   __DORIS_SEQUENCE_COL__ will be marked in skip bitmap depends on whether it's specified in that row
        // - If the table has sequence map column, __DORIS_SEQUENCE_COL__ will not be put in _input_tuple_desc,
        //   so __DORIS_SEQUENCE_COL__ will be ommited if it't specified in a row and will not be marked in skip bitmap.
        //   So we should mark __DORIS_SEQUENCE_COL__ in skip bitmap here if the corresponding sequence map column us marked
        if (_sequence_map_col_uid != -1) {
            for (int j = 0; j < rows; ++j) {
                if ((*skip_bitmaps)[j].contains(_sequence_map_col_uid)) {
                    (*skip_bitmaps)[j].add(_sequence_col_uid);
                }
            }
        }
    }

    // for (auto slot_desc : _output_tuple_desc->slots()) {
    for (int i = 0; i < mutable_output_columns.size(); ++i) {
        auto slot_desc = _output_tuple_desc->slots()[i];
        if (!slot_desc->is_materialized()) {
            continue;
        }
        int dest_index = ctx_idx;
        vectorized::ColumnPtr column_ptr;

        auto& ctx = _dest_vexpr_ctx[dest_index];
        int result_column_id = -1;
        // PT1 => dest primitive type
        RETURN_IF_ERROR(ctx->execute(_src_block_ptr, &result_column_id));
        column_ptr = _src_block_ptr->get_by_position(result_column_id).column;
        // column_ptr maybe a ColumnConst, convert it to a normal column
        column_ptr = column_ptr->convert_to_full_column_if_const();
        DCHECK(column_ptr);

        // because of src_slot_desc is always be nullable, so the column_ptr after do dest_expr
        // is likely to be nullable
        if (LIKELY(column_ptr->is_nullable())) {
            const auto* nullable_column =
                    reinterpret_cast<const vectorized::ColumnNullable*>(column_ptr.get());
            for (int i = 0; i < rows; ++i) {
                if (filter_map[i] && nullable_column->is_null_at(i)) {
                    // skip checks for non-mentioned columns in flexible partial update
                    if (skip_bitmaps == nullptr ||
                        !skip_bitmaps->at(i).contains(slot_desc->col_unique_id())) {
                        // clang-format off
                        if (_strict_mode && (_src_slot_descs_order_by_dest[dest_index]) &&
                            !_src_block_ptr->get_by_position(_dest_slot_to_src_slot_index[dest_index]).column->is_null_at(i)) {
                            filter_map[i] = false;
                            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                                [&]() -> std::string {
                                    return _src_block_ptr->dump_one_line(i, _num_of_columns_from_file);
                                },
                                [&]() -> std::string {
                                    auto raw_value =
                                            _src_block_ptr->get_by_position(_dest_slot_to_src_slot_index[dest_index]).column->get_data_at(i);
                                    std::string raw_string = raw_value.to_string();
                                    fmt::memory_buffer error_msg;
                                    fmt::format_to(error_msg,"column({}) value is incorrect while strict mode is {}, src value is {}",
                                            slot_desc->col_name(), _strict_mode, raw_string);
                                    return fmt::to_string(error_msg);
                                }));
                        } else if (!slot_desc->is_nullable()) {
                            filter_map[i] = false;
                            RETURN_IF_ERROR(_state->append_error_msg_to_file(
                                [&]() -> std::string {
                                    return _src_block_ptr->dump_one_line(i, _num_of_columns_from_file);
                                },
                                [&]() -> std::string {
                                    fmt::memory_buffer error_msg;
                                    fmt::format_to(error_msg, "column({}) values is null while columns is not nullable", slot_desc->col_name());
                                    return fmt::to_string(error_msg);
                                }));
                        }
                        // clang-format on
                    }
                }
            }
            if (!slot_desc->is_nullable()) {
                column_ptr = remove_nullable(column_ptr);
            }
        } else if (slot_desc->is_nullable()) {
            column_ptr = make_nullable(column_ptr);
        }
        mutable_output_columns[i]->insert_range_from(*column_ptr, 0, rows);
        ctx_idx++;
    }

    // after do the dest block insert operation, clear _src_block to remove the reference of origin column
    _src_block_ptr->clear();

    size_t dest_size = block->columns();
    // do filter
    block->insert(vectorized::ColumnWithTypeAndName(std::move(filter_column),
                                                    std::make_shared<vectorized::DataTypeUInt8>(),
                                                    "filter column"));
    RETURN_IF_ERROR(vectorized::Block::filter_block(block, dest_size, dest_size));

    _counter.num_rows_filtered += rows - block->rows();
    return Status::OK();
}

Status FileScanner::_truncate_char_or_varchar_columns(Block* block) {
    // Truncate char columns or varchar columns if size is smaller than file columns
    // or not found in the file column schema.
    if (!_state->query_options().truncate_char_or_varchar_columns) {
        return Status::OK();
    }
    int idx = 0;
    for (auto slot_desc : _real_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        const auto& type = slot_desc->type();
        if (type->get_primitive_type() != TYPE_VARCHAR && type->get_primitive_type() != TYPE_CHAR) {
            ++idx;
            continue;
        }
        auto iter = _source_file_col_name_types.find(slot_desc->col_name());
        if (iter != _source_file_col_name_types.end()) {
            const auto file_type_desc = _source_file_col_name_types[slot_desc->col_name()];
            int l = -1;
            if (auto* ftype = check_and_get_data_type<DataTypeString>(
                        remove_nullable(file_type_desc).get())) {
                l = ftype->len();
            }
            if ((assert_cast<const DataTypeString*>(remove_nullable(type).get())->len() > 0) &&
                (assert_cast<const DataTypeString*>(remove_nullable(type).get())->len() < l ||
                 l < 0)) {
                _truncate_char_or_varchar_column(
                        block, idx,
                        assert_cast<const DataTypeString*>(remove_nullable(type).get())->len());
            }
        } else {
            _truncate_char_or_varchar_column(
                    block, idx,
                    assert_cast<const DataTypeString*>(remove_nullable(type).get())->len());
        }
        ++idx;
    }
    return Status::OK();
}

// VARCHAR substring(VARCHAR str, INT pos[, INT len])
void FileScanner::_truncate_char_or_varchar_column(Block* block, int idx, int len) {
    auto int_type = std::make_shared<DataTypeInt32>();
    size_t num_columns_without_result = block->columns();
    const ColumnNullable* col_nullable =
            assert_cast<const ColumnNullable*>(block->get_by_position(idx).column.get());
    const ColumnPtr& string_column_ptr = col_nullable->get_nested_column_ptr();
    ColumnPtr null_map_column_ptr = col_nullable->get_null_map_column_ptr();
    block->replace_by_position(idx, std::move(string_column_ptr));
    block->insert({int_type->create_column_const(block->rows(), to_field<TYPE_INT>(1)), int_type,
                   "const 1"}); // pos is 1
    block->insert({int_type->create_column_const(block->rows(), to_field<TYPE_INT>(len)), int_type,
                   fmt::format("const {}", len)});                          // len
    block->insert({nullptr, std::make_shared<DataTypeString>(), "result"}); // result column
    ColumnNumbers temp_arguments(3);
    temp_arguments[0] = idx;                            // str column
    temp_arguments[1] = num_columns_without_result;     // pos
    temp_arguments[2] = num_columns_without_result + 1; // len
    size_t result_column_id = num_columns_without_result + 2;

    SubstringUtil::substring_execute(*block, temp_arguments, result_column_id, block->rows());
    auto res = ColumnNullable::create(block->get_by_position(result_column_id).column,
                                      null_map_column_ptr);
    block->replace_by_position(idx, std::move(res));
    Block::erase_useless_column(block, num_columns_without_result);
}

Status FileScanner::_create_row_id_column_iterator() {
    auto& id_file_map = _state->get_id_file_map();
    auto file_id = id_file_map->get_file_mapping_id(std::make_shared<FileMapping>(
            ((pipeline::FileScanLocalState*)_local_state)->parent_id(), _current_range,
            _should_enable_file_meta_cache()));
    _row_id_column_iterator_pair.first = std::make_shared<RowIdColumnIteratorV2>(
            IdManager::ID_VERSION, BackendOptions::get_backend_id(), file_id);
    return Status::OK();
}

Status FileScanner::_get_next_reader() {
    while (true) {
        if (_cur_reader) {
            _cur_reader->collect_profile_before_close();
            RETURN_IF_ERROR(_cur_reader->close());
            _state->update_num_finished_scan_range(1);
        }
        _cur_reader.reset(nullptr);
        _src_block_init = false;
        bool has_next = _first_scan_range;
        if (!_first_scan_range) {
            RETURN_IF_ERROR(_split_source->get_next(&has_next, &_current_range));
        }
        _first_scan_range = false;
        if (!has_next || _should_stop) {
            _scanner_eof = true;
            return Status::OK();
        }

        const TFileRangeDesc& range = _current_range;
        _current_range_path = range.path;

        if (!_partition_slot_descs.empty()) {
            // we need get partition columns first for runtime filter partition pruning
            RETURN_IF_ERROR(_generate_parititon_columns());

            if (_state->query_options().enable_runtime_filter_partition_prune) {
                // if enable_runtime_filter_partition_prune is true, we need to check whether this range can be filtered out
                // by runtime filter partition prune
                if (_push_down_conjuncts.size() < _conjuncts.size()) {
                    // there are new runtime filters, need to re-init runtime filter partition pruning ctxs
                    _init_runtime_filter_partition_prune_ctxs();
                }

                bool can_filter_all = false;
                RETURN_IF_ERROR(_process_runtime_filters_partition_prune(can_filter_all));
                if (can_filter_all) {
                    // this range can be filtered out by runtime filter partition pruning
                    // so we need to skip this range
                    COUNTER_UPDATE(_runtime_filter_partition_pruned_range_counter, 1);
                    continue;
                }
            }
        }

        // create reader for specific format
        Status init_status = Status::OK();
        TFileFormatType::type format_type = _get_current_format_type();
        // JNI reader can only push down column value range
        bool push_down_predicates =
                !_is_load && _params->format_type != TFileFormatType::FORMAT_JNI;
        // for compatibility, this logic is deprecated in 3.1
        if (format_type == TFileFormatType::FORMAT_JNI && range.__isset.table_format_params) {
            if (range.table_format_params.table_format_type == "paimon" &&
                !range.table_format_params.paimon_params.__isset.paimon_split) {
                // use native reader
                auto format = range.table_format_params.paimon_params.file_format;
                if (format == "orc") {
                    format_type = TFileFormatType::FORMAT_ORC;
                } else if (format == "parquet") {
                    format_type = TFileFormatType::FORMAT_PARQUET;
                } else {
                    return Status::InternalError("Not supported paimon file format: {}", format);
                }
            }
        }

        bool need_to_get_parsed_schema = false;
        switch (format_type) {
        case TFileFormatType::FORMAT_JNI: {
            if (range.__isset.table_format_params &&
                range.table_format_params.table_format_type == "max_compute") {
                const auto* mc_desc = static_cast<const MaxComputeTableDescriptor*>(
                        _real_tuple_desc->table_desc());
                if (!mc_desc->init_status()) {
                    return mc_desc->init_status();
                }
                std::unique_ptr<MaxComputeJniReader> mc_reader = MaxComputeJniReader::create_unique(
                        mc_desc, range.table_format_params.max_compute_params, _file_slot_descs,
                        range, _state, _profile);
                init_status = mc_reader->init_reader(_colname_to_value_range);
                _cur_reader = std::move(mc_reader);
            } else if (range.__isset.table_format_params &&
                       range.table_format_params.table_format_type == "paimon") {
                _cur_reader = PaimonJniReader::create_unique(_file_slot_descs, _state, _profile,
                                                             range, _params);
                init_status = ((PaimonJniReader*)(_cur_reader.get()))
                                      ->init_reader(_colname_to_value_range);
            } else if (range.__isset.table_format_params &&
                       range.table_format_params.table_format_type == "hudi") {
                _cur_reader = HudiJniReader::create_unique(*_params,
                                                           range.table_format_params.hudi_params,
                                                           _file_slot_descs, _state, _profile);
                init_status =
                        ((HudiJniReader*)_cur_reader.get())->init_reader(_colname_to_value_range);
            } else if (range.__isset.table_format_params &&
                       range.table_format_params.table_format_type == "lakesoul") {
                _cur_reader =
                        LakeSoulJniReader::create_unique(range.table_format_params.lakesoul_params,
                                                         _file_slot_descs, _state, _profile);
                init_status = ((LakeSoulJniReader*)_cur_reader.get())
                                      ->init_reader(_colname_to_value_range);
            } else if (range.__isset.table_format_params &&
                       range.table_format_params.table_format_type == "trino_connector") {
                _cur_reader = TrinoConnectorJniReader::create_unique(_file_slot_descs, _state,
                                                                     _profile, range);
                init_status = ((TrinoConnectorJniReader*)(_cur_reader.get()))
                                      ->init_reader(_colname_to_value_range);
            }
            break;
        }
        case TFileFormatType::FORMAT_PARQUET: {
            std::unique_ptr<ParquetReader> parquet_reader = ParquetReader::create_unique(
                    _profile, *_params, range, _state->query_options().batch_size,
                    const_cast<cctz::time_zone*>(&_state->timezone_obj()), _io_ctx.get(), _state,
                    _should_enable_file_meta_cache() ? ExecEnv::GetInstance()->file_meta_cache()
                                                     : nullptr,
                    _state->query_options().enable_parquet_lazy_mat);

            if (_row_id_column_iterator_pair.second != -1) {
                RETURN_IF_ERROR(_create_row_id_column_iterator());
                parquet_reader->set_row_id_column_iterator(_row_id_column_iterator_pair);
            }

            // ATTN: the push down agg type may be set back to NONE,
            // see IcebergTableReader::init_row_filters for example.
            parquet_reader->set_push_down_agg_type(_get_push_down_agg_type());
            if (push_down_predicates) {
                RETURN_IF_ERROR(_process_late_arrival_conjuncts());
            }
            RETURN_IF_ERROR(_init_parquet_reader(std::move(parquet_reader)));

            need_to_get_parsed_schema = true;
            break;
        }
        case TFileFormatType::FORMAT_ORC: {
            std::unique_ptr<OrcReader> orc_reader = OrcReader::create_unique(
                    _profile, _state, *_params, range, _state->query_options().batch_size,
                    _state->timezone(), _io_ctx.get(), _state->query_options().enable_orc_lazy_mat);
            if (_row_id_column_iterator_pair.second != -1) {
                RETURN_IF_ERROR(_create_row_id_column_iterator());
                orc_reader->set_row_id_column_iterator(_row_id_column_iterator_pair);
            }

            orc_reader->set_push_down_agg_type(_get_push_down_agg_type());
            if (push_down_predicates) {
                RETURN_IF_ERROR(_process_late_arrival_conjuncts());
            }
            RETURN_IF_ERROR(_init_orc_reader(std::move(orc_reader)));

            need_to_get_parsed_schema = true;
            break;
        }
        case TFileFormatType::FORMAT_CSV_PLAIN:
        case TFileFormatType::FORMAT_CSV_GZ:
        case TFileFormatType::FORMAT_CSV_BZ2:
        case TFileFormatType::FORMAT_CSV_LZ4FRAME:
        case TFileFormatType::FORMAT_CSV_LZ4BLOCK:
        case TFileFormatType::FORMAT_CSV_LZOP:
        case TFileFormatType::FORMAT_CSV_DEFLATE:
        case TFileFormatType::FORMAT_CSV_SNAPPYBLOCK:
        case TFileFormatType::FORMAT_PROTO: {
            auto reader = CsvReader::create_unique(_state, _profile, &_counter, *_params, range,
                                                   _file_slot_descs, _io_ctx.get());

            init_status = reader->init_reader(_is_load);
            _cur_reader = std::move(reader);
            break;
        }
        case TFileFormatType::FORMAT_TEXT: {
            auto reader = TextReader::create_unique(_state, _profile, &_counter, *_params, range,
                                                    _file_slot_descs, _io_ctx.get());
            init_status = reader->init_reader(_is_load);
            _cur_reader = std::move(reader);
            break;
        }
        case TFileFormatType::FORMAT_JSON: {
            _cur_reader =
                    NewJsonReader::create_unique(_state, _profile, &_counter, *_params, range,
                                                 _file_slot_descs, &_scanner_eof, _io_ctx.get());
            init_status = ((NewJsonReader*)(_cur_reader.get()))
                                  ->init_reader(_col_default_value_ctx, _is_load);
            break;
        }
        case TFileFormatType::FORMAT_AVRO: {
            _cur_reader = AvroJNIReader::create_unique(_state, _profile, *_params, _file_slot_descs,
                                                       range);
            init_status =
                    ((AvroJNIReader*)(_cur_reader.get()))->init_reader(_colname_to_value_range);
            break;
        }
        case TFileFormatType::FORMAT_WAL: {
            _cur_reader = WalReader::create_unique(_state);
            init_status = ((WalReader*)(_cur_reader.get()))->init_reader(_output_tuple_desc);
            break;
        }
        case TFileFormatType::FORMAT_ARROW: {
            _cur_reader = ArrowStreamReader::create_unique(_state, _profile, &_counter, *_params,
                                                           range, _file_slot_descs, _io_ctx.get());
            init_status = ((ArrowStreamReader*)(_cur_reader.get()))->init_reader();
            break;
        }
        default:
            return Status::NotSupported("Not supported create reader for file format: {}.",
                                        to_string(_params->format_type));
        }

        if (_cur_reader == nullptr) {
            return Status::NotSupported(
                    "Not supported create reader for table format: {} / file format: {}.",
                    range.__isset.table_format_params ? range.table_format_params.table_format_type
                                                      : "NotSet",
                    to_string(_params->format_type));
        }
        COUNTER_UPDATE(_file_counter, 1);
        // The FileScanner for external table may try to open not exist files,
        // Because FE file cache for external table may out of date.
        // So, NOT_FOUND for FileScanner is not a fail case.
        // Will remove this after file reader refactor.
        if (init_status.is<END_OF_FILE>()) {
            COUNTER_UPDATE(_empty_file_counter, 1);
            continue;
        } else if (init_status.is<ErrorCode::NOT_FOUND>()) {
            if (config::ignore_not_found_file_in_external_table) {
                COUNTER_UPDATE(_not_found_file_counter, 1);
                continue;
            }
            return Status::InternalError("failed to find reader, err: {}", init_status.to_string());
        } else if (!init_status.ok()) {
            return Status::InternalError("failed to init reader, err: {}", init_status.to_string());
        }

        _cur_reader->set_push_down_agg_type(_get_push_down_agg_type());
        RETURN_IF_ERROR(_set_fill_or_truncate_columns(need_to_get_parsed_schema));
        _cur_reader_eof = false;
        break;
    }
    return Status::OK();
}

Status FileScanner::_init_parquet_reader(std::unique_ptr<ParquetReader>&& parquet_reader) {
    const TFileRangeDesc& range = _current_range;
    Status init_status = Status::OK();

    if (range.__isset.table_format_params &&
        range.table_format_params.table_format_type == "iceberg") {
        std::unique_ptr<IcebergParquetReader> iceberg_reader =
                IcebergParquetReader::create_unique(std::move(parquet_reader), _profile, _state,
                                                    *_params, range, _kv_cache, _io_ctx.get());
        init_status = iceberg_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), _col_name_to_slot_id,
                &_not_single_slot_filter_conjuncts, &_slot_id_to_filter_conjuncts);
        _cur_reader = std::move(iceberg_reader);
    } else if (range.__isset.table_format_params &&
               range.table_format_params.table_format_type == "paimon") {
        std::unique_ptr<PaimonParquetReader> paimon_reader = PaimonParquetReader::create_unique(
                std::move(parquet_reader), _profile, _state, *_params, range, _io_ctx.get());
        init_status = paimon_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), _col_name_to_slot_id,
                &_not_single_slot_filter_conjuncts, &_slot_id_to_filter_conjuncts);
        RETURN_IF_ERROR(paimon_reader->init_row_filters());
        _cur_reader = std::move(paimon_reader);
    } else if (range.__isset.table_format_params &&
               range.table_format_params.table_format_type == "hudi") {
        std::unique_ptr<HudiParquetReader> hudi_reader = HudiParquetReader::create_unique(
                std::move(parquet_reader), _profile, _state, *_params, range, _io_ctx.get());
        init_status = hudi_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), _col_name_to_slot_id,
                &_not_single_slot_filter_conjuncts, &_slot_id_to_filter_conjuncts);
        _cur_reader = std::move(hudi_reader);
    } else if (range.table_format_params.table_format_type == "hive") {
        auto hive_reader = HiveParquetReader::create_unique(std::move(parquet_reader), _profile,
                                                            _state, *_params, range, _io_ctx.get());
        init_status = hive_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), _col_name_to_slot_id,
                &_not_single_slot_filter_conjuncts, &_slot_id_to_filter_conjuncts);
        _cur_reader = std::move(hive_reader);
    } else if (range.table_format_params.table_format_type == "tvf") {
        const FieldDescriptor* parquet_meta = nullptr;
        RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&parquet_meta));
        DCHECK(parquet_meta != nullptr);

        // TVF will first `get_parsed_schema` to obtain file information from BE, and FE will convert
        // the column names to lowercase (because the query process is case-insensitive),
        // so the lowercase file column names are used here to match the read columns.
        std::shared_ptr<TableSchemaChangeHelper::Node> tvf_info_node = nullptr;
        RETURN_IF_ERROR(TableSchemaChangeHelper::BuildTableInfoUtil::by_parquet_name(
                _real_tuple_desc, *parquet_meta, tvf_info_node));
        init_status = parquet_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), _col_name_to_slot_id,
                &_not_single_slot_filter_conjuncts, &_slot_id_to_filter_conjuncts, tvf_info_node);
        _cur_reader = std::move(parquet_reader);
    } else if (_is_load) {
        const FieldDescriptor* parquet_meta = nullptr;
        RETURN_IF_ERROR(parquet_reader->get_file_metadata_schema(&parquet_meta));
        DCHECK(parquet_meta != nullptr);

        // Load is case-insensitive, so you to match the columns in the file.
        std::map<std::string, std::string> file_lower_name_to_native;
        for (const auto& parquet_field : parquet_meta->get_fields_schema()) {
            file_lower_name_to_native.emplace(doris::to_lower(parquet_field.name),
                                              parquet_field.name);
        }
        auto load_info_node = std::make_shared<TableSchemaChangeHelper::StructNode>();
        for (const auto slot : _real_tuple_desc->slots()) {
            if (file_lower_name_to_native.contains(slot->col_name())) {
                load_info_node->add_children(slot->col_name(),
                                             file_lower_name_to_native[slot->col_name()],
                                             TableSchemaChangeHelper::ConstNode::get_instance());
                // For Load, `file_scanner` will create block columns using the file type,
                // there is no schema change when reading inside the struct,
                // so use `TableSchemaChangeHelper::ConstNode`.
            } else {
                load_info_node->add_not_exist_children(slot->col_name());
            }
        }

        init_status = parquet_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), _col_name_to_slot_id,
                &_not_single_slot_filter_conjuncts, &_slot_id_to_filter_conjuncts, load_info_node);
        _cur_reader = std::move(parquet_reader);
    }

    return init_status;
}

Status FileScanner::_init_orc_reader(std::unique_ptr<OrcReader>&& orc_reader) {
    const TFileRangeDesc& range = _current_range;
    Status init_status = Status::OK();

    if (range.__isset.table_format_params &&
        range.table_format_params.table_format_type == "transactional_hive") {
        std::unique_ptr<TransactionalHiveReader> tran_orc_reader =
                TransactionalHiveReader::create_unique(std::move(orc_reader), _profile, _state,
                                                       *_params, range, _io_ctx.get());
        init_status = tran_orc_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), &_not_single_slot_filter_conjuncts,
                &_slot_id_to_filter_conjuncts);
        RETURN_IF_ERROR(tran_orc_reader->init_row_filters());
        _cur_reader = std::move(tran_orc_reader);
    } else if (range.__isset.table_format_params &&
               range.table_format_params.table_format_type == "iceberg") {
        std::unique_ptr<IcebergOrcReader> iceberg_reader = IcebergOrcReader::create_unique(
                std::move(orc_reader), _profile, _state, *_params, range, _kv_cache, _io_ctx.get());

        init_status = iceberg_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), _col_name_to_slot_id,
                &_not_single_slot_filter_conjuncts, &_slot_id_to_filter_conjuncts);
        _cur_reader = std::move(iceberg_reader);
    } else if (range.__isset.table_format_params &&
               range.table_format_params.table_format_type == "paimon") {
        std::unique_ptr<PaimonOrcReader> paimon_reader = PaimonOrcReader::create_unique(
                std::move(orc_reader), _profile, _state, *_params, range, _io_ctx.get());

        init_status = paimon_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), &_not_single_slot_filter_conjuncts,
                &_slot_id_to_filter_conjuncts);
        RETURN_IF_ERROR(paimon_reader->init_row_filters());
        _cur_reader = std::move(paimon_reader);
    } else if (range.__isset.table_format_params &&
               range.table_format_params.table_format_type == "hudi") {
        std::unique_ptr<HudiOrcReader> hudi_reader = HudiOrcReader::create_unique(
                std::move(orc_reader), _profile, _state, *_params, range, _io_ctx.get());

        init_status = hudi_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), &_not_single_slot_filter_conjuncts,
                &_slot_id_to_filter_conjuncts);
        _cur_reader = std::move(hudi_reader);
    } else if (range.__isset.table_format_params &&
               range.table_format_params.table_format_type == "hive") {
        std::unique_ptr<HiveOrcReader> hive_reader = HiveOrcReader::create_unique(
                std::move(orc_reader), _profile, _state, *_params, range, _io_ctx.get());

        init_status = hive_reader->init_reader(
                _file_col_names, _colname_to_value_range, _push_down_conjuncts, _real_tuple_desc,
                _default_val_row_desc.get(), &_not_single_slot_filter_conjuncts,
                &_slot_id_to_filter_conjuncts);
        _cur_reader = std::move(hive_reader);
    } else if (range.__isset.table_format_params &&
               range.table_format_params.table_format_type == "tvf") {
        const orc::Type* orc_type_ptr = nullptr;
        RETURN_IF_ERROR(orc_reader->get_file_type(&orc_type_ptr));

        std::shared_ptr<TableSchemaChangeHelper::Node> tvf_info_node = nullptr;
        RETURN_IF_ERROR(TableSchemaChangeHelper::BuildTableInfoUtil::by_orc_name(
                _real_tuple_desc, orc_type_ptr, tvf_info_node));
        init_status = orc_reader->init_reader(
                &_file_col_names, _colname_to_value_range, _push_down_conjuncts, false,
                _real_tuple_desc, _default_val_row_desc.get(), &_not_single_slot_filter_conjuncts,
                &_slot_id_to_filter_conjuncts, tvf_info_node);
        _cur_reader = std::move(orc_reader);
    } else if (_is_load) {
        const orc::Type* orc_type_ptr = nullptr;
        RETURN_IF_ERROR(orc_reader->get_file_type(&orc_type_ptr));

        std::map<std::string, std::string> file_lower_name_to_native;
        for (uint64_t idx = 0; idx < orc_type_ptr->getSubtypeCount(); idx++) {
            file_lower_name_to_native.emplace(doris::to_lower(orc_type_ptr->getFieldName(idx)),
                                              orc_type_ptr->getFieldName(idx));
        }

        auto load_info_node = std::make_shared<TableSchemaChangeHelper::StructNode>();
        for (const auto slot : _real_tuple_desc->slots()) {
            if (file_lower_name_to_native.contains(slot->col_name())) {
                load_info_node->add_children(slot->col_name(),
                                             file_lower_name_to_native[slot->col_name()],
                                             TableSchemaChangeHelper::ConstNode::get_instance());
            } else {
                load_info_node->add_not_exist_children(slot->col_name());
            }
        }
        init_status = orc_reader->init_reader(
                &_file_col_names, _colname_to_value_range, _push_down_conjuncts, false,
                _real_tuple_desc, _default_val_row_desc.get(), &_not_single_slot_filter_conjuncts,
                &_slot_id_to_filter_conjuncts, load_info_node);
        _cur_reader = std::move(orc_reader);
    }

    return init_status;
}

Status FileScanner::_set_fill_or_truncate_columns(bool need_to_get_parsed_schema) {
    _missing_cols.clear();
    _slot_lower_name_to_col_type.clear();
    std::unordered_map<std::string, DataTypePtr> name_to_col_type;
    RETURN_IF_ERROR(_cur_reader->get_columns(&name_to_col_type, &_missing_cols));
    for (const auto& [col_name, col_type] : name_to_col_type) {
        _slot_lower_name_to_col_type.emplace(to_lower(col_name), col_type);
    }

    RETURN_IF_ERROR(_generate_missing_columns());
    RETURN_IF_ERROR(_cur_reader->set_fill_columns(_partition_col_descs, _missing_col_descs));
    if (VLOG_NOTICE_IS_ON && !_missing_cols.empty() && _is_load) {
        fmt::memory_buffer col_buf;
        for (auto& col : _missing_cols) {
            fmt::format_to(col_buf, " {}", col);
        }
        VLOG_NOTICE << fmt::format("Unknown columns:{} in file {}", fmt::to_string(col_buf),
                                   _current_range.path);
    }

    RETURN_IF_ERROR(_generate_truncate_columns(need_to_get_parsed_schema));
    return Status::OK();
}

Status FileScanner::_generate_truncate_columns(bool need_to_get_parsed_schema) {
    _source_file_col_name_types.clear();
    //  The col names and types of source file, such as parquet, orc files.
    if (_state->query_options().truncate_char_or_varchar_columns && need_to_get_parsed_schema) {
        std::vector<std::string> source_file_col_names;
        std::vector<DataTypePtr> source_file_col_types;
        Status status =
                _cur_reader->get_parsed_schema(&source_file_col_names, &source_file_col_types);
        if (!status.ok() && status.code() != TStatusCode::NOT_IMPLEMENTED_ERROR) {
            return status;
        }
        DCHECK(source_file_col_names.size() == source_file_col_types.size());
        for (int i = 0; i < source_file_col_names.size(); ++i) {
            _source_file_col_name_types[to_lower(source_file_col_names[i])] =
                    source_file_col_types[i];
        }
    }
    return Status::OK();
}

Status FileScanner::prepare_for_read_lines(const TFileRangeDesc& range) {
    _current_range = range;

    _file_cache_statistics.reset(new io::FileCacheStatistics());
    _file_reader_stats.reset(new io::FileReaderStats());

    RETURN_IF_ERROR(_init_io_ctx());
    _io_ctx->file_cache_stats = _file_cache_statistics.get();
    _io_ctx->file_reader_stats = _file_reader_stats.get();
    _default_val_row_desc.reset(new RowDescriptor((TupleDescriptor*)_real_tuple_desc, false));
    RETURN_IF_ERROR(_init_expr_ctxes());

    // Since only one column is read from the file, there is no need to filter, so set these variables to empty.
    static std::unordered_map<std::string, ColumnValueRangeType> colname_to_value_range;
    _colname_to_value_range = &colname_to_value_range;
    _push_down_conjuncts.clear();
    _not_single_slot_filter_conjuncts.clear();
    _slot_id_to_filter_conjuncts.clear();
    _kv_cache = nullptr;
    return Status::OK();
}

Status FileScanner::read_lines_from_range(const TFileRangeDesc& range,
                                          const std::list<int64_t>& row_ids, Block* result_block,
                                          const ExternalFileMappingInfo& external_info,
                                          int64_t* init_reader_ms, int64_t* get_block_ms) {
    _current_range = range;
    RETURN_IF_ERROR(_generate_parititon_columns());

    TFileFormatType::type format_type = _get_current_format_type();
    Status init_status = Status::OK();

    RETURN_IF_ERROR(scope_timer_run(
            [&]() -> Status {
                switch (format_type) {
                case TFileFormatType::FORMAT_PARQUET: {
                    std::unique_ptr<vectorized::ParquetReader> parquet_reader =
                            vectorized::ParquetReader::create_unique(
                                    _profile, *_params, range, 1,
                                    const_cast<cctz::time_zone*>(&_state->timezone_obj()),
                                    _io_ctx.get(), _state,
                                    external_info.enable_file_meta_cache
                                            ? ExecEnv::GetInstance()->file_meta_cache()
                                            : nullptr,
                                    false);

                    RETURN_IF_ERROR(parquet_reader->set_read_lines_mode(row_ids));
                    RETURN_IF_ERROR(_init_parquet_reader(std::move(parquet_reader)));
                    break;
                }
                case TFileFormatType::FORMAT_ORC: {
                    std::unique_ptr<vectorized::OrcReader> orc_reader =
                            vectorized::OrcReader::create_unique(_profile, _state, *_params, range,
                                                                 1, _state->timezone(),
                                                                 _io_ctx.get(), false);

                    RETURN_IF_ERROR(orc_reader->set_read_lines_mode(row_ids));
                    RETURN_IF_ERROR(_init_orc_reader(std::move(orc_reader)));
                    break;
                }
                default: {
                    return Status::NotSupported(
                            "Not support create lines reader for file format: {},"
                            "only support parquet and orc.",
                            to_string(_params->format_type));
                }
                }
                return Status::OK();
            },
            init_reader_ms));

    RETURN_IF_ERROR(_set_fill_or_truncate_columns(true));
    _cur_reader_eof = false;

    RETURN_IF_ERROR(scope_timer_run(
            [&]() -> Status {
                while (!_cur_reader_eof) {
                    bool eof = false;
                    RETURN_IF_ERROR(_get_block_impl(_state, result_block, &eof));
                }
                return Status::OK();
            },
            get_block_ms));

    _cur_reader->collect_profile_before_close();
    RETURN_IF_ERROR(_cur_reader->close());
    return Status::OK();
}

Status FileScanner::_generate_parititon_columns() {
    _partition_col_descs.clear();
    const TFileRangeDesc& range = _current_range;
    if (range.__isset.columns_from_path && !_partition_slot_descs.empty()) {
        for (const auto& slot_desc : _partition_slot_descs) {
            if (slot_desc) {
                auto it = _partition_slot_index_map.find(slot_desc->id());
                if (it == std::end(_partition_slot_index_map)) {
                    return Status::InternalError("Unknown source slot descriptor, slot_id={}",
                                                 slot_desc->id());
                }
                const std::string& column_from_path = range.columns_from_path[it->second];
                const char* data = column_from_path.c_str();
                size_t size = column_from_path.size();
                if (size == 4 && memcmp(data, "null", 4) == 0) {
                    data = const_cast<char*>("\\N");
                }
                _partition_col_descs.emplace(slot_desc->col_name(),
                                             std::make_tuple(data, slot_desc));
            }
        }
    }
    return Status::OK();
}

Status FileScanner::_generate_missing_columns() {
    _missing_col_descs.clear();
    if (!_missing_cols.empty()) {
        for (auto slot_desc : _real_tuple_desc->slots()) {
            if (!slot_desc->is_materialized()) {
                continue;
            }
            if (_missing_cols.find(slot_desc->col_name()) == _missing_cols.end()) {
                continue;
            }

            auto it = _col_default_value_ctx.find(slot_desc->col_name());
            if (it == _col_default_value_ctx.end()) {
                return Status::InternalError("failed to find default value expr for slot: {}",
                                             slot_desc->col_name());
            }
            _missing_col_descs.emplace(slot_desc->col_name(), it->second);
        }
    }
    return Status::OK();
}

Status FileScanner::_init_expr_ctxes() {
    std::map<SlotId, int> full_src_index_map;
    std::map<SlotId, SlotDescriptor*> full_src_slot_map;
    std::map<std::string, int> partition_name_to_key_index_map;
    int index = 0;
    for (const auto& slot_desc : _real_tuple_desc->slots()) {
        full_src_slot_map.emplace(slot_desc->id(), slot_desc);
        full_src_index_map.emplace(slot_desc->id(), index++);
    }

    // For external table query, find the index of column in path.
    // Because query doesn't always search for all columns in a table
    // and the order of selected columns is random.
    // All ranges in _ranges vector should have identical columns_from_path_keys
    // because they are all file splits for the same external table.
    // So here use the first element of _ranges to fill the partition_name_to_key_index_map
    if (_current_range.__isset.columns_from_path_keys) {
        std::vector<std::string> key_map = _current_range.columns_from_path_keys;
        if (!key_map.empty()) {
            for (size_t i = 0; i < key_map.size(); i++) {
                partition_name_to_key_index_map.emplace(key_map[i], i);
            }
        }
    }

    _num_of_columns_from_file = _params->num_of_columns_from_file;

    for (const auto& slot_info : _params->required_slots) {
        auto slot_id = slot_info.slot_id;
        auto it = full_src_slot_map.find(slot_id);
        if (it == std::end(full_src_slot_map)) {
            return Status::InternalError(
                    fmt::format("Unknown source slot descriptor, slot_id={}", slot_id));
        }
        if (it->second->col_name().starts_with(BeConsts::GLOBAL_ROWID_COL)) {
            _row_id_column_iterator_pair.second = _default_val_row_desc->get_column_id(slot_id);
            continue;
        }
        if (slot_info.is_file_slot) {
            _file_slot_descs.emplace_back(it->second);
            _file_col_names.push_back(it->second->col_name());
        } else {
            _partition_slot_descs.emplace_back(it->second);
            if (_is_load) {
                auto iti = full_src_index_map.find(slot_id);
                _partition_slot_index_map.emplace(slot_id, iti->second - _num_of_columns_from_file);
            } else {
                auto kit = partition_name_to_key_index_map.find(it->second->col_name());
                _partition_slot_index_map.emplace(slot_id, kit->second);
            }
        }
    }

    // set column name to default value expr map
    for (auto slot_desc : _real_tuple_desc->slots()) {
        if (!slot_desc->is_materialized()) {
            continue;
        }
        vectorized::VExprContextSPtr ctx;
        auto it = _params->default_value_of_src_slot.find(slot_desc->id());
        if (it != std::end(_params->default_value_of_src_slot)) {
            if (!it->second.nodes.empty()) {
                RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(it->second, ctx));
                RETURN_IF_ERROR(ctx->prepare(_state, *_default_val_row_desc));
                RETURN_IF_ERROR(ctx->open(_state));
            }
            // if expr is empty, the default value will be null
            _col_default_value_ctx.emplace(slot_desc->col_name(), ctx);
        }
    }

    if (_is_load) {
        // follow desc expr map is only for load task.
        bool has_slot_id_map = _params->__isset.dest_sid_to_src_sid_without_trans;
        int idx = 0;
        for (auto slot_desc : _output_tuple_desc->slots()) {
            if (!slot_desc->is_materialized()) {
                continue;
            }
            auto it = _params->expr_of_dest_slot.find(slot_desc->id());
            if (it == std::end(_params->expr_of_dest_slot)) {
                return Status::InternalError("No expr for dest slot, id={}, name={}",
                                             slot_desc->id(), slot_desc->col_name());
            }

            vectorized::VExprContextSPtr ctx;
            if (!it->second.nodes.empty()) {
                RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(it->second, ctx));
                RETURN_IF_ERROR(ctx->prepare(_state, *_src_row_desc));
                RETURN_IF_ERROR(ctx->open(_state));
            }
            _dest_vexpr_ctx.emplace_back(ctx);
            _dest_slot_name_to_idx[slot_desc->col_name()] = idx++;

            if (has_slot_id_map) {
                auto it1 = _params->dest_sid_to_src_sid_without_trans.find(slot_desc->id());
                if (it1 == std::end(_params->dest_sid_to_src_sid_without_trans)) {
                    _src_slot_descs_order_by_dest.emplace_back(nullptr);
                } else {
                    auto _src_slot_it = full_src_slot_map.find(it1->second);
                    if (_src_slot_it == std::end(full_src_slot_map)) {
                        return Status::InternalError("No src slot {} in src slot descs",
                                                     it1->second);
                    }
                    _dest_slot_to_src_slot_index.emplace(_src_slot_descs_order_by_dest.size(),
                                                         full_src_index_map[_src_slot_it->first]);
                    _src_slot_descs_order_by_dest.emplace_back(_src_slot_it->second);
                }
            }
        }
    }
    return Status::OK();
}

Status FileScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }

    if (_cur_reader) {
        RETURN_IF_ERROR(_cur_reader->close());
    }

    RETURN_IF_ERROR(Scanner::close(state));
    return Status::OK();
}

void FileScanner::try_stop() {
    Scanner::try_stop();
    if (_io_ctx) {
        _io_ctx->should_stop = true;
    }
}

void FileScanner::update_realtime_counters() {
    pipeline::FileScanLocalState* local_state =
            static_cast<pipeline::FileScanLocalState*>(_local_state);

    COUNTER_UPDATE(local_state->_scan_bytes, _file_reader_stats->read_bytes);
    COUNTER_UPDATE(local_state->_scan_rows, _file_reader_stats->read_rows);

    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_rows(
            _file_reader_stats->read_rows);
    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes(
            _file_reader_stats->read_bytes);

    if (_file_cache_statistics->bytes_read_from_local == 0 &&
        _file_cache_statistics->bytes_read_from_remote == 0) {
        _state->get_query_ctx()
                ->resource_ctx()
                ->io_context()
                ->update_scan_bytes_from_remote_storage(_file_reader_stats->read_bytes);
        DorisMetrics::instance()->query_scan_bytes_from_local->increment(
                _file_reader_stats->read_bytes);
    } else {
        _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes_from_local_storage(
                _file_cache_statistics->bytes_read_from_local);
        _state->get_query_ctx()
                ->resource_ctx()
                ->io_context()
                ->update_scan_bytes_from_remote_storage(
                        _file_cache_statistics->bytes_read_from_remote);
        DorisMetrics::instance()->query_scan_bytes_from_local->increment(
                _file_cache_statistics->bytes_read_from_local);
        DorisMetrics::instance()->query_scan_bytes_from_remote->increment(
                _file_cache_statistics->bytes_read_from_remote);
    }

    COUNTER_UPDATE(_file_read_bytes_counter, _file_reader_stats->read_bytes);

    DorisMetrics::instance()->query_scan_bytes->increment(_file_reader_stats->read_bytes);
    DorisMetrics::instance()->query_scan_rows->increment(_file_reader_stats->read_rows);

    _file_reader_stats->read_bytes = 0;
    _file_reader_stats->read_rows = 0;
    _file_cache_statistics->bytes_read_from_local = 0;
    _file_cache_statistics->bytes_read_from_remote = 0;
}

void FileScanner::_collect_profile_before_close() {
    Scanner::_collect_profile_before_close();
    if (config::enable_file_cache && _state->query_options().enable_file_cache &&
        _profile != nullptr) {
        io::FileCacheProfileReporter cache_profile(_profile);
        cache_profile.update(_file_cache_statistics.get());
    }

    if (_cur_reader != nullptr) {
        _cur_reader->collect_profile_before_close();
    }

    pipeline::FileScanLocalState* local_state =
            static_cast<pipeline::FileScanLocalState*>(_local_state);
    COUNTER_UPDATE(local_state->_scan_bytes, _file_reader_stats->read_bytes);
    COUNTER_UPDATE(local_state->_scan_rows, _file_reader_stats->read_rows);

    COUNTER_UPDATE(_file_read_bytes_counter, _file_reader_stats->read_bytes);
    COUNTER_UPDATE(_file_read_calls_counter, _file_reader_stats->read_calls);
    COUNTER_UPDATE(_file_read_time_counter, _file_reader_stats->read_time_ns);

    DorisMetrics::instance()->query_scan_bytes->increment(_file_reader_stats->read_bytes);
    DorisMetrics::instance()->query_scan_rows->increment(_file_reader_stats->read_rows);
}

} // namespace doris::vectorized
