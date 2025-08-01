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

#include "vec/exec/scan/olap_scanner.h"

#include <gen_cpp/Descriptors_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stdlib.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <atomic>
#include <iterator>
#include <ostream>
#include <set>

#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet_hotspot.h"
#include "cloud/config.h"
#include "common/config.h"
#include "common/consts.h"
#include "common/logging.h"
#include "exec/olap_utils.h"
#include "exprs/function_filter.h"
#include "io/cache/block_file_cache_profile.h"
#include "io/io_common.h"
#include "olap/id_manager.h"
#include "olap/inverted_index_profile.h"
#include "olap/olap_common.h"
#include "olap/olap_tuple.h"
#include "olap/schema_cache.h"
#include "olap/storage_engine.h"
#include "olap/tablet_schema.h"
#include "pipeline/exec/olap_scan_operator.h"
#include "runtime/descriptors.h"
#include "runtime/exec_env.h"
#include "runtime/runtime_state.h"
#include "service/backend_options.h"
#include "util/doris_metrics.h"
#include "util/runtime_profile.h"
#include "vec/common/schema_util.h"
#include "vec/core/block.h"
#include "vec/exec/scan/scan_node.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/json/path_in_data.h"
#include "vec/olap/block_reader.h"

namespace doris::vectorized {

using ReadSource = TabletReader::ReadSource;

OlapScanner::OlapScanner(pipeline::ScanLocalStateBase* parent, OlapScanner::Params&& params)
        : Scanner(params.state, parent, params.limit, params.profile),
          _key_ranges(std::move(params.key_ranges)),
          _tablet_reader_params({
                  .tablet = std::move(params.tablet),
                  .tablet_schema {},
                  .aggregation = params.aggregation,
                  .version = {0, params.version},
                  .start_key {},
                  .end_key {},
                  .conditions {},
                  .bloom_filters {},
                  .bitmap_filters {},
                  .in_filters {},
                  .function_filters {},
                  .delete_predicates {},
                  .target_cast_type_for_variants {},
                  .rs_splits {},
                  .return_columns {},
                  .output_columns {},
                  .remaining_conjunct_roots {},
                  .common_expr_ctxs_push_down {},
                  .topn_filter_source_node_ids {},
                  .filter_block_conjuncts {},
                  .key_group_cluster_key_idxes {},
                  .virtual_column_exprs {},
                  .vir_cid_to_idx_in_block {},
                  .vir_col_idx_to_type {},
          }) {
    _tablet_reader_params.set_read_source(std::move(params.read_source));
    _is_init = false;
}

static std::string read_columns_to_string(TabletSchemaSPtr tablet_schema,
                                          const std::vector<uint32_t>& read_columns) {
    // avoid too long for one line,
    // it is hard to display in `show profile` stmt if one line is too long.
    const int col_per_line = 10;
    int i = 0;
    std::string read_columns_string;
    read_columns_string += "[";
    for (auto it = read_columns.cbegin(); it != read_columns.cend(); it++) {
        if (it != read_columns.cbegin()) {
            read_columns_string += ", ";
        }
        read_columns_string += tablet_schema->columns().at(*it)->name();
        if (i >= col_per_line) {
            read_columns_string += "\n";
            i = 0;
        } else {
            ++i;
        }
    }
    read_columns_string += "]";
    return read_columns_string;
}

Status OlapScanner::init() {
    _is_init = true;
    auto* local_state = static_cast<pipeline::OlapScanLocalState*>(_local_state);
    auto& tablet = _tablet_reader_params.tablet;
    auto& tablet_schema = _tablet_reader_params.tablet_schema;

    for (auto& ctx : local_state->_common_expr_ctxs_push_down) {
        VExprContextSPtr context;
        RETURN_IF_ERROR(ctx->clone(_state, context));
        _common_expr_ctxs_push_down.emplace_back(context);
    }

    for (auto pair : local_state->_slot_id_to_virtual_column_expr) {
        // Scanner will be executed in a different thread, so we need to clone the context.
        VExprContextSPtr context;
        RETURN_IF_ERROR(pair.second->clone(_state, context));
        _slot_id_to_virtual_column_expr[pair.first] = context;
    }

    _slot_id_to_index_in_block = local_state->_slot_id_to_index_in_block;
    _slot_id_to_col_type = local_state->_slot_id_to_col_type;

    // set limit to reduce end of rowset and segment mem use
    _tablet_reader = std::make_unique<BlockReader>();
    // batch size is passed down to segment iterator, use _state->batch_size()
    // instead of _parent->limit(), because if _parent->limit() is a very small
    // value (e.g. select a from t where a .. and b ... limit 1),
    // it will be very slow when reading data in segment iterator
    _tablet_reader->set_batch_size(_state->batch_size());

    TabletSchemaSPtr cached_schema;
    std::string schema_key;
    {
        TOlapScanNode& olap_scan_node = local_state->olap_scan_node();
        if (olap_scan_node.__isset.schema_version && olap_scan_node.__isset.columns_desc &&
            !olap_scan_node.columns_desc.empty() &&
            olap_scan_node.columns_desc[0].col_unique_id >= 0 && // Why check first column?
            tablet->tablet_schema()->num_variant_columns() == 0 &&
            tablet->tablet_schema()->num_virtual_columns() == 0) {
            schema_key =
                    SchemaCache::get_schema_key(tablet->tablet_id(), olap_scan_node.columns_desc,
                                                olap_scan_node.schema_version);
            cached_schema = SchemaCache::instance()->get_schema(schema_key);
        }
        if (cached_schema) {
            tablet_schema = cached_schema;
        } else {
            tablet_schema = std::make_shared<TabletSchema>();
            tablet_schema->copy_from(*tablet->tablet_schema());
            if (olap_scan_node.__isset.columns_desc && !olap_scan_node.columns_desc.empty() &&
                olap_scan_node.columns_desc[0].col_unique_id >= 0) {
                // Originally scanner get TabletSchema from tablet object in BE.
                // To support lightweight schema change for adding / dropping columns,
                // tabletschema is bounded to rowset and tablet's schema maybe outdated,
                //  so we have to use schema from a query plan witch FE puts it in query plans.
                tablet_schema->clear_columns();
                for (const auto& column_desc : olap_scan_node.columns_desc) {
                    tablet_schema->append_column(TabletColumn(column_desc));
                }
                if (olap_scan_node.__isset.schema_version) {
                    tablet_schema->set_schema_version(olap_scan_node.schema_version);
                }
            }
            if (olap_scan_node.__isset.indexes_desc) {
                tablet_schema->update_indexes_from_thrift(olap_scan_node.indexes_desc);
            }
        }

        if (_tablet_reader_params.rs_splits.empty()) {
            // Non-pipeline mode, Tablet : Scanner = 1 : 1
            // acquire tablet rowset readers at the beginning of the scan node
            // to prevent this case: when there are lots of olap scanners to run for example 10000
            // the rowsets maybe compacted when the last olap scanner starts
            ReadSource read_source;

            if (config::is_cloud_mode()) {
                // FIXME(plat1ko): Avoid pointer cast
                ExecEnv::GetInstance()->storage_engine().to_cloud().tablet_hotspot().count(*tablet);
            }

            auto st = tablet->capture_rs_readers(_tablet_reader_params.version,
                                                 &read_source.rs_splits,
                                                 _state->skip_missing_version());
            if (!st.ok()) {
                LOG(WARNING) << "fail to init reader.res=" << st;
                return st;
            }
            if (config::enable_mow_verbose_log && tablet->enable_unique_key_merge_on_write()) {
                LOG_INFO("finish capture_rs_readers for tablet={}, query_id={}",
                         tablet->tablet_id(), print_id(_state->query_id()));
            }

            if (!_state->skip_delete_predicate()) {
                read_source.fill_delete_predicates();
            }
            _tablet_reader_params.set_read_source(std::move(read_source));
        }

        // Initialize tablet_reader_params
        RETURN_IF_ERROR(_init_tablet_reader_params(_key_ranges, local_state->_olap_filters,
                                                   local_state->_filter_predicates,
                                                   local_state->_push_down_functions));
    }

    // add read columns in profile
    if (_state->enable_profile()) {
        _profile->add_info_string("ReadColumns",
                                  read_columns_to_string(tablet_schema, _return_columns));
    }

    if (!cached_schema && !schema_key.empty()) {
        SchemaCache::instance()->insert_schema(schema_key, tablet_schema);
    }

    return Status::OK();
}

Status OlapScanner::open(RuntimeState* state) {
    RETURN_IF_ERROR(Scanner::open(state));
    SCOPED_TIMER(_local_state->cast<pipeline::OlapScanLocalState>()._reader_init_timer);

    auto res = _tablet_reader->init(_tablet_reader_params);
    if (!res.ok()) {
        std::stringstream ss;
        ss << "failed to initialize storage reader. tablet="
           << _tablet_reader_params.tablet->tablet_id() << ", res=" << res
           << ", backend=" << BackendOptions::get_localhost();
        return Status::InternalError(ss.str());
    }

    // Do not hold rs_splits any more to release memory.
    _tablet_reader_params.rs_splits.clear();

    return Status::OK();
}

// it will be called under tablet read lock because capture rs readers need
Status OlapScanner::_init_tablet_reader_params(
        const std::vector<OlapScanRange*>& key_ranges,
        const std::vector<FilterOlapParam<TCondition>>& filters,
        const pipeline::FilterPredicates& filter_predicates,
        const std::vector<FunctionFilter>& function_filters) {
    // if the table with rowset [0-x] or [0-1] [2-y], and [0-1] is empty
    const bool single_version = _tablet_reader_params.has_single_version();

    if (_state->skip_storage_engine_merge()) {
        _tablet_reader_params.direct_mode = true;
        _tablet_reader_params.aggregation = true;
    } else {
        auto push_down_agg_type = _local_state->get_push_down_agg_type();
        _tablet_reader_params.direct_mode = _tablet_reader_params.aggregation || single_version ||
                                            (push_down_agg_type != TPushAggOp::NONE &&
                                             push_down_agg_type != TPushAggOp::COUNT_ON_INDEX);
    }

    RETURN_IF_ERROR(_init_variant_columns());
    RETURN_IF_ERROR(_init_return_columns());

    _tablet_reader_params.reader_type = ReaderType::READER_QUERY;
    _tablet_reader_params.push_down_agg_type_opt = _local_state->get_push_down_agg_type();

    // TODO: If a new runtime filter arrives after `_conjuncts` move to `_common_expr_ctxs_push_down`,
    if (_common_expr_ctxs_push_down.empty()) {
        for (auto& conjunct : _conjuncts) {
            _tablet_reader_params.remaining_conjunct_roots.emplace_back(conjunct->root());
        }
    } else {
        for (auto& ctx : _common_expr_ctxs_push_down) {
            _tablet_reader_params.remaining_conjunct_roots.emplace_back(ctx->root());
        }
    }

    _tablet_reader_params.common_expr_ctxs_push_down = _common_expr_ctxs_push_down;
    _tablet_reader_params.virtual_column_exprs = _virtual_column_exprs;
    _tablet_reader_params.vir_cid_to_idx_in_block = _vir_cid_to_idx_in_block;
    _tablet_reader_params.vir_col_idx_to_type = _vir_col_idx_to_type;
    _tablet_reader_params.output_columns =
            ((pipeline::OlapScanLocalState*)_local_state)->_maybe_read_column_ids;
    for (const auto& ele :
         ((pipeline::OlapScanLocalState*)_local_state)->_cast_types_for_variants) {
        _tablet_reader_params.target_cast_type_for_variants[ele.first] =
                ele.second->get_primitive_type();
    };
    // Condition
    for (auto& filter : filters) {
        _tablet_reader_params.conditions.push_back(filter);
    }

    std::copy(filter_predicates.bloom_filters.cbegin(), filter_predicates.bloom_filters.cend(),
              std::inserter(_tablet_reader_params.bloom_filters,
                            _tablet_reader_params.bloom_filters.begin()));
    std::copy(filter_predicates.bitmap_filters.cbegin(), filter_predicates.bitmap_filters.cend(),
              std::inserter(_tablet_reader_params.bitmap_filters,
                            _tablet_reader_params.bitmap_filters.begin()));

    std::copy(filter_predicates.in_filters.cbegin(), filter_predicates.in_filters.cend(),
              std::inserter(_tablet_reader_params.in_filters,
                            _tablet_reader_params.in_filters.begin()));

    std::copy(function_filters.cbegin(), function_filters.cend(),
              std::inserter(_tablet_reader_params.function_filters,
                            _tablet_reader_params.function_filters.begin()));

    auto& tablet = _tablet_reader_params.tablet;
    auto& tablet_schema = _tablet_reader_params.tablet_schema;
    // Merge the columns in delete predicate that not in latest schema in to current tablet schema
    for (auto& del_pred : _tablet_reader_params.delete_predicates) {
        tablet_schema->merge_dropped_columns(*del_pred->tablet_schema());
    }

    // Range
    for (auto* key_range : key_ranges) {
        if (key_range->begin_scan_range.size() == 1 &&
            key_range->begin_scan_range.get_value(0) == NEGATIVE_INFINITY) {
            continue;
        }

        _tablet_reader_params.start_key_include = key_range->begin_include;
        _tablet_reader_params.end_key_include = key_range->end_include;

        _tablet_reader_params.start_key.push_back(key_range->begin_scan_range);
        _tablet_reader_params.end_key.push_back(key_range->end_scan_range);
    }

    _tablet_reader_params.profile = _local_state->custom_profile();
    _tablet_reader_params.runtime_state = _state;

    _tablet_reader_params.origin_return_columns = &_return_columns;
    _tablet_reader_params.tablet_columns_convert_to_null_set = &_tablet_columns_convert_to_null_set;

    if (_tablet_reader_params.direct_mode) {
        _tablet_reader_params.return_columns = _return_columns;
    } else {
        // we need to fetch all key columns to do the right aggregation on storage engine side.
        for (size_t i = 0; i < tablet_schema->num_key_columns(); ++i) {
            _tablet_reader_params.return_columns.push_back(i);
        }
        for (auto index : _return_columns) {
            if (tablet_schema->column(index).is_key()) {
                continue;
            }
            _tablet_reader_params.return_columns.push_back(index);
        }
        // expand the sequence column
        if (tablet_schema->has_sequence_col()) {
            bool has_replace_col = false;
            for (auto col : _return_columns) {
                if (tablet_schema->column(col).aggregation() ==
                    FieldAggregationMethod::OLAP_FIELD_AGGREGATION_REPLACE) {
                    has_replace_col = true;
                    break;
                }
            }
            if (auto sequence_col_idx = tablet_schema->sequence_col_idx();
                has_replace_col && std::find(_return_columns.begin(), _return_columns.end(),
                                             sequence_col_idx) == _return_columns.end()) {
                _tablet_reader_params.return_columns.push_back(sequence_col_idx);
            }
        }
    }

    _tablet_reader_params.use_page_cache = _state->enable_page_cache();

    if (tablet->enable_unique_key_merge_on_write() && !_state->skip_delete_bitmap()) {
        _tablet_reader_params.delete_bitmap = &tablet->tablet_meta()->delete_bitmap();
    }

    DBUG_EXECUTE_IF("NewOlapScanner::_init_tablet_reader_params.block", DBUG_BLOCK);

    if (!_state->skip_storage_engine_merge()) {
        TOlapScanNode& olap_scan_node =
                ((pipeline::OlapScanLocalState*)_local_state)->olap_scan_node();
        // order by table keys optimization for topn
        // will only read head/tail of data file since it's already sorted by keys
        if (olap_scan_node.__isset.sort_info && !olap_scan_node.sort_info.is_asc_order.empty()) {
            _limit = _local_state->limit_per_scanner();
            _tablet_reader_params.read_orderby_key = true;
            if (!olap_scan_node.sort_info.is_asc_order[0]) {
                _tablet_reader_params.read_orderby_key_reverse = true;
            }
            _tablet_reader_params.read_orderby_key_num_prefix_columns =
                    olap_scan_node.sort_info.is_asc_order.size();
            _tablet_reader_params.read_orderby_key_limit = _limit;
            _tablet_reader_params.filter_block_conjuncts = _conjuncts;
        }

        // set push down topn filter
        _tablet_reader_params.topn_filter_source_node_ids =
                ((pipeline::OlapScanLocalState*)_local_state)
                        ->get_topn_filter_source_node_ids(_state, true);
        if (!_tablet_reader_params.topn_filter_source_node_ids.empty()) {
            _tablet_reader_params.topn_filter_target_node_id =
                    ((pipeline::OlapScanLocalState*)_local_state)->parent()->node_id();
        }
    }

    // If this is a Two-Phase read query, and we need to delay the release of Rowset
    // by rowset->update_delayed_expired_timestamp().This could expand the lifespan of Rowset
    if (tablet_schema->field_index(BeConsts::ROWID_COL) >= 0) {
        constexpr static int delayed_s = 60;
        for (auto rs_reader : _tablet_reader_params.rs_splits) {
            uint64_t delayed_expired_timestamp =
                    UnixSeconds() + _tablet_reader_params.runtime_state->execution_timeout() +
                    delayed_s;
            rs_reader.rs_reader->rowset()->update_delayed_expired_timestamp(
                    delayed_expired_timestamp);
            ExecEnv::GetInstance()->storage_engine().add_quering_rowset(
                    rs_reader.rs_reader->rowset());
        }
    }

    if (tablet_schema->has_global_row_id()) {
        auto& id_file_map = _state->get_id_file_map();
        for (auto rs_reader : _tablet_reader_params.rs_splits) {
            id_file_map->add_temp_rowset(rs_reader.rs_reader->rowset());
        }
    }

    return Status::OK();
}

Status OlapScanner::_init_variant_columns() {
    auto& tablet_schema = _tablet_reader_params.tablet_schema;
    if (tablet_schema->num_variant_columns() == 0) {
        return Status::OK();
    }
    // Parent column has path info to distinction from each other
    for (auto slot : _output_tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }
        if (slot->type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
            // Such columns are not exist in frontend schema info, so we need to
            // add them into tablet_schema for later column indexing.
            TabletColumn subcol = TabletColumn::create_materialized_variant_column(
                    tablet_schema->column_by_uid(slot->col_unique_id()).name_lower_case(),
                    slot->column_paths(), slot->col_unique_id());
            if (tablet_schema->field_index(*subcol.path_info_ptr()) < 0) {
                tablet_schema->append_column(subcol, TabletSchema::ColumnType::VARIANT);
            }
        }
    }
    schema_util::inherit_column_attributes(tablet_schema);
    return Status::OK();
}

Status OlapScanner::_init_return_columns() {
    for (auto* slot : _output_tuple_desc->slots()) {
        if (!slot->is_materialized()) {
            continue;
        }

        // variant column using path to index a column
        int32_t index = 0;
        auto& tablet_schema = _tablet_reader_params.tablet_schema;
        if (slot->type()->get_primitive_type() == PrimitiveType::TYPE_VARIANT) {
            index = tablet_schema->field_index(PathInData(
                    tablet_schema->column_by_uid(slot->col_unique_id()).name_lower_case(),
                    slot->column_paths()));
        } else {
            index = slot->col_unique_id() >= 0 ? tablet_schema->field_index(slot->col_unique_id())
                                               : tablet_schema->field_index(slot->col_name());
        }

        if (index < 0) {
            return Status::InternalError(
                    "field name is invalid. field={}, field_name_to_index={}, col_unique_id={}",
                    slot->col_name(), tablet_schema->get_all_field_names(), slot->col_unique_id());
        }

        if (slot->get_virtual_column_expr()) {
            ColumnId virtual_column_cid = index;
            _virtual_column_exprs[virtual_column_cid] = _slot_id_to_virtual_column_expr[slot->id()];
            size_t idx_in_block = _slot_id_to_index_in_block[slot->id()];
            _vir_cid_to_idx_in_block[virtual_column_cid] = idx_in_block;
            _vir_col_idx_to_type[idx_in_block] = _slot_id_to_col_type[slot->id()];

            VLOG_DEBUG << fmt::format(
                    "Virtual column, slot id: {}, cid {}, column index: {}, type: {}", slot->id(),
                    virtual_column_cid, _vir_cid_to_idx_in_block[virtual_column_cid],
                    _vir_col_idx_to_type[idx_in_block]->get_name());
        }

        _return_columns.push_back(index);
        if (slot->is_nullable() && !tablet_schema->column(index).is_nullable()) {
            _tablet_columns_convert_to_null_set.emplace(index);
        } else if (!slot->is_nullable() && tablet_schema->column(index).is_nullable()) {
            return Status::Error<ErrorCode::INVALID_SCHEMA>(
                    "slot(id: {}, name: {})'s nullable does not match "
                    "column(tablet id: {}, index: {}, name: {}) ",
                    slot->id(), slot->col_name(), tablet_schema->table_id(), index,
                    tablet_schema->column(index).name());
        }
    }

    if (_return_columns.empty()) {
        return Status::InternalError("failed to build storage scanner, no materialized slot!");
    }
    return Status::OK();
}

doris::TabletStorageType OlapScanner::get_storage_type() {
    if (config::is_cloud_mode()) {
        // we don't have cold storage in cloud mode, all storage is treated as local
        return doris::TabletStorageType::STORAGE_TYPE_LOCAL;
    }
    int local_reader = 0;
    for (const auto& reader : _tablet_reader_params.rs_splits) {
        local_reader += reader.rs_reader->rowset()->is_local();
    }
    int total_reader = _tablet_reader_params.rs_splits.size();

    if (local_reader == total_reader) {
        return doris::TabletStorageType::STORAGE_TYPE_LOCAL;
    } else if (local_reader == 0) {
        return doris::TabletStorageType::STORAGE_TYPE_REMOTE;
    }
    return doris::TabletStorageType::STORAGE_TYPE_REMOTE_AND_LOCAL;
}

Status OlapScanner::_get_block_impl(RuntimeState* state, Block* block, bool* eof) {
    // Read one block from block reader
    // ATTN: Here we need to let the _get_block_impl method guarantee the semantics of the interface,
    // that is, eof can be set to true only when the returned block is empty.
    RETURN_IF_ERROR(_tablet_reader->next_block_with_aggregation(block, eof));
    if (block->rows() > 0) {
        _tablet_reader_params.tablet->read_block_count.fetch_add(1, std::memory_order_relaxed);
        *eof = false;
    }
    return Status::OK();
}

Status OlapScanner::close(RuntimeState* state) {
    if (_is_closed) {
        return Status::OK();
    }
    RETURN_IF_ERROR(Scanner::close(state));
    return Status::OK();
}

void OlapScanner::update_realtime_counters() {
    pipeline::OlapScanLocalState* local_state =
            static_cast<pipeline::OlapScanLocalState*>(_local_state);
    const OlapReaderStatistics& stats = _tablet_reader->stats();
    COUNTER_UPDATE(local_state->_read_compressed_counter, stats.compressed_bytes_read);
    COUNTER_UPDATE(local_state->_scan_bytes, stats.uncompressed_bytes_read);
    COUNTER_UPDATE(local_state->_scan_rows, stats.raw_rows_read);

    // Make sure the scan bytes and scan rows counter in audit log is the same as the counter in
    // doris metrics.
    // ScanBytes is the uncompressed bytes read from local + remote
    // bytes_read_from_local is the compressed bytes read from local
    // bytes_read_from_remote is the compressed bytes read from remote
    // scan bytes > bytes_read_from_local + bytes_read_from_remote
    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_rows(stats.raw_rows_read);
    _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes(
            stats.uncompressed_bytes_read);

    // In case of no cache, we still need to update the IO stats. uncompressed bytes read == local + remote
    if (stats.file_cache_stats.bytes_read_from_local == 0 &&
        stats.file_cache_stats.bytes_read_from_remote == 0) {
        _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes_from_local_storage(
                stats.compressed_bytes_read);
        DorisMetrics::instance()->query_scan_bytes_from_local->increment(
                stats.compressed_bytes_read);
    } else {
        _state->get_query_ctx()->resource_ctx()->io_context()->update_scan_bytes_from_local_storage(
                stats.file_cache_stats.bytes_read_from_local);
        _state->get_query_ctx()
                ->resource_ctx()
                ->io_context()
                ->update_scan_bytes_from_remote_storage(
                        stats.file_cache_stats.bytes_read_from_remote);
        DorisMetrics::instance()->query_scan_bytes_from_local->increment(
                stats.file_cache_stats.bytes_read_from_local);
        DorisMetrics::instance()->query_scan_bytes_from_remote->increment(
                stats.file_cache_stats.bytes_read_from_remote);
    }

    _tablet_reader->mutable_stats()->compressed_bytes_read = 0;
    _tablet_reader->mutable_stats()->uncompressed_bytes_read = 0;
    _tablet_reader->mutable_stats()->raw_rows_read = 0;
    _tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_local = 0;
    _tablet_reader->mutable_stats()->file_cache_stats.bytes_read_from_remote = 0;
}

void OlapScanner::_collect_profile_before_close() {
    //  Please don't directly enable the profile here, we need to set QueryStatistics using the counter inside.
    if (_has_updated_counter) {
        return;
    }
    _has_updated_counter = true;
    _tablet_reader->update_profile(_profile);

    Scanner::_collect_profile_before_close();

    // Update counters for OlapScanner
    // Update counters from tablet reader's stats
    auto& stats = _tablet_reader->stats();
    auto* local_state = (pipeline::OlapScanLocalState*)_local_state;
    COUNTER_UPDATE(local_state->_io_timer, stats.io_ns);
    COUNTER_UPDATE(local_state->_read_compressed_counter, stats.compressed_bytes_read);
    COUNTER_UPDATE(local_state->_scan_bytes, stats.uncompressed_bytes_read);
    COUNTER_UPDATE(local_state->_decompressor_timer, stats.decompress_ns);
    COUNTER_UPDATE(local_state->_read_uncompressed_counter, stats.uncompressed_bytes_read);
    COUNTER_UPDATE(local_state->_block_load_timer, stats.block_load_ns);
    COUNTER_UPDATE(local_state->_block_load_counter, stats.blocks_load);
    COUNTER_UPDATE(local_state->_block_fetch_timer, stats.block_fetch_ns);
    COUNTER_UPDATE(local_state->_delete_bitmap_get_agg_timer, stats.delete_bitmap_get_agg_ns);
    COUNTER_UPDATE(local_state->_scan_rows, stats.raw_rows_read);
    COUNTER_UPDATE(local_state->_vec_cond_timer, stats.vec_cond_ns);
    COUNTER_UPDATE(local_state->_short_cond_timer, stats.short_cond_ns);
    COUNTER_UPDATE(local_state->_expr_filter_timer, stats.expr_filter_ns);
    COUNTER_UPDATE(local_state->_block_init_timer, stats.block_init_ns);
    COUNTER_UPDATE(local_state->_block_init_seek_timer, stats.block_init_seek_ns);
    COUNTER_UPDATE(local_state->_block_init_seek_counter, stats.block_init_seek_num);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_keys_timer,
                   stats.generate_row_ranges_by_keys_ns);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_column_conditions_timer,
                   stats.generate_row_ranges_by_column_conditions_ns);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_bf_timer,
                   stats.generate_row_ranges_by_bf_ns);
    COUNTER_UPDATE(local_state->_collect_iterator_merge_next_timer,
                   stats.collect_iterator_merge_next_timer);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_zonemap_timer,
                   stats.generate_row_ranges_by_zonemap_ns);
    COUNTER_UPDATE(local_state->_segment_generate_row_range_by_dict_timer,
                   stats.generate_row_ranges_by_dict_ns);
    COUNTER_UPDATE(local_state->_predicate_column_read_timer, stats.predicate_column_read_ns);
    COUNTER_UPDATE(local_state->_non_predicate_column_read_timer, stats.non_predicate_read_ns);
    COUNTER_UPDATE(local_state->_predicate_column_read_seek_timer,
                   stats.predicate_column_read_seek_ns);
    COUNTER_UPDATE(local_state->_predicate_column_read_seek_counter,
                   stats.predicate_column_read_seek_num);
    COUNTER_UPDATE(local_state->_lazy_read_timer, stats.lazy_read_ns);
    COUNTER_UPDATE(local_state->_lazy_read_seek_timer, stats.block_lazy_read_seek_ns);
    COUNTER_UPDATE(local_state->_lazy_read_seek_counter, stats.block_lazy_read_seek_num);
    COUNTER_UPDATE(local_state->_output_col_timer, stats.output_col_ns);
    COUNTER_UPDATE(local_state->_rows_vec_cond_filtered_counter, stats.rows_vec_cond_filtered);
    COUNTER_UPDATE(local_state->_rows_short_circuit_cond_filtered_counter,
                   stats.rows_short_circuit_cond_filtered);
    COUNTER_UPDATE(local_state->_rows_expr_cond_filtered_counter, stats.rows_expr_cond_filtered);
    COUNTER_UPDATE(local_state->_rows_vec_cond_input_counter, stats.vec_cond_input_rows);
    COUNTER_UPDATE(local_state->_rows_short_circuit_cond_input_counter,
                   stats.short_circuit_cond_input_rows);
    COUNTER_UPDATE(local_state->_rows_expr_cond_input_counter, stats.expr_cond_input_rows);
    COUNTER_UPDATE(local_state->_stats_filtered_counter, stats.rows_stats_filtered);
    COUNTER_UPDATE(local_state->_stats_rp_filtered_counter, stats.rows_stats_rp_filtered);
    COUNTER_UPDATE(local_state->_dict_filtered_counter, stats.segment_dict_filtered);
    COUNTER_UPDATE(local_state->_bf_filtered_counter, stats.rows_bf_filtered);
    COUNTER_UPDATE(local_state->_del_filtered_counter, stats.rows_del_filtered);
    COUNTER_UPDATE(local_state->_del_filtered_counter, stats.rows_del_by_bitmap);
    COUNTER_UPDATE(local_state->_del_filtered_counter, stats.rows_vec_del_cond_filtered);
    COUNTER_UPDATE(local_state->_conditions_filtered_counter, stats.rows_conditions_filtered);
    COUNTER_UPDATE(local_state->_key_range_filtered_counter, stats.rows_key_range_filtered);
    COUNTER_UPDATE(local_state->_total_pages_num_counter, stats.total_pages_num);
    COUNTER_UPDATE(local_state->_cached_pages_num_counter, stats.cached_pages_num);
    COUNTER_UPDATE(local_state->_bitmap_index_filter_counter, stats.rows_bitmap_index_filtered);
    COUNTER_UPDATE(local_state->_bitmap_index_filter_timer, stats.bitmap_index_filter_timer);
    COUNTER_UPDATE(local_state->_inverted_index_filter_counter, stats.rows_inverted_index_filtered);
    COUNTER_UPDATE(local_state->_inverted_index_filter_timer, stats.inverted_index_filter_timer);
    COUNTER_UPDATE(local_state->_inverted_index_query_cache_hit_counter,
                   stats.inverted_index_query_cache_hit);
    COUNTER_UPDATE(local_state->_inverted_index_query_cache_miss_counter,
                   stats.inverted_index_query_cache_miss);
    COUNTER_UPDATE(local_state->_inverted_index_query_timer, stats.inverted_index_query_timer);
    COUNTER_UPDATE(local_state->_inverted_index_query_null_bitmap_timer,
                   stats.inverted_index_query_null_bitmap_timer);
    COUNTER_UPDATE(local_state->_inverted_index_query_bitmap_copy_timer,
                   stats.inverted_index_query_bitmap_copy_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_open_timer,
                   stats.inverted_index_searcher_open_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_search_timer,
                   stats.inverted_index_searcher_search_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_search_init_timer,
                   stats.inverted_index_searcher_search_init_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_search_exec_timer,
                   stats.inverted_index_searcher_search_exec_timer);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_cache_hit_counter,
                   stats.inverted_index_searcher_cache_hit);
    COUNTER_UPDATE(local_state->_inverted_index_searcher_cache_miss_counter,
                   stats.inverted_index_searcher_cache_miss);
    COUNTER_UPDATE(local_state->_inverted_index_downgrade_count_counter,
                   stats.inverted_index_downgrade_count);

    InvertedIndexProfileReporter inverted_index_profile;
    inverted_index_profile.update(local_state->_index_filter_profile.get(),
                                  &stats.inverted_index_stats);

    if (config::enable_file_cache) {
        io::FileCacheProfileReporter cache_profile(local_state->_segment_profile.get());
        cache_profile.update(&stats.file_cache_stats);
    }
    COUNTER_UPDATE(local_state->_output_index_result_column_timer,
                   stats.output_index_result_column_timer);
    COUNTER_UPDATE(local_state->_filtered_segment_counter, stats.filtered_segment_number);
    COUNTER_UPDATE(local_state->_total_segment_counter, stats.total_segment_number);

    COUNTER_UPDATE(local_state->_tablet_reader_init_timer, stats.tablet_reader_init_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_capture_rs_readers_timer,
                   stats.tablet_reader_capture_rs_readers_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_return_columns_timer,
                   stats.tablet_reader_init_return_columns_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_keys_param_timer,
                   stats.tablet_reader_init_keys_param_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_orderby_keys_param_timer,
                   stats.tablet_reader_init_orderby_keys_param_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_conditions_param_timer,
                   stats.tablet_reader_init_conditions_param_timer_ns);
    COUNTER_UPDATE(local_state->_tablet_reader_init_delete_condition_param_timer,
                   stats.tablet_reader_init_delete_condition_param_timer_ns);
    COUNTER_UPDATE(local_state->_block_reader_vcollect_iter_init_timer,
                   stats.block_reader_vcollect_iter_init_timer_ns);
    COUNTER_UPDATE(local_state->_block_reader_rs_readers_init_timer,
                   stats.block_reader_rs_readers_init_timer_ns);
    COUNTER_UPDATE(local_state->_block_reader_build_heap_init_timer,
                   stats.block_reader_build_heap_init_timer_ns);

    COUNTER_UPDATE(local_state->_rowset_reader_get_segment_iterators_timer,
                   stats.rowset_reader_get_segment_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_rowset_reader_create_iterators_timer,
                   stats.rowset_reader_create_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_rowset_reader_init_iterators_timer,
                   stats.rowset_reader_init_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_rowset_reader_load_segments_timer,
                   stats.rowset_reader_load_segments_timer_ns);

    COUNTER_UPDATE(local_state->_segment_iterator_init_timer, stats.segment_iterator_init_timer_ns);
    COUNTER_UPDATE(local_state->_segment_iterator_init_return_column_iterators_timer,
                   stats.segment_iterator_init_return_column_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_segment_iterator_init_bitmap_index_iterators_timer,
                   stats.segment_iterator_init_bitmap_index_iterators_timer_ns);
    COUNTER_UPDATE(local_state->_segment_iterator_init_index_iterators_timer,
                   stats.segment_iterator_init_index_iterators_timer_ns);

    COUNTER_UPDATE(local_state->_segment_create_column_readers_timer,
                   stats.segment_create_column_readers_timer_ns);
    COUNTER_UPDATE(local_state->_segment_load_index_timer, stats.segment_load_index_timer_ns);

    // Update metrics
    DorisMetrics::instance()->query_scan_bytes->increment(
            local_state->_read_uncompressed_counter->value());
    DorisMetrics::instance()->query_scan_rows->increment(local_state->_scan_rows->value());
    auto& tablet = _tablet_reader_params.tablet;
    tablet->query_scan_bytes->increment(local_state->_read_uncompressed_counter->value());
    tablet->query_scan_rows->increment(local_state->_scan_rows->value());
    tablet->query_scan_count->increment(1);
}

} // namespace doris::vectorized
