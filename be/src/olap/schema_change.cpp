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

#include "olap/schema_change.h"

#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>
#include <thrift/protocol/TDebugProtocol.h>

#include <algorithm>
#include <exception>
#include <map>
#include <memory>
#include <mutex>
#include <roaring/roaring.hh>
#include <tuple>
#include <utility>

#include "agent/be_exec_version_manager.h"
#include "cloud/cloud_schema_change_job.h"
#include "cloud/config.h"
#include "common/cast_set.h"
#include "common/consts.h"
#include "common/logging.h"
#include "common/signal_handler.h"
#include "common/status.h"
#include "exec/schema_scanner/schema_metadata_name_ids_scanner.h"
#include "io/fs/file_system.h"
#include "io/io_common.h"
#include "olap/base_tablet.h"
#include "olap/data_dir.h"
#include "olap/delete_handler.h"
#include "olap/field.h"
#include "olap/iterators.h"
#include "olap/merger.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/pending_rowset_helper.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_writer.h"
#include "olap/rowset/segment_v2/segment.h"
#include "olap/schema.h"
#include "olap/segment_loader.h"
#include "olap/storage_engine.h"
#include "olap/tablet.h"
#include "olap/tablet_fwd.h"
#include "olap/tablet_manager.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "olap/wrapper_field.h"
#include "runtime/exec_env.h"
#include "runtime/memory/mem_tracker.h"
#include "runtime/runtime_state.h"
#include "util/debug_points.h"
#include "util/defer_op.h"
#include "util/trace.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/aggregate_functions/aggregate_function_reader.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/schema_util.h"
#include "vec/core/block.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/exprs/vexpr.h"
#include "vec/exprs/vexpr_context.h"
#include "vec/olap/olap_data_convertor.h"

namespace doris {

#include "common/compile_check_begin.h"

class CollectionValue;

using namespace ErrorCode;

constexpr int ALTER_TABLE_BATCH_SIZE = 4064;

class MultiBlockMerger {
public:
    MultiBlockMerger(BaseTabletSPtr tablet) : _tablet(tablet), _cmp(*tablet) {}

    Status merge(const std::vector<std::unique_ptr<vectorized::Block>>& blocks,
                 RowsetWriter* rowset_writer, uint64_t* merged_rows) {
        int rows = 0;
        for (const auto& block : blocks) {
            rows += block->rows();
        }
        if (!rows) {
            return Status::OK();
        }

        std::vector<RowRef> row_refs;
        row_refs.reserve(rows);
        for (const auto& block : blocks) {
            for (uint16_t i = 0; i < block->rows(); i++) {
                row_refs.emplace_back(block.get(), i);
            }
        }
        // TODO: try to use pdqsort to replace std::sort
        // The block version is incremental.
        std::stable_sort(row_refs.begin(), row_refs.end(), _cmp);

        auto finalized_block = _tablet->tablet_schema()->create_block();
        int columns = finalized_block.columns();
        *merged_rows += rows;

        if (_tablet->keys_type() == KeysType::AGG_KEYS) {
            auto tablet_schema = _tablet->tablet_schema();
            int key_number = cast_set<int>(_tablet->num_key_columns());

            std::vector<vectorized::AggregateFunctionPtr> agg_functions;
            std::vector<vectorized::AggregateDataPtr> agg_places;

            for (int i = key_number; i < columns; i++) {
                try {
                    vectorized::AggregateFunctionPtr function =
                            tablet_schema->column(i).get_aggregate_function(
                                    vectorized::AGG_LOAD_SUFFIX,
                                    tablet_schema->column(i).get_be_exec_version());
                    if (!function) {
                        return Status::InternalError(
                                "could not find aggregate function on column {}, aggregation={}",
                                tablet_schema->column(i).name(),
                                tablet_schema->column(i).aggregation());
                    }
                    agg_functions.push_back(function);
                    // create aggregate data
                    auto* place = new char[function->size_of_data()];
                    function->create(place);
                    agg_places.push_back(place);
                } catch (...) {
                    for (int j = 0; j < i - key_number; ++j) {
                        agg_functions[j]->destroy(agg_places[j]);
                        delete[] agg_places[j];
                    }
                    throw;
                }
            }

            DEFER({
                for (int i = 0; i < columns - key_number; i++) {
                    agg_functions[i]->destroy(agg_places[i]);
                    delete[] agg_places[i];
                }
            });

            for (int i = 0; i < rows; i++) {
                auto row_ref = row_refs[i];
                for (int j = key_number; j < columns; j++) {
                    const auto* column_ptr = row_ref.get_column(j).get();
                    agg_functions[j - key_number]->add(
                            agg_places[j - key_number],
                            const_cast<const vectorized::IColumn**>(&column_ptr), row_ref.position,
                            _arena);
                }

                if (i == rows - 1 || _cmp.compare(row_refs[i], row_refs[i + 1])) {
                    for (int j = 0; j < key_number; j++) {
                        finalized_block.get_by_position(j).column->assume_mutable()->insert_from(
                                *row_ref.get_column(j), row_ref.position);
                    }

                    for (int j = key_number; j < columns; j++) {
                        agg_functions[j - key_number]->insert_result_into(
                                agg_places[j - key_number],
                                finalized_block.get_by_position(j).column->assume_mutable_ref());
                        agg_functions[j - key_number]->reset(agg_places[j - key_number]);
                    }

                    if (i == rows - 1 || finalized_block.rows() == ALTER_TABLE_BATCH_SIZE) {
                        *merged_rows -= finalized_block.rows();
                        RETURN_IF_ERROR(rowset_writer->add_block(&finalized_block));
                        finalized_block.clear_column_data();
                    }
                }
            }
        } else {
            std::vector<RowRef> pushed_row_refs;
            if (_tablet->keys_type() == KeysType::DUP_KEYS) {
                std::swap(pushed_row_refs, row_refs);
            } else if (_tablet->keys_type() == KeysType::UNIQUE_KEYS) {
                for (int i = 0; i < rows; i++) {
                    if (i == rows - 1 || _cmp.compare(row_refs[i], row_refs[i + 1])) {
                        pushed_row_refs.push_back(row_refs[i]);
                    }
                }
                if (!_tablet->tablet_schema()->cluster_key_uids().empty()) {
                    std::vector<uint32_t> ids;
                    for (const auto& cid : _tablet->tablet_schema()->cluster_key_uids()) {
                        auto index = _tablet->tablet_schema()->field_index(cid);
                        if (index == -1) {
                            return Status::InternalError(
                                    "could not find cluster key column with unique_id=" +
                                    std::to_string(cid) + " in tablet schema");
                        }
                        ids.push_back(index);
                    }
                    // sort by cluster key
                    std::stable_sort(pushed_row_refs.begin(), pushed_row_refs.end(),
                                     ClusterKeyRowRefComparator(ids));
                }
            }

            // update real inserted row number
            rows = cast_set<int>(pushed_row_refs.size());
            *merged_rows -= rows;

            for (int i = 0; i < rows; i += ALTER_TABLE_BATCH_SIZE) {
                int limit = std::min(ALTER_TABLE_BATCH_SIZE, rows - i);

                for (int idx = 0; idx < columns; idx++) {
                    auto column = finalized_block.get_by_position(idx).column->assume_mutable();

                    for (int j = 0; j < limit; j++) {
                        auto row_ref = pushed_row_refs[i + j];
                        column->insert_from(*row_ref.get_column(idx), row_ref.position);
                    }
                }
                RETURN_IF_ERROR(rowset_writer->add_block(&finalized_block));
                finalized_block.clear_column_data();
            }
        }

        RETURN_IF_ERROR(rowset_writer->flush());
        return Status::OK();
    }

private:
    struct RowRef {
        RowRef(vectorized::Block* block_, uint16_t position_)
                : block(block_), position(position_) {}
        vectorized::ColumnPtr get_column(int index) const {
            return block->get_by_position(index).column;
        }
        const vectorized::Block* block;
        uint16_t position;
    };

    struct RowRefComparator {
        RowRefComparator(const BaseTablet& tablet) : _num_columns(tablet.num_key_columns()) {}

        int compare(const RowRef& lhs, const RowRef& rhs) const {
            // Notice: does not compare sequence column for mow table
            // read from rowsets with delete bitmap, so there should be no duplicated keys
            return lhs.block->compare_at(lhs.position, rhs.position, _num_columns, *rhs.block, -1);
        }

        bool operator()(const RowRef& lhs, const RowRef& rhs) const {
            return compare(lhs, rhs) < 0;
        }

        const size_t _num_columns;
    };

    struct ClusterKeyRowRefComparator {
        ClusterKeyRowRefComparator(std::vector<uint32_t> columns) : _columns(columns) {}

        int compare(const RowRef& lhs, const RowRef& rhs) const {
            return lhs.block->compare_at(lhs.position, rhs.position, &_columns, *rhs.block, -1);
        }

        bool operator()(const RowRef& lhs, const RowRef& rhs) const {
            return compare(lhs, rhs) < 0;
        }

        const std::vector<uint32_t> _columns;
    };

    BaseTabletSPtr _tablet;
    RowRefComparator _cmp;
    vectorized::Arena _arena;
};

BlockChanger::BlockChanger(TabletSchemaSPtr tablet_schema, DescriptorTbl desc_tbl)
        : _desc_tbl(std::move(desc_tbl)) {
    _schema_mapping.resize(tablet_schema->num_columns());
}

BlockChanger::~BlockChanger() {
    for (auto it = _schema_mapping.begin(); it != _schema_mapping.end(); ++it) {
        SAFE_DELETE(it->default_value);
    }
    _schema_mapping.clear();
}

ColumnMapping* BlockChanger::get_mutable_column_mapping(size_t column_index) {
    if (column_index >= _schema_mapping.size()) {
        return nullptr;
    }

    return &(_schema_mapping[column_index]);
}

Status BlockChanger::change_block(vectorized::Block* ref_block,
                                  vectorized::Block* new_block) const {
    std::unique_ptr<RuntimeState> state = RuntimeState::create_unique();
    state->set_desc_tbl(&_desc_tbl);
    state->set_be_exec_version(_fe_compatible_version);
    RowDescriptor row_desc =
            RowDescriptor(_desc_tbl.get_tuple_descriptor(_desc_tbl.get_row_tuples()[0]), false);

    if (_where_expr != nullptr) {
        vectorized::VExprContextSPtr ctx = nullptr;
        RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(*_where_expr, ctx));
        RETURN_IF_ERROR(ctx->prepare(state.get(), row_desc));
        RETURN_IF_ERROR(ctx->open(state.get()));

        RETURN_IF_ERROR(
                vectorized::VExprContext::filter_block(ctx.get(), ref_block, ref_block->columns()));
    }

    const int row_num = cast_set<int>(ref_block->rows());
    const int new_schema_cols_num = new_block->columns();

    // will be used for swaping ref_block[entry.first] and new_block[entry.second]
    std::list<std::pair<int, int>> swap_idx_list;
    for (int idx = 0; idx < new_schema_cols_num; idx++) {
        auto expr = _schema_mapping[idx].expr;
        if (expr != nullptr) {
            vectorized::VExprContextSPtr ctx;
            RETURN_IF_ERROR(vectorized::VExpr::create_expr_tree(*expr, ctx));
            RETURN_IF_ERROR(ctx->prepare(state.get(), row_desc));
            RETURN_IF_ERROR(ctx->open(state.get()));

            int result_tmp_column_idx = -1;
            RETURN_IF_ERROR(ctx->execute(ref_block, &result_tmp_column_idx));
            auto& result_tmp_column_def = ref_block->get_by_position(result_tmp_column_idx);
            if (!result_tmp_column_def.column) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "result column={} is nullptr, input expr={}", result_tmp_column_def.name,
                        apache::thrift::ThriftDebugString(*expr));
            }
            ref_block->replace_by_position_if_const(result_tmp_column_idx);

            if (result_tmp_column_def.column->size() != row_num) {
                return Status::Error<ErrorCode::INTERNAL_ERROR>(
                        "result size invalid, expect={}, real={}; input expr={}, block={}", row_num,
                        result_tmp_column_def.column->size(),
                        apache::thrift::ThriftDebugString(*expr), ref_block->dump_structure());
            }

            if (_type == SCHEMA_CHANGE) {
                // danger casts (expected to be rejected by upstream caller) may cause data to be null and result in data loss in schema change
                // for rollup, this check is unecessary, and ref columns are not set in this case, it works on exprs

                // column_idx in base schema
                int32_t ref_column_idx = _schema_mapping[idx].ref_column_idx;
                DCHECK_GE(ref_column_idx, 0);
                auto& ref_column_def = ref_block->get_by_position(ref_column_idx);
                RETURN_IF_ERROR(
                        _check_cast_valid(ref_column_def.column, result_tmp_column_def.column));
            }
            swap_idx_list.emplace_back(result_tmp_column_idx, idx);
        } else if (_schema_mapping[idx].ref_column_idx < 0) {
            // new column, write default value
            auto* value = _schema_mapping[idx].default_value;
            auto column = new_block->get_by_position(idx).column->assume_mutable();
            if (value->is_null()) {
                DCHECK(column->is_nullable());
                column->insert_many_defaults(row_num);
            } else {
                auto type_info = get_type_info(_schema_mapping[idx].new_column);
                DefaultValueColumnIterator::insert_default_data(type_info.get(), value->size(),
                                                                value->ptr(), column, row_num);
            }
        } else {
            // same type, just swap column
            swap_idx_list.emplace_back(_schema_mapping[idx].ref_column_idx, idx);
        }
    }

    for (auto it : swap_idx_list) {
        auto& ref_col = ref_block->get_by_position(it.first).column;
        auto& new_col = new_block->get_by_position(it.second).column;

        bool ref_col_nullable = ref_col->is_nullable();
        bool new_col_nullable = new_col->is_nullable();

        if (ref_col_nullable != new_col_nullable) {
            // not nullable to nullable
            if (new_col_nullable) {
                auto* new_nullable_col =
                        assert_cast<vectorized::ColumnNullable*>(new_col->assume_mutable().get());

                new_nullable_col->change_nested_column(ref_col);
                new_nullable_col->get_null_map_data().resize_fill(ref_col->size());
            } else {
                // nullable to not nullable:
                // suppose column `c_phone` is originally varchar(16) NOT NULL,
                // then do schema change `alter table test modify column c_phone int not null`,
                // the cast expr of schema change is `CastExpr(CAST String to Nullable(Int32))`,
                // so need to handle nullable to not nullable here
                auto* ref_nullable_col =
                        assert_cast<vectorized::ColumnNullable*>(ref_col->assume_mutable().get());

                new_col = ref_nullable_col->get_nested_column_ptr();
            }
        } else {
            new_block->get_by_position(it.second).column =
                    ref_block->get_by_position(it.first).column;
        }
    }
    return Status::OK();
}

// This check can prevent schema-change from causing data loss after type cast
Status BlockChanger::_check_cast_valid(vectorized::ColumnPtr input_column,
                                       vectorized::ColumnPtr output_column) {
    if (input_column->size() != output_column->size()) {
        return Status::InternalError(
                "column size is changed, input_column_size={}, output_column_size={}; "
                "input_column={}",
                input_column->size(), output_column->size(), input_column->get_name());
    }
    DCHECK_EQ(input_column->size(), output_column->size())
            << "length check should have done before calling this function!";

    if (input_column->is_nullable() != output_column->is_nullable()) {
        if (input_column->is_nullable()) {
            const auto* ref_null_map =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(input_column.get())
                            ->get_null_map_column()
                            .get_data()
                            .data();

            bool is_changed = false;
            for (size_t i = 0; i < input_column->size(); i++) {
                is_changed |= ref_null_map[i];
            }
            if (is_changed) {
                return Status::DataQualityError(
                        "some null data is changed to not null, intput_column={}",
                        input_column->get_name());
            }
        } else {
            const auto& null_map_column =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(
                            output_column.get())
                            ->get_null_map_column();
            const auto& nested_column =
                    vectorized::check_and_get_column<vectorized::ColumnNullable>(
                            output_column.get())
                            ->get_nested_column();
            const auto* new_null_map = null_map_column.get_data().data();

            if (null_map_column.size() != output_column->size()) {
                return Status::InternalError(
                        "null_map_column size mismatch output_column_size, "
                        "null_map_column_size={}, output_column_size={}; input_column={}",
                        null_map_column.size(), output_column->size(), input_column->get_name());
            }

            if (nested_column.size() != output_column->size()) {
                return Status::InternalError(
                        "nested_column size is changed, nested_column_size={}, "
                        "ouput_column_size={}; input_column={}",
                        nested_column.size(), output_column->size(), input_column->get_name());
            }

            bool is_changed = false;
            for (size_t i = 0; i < input_column->size(); i++) {
                is_changed |= new_null_map[i];
            }
            if (is_changed) {
                return Status::DataQualityError(
                        "some not null data is changed to null, intput_column={}",
                        input_column->get_name());
            }
        }
    }

    if (input_column->is_nullable() && output_column->is_nullable()) {
        const auto* ref_null_map =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(input_column.get())
                        ->get_null_map_column()
                        .get_data()
                        .data();
        const auto* new_null_map =
                vectorized::check_and_get_column<vectorized::ColumnNullable>(output_column.get())
                        ->get_null_map_column()
                        .get_data()
                        .data();

        bool is_changed = false;
        for (size_t i = 0; i < input_column->size(); i++) {
            is_changed |= (ref_null_map[i] != new_null_map[i]);
        }
        if (is_changed) {
            return Status::DataQualityError(
                    "null map is changed after calculation, input_column={}",
                    input_column->get_name());
        }
    }
    return Status::OK();
}

Status LinkedSchemaChange::process(RowsetReaderSharedPtr rowset_reader, RowsetWriter* rowset_writer,
                                   BaseTabletSPtr new_tablet, BaseTabletSPtr base_tablet,
                                   TabletSchemaSPtr base_tablet_schema,
                                   TabletSchemaSPtr new_tablet_schema) {
    Status status = rowset_writer->add_rowset_for_linked_schema_change(rowset_reader->rowset());
    if (!status) {
        LOG(WARNING) << "fail to convert rowset."
                     << ", new_tablet=" << new_tablet->tablet_id()
                     << ", version=" << rowset_writer->version().first << "-"
                     << rowset_writer->version().second << ", error status " << status;
        return status;
    }
    // copy delete bitmap to new tablet.
    if (new_tablet->keys_type() == UNIQUE_KEYS && new_tablet->enable_unique_key_merge_on_write()) {
        DeleteBitmap origin_delete_bitmap(base_tablet->tablet_id());
        base_tablet->tablet_meta()->delete_bitmap().subset(
                {rowset_reader->rowset()->rowset_id(), 0, 0},
                {rowset_reader->rowset()->rowset_id(), UINT32_MAX, INT64_MAX},
                &origin_delete_bitmap);
        for (auto& iter : origin_delete_bitmap.delete_bitmap) {
            int ret = new_tablet->tablet_meta()->delete_bitmap().set(
                    {rowset_writer->rowset_id(), std::get<1>(iter.first), std::get<2>(iter.first)},
                    iter.second);
            DCHECK(ret == 1);
        }
    }
    return Status::OK();
}

Status VSchemaChangeDirectly::_inner_process(RowsetReaderSharedPtr rowset_reader,
                                             RowsetWriter* rowset_writer, BaseTabletSPtr new_tablet,
                                             TabletSchemaSPtr base_tablet_schema,
                                             TabletSchemaSPtr new_tablet_schema) {
    bool eof = false;
    do {
        auto new_block = vectorized::Block::create_unique(new_tablet_schema->create_block());
        auto ref_block = vectorized::Block::create_unique(base_tablet_schema->create_block());

        auto st = rowset_reader->next_block(ref_block.get());
        if (!st) {
            if (st.is<ErrorCode::END_OF_FILE>()) {
                if (ref_block->rows() == 0) {
                    break;
                } else {
                    eof = true;
                }
            } else {
                return st;
            }
        }

        RETURN_IF_ERROR(_changer.change_block(ref_block.get(), new_block.get()));
        RETURN_IF_ERROR(rowset_writer->add_block(new_block.get()));
    } while (!eof);

    RETURN_IF_ERROR(rowset_writer->flush());
    return Status::OK();
}

VBaseSchemaChangeWithSorting::VBaseSchemaChangeWithSorting(const BlockChanger& changer,
                                                           size_t memory_limitation)
        : _changer(changer),
          _memory_limitation(memory_limitation),
          _temp_delta_versions(Version::mock()) {
    _mem_tracker = std::make_unique<MemTracker>(
            fmt::format("VSchemaChangeWithSorting:changer={}", std::to_string(int64_t(&changer))));
}

Status VBaseSchemaChangeWithSorting::_inner_process(RowsetReaderSharedPtr rowset_reader,
                                                    RowsetWriter* rowset_writer,
                                                    BaseTabletSPtr new_tablet,
                                                    TabletSchemaSPtr base_tablet_schema,
                                                    TabletSchemaSPtr new_tablet_schema) {
    // for internal sorting
    std::vector<std::unique_ptr<vectorized::Block>> blocks;

    RowsetSharedPtr rowset = rowset_reader->rowset();
    SegmentsOverlapPB segments_overlap = rowset->rowset_meta()->segments_overlap();
    int64_t newest_write_timestamp = rowset->newest_write_timestamp();
    _temp_delta_versions.first = _temp_delta_versions.second;
    _src_rowsets.clear(); // init _src_rowsets
    auto create_rowset = [&]() -> Status {
        if (blocks.empty()) {
            return Status::OK();
        }

        auto rowset = DORIS_TRY(_internal_sorting(
                blocks, Version(_temp_delta_versions.second, _temp_delta_versions.second + 1),
                newest_write_timestamp, new_tablet, BETA_ROWSET, segments_overlap,
                new_tablet_schema));
        _src_rowsets.push_back(std::move(rowset));
        for (auto& block : blocks) {
            _mem_tracker->release(block->allocated_bytes());
        }
        blocks.clear();

        // increase temp version
        _temp_delta_versions.second += 2;
        return Status::OK();
    };

    auto new_block = vectorized::Block::create_unique(new_tablet_schema->create_block());

    bool eof = false;
    do {
        auto ref_block = vectorized::Block::create_unique(base_tablet_schema->create_block());
        auto st = rowset_reader->next_block(ref_block.get());
        if (!st) {
            if (st.is<ErrorCode::END_OF_FILE>()) {
                if (ref_block->rows() == 0) {
                    break;
                } else {
                    eof = true;
                }
            } else {
                return st;
            }
        }

        RETURN_IF_ERROR(_changer.change_block(ref_block.get(), new_block.get()));

        constexpr double HOLD_BLOCK_MEMORY_RATE =
                0.66; // Reserve some memory for use by other parts of this job
        if (_mem_tracker->consumption() + new_block->allocated_bytes() > _memory_limitation ||
            cast_set<double>(_mem_tracker->consumption()) >
                    cast_set<double>(_memory_limitation) * HOLD_BLOCK_MEMORY_RATE ||
            DebugPoints::instance()->is_enable(
                    "VBaseSchemaChangeWithSorting._inner_process.create_rowset")) {
            RETURN_IF_ERROR(create_rowset());

            if (_mem_tracker->consumption() + new_block->allocated_bytes() > _memory_limitation) {
                return Status::Error<INVALID_ARGUMENT>(
                        "Memory limitation is too small for Schema Change. _memory_limitation={}, "
                        "new_block->allocated_bytes()={}, consumption={}",
                        _memory_limitation, new_block->allocated_bytes(),
                        _mem_tracker->consumption());
            }
        }
        _mem_tracker->consume(new_block->allocated_bytes());

        // move unique ptr
        blocks.push_back(vectorized::Block::create_unique(new_tablet_schema->create_block()));
        swap(blocks.back(), new_block);
    } while (!eof);

    RETURN_IF_ERROR(create_rowset());

    if (_src_rowsets.empty()) {
        RETURN_IF_ERROR(rowset_writer->flush());
    } else {
        RETURN_IF_ERROR(
                _external_sorting(_src_rowsets, rowset_writer, new_tablet, new_tablet_schema));
    }

    return Status::OK();
}

Result<RowsetSharedPtr> VBaseSchemaChangeWithSorting::_internal_sorting(
        const std::vector<std::unique_ptr<vectorized::Block>>& blocks, const Version& version,
        int64_t newest_write_timestamp, BaseTabletSPtr new_tablet, RowsetTypePB new_rowset_type,
        SegmentsOverlapPB segments_overlap, TabletSchemaSPtr new_tablet_schema) {
    uint64_t merged_rows = 0;
    MultiBlockMerger merger(new_tablet);
    RowsetWriterContext context;
    context.version = version;
    context.rowset_state = VISIBLE;
    context.segments_overlap = segments_overlap;
    context.tablet_schema = new_tablet_schema;
    context.newest_write_timestamp = newest_write_timestamp;
    context.write_type = DataWriteType::TYPE_SCHEMA_CHANGE;
    std::unique_ptr<RowsetWriter> rowset_writer;
    // TODO(plat1ko): Use monad op
    if (auto result = new_tablet->create_rowset_writer(context, false); !result.has_value())
            [[unlikely]] {
        return unexpected(std::move(result).error());
    } else {
        rowset_writer = std::move(result).value();
    }
    RETURN_IF_ERROR_RESULT(merger.merge(blocks, rowset_writer.get(), &merged_rows));
    _add_merged_rows(merged_rows);
    RowsetSharedPtr rowset;
    RETURN_IF_ERROR_RESULT(rowset_writer->build(rowset));
    return rowset;
}

Result<RowsetSharedPtr> VLocalSchemaChangeWithSorting::_internal_sorting(
        const std::vector<std::unique_ptr<vectorized::Block>>& blocks, const Version& version,
        int64_t newest_write_timestamp, BaseTabletSPtr new_tablet, RowsetTypePB new_rowset_type,
        SegmentsOverlapPB segments_overlap, TabletSchemaSPtr new_tablet_schema) {
    uint64_t merged_rows = 0;
    MultiBlockMerger merger(new_tablet);
    RowsetWriterContext context;
    context.version = version;
    context.rowset_state = VISIBLE;
    context.segments_overlap = segments_overlap;
    context.tablet_schema = new_tablet_schema;
    context.newest_write_timestamp = newest_write_timestamp;
    context.write_type = DataWriteType::TYPE_SCHEMA_CHANGE;
    std::unique_ptr<RowsetWriter> rowset_writer;
    // TODO(plat1ko): Use monad op
    if (auto result = new_tablet->create_rowset_writer(context, false); !result.has_value())
            [[unlikely]] {
        return unexpected(std::move(result).error());
    } else {
        rowset_writer = std::move(result).value();
    }
    auto guard = _local_storage_engine.pending_local_rowsets().add(context.rowset_id);
    _pending_rs_guards.push_back(std::move(guard));
    RETURN_IF_ERROR_RESULT(merger.merge(blocks, rowset_writer.get(), &merged_rows));
    _add_merged_rows(merged_rows);
    RowsetSharedPtr rowset;
    RETURN_IF_ERROR_RESULT(rowset_writer->build(rowset));
    return rowset;
}

Status VBaseSchemaChangeWithSorting::_external_sorting(std::vector<RowsetSharedPtr>& src_rowsets,
                                                       RowsetWriter* rowset_writer,
                                                       BaseTabletSPtr new_tablet,
                                                       TabletSchemaSPtr new_tablet_schema) {
    std::vector<RowsetReaderSharedPtr> rs_readers;
    for (auto& rowset : src_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        rs_readers.push_back(rs_reader);
    }

    Merger::Statistics stats;
    if (!new_tablet_schema->cluster_key_uids().empty()) {
        // schema change read rowsets with delete bitmap, so there should be no duplicated keys
        // RETURN_IF_ERROR(Compaction::update_delete_bitmap());
        int64_t way_num = 0;
        int64_t input_rowsets_data_size = 0;
        int64_t input_row_num = 0;
        for (auto& rowset : src_rowsets) {
            way_num += rowset->rowset_meta()->get_merge_way_num();
            input_rowsets_data_size += rowset->data_disk_size();
            input_row_num += rowset->num_rows();
        }
        int64_t avg_segment_rows = config::vertical_compaction_max_segment_size /
                                   (input_rowsets_data_size / (input_row_num + 1) + 1);
        RETURN_IF_ERROR(Merger::vertical_merge_rowsets(
                new_tablet, ReaderType::READER_ALTER_TABLE, *new_tablet_schema, rs_readers,
                rowset_writer, cast_set<uint32_t>(avg_segment_rows), way_num, &stats));
    } else {
        RETURN_IF_ERROR(Merger::vmerge_rowsets(new_tablet, ReaderType::READER_ALTER_TABLE,
                                               *new_tablet_schema, rs_readers, rowset_writer,
                                               &stats));
    }
    _add_merged_rows(stats.merged_rows);
    _add_filtered_rows(stats.filtered_rows);
    return Status::OK();
}

Status VLocalSchemaChangeWithSorting::_inner_process(RowsetReaderSharedPtr rowset_reader,
                                                     RowsetWriter* rowset_writer,
                                                     BaseTabletSPtr new_tablet,
                                                     TabletSchemaSPtr base_tablet_schema,
                                                     TabletSchemaSPtr new_tablet_schema) {
    Defer defer {[&]() {
        // remove the intermediate rowsets generated by internal sorting
        for (auto& row_set : _src_rowsets) {
            _local_storage_engine.add_unused_rowset(row_set);
        }
    }};
    _pending_rs_guards.clear();
    return VBaseSchemaChangeWithSorting::_inner_process(rowset_reader, rowset_writer, new_tablet,
                                                        base_tablet_schema, new_tablet_schema);
}

Status SchemaChangeJob::process_alter_tablet(const TAlterTabletReqV2& request) {
    if (!request.__isset.desc_tbl) {
        return Status::Error<INVALID_ARGUMENT>(
                "desc_tbl is not set. Maybe the FE version is not equal to the BE "
                "version.");
    }
    if (_base_tablet == nullptr) {
        return Status::Error<TABLE_NOT_FOUND>("fail to find base tablet. base_tablet={}",
                                              request.base_tablet_id);
    }
    if (_new_tablet == nullptr) {
        return Status::Error<TABLE_NOT_FOUND>("fail to find new tablet. new_tablet={}",
                                              request.new_tablet_id);
    }

    LOG(INFO) << "begin to do request alter tablet: base_tablet_id=" << request.base_tablet_id
              << ", new_tablet_id=" << request.new_tablet_id
              << ", alter_version=" << request.alter_version;

    // Lock schema_change_lock util schema change info is stored in tablet header
    static constexpr long TRY_LOCK_TIMEOUT = 30;
    std::unique_lock schema_change_lock(_base_tablet->get_schema_change_lock(), std::defer_lock);
    bool owns_lock = schema_change_lock.try_lock_for(std::chrono::seconds(TRY_LOCK_TIMEOUT));

    if (!owns_lock) {
        return Status::Error<TRY_LOCK_FAILED>(
                "Failed to obtain schema change lock, there might be inverted index being "
                "built or cooldown runnning on base_tablet={}",
                request.base_tablet_id);
    }

    Status res = _do_process_alter_tablet(request);
    LOG(INFO) << "finished alter tablet process, res=" << res;
    DBUG_EXECUTE_IF("SchemaChangeJob::process_alter_tablet.leave.sleep", { sleep(5); });
    return res;
}

SchemaChangeJob::SchemaChangeJob(StorageEngine& local_storage_engine,
                                 const TAlterTabletReqV2& request, const std::string& job_id)
        : _local_storage_engine(local_storage_engine) {
    _base_tablet = _local_storage_engine.tablet_manager()->get_tablet(request.base_tablet_id);
    _new_tablet = _local_storage_engine.tablet_manager()->get_tablet(request.new_tablet_id);
    if (_base_tablet && _new_tablet) {
        _base_tablet_schema = std::make_shared<TabletSchema>();
        _base_tablet_schema->update_tablet_columns(*_base_tablet->tablet_schema(), request.columns);
        // The request only include column info, do not include bitmap or bloomfilter index info,
        // So we also need to copy index info from the real base tablet
        _base_tablet_schema->update_index_info_from(*_base_tablet->tablet_schema());
        // During a schema change, the extracted columns of a variant should not be included in the tablet schema.
        // This is because the schema change for a variant needs to ignore the extracted columns.
        // Otherwise, the schema types in different rowsets might be inconsistent. When performing a schema change,
        // the complete variant is constructed by reading all the sub-columns of the variant.
        _new_tablet_schema = _new_tablet->tablet_schema()->copy_without_variant_extracted_columns();
    }
    _job_id = job_id;
}

// In the past schema change and rollup will create new tablet  and will wait for txns starting before the task to finished
// It will cost a lot of time to wait and the task is very difficult to understand.
// In alter task v2, FE will call BE to create tablet and send an alter task to BE to convert historical data.
// The admin should upgrade all BE and then upgrade FE.
// Should delete the old code after upgrade finished.
Status SchemaChangeJob::_do_process_alter_tablet(const TAlterTabletReqV2& request) {
    DBUG_EXECUTE_IF("SchemaChangeJob._do_process_alter_tablet.sleep", { sleep(10); })
    Status res;
    signal::tablet_id = _base_tablet->get_table_id();

    // check if tablet's state is not_ready, if it is ready, it means the tablet already finished
    // check whether the tablet's max continuous version == request.version
    if (_new_tablet->tablet_state() != TABLET_NOTREADY) {
        res = _validate_alter_result(request);
        LOG(INFO) << "tablet's state=" << _new_tablet->tablet_state()
                  << " the convert job already finished, check its version"
                  << " res=" << res;
        return res;
    }
    _new_tablet->set_alter_failed(false);
    Defer defer([this] {
        // if tablet state is not TABLET_RUNNING when return, indicates that alter has failed.
        if (_new_tablet->tablet_state() != TABLET_RUNNING) {
            _new_tablet->set_alter_failed(true);
        }
    });

    LOG(INFO) << "finish to validate alter tablet request. begin to convert data from base tablet "
                 "to new tablet"
              << " base_tablet=" << _base_tablet->tablet_id()
              << " new_tablet=" << _new_tablet->tablet_id();

    std::shared_lock base_migration_rlock(_base_tablet->get_migration_lock(), std::try_to_lock);
    if (!base_migration_rlock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>(
                "SchemaChangeJob::_do_process_alter_tablet get lock failed");
    }
    std::shared_lock new_migration_rlock(_new_tablet->get_migration_lock(), std::try_to_lock);
    if (!new_migration_rlock.owns_lock()) {
        return Status::Error<TRY_LOCK_FAILED>(
                "SchemaChangeJob::_do_process_alter_tablet get lock failed");
    }

    std::vector<Version> versions_to_be_changed;
    int64_t end_version = -1;
    // reader_context is stack variables, it's lifetime should keep the same
    // with rs_readers
    RowsetReaderContext reader_context;
    std::vector<RowSetSplits> rs_splits;
    // delete handlers for new tablet
    DeleteHandler delete_handler;
    std::vector<ColumnId> return_columns;

    // Use tablet schema directly from base tablet, they are the newest schema, not contain
    // dropped column during light weight schema change.
    // But the tablet schema in base tablet maybe not the latest from FE, so that if fe pass through
    // a tablet schema, then use request schema.
    size_t num_cols =
            request.columns.empty() ? _base_tablet_schema->num_columns() : request.columns.size();
    return_columns.resize(num_cols);
    for (int i = 0; i < num_cols; ++i) {
        return_columns[i] = i;
    }
    std::vector<uint32_t> cluster_key_idxes;

    DBUG_EXECUTE_IF("SchemaChangeJob::_do_process_alter_tablet.block", DBUG_BLOCK);

    // begin to find deltas to convert from base tablet to new tablet so that
    // obtain base tablet and new tablet's push lock and header write lock to prevent loading data
    {
        std::lock_guard base_tablet_lock(_base_tablet->get_push_lock());
        std::lock_guard new_tablet_lock(_new_tablet->get_push_lock());
        std::lock_guard base_tablet_wlock(_base_tablet->get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        std::lock_guard<std::shared_mutex> new_tablet_wlock(_new_tablet->get_header_lock());

        do {
            RowsetSharedPtr max_rowset;
            // get history data to be converted and it will check if there is hold in base tablet
            res = _get_versions_to_be_changed(&versions_to_be_changed, &max_rowset);
            if (!res) {
                LOG(WARNING) << "fail to get version to be changed. res=" << res;
                break;
            }

            DBUG_EXECUTE_IF("SchemaChangeJob.process_alter_tablet.alter_fail", {
                res = Status::InternalError(
                        "inject alter tablet failed. base_tablet={}, new_tablet={}",
                        request.base_tablet_id, request.new_tablet_id);
                LOG(WARNING) << "inject error. res=" << res;
                break;
            });

            // should check the max_version >= request.alter_version, if not the convert is useless
            if (max_rowset == nullptr || max_rowset->end_version() < request.alter_version) {
                res = Status::InternalError(
                        "base tablet's max version={} is less than request version={}",
                        (max_rowset == nullptr ? 0 : max_rowset->end_version()),
                        request.alter_version);
                break;
            }
            // before calculating version_to_be_changed,
            // remove all data from new tablet, prevent to rewrite data(those double pushed when wait)
            LOG(INFO) << "begin to remove all data before end version from new tablet to prevent "
                         "rewrite."
                      << " new_tablet=" << _new_tablet->tablet_id()
                      << ", end_version=" << max_rowset->end_version();
            std::vector<RowsetSharedPtr> rowsets_to_delete;
            std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
            _new_tablet->acquire_version_and_rowsets(&version_rowsets);
            std::sort(version_rowsets.begin(), version_rowsets.end(),
                      [](const std::pair<Version, RowsetSharedPtr>& l,
                         const std::pair<Version, RowsetSharedPtr>& r) {
                          return l.first.first < r.first.first;
                      });
            for (auto& pair : version_rowsets) {
                if (pair.first.second <= max_rowset->end_version()) {
                    rowsets_to_delete.push_back(pair.second);
                } else if (pair.first.first <= max_rowset->end_version()) {
                    // If max version is [X-10] and new tablet has version [7-9][10-12],
                    // we only can remove [7-9] from new tablet. If we add [X-10] to new tablet, it will has version
                    // cross: [X-10] [10-12].
                    // So, we should return OLAP_ERR_VERSION_ALREADY_MERGED for fast fail.
                    return Status::Error<VERSION_ALREADY_MERGED>(
                            "New tablet has a version {} crossing base tablet's max_version={}",
                            pair.first.to_string(), max_rowset->end_version());
                }
            }
            std::vector<RowsetSharedPtr> empty_vec;
            RETURN_IF_ERROR(_new_tablet->delete_rowsets(rowsets_to_delete, false));
            // inherit cumulative_layer_point from base_tablet
            // check if new_tablet.ce_point > base_tablet.ce_point?
            _new_tablet->set_cumulative_layer_point(-1);
            // save tablet meta
            _new_tablet->save_meta();
            for (auto& rowset : rowsets_to_delete) {
                // do not call rowset.remove directly, using gc thread to delete it
                _local_storage_engine.add_unused_rowset(rowset);
            }

            // init one delete handler
            for (auto& version : versions_to_be_changed) {
                end_version = std::max(end_version, version.second);
            }

            // acquire data sources correspond to history versions
            RETURN_IF_ERROR(
                    _base_tablet->capture_rs_readers_unlocked(versions_to_be_changed, &rs_splits));
            if (rs_splits.empty()) {
                res = Status::Error<ALTER_DELTA_DOES_NOT_EXISTS>(
                        "fail to acquire all data sources. version_num={}, data_source_num={}",
                        versions_to_be_changed.size(), rs_splits.size());
                break;
            }
            std::vector<RowsetMetaSharedPtr> del_preds;
            for (auto&& split : rs_splits) {
                const auto& rs_meta = split.rs_reader->rowset()->rowset_meta();
                if (!rs_meta->has_delete_predicate() || rs_meta->start_version() > end_version) {
                    continue;
                }
                _base_tablet_schema->merge_dropped_columns(*rs_meta->tablet_schema());
                del_preds.push_back(rs_meta);
            }
            res = delete_handler.init(_base_tablet_schema, del_preds, end_version);
            if (!res) {
                LOG(WARNING) << "init delete handler failed. base_tablet="
                             << _base_tablet->tablet_id() << ", end_version=" << end_version;
                break;
            }

            reader_context.reader_type = ReaderType::READER_ALTER_TABLE;
            reader_context.tablet_schema = _base_tablet_schema;
            reader_context.need_ordered_result = true;
            reader_context.delete_handler = &delete_handler;
            reader_context.return_columns = &return_columns;
            reader_context.sequence_id_idx = reader_context.tablet_schema->sequence_col_idx();
            reader_context.is_unique = _base_tablet->keys_type() == UNIQUE_KEYS;
            reader_context.batch_size = ALTER_TABLE_BATCH_SIZE;
            reader_context.delete_bitmap = &_base_tablet->tablet_meta()->delete_bitmap();
            reader_context.version = Version(0, end_version);
            if (!_base_tablet_schema->cluster_key_uids().empty()) {
                for (const auto& uid : _base_tablet_schema->cluster_key_uids()) {
                    cluster_key_idxes.emplace_back(_base_tablet_schema->field_index(uid));
                }
                reader_context.read_orderby_key_columns = &cluster_key_idxes;
                reader_context.is_unique = false;
                reader_context.sequence_id_idx = -1;
            }
            for (auto& rs_split : rs_splits) {
                res = rs_split.rs_reader->init(&reader_context);
                if (!res) {
                    LOG(WARNING) << "failed to init rowset reader: " << _base_tablet->tablet_id();
                    break;
                }
            }
        } while (false);
    }

    do {
        if (!res) {
            break;
        }
        SchemaChangeParams sc_params;

        RETURN_IF_ERROR(
                DescriptorTbl::create(&sc_params.pool, request.desc_tbl, &sc_params.desc_tbl));
        sc_params.ref_rowset_readers.reserve(rs_splits.size());
        for (RowSetSplits& split : rs_splits) {
            sc_params.ref_rowset_readers.emplace_back(split.rs_reader);
        }
        sc_params.delete_handler = &delete_handler;
        sc_params.be_exec_version = request.be_exec_version;
        DCHECK(request.__isset.alter_tablet_type);
        switch (request.alter_tablet_type) {
        case TAlterTabletType::SCHEMA_CHANGE:
            sc_params.alter_tablet_type = AlterTabletType::SCHEMA_CHANGE;
            break;
        case TAlterTabletType::ROLLUP:
            sc_params.alter_tablet_type = AlterTabletType::ROLLUP;
            break;
        case TAlterTabletType::MIGRATION:
            sc_params.alter_tablet_type = AlterTabletType::MIGRATION;
            break;
        }
        if (request.__isset.materialized_view_params) {
            for (auto item : request.materialized_view_params) {
                AlterMaterializedViewParam mv_param;
                mv_param.column_name = item.column_name;

                if (item.__isset.mv_expr) {
                    mv_param.expr = std::make_shared<TExpr>(item.mv_expr);
                }
                sc_params.materialized_params_map.insert(
                        std::make_pair(to_lower(item.column_name), mv_param));
            }
        }
        {
            std::lock_guard<std::shared_mutex> wrlock(_mutex);
            _tablet_ids_in_converting.insert(_new_tablet->tablet_id());
        }
        int64_t real_alter_version = 0;
        sc_params.enable_unique_key_merge_on_write =
                _new_tablet->enable_unique_key_merge_on_write();
        res = _convert_historical_rowsets(sc_params, &real_alter_version);
        {
            std::lock_guard<std::shared_mutex> wrlock(_mutex);
            _tablet_ids_in_converting.erase(_new_tablet->tablet_id());
        }
        if (!res) {
            break;
        }

        DCHECK_GE(real_alter_version, request.alter_version);

        if (_new_tablet->keys_type() == UNIQUE_KEYS &&
            _new_tablet->enable_unique_key_merge_on_write()) {
            res = _calc_delete_bitmap_for_mow_table(real_alter_version);
            if (!res) {
                break;
            }
        } else {
            // set state to ready
            std::lock_guard<std::shared_mutex> new_wlock(_new_tablet->get_header_lock());
            SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
            res = _new_tablet->set_tablet_state(TabletState::TABLET_RUNNING);
            if (!res) {
                break;
            }
            _new_tablet->save_meta();
        }
    } while (false);

    if (res) {
        // _validate_alter_result should be outside the above while loop.
        // to avoid requiring the header lock twice.
        res = _validate_alter_result(request);
    }

    // if failed convert history data, then just remove the new tablet
    if (!res) {
        LOG(WARNING) << "failed to alter tablet. base_tablet=" << _base_tablet->tablet_id()
                     << ", drop new_tablet=" << _new_tablet->tablet_id();
        // do not drop the new tablet and its data. GC thread will
    }

    return res;
}

bool SchemaChangeJob::tablet_in_converting(int64_t tablet_id) {
    std::shared_lock rdlock(_mutex);
    return _tablet_ids_in_converting.find(tablet_id) != _tablet_ids_in_converting.end();
}

Status SchemaChangeJob::_get_versions_to_be_changed(std::vector<Version>* versions_to_be_changed,
                                                    RowsetSharedPtr* max_rowset) {
    RowsetSharedPtr rowset = _base_tablet->get_rowset_with_max_version();
    if (rowset == nullptr) {
        return Status::Error<ALTER_DELTA_DOES_NOT_EXISTS>("Tablet has no version. base_tablet={}",
                                                          _base_tablet->tablet_id());
    }
    *max_rowset = rowset;

    RETURN_IF_ERROR(_base_tablet->capture_consistent_versions_unlocked(
            Version(0, rowset->version().second), versions_to_be_changed, false, false));

    return Status::OK();
}

// The `real_alter_version` parameter indicates that the version of [0-real_alter_version] is
// converted from a base tablet, only used for the mow table now.
Status SchemaChangeJob::_convert_historical_rowsets(const SchemaChangeParams& sc_params,
                                                    int64_t* real_alter_version) {
    LOG(INFO) << "begin to convert historical rowsets for new_tablet from base_tablet."
              << " base_tablet=" << _base_tablet->tablet_id()
              << ", new_tablet=" << _new_tablet->tablet_id() << ", job_id=" << _job_id;

    // find end version
    int64_t end_version = -1;
    for (const auto& ref_rowset_reader : sc_params.ref_rowset_readers) {
        if (ref_rowset_reader->version().second > end_version) {
            end_version = ref_rowset_reader->version().second;
        }
    }

    // Add filter information in change, and filter column information will be set in parse_request
    // And filter some data every time the row block changes
    BlockChanger changer(_new_tablet_schema, *sc_params.desc_tbl);

    bool sc_sorting = false;
    bool sc_directly = false;

    // a.Parse the Alter request and convert it into an internal representation
    Status res = parse_request(sc_params, _base_tablet_schema.get(), _new_tablet_schema.get(),
                               &changer, &sc_sorting, &sc_directly);
    LOG(INFO) << "schema change type, sc_sorting: " << sc_sorting
              << ", sc_directly: " << sc_directly << ", base_tablet=" << _base_tablet->tablet_id()
              << ", new_tablet=" << _new_tablet->tablet_id();

    auto process_alter_exit = [&]() -> Status {
        {
            // save tablet meta here because rowset meta is not saved during add rowset
            std::lock_guard new_wlock(_new_tablet->get_header_lock());
            SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
            _new_tablet->save_meta();
        }
        if (res) {
            Version test_version(0, end_version);
            res = _new_tablet->check_version_integrity(test_version);
        }

        LOG(INFO) << "finish converting rowsets for new_tablet from base_tablet. "
                  << "base_tablet=" << _base_tablet->tablet_id()
                  << ", new_tablet=" << _new_tablet->tablet_id();
        return res;
    };

    if (!res) {
        LOG(WARNING) << "failed to parse the request. res=" << res;
        return process_alter_exit();
    }

    if (!sc_sorting && !sc_directly && sc_params.alter_tablet_type == AlterTabletType::ROLLUP) {
        res = Status::Error<SCHEMA_SCHEMA_INVALID>(
                "Don't support to add materialized view by linked schema change");
        return process_alter_exit();
    }

    // b. Generate historical data converter
    auto sc_procedure = _get_sc_procedure(
            changer, sc_sorting, sc_directly,
            _local_storage_engine.memory_limitation_bytes_per_thread_for_schema_change());

    DBUG_EXECUTE_IF("SchemaChangeJob::_convert_historical_rowsets.block", DBUG_BLOCK);

    // c.Convert historical data
    bool have_failure_rowset = false;
    for (const auto& rs_reader : sc_params.ref_rowset_readers) {
        // set status for monitor
        // As long as there is a new_table as running, ref table is set as running
        // NOTE If the first sub_table fails first, it will continue to go as normal here
        // When tablet create new rowset writer, it may change rowset type, in this case
        // linked schema change will not be used.
        RowsetWriterContext context;
        context.version = rs_reader->version();
        context.rowset_state = VISIBLE;
        context.segments_overlap = rs_reader->rowset()->rowset_meta()->segments_overlap();
        context.tablet_schema = _new_tablet_schema;
        context.newest_write_timestamp = rs_reader->newest_write_timestamp();

        if (!rs_reader->rowset()->is_local()) {
            auto maybe_resource = rs_reader->rowset()->rowset_meta()->remote_storage_resource();
            if (!maybe_resource) {
                return maybe_resource.error();
            }
            context.storage_resource = *maybe_resource.value();
        }

        context.write_type = DataWriteType::TYPE_SCHEMA_CHANGE;
        // TODO if support VerticalSegmentWriter, also need to handle cluster key primary key index
        bool vertical = false;
        if (sc_sorting && !_new_tablet->tablet_schema()->cluster_key_uids().empty()) {
            // see VBaseSchemaChangeWithSorting::_external_sorting
            vertical = true;
        }
        auto result = _new_tablet->create_rowset_writer(context, vertical);
        if (!result.has_value()) {
            res = Status::Error<ROWSET_BUILDER_INIT>("create_rowset_writer failed, reason={}",
                                                     result.error().to_string());
            return process_alter_exit();
        }
        auto rowset_writer = std::move(result).value();
        auto pending_rs_guard = _local_storage_engine.add_pending_rowset(context);

        if (res = sc_procedure->process(rs_reader, rowset_writer.get(), _new_tablet, _base_tablet,
                                        _base_tablet_schema, _new_tablet_schema);
            !res) {
            LOG(WARNING) << "failed to process the version."
                         << " version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second << ", " << res.to_string();
            return process_alter_exit();
        }
        // Add the new version of the data to the header
        // In order to prevent the occurrence of deadlock, we must first lock the old table, and then lock the new table
        std::lock_guard lock(_new_tablet->get_push_lock());
        RowsetSharedPtr new_rowset;
        if (!(res = rowset_writer->build(new_rowset)).ok()) {
            LOG(WARNING) << "failed to build rowset, exit alter process";
            return process_alter_exit();
        }
        res = _new_tablet->add_rowset(new_rowset);
        if (res.is<PUSH_VERSION_ALREADY_EXIST>()) {
            LOG(WARNING) << "version already exist, version revert occurred. "
                         << "tablet=" << _new_tablet->tablet_id() << ", version='"
                         << rs_reader->version().first << "-" << rs_reader->version().second;
            _local_storage_engine.add_unused_rowset(new_rowset);
            have_failure_rowset = true;
            res = Status::OK();
        } else if (!res) {
            LOG(WARNING) << "failed to register new version. "
                         << " tablet=" << _new_tablet->tablet_id()
                         << ", version=" << rs_reader->version().first << "-"
                         << rs_reader->version().second;
            _local_storage_engine.add_unused_rowset(new_rowset);
            return process_alter_exit();
        } else {
            VLOG_NOTICE << "register new version. tablet=" << _new_tablet->tablet_id()
                        << ", version=" << rs_reader->version().first << "-"
                        << rs_reader->version().second;
        }
        if (!have_failure_rowset) {
            *real_alter_version = rs_reader->version().second;
        }

        VLOG_TRACE << "succeed to convert a history version."
                   << " version=" << rs_reader->version().first << "-"
                   << rs_reader->version().second;
    }

    // XXX:The SchemaChange state should not be canceled at this time, because the new Delta has to be converted to the old and new Schema version
    return process_alter_exit();
}

static const std::string WHERE_SIGN_LOWER = to_lower("__DORIS_WHERE_SIGN__");

// @static
// Analyze the mapping of the column and the mapping of the filter key
Status SchemaChangeJob::parse_request(const SchemaChangeParams& sc_params,
                                      TabletSchema* base_tablet_schema,
                                      TabletSchema* new_tablet_schema, BlockChanger* changer,
                                      bool* sc_sorting, bool* sc_directly) {
    changer->set_type(sc_params.alter_tablet_type);
    changer->set_compatible_version(sc_params.be_exec_version);

    const std::unordered_map<std::string, AlterMaterializedViewParam>& materialized_function_map =
            sc_params.materialized_params_map;
    DescriptorTbl desc_tbl = *sc_params.desc_tbl;

    // set column mapping
    for (size_t i = 0, new_schema_size = new_tablet_schema->num_columns(); i < new_schema_size;
         ++i) {
        const TabletColumn& new_column = new_tablet_schema->column(i);
        const std::string& column_name_lower = to_lower(new_column.name());
        ColumnMapping* column_mapping = changer->get_mutable_column_mapping(i);
        column_mapping->new_column = &new_column;

        column_mapping->ref_column_idx = base_tablet_schema->field_index(new_column.name());

        if (materialized_function_map.find(column_name_lower) != materialized_function_map.end()) {
            auto mv_param = materialized_function_map.find(column_name_lower)->second;
            column_mapping->expr = mv_param.expr;
            if (column_mapping->expr != nullptr) {
                continue;
            }
        }

        if (column_mapping->ref_column_idx >= 0) {
            continue;
        }

        if (sc_params.alter_tablet_type == ROLLUP) {
            std::string materialized_function_map_str;
            for (auto str : materialized_function_map) {
                if (!materialized_function_map_str.empty()) {
                    materialized_function_map_str += ',';
                }
                materialized_function_map_str += str.first;
            }
            return Status::InternalError(
                    "referenced column was missing. [column={},materialized_function_map={}]",
                    new_column.name(), materialized_function_map_str);
        }

        if (new_column.name().find("__doris_shadow_") == 0) {
            // Should delete in the future, just a protection for bug.
            LOG(INFO) << "a shadow column is encountered " << new_column.name();
            return Status::InternalError("failed due to operate on shadow column");
        }
        // Newly added column go here
        column_mapping->ref_column_idx = -1;

        if (i < base_tablet_schema->num_short_key_columns()) {
            *sc_directly = true;
        }
        RETURN_IF_ERROR(
                _init_column_mapping(column_mapping, new_column, new_column.default_value()));

        LOG(INFO) << "A column with default value will be added after schema changing. "
                  << "column=" << new_column.name()
                  << ", default_value=" << new_column.default_value();
    }

    if (materialized_function_map.contains(WHERE_SIGN_LOWER)) {
        changer->set_where_expr(materialized_function_map.find(WHERE_SIGN_LOWER)->second.expr);
    }

    // If the reference sequence of the Key column is out of order, it needs to be reordered
    int num_default_value = 0;

    for (int i = 0, new_schema_size = cast_set<int>(new_tablet_schema->num_key_columns());
         i < new_schema_size; ++i) {
        ColumnMapping* column_mapping = changer->get_mutable_column_mapping(i);

        if (!column_mapping->has_reference()) {
            num_default_value++;
            continue;
        }

        if (column_mapping->ref_column_idx != i - num_default_value) {
            *sc_sorting = true;
            return Status::OK();
        }
    }

    if (base_tablet_schema->keys_type() != new_tablet_schema->keys_type()) {
        // only when base table is dup and mv is agg
        // the rollup job must be reagg.
        *sc_sorting = true;
        return Status::OK();
    }

    // If the sort of key has not been changed but the new keys num is less then base's,
    // the new table should be re agg.
    // So we also need to set sc_sorting = true.
    // A, B, C are keys(sort keys), D is value
    // followings need resort:
    //      old keys:    A   B   C   D
    //      new keys:    A   B
    if (new_tablet_schema->keys_type() != KeysType::DUP_KEYS &&
        new_tablet_schema->num_key_columns() < base_tablet_schema->num_key_columns()) {
        // this is a table with aggregate key type, and num of key columns in new schema
        // is less, which means the data in new tablet should be more aggregated.
        // so we use sorting schema change to sort and merge the data.
        *sc_sorting = true;
        return Status::OK();
    }

    if (sc_params.alter_tablet_type == ROLLUP) {
        *sc_directly = true;
        return Status::OK();
    }

    if (sc_params.enable_unique_key_merge_on_write &&
        new_tablet_schema->num_key_columns() > base_tablet_schema->num_key_columns()) {
        *sc_directly = true;
        return Status::OK();
    }

    if (base_tablet_schema->num_short_key_columns() != new_tablet_schema->num_short_key_columns()) {
        // the number of short_keys changed, can't do linked schema change
        *sc_directly = true;
        return Status::OK();
    }

    if (!sc_params.delete_handler->empty()) {
        // there exists delete condition in header, can't do linked schema change
        *sc_directly = true;
        return Status::OK();
    }

    // if new tablet enable row store, or new tablet has different row store columns
    if ((!base_tablet_schema->exist_column(BeConsts::ROW_STORE_COL) &&
         new_tablet_schema->exist_column(BeConsts::ROW_STORE_COL)) ||
        !std::equal(new_tablet_schema->row_columns_uids().begin(),
                    new_tablet_schema->row_columns_uids().end(),
                    base_tablet_schema->row_columns_uids().begin(),
                    base_tablet_schema->row_columns_uids().end())) {
        *sc_directly = true;
    }

    for (size_t i = 0; i < new_tablet_schema->num_columns(); ++i) {
        ColumnMapping* column_mapping = changer->get_mutable_column_mapping(i);
        if (column_mapping->expr != nullptr) {
            *sc_directly = true;
            return Status::OK();
        } else if (column_mapping->ref_column_idx >= 0) {
            // index changed
            if (vectorized::schema_util::has_schema_index_diff(
                        new_tablet_schema, base_tablet_schema, cast_set<int32_t>(i),
                        column_mapping->ref_column_idx)) {
                *sc_directly = true;
                return Status::OK();
            }
        }
    }

    // if rs_reader has remote files, link schema change is not supported,
    // use directly schema change instead.
    if (!(*sc_directly) && !(*sc_sorting)) {
        // check has remote rowset
        // work for cloud and cold storage
        for (const auto& rs_reader : sc_params.ref_rowset_readers) {
            if (!rs_reader->rowset()->is_local()) {
                *sc_directly = true;
                break;
            }
        }
    }

    return Status::OK();
}

Status SchemaChangeJob::_init_column_mapping(ColumnMapping* column_mapping,
                                             const TabletColumn& column_schema,
                                             const std::string& value) {
    if (auto field = WrapperField::create(column_schema); field.has_value()) {
        column_mapping->default_value = field.value();
    } else {
        return field.error();
    }

    if (column_schema.is_nullable() && value.length() == 0) {
        column_mapping->default_value->set_null();
    } else {
        RETURN_IF_ERROR(column_mapping->default_value->from_string(value, column_schema.precision(),
                                                                   column_schema.frac()));
    }

    return Status::OK();
}

Status SchemaChangeJob::_validate_alter_result(const TAlterTabletReqV2& request) {
    Version max_continuous_version = {-1, 0};
    _new_tablet->max_continuous_version_from_beginning(&max_continuous_version);
    LOG(INFO) << "find max continuous version of tablet=" << _new_tablet->tablet_id()
              << ", start_version=" << max_continuous_version.first
              << ", end_version=" << max_continuous_version.second;
    if (max_continuous_version.second < request.alter_version) {
        return Status::InternalError("result version={} is less than request version={}",
                                     max_continuous_version.second, request.alter_version);
    }

    std::vector<std::pair<Version, RowsetSharedPtr>> version_rowsets;
    {
        std::shared_lock rdlock(_new_tablet->get_header_lock());
        _new_tablet->acquire_version_and_rowsets(&version_rowsets);
    }
    for (auto& pair : version_rowsets) {
        RowsetSharedPtr rowset = pair.second;
        if (!rowset->check_file_exist()) {
            return Status::Error<NOT_FOUND>(
                    "SchemaChangeJob::_validate_alter_result meet invalid rowset");
        }
    }
    return Status::OK();
}

// For unique with merge-on-write table, should process delete bitmap here.
// 1. During double write, the newly imported rowsets does not calculate
// delete bitmap and publish successfully.
// 2. After conversion, calculate delete bitmap for the rowsets imported
// during double write. During this period, new data can still be imported
// witout calculating delete bitmap and publish successfully.
// 3. Block the new publish, calculate the delete bitmap of the
// incremental rowsets.
// 4. Switch the tablet status to TABLET_RUNNING. The newly imported
// data will calculate delete bitmap.
Status SchemaChangeJob::_calc_delete_bitmap_for_mow_table(int64_t alter_version) {
    DBUG_EXECUTE_IF("SchemaChangeJob._calc_delete_bitmap_for_mow_table.random_failed", {
        if (rand() % 100 < (100 * dp->param("percent", 0.1))) {
            LOG_WARNING("SchemaChangeJob._calc_delete_bitmap_for_mow_table.random_failed");
            return Status::InternalError("debug schema change calc delete bitmap random failed");
        }
    });

    // can't do compaction when calc delete bitmap, if the rowset being calculated does
    // a compaction, it may cause the delete bitmap to be missed.
    std::lock_guard base_compaction_lock(_new_tablet->get_base_compaction_lock());
    std::lock_guard cumu_compaction_lock(_new_tablet->get_cumulative_compaction_lock());

    // step 2
    int64_t max_version = _new_tablet->max_version().second;
    std::vector<RowsetSharedPtr> rowsets;
    if (alter_version < max_version) {
        LOG(INFO) << "alter table for unique with merge-on-write, calculate delete bitmap of "
                  << "double write rowsets for version: " << alter_version + 1 << "-" << max_version
                  << " new_tablet=" << _new_tablet->tablet_id();
        std::shared_lock rlock(_new_tablet->get_header_lock());
        RETURN_IF_ERROR(_new_tablet->capture_consistent_rowsets_unlocked(
                {alter_version + 1, max_version}, &rowsets));
    }
    for (auto rowset_ptr : rowsets) {
        std::lock_guard rwlock(_new_tablet->get_rowset_update_lock());
        std::shared_lock rlock(_new_tablet->get_header_lock());
        RETURN_IF_ERROR(Tablet::update_delete_bitmap_without_lock(_new_tablet, rowset_ptr));
    }

    // step 3
    std::lock_guard rwlock(_new_tablet->get_rowset_update_lock());
    std::lock_guard new_wlock(_new_tablet->get_header_lock());
    SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
    int64_t new_max_version = _new_tablet->max_version_unlocked();
    rowsets.clear();
    if (max_version < new_max_version) {
        LOG(INFO) << "alter table for unique with merge-on-write, calculate delete bitmap of "
                  << "incremental rowsets for version: " << max_version + 1 << "-"
                  << new_max_version << " new_tablet=" << _new_tablet->tablet_id();
        RETURN_IF_ERROR(_new_tablet->capture_consistent_rowsets_unlocked(
                {max_version + 1, new_max_version}, &rowsets));
    }
    for (auto&& rowset_ptr : rowsets) {
        RETURN_IF_ERROR(Tablet::update_delete_bitmap_without_lock(_new_tablet, rowset_ptr));
    }
    // step 4
    RETURN_IF_ERROR(_new_tablet->set_tablet_state(TabletState::TABLET_RUNNING));
    _new_tablet->save_meta();
    return Status::OK();
}

#include "common/compile_check_end.h"

} // namespace doris
