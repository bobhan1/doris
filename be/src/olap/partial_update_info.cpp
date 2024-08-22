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

#include "olap/partial_update_info.h"

#include <gen_cpp/olap_file.pb.h>

#include "olap/base_tablet.h"
#include "olap/olap_common.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/tablet_schema.h"
#include "olap/utils.h"
#include "util/bitmap_value.h"
#include "vec/common/assert_cast.h"
#include "vec/core/block.h"

namespace doris {

void PartialUpdateInfo::init(const TabletSchema& tablet_schema, bool fixed_partial_update,
                             bool flexible_partial_update,
                             const std::set<string>& partial_update_cols, bool is_strict_mode,
                             int64_t timestamp_ms, const std::string& timezone,
                             const std::string& auto_increment_column, int64_t cur_max_version) {
    DCHECK(!(fixed_partial_update && flexible_partial_update))
            << "fixed_partial_update and flexible_partial_update can not be set simutanously!";
    if (fixed_partial_update) {
        partial_update_mode = PartialUpdateModePB::FIXED;
    } else if (flexible_partial_update) {
        partial_update_mode = PartialUpdateModePB::FLEXIBLE;
    } else {
        partial_update_mode = PartialUpdateModePB::NONE;
    }
    partial_update_input_columns = partial_update_cols;
    max_version_in_flush_phase = cur_max_version;
    this->timestamp_ms = timestamp_ms;
    this->timezone = timezone;
    missing_cids.clear();
    update_cids.clear();

    for (auto i = 0; i < tablet_schema.num_columns(); ++i) {
        if (fixed_partial_update) {
            auto tablet_column = tablet_schema.column(i);
            if (!partial_update_input_columns.contains(tablet_column.name())) {
                missing_cids.emplace_back(i);
                if (!tablet_column.has_default_value() && !tablet_column.is_nullable() &&
                    tablet_schema.auto_increment_column() != tablet_column.name()) {
                    can_insert_new_rows_in_partial_update = false;
                }
            } else {
                update_cids.emplace_back(i);
            }
            if (auto_increment_column == tablet_column.name()) {
                is_schema_contains_auto_inc_column = true;
            }
        } else {
            // in flexible partial update, missing cids is all non sort keys' cid
            if (i >= tablet_schema.num_key_columns()) {
                missing_cids.emplace_back(i);
            }
        }
    }
    this->is_strict_mode = is_strict_mode;
    is_input_columns_contains_auto_inc_column =
            is_fixed_partial_update() &&
            partial_update_input_columns.contains(auto_increment_column);
    _generate_default_values_for_missing_cids(tablet_schema);
}

void PartialUpdateInfo::to_pb(PartialUpdateInfoPB* partial_update_info_pb) const {
    partial_update_info_pb->set_partial_update_mode(partial_update_mode);
    partial_update_info_pb->set_max_version_in_flush_phase(max_version_in_flush_phase);
    for (const auto& col : partial_update_input_columns) {
        partial_update_info_pb->add_partial_update_input_columns(col);
    }
    for (auto cid : missing_cids) {
        partial_update_info_pb->add_missing_cids(cid);
    }
    for (auto cid : update_cids) {
        partial_update_info_pb->add_update_cids(cid);
    }
    partial_update_info_pb->set_can_insert_new_rows_in_partial_update(
            can_insert_new_rows_in_partial_update);
    partial_update_info_pb->set_is_strict_mode(is_strict_mode);
    partial_update_info_pb->set_timestamp_ms(timestamp_ms);
    partial_update_info_pb->set_timezone(timezone);
    partial_update_info_pb->set_is_input_columns_contains_auto_inc_column(
            is_input_columns_contains_auto_inc_column);
    partial_update_info_pb->set_is_schema_contains_auto_inc_column(
            is_schema_contains_auto_inc_column);
    for (const auto& value : default_values) {
        partial_update_info_pb->add_default_values(value);
    }
}

void PartialUpdateInfo::from_pb(PartialUpdateInfoPB* partial_update_info_pb) {
    if (!partial_update_info_pb->has_partial_update_mode()) {
        // for backward compatibility
        if (partial_update_info_pb->is_partial_update()) {
            partial_update_mode = PartialUpdateModePB::FIXED;
        } else {
            partial_update_mode = PartialUpdateModePB::NONE;
        }
    } else {
        partial_update_mode = partial_update_info_pb->partial_update_mode();
    }
    max_version_in_flush_phase = partial_update_info_pb->has_max_version_in_flush_phase()
                                         ? partial_update_info_pb->max_version_in_flush_phase()
                                         : -1;
    partial_update_input_columns.clear();
    for (const auto& col : partial_update_info_pb->partial_update_input_columns()) {
        partial_update_input_columns.insert(col);
    }
    missing_cids.clear();
    for (auto cid : partial_update_info_pb->missing_cids()) {
        missing_cids.push_back(cid);
    }
    update_cids.clear();
    for (auto cid : partial_update_info_pb->update_cids()) {
        update_cids.push_back(cid);
    }
    can_insert_new_rows_in_partial_update =
            partial_update_info_pb->can_insert_new_rows_in_partial_update();
    is_strict_mode = partial_update_info_pb->is_strict_mode();
    timestamp_ms = partial_update_info_pb->timestamp_ms();
    timezone = partial_update_info_pb->timezone();
    is_input_columns_contains_auto_inc_column =
            partial_update_info_pb->is_input_columns_contains_auto_inc_column();
    is_schema_contains_auto_inc_column =
            partial_update_info_pb->is_schema_contains_auto_inc_column();
    default_values.clear();
    for (const auto& value : partial_update_info_pb->default_values()) {
        default_values.push_back(value);
    }
}

std::string PartialUpdateInfo::summary() const {
    std::string mode;
    switch (partial_update_mode) {
    case PartialUpdateModePB::NONE:
        mode = "none";
        break;
    case PartialUpdateModePB::FIXED:
        mode = "fixed partial update";
        break;
    case PartialUpdateModePB::FLEXIBLE:
        mode = "flexible partial update";
        break;
    }
    return fmt::format(
            "mode={}, update_cids={}, missing_cids={}, is_strict_mode={}, "
            "max_version_in_flush_phase={}",
            mode, update_cids.size(), missing_cids.size(), is_strict_mode,
            max_version_in_flush_phase);
}

void PartialUpdateInfo::_generate_default_values_for_missing_cids(
        const TabletSchema& tablet_schema) {
    for (unsigned int cur_cid : missing_cids) {
        const auto& column = tablet_schema.column(cur_cid);
        if (column.has_default_value()) {
            std::string default_value;
            if (UNLIKELY(column.type() == FieldType::OLAP_FIELD_TYPE_DATETIMEV2 &&
                         to_lower(column.default_value()).find(to_lower("CURRENT_TIMESTAMP")) !=
                                 std::string::npos)) {
                DateV2Value<DateTimeV2ValueType> dtv;
                dtv.from_unixtime(timestamp_ms / 1000, timezone);
                default_value = dtv.debug_string();
            } else if (UNLIKELY(column.type() == FieldType::OLAP_FIELD_TYPE_DATEV2 &&
                                to_lower(column.default_value()).find(to_lower("CURRENT_DATE")) !=
                                        std::string::npos)) {
                DateV2Value<DateV2ValueType> dv;
                dv.from_unixtime(timestamp_ms / 1000, timezone);
                default_value = dv.debug_string();
            } else {
                default_value = column.default_value();
            }
            default_values.emplace_back(default_value);
        } else {
            // place an empty string here
            default_values.emplace_back();
        }
    }
    CHECK_EQ(missing_cids.size(), default_values.size());
}

void FixedReadPlan::prepare_to_read(const RowLocation& row_location, size_t pos) {
    plan[row_location.rowset_id][row_location.segment_id].emplace_back(row_location.row_id, pos);
}

// read columns by read plan
// read_index: ori_pos-> block_idx
Status FixedReadPlan::read_columns_by_plan(
        const TabletSchema& tablet_schema, const std::vector<uint32_t> cids_to_read,
        const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset, vectorized::Block& block,
        std::map<uint32_t, uint32_t>* read_index, const signed char* __restrict skip_map) const {
    bool has_row_column = tablet_schema.has_row_store_for_all_columns();
    auto mutable_columns = block.mutate_columns();
    size_t read_idx = 0;
    for (const auto& [rowset_id, segment_row_mappings] : plan) {
        for (const auto& [segment_id, mappings] : segment_row_mappings) {
            auto rowset_iter = rsid_to_rowset.find(rowset_id);
            CHECK(rowset_iter != rsid_to_rowset.end());
            std::vector<uint32_t> rids;
            for (auto [rid, pos] : mappings) {
                if (skip_map && skip_map[pos]) {
                    continue;
                }
                rids.emplace_back(rid);
                (*read_index)[pos] = read_idx++;
            }
            if (has_row_column) {
                auto st = doris::BaseTablet::fetch_value_through_row_column(
                        rowset_iter->second, tablet_schema, segment_id, rids, cids_to_read, block);
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value through row column";
                    return st;
                }
                continue;
            }
            for (size_t cid = 0; cid < mutable_columns.size(); ++cid) {
                TabletColumn tablet_column = tablet_schema.column(cids_to_read[cid]);
                auto st = doris::BaseTablet::fetch_value_by_rowids(
                        rowset_iter->second, segment_id, rids, tablet_column, mutable_columns[cid]);
                // set read value to output block
                if (!st.ok()) {
                    LOG(WARNING) << "failed to fetch value";
                    return st;
                }
            }
        }
    }
    block.set_columns(std::move(mutable_columns));
    return Status::OK();
}

Status FixedReadPlan::fill_missing_columns(
        RowsetWriterContext* rowset_ctx, const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        const TabletSchema& tablet_schema, vectorized::Block& full_block,
        const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
        const size_t& segment_start_pos, const vectorized::Block* block) const {
    auto mutable_full_columns = full_block.mutate_columns();
    // create old value columns
    const auto& missing_cids = rowset_ctx->partial_update_info->missing_cids;
    auto old_value_block = tablet_schema.create_block_by_cids(missing_cids);
    CHECK_EQ(missing_cids.size(), old_value_block.columns());

    // segment pos to write -> rowid to read in old_value_block
    std::map<uint32_t, uint32_t> read_index;
    RETURN_IF_ERROR(read_columns_by_plan(tablet_schema, missing_cids, rsid_to_rowset,
                                         old_value_block, &read_index, nullptr));

    const auto* delete_sign_column_data = BaseTablet::get_delete_sign_column_data(old_value_block);

    // build default value columns
    auto default_value_block = old_value_block.clone_empty();
    if (has_default_or_nullable || delete_sign_column_data != nullptr) {
        RETURN_IF_ERROR(BaseTablet::generate_default_value_block(
                tablet_schema, missing_cids, rowset_ctx->partial_update_info->default_values,
                old_value_block, default_value_block));
    }
    auto mutable_default_value_columns = default_value_block.mutate_columns();

    // fill all missing value from mutable_old_columns, need to consider default value and null value
    for (auto idx = 0; idx < use_default_or_null_flag.size(); idx++) {
        // `use_default_or_null_flag[idx] == false` doesn't mean that we should read values from the old row
        // for the missing columns. For example, if a table has sequence column, the rows with DELETE_SIGN column
        // marked will not be marked in delete bitmap(see https://github.com/apache/doris/pull/24011), so it will
        // be found in Tablet::lookup_row_key() and `use_default_or_null_flag[idx]` will be false. But we should not
        // read values from old rows for missing values in this occasion. So we should read the DELETE_SIGN column
        // to check if a row REALLY exists in the table.
        auto segment_pos = idx + segment_start_pos;
        auto pos_in_old_block = read_index[segment_pos];
        if (use_default_or_null_flag[idx] || (delete_sign_column_data != nullptr &&
                                              delete_sign_column_data[pos_in_old_block] != 0)) {
            for (auto i = 0; i < missing_cids.size(); ++i) {
                // if the column has default value, fill it with default value
                // otherwise, if the column is nullable, fill it with null value
                const auto& tablet_column = tablet_schema.column(missing_cids[i]);
                auto& missing_col = mutable_full_columns[missing_cids[i]];
                // clang-format off
                if (tablet_column.has_default_value()) {
                    missing_col->insert_from(*mutable_default_value_columns[i], 0);
                } else if (tablet_column.is_nullable()) {
                    auto* nullable_column =
                            assert_cast<vectorized::ColumnNullable*, TypeCheckOnRelease::DISABLE>(missing_col.get());
                    nullable_column->insert_null_elements(1);
                } else if (tablet_schema.auto_increment_column() == tablet_column.name()) {
                    const auto& column =
                            *DORIS_TRY(rowset_ctx->tablet_schema->column(tablet_column.name()));
                    DCHECK(column.type() == FieldType::OLAP_FIELD_TYPE_BIGINT);
                    auto* auto_inc_column =
                            assert_cast<vectorized::ColumnInt64*, TypeCheckOnRelease::DISABLE>(missing_col.get());
                    auto_inc_column->insert(
                            (assert_cast<const vectorized::ColumnInt64*, TypeCheckOnRelease::DISABLE>(
                                     block->get_by_name("__PARTIAL_UPDATE_AUTO_INC_COLUMN__").column.get()))->get_element(idx));
                } else {
                    // If the control flow reaches this branch, the column neither has default value
                    // nor is nullable. It means that the row's delete sign is marked, and the value
                    // columns are useless and won't be read. So we can just put arbitary values in the cells
                    missing_col->insert_default();
                }
                // clang-format on
            }
            continue;
        }
        for (auto i = 0; i < missing_cids.size(); ++i) {
            mutable_full_columns[missing_cids[i]]->insert_from(
                    *old_value_block.get_by_position(i).column, pos_in_old_block);
        }
    }
    full_block.set_columns(std::move(mutable_full_columns));
    return Status::OK();
}

void FlexibleReadPlan::prepare_to_read(const RowLocation& row_location, size_t pos,
                                       const BitmapValue& skip_bitmap) {
    for (uint64_t col_uid : skip_bitmap) {
        plan[row_location.rowset_id][row_location.segment_id][col_uid].emplace_back(
                row_location.row_id, pos);
    }
}

Status FlexibleReadPlan::read_columns_by_plan(
        const TabletSchema& tablet_schema,
        const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        vectorized::Block& old_value_block,
        std::map<uint32_t, std::map<uint32_t, uint32_t>>* read_index,
        const signed char* __restrict skip_map) const {
    auto mutable_columns = old_value_block.mutate_columns();

    // cid -> next rid to fill in block
    std::map<uint32_t, uint32_t> next_read_idx;
    for (std::size_t cid {0}; cid < tablet_schema.num_columns(); cid++) {
        next_read_idx[cid] = 0;
    }

    for (const auto& [rowset_id, segment_mappings] : plan) {
        for (const auto& [segment_id, uid_mappings] : segment_mappings) {
            for (const auto& [col_uid, mappings] : uid_mappings) {
                auto rowset_iter = rsid_to_rowset.find(rowset_id);
                CHECK(rowset_iter != rsid_to_rowset.end());
                auto cid = tablet_schema.field_index(col_uid);
                DCHECK(cid != -1);
                DCHECK(cid >= tablet_schema.num_key_columns());
                std::vector<uint32_t> rids;
                for (auto [rid, pos] : mappings) {
                    if (skip_map && skip_map[pos]) {
                        continue;
                    }
                    rids.emplace_back(rid);
                    (*read_index)[cid][pos] = next_read_idx[cid]++;
                }

                TabletColumn tablet_column = tablet_schema.column(cid);
                auto idx = cid - tablet_schema.num_key_columns();
                RETURN_IF_ERROR(doris::BaseTablet::fetch_value_by_rowids(
                        rowset_iter->second, segment_id, rids, tablet_column,
                        mutable_columns[idx]));
            }
        }
    }
    // !!!ATTENTION!!!: columns in block may have different size because every row has different columns to update
    old_value_block.set_columns(std::move(mutable_columns));
    return Status::OK();
}

Status FlexibleReadPlan::fill_non_sort_key_columns(
        RowsetWriterContext* rowset_ctx, const std::map<RowsetId, RowsetSharedPtr>& rsid_to_rowset,
        const TabletSchema& tablet_schema, vectorized::Block& full_block,
        const std::vector<bool>& use_default_or_null_flag, bool has_default_or_nullable,
        const std::size_t segment_start_pos, const std::size_t block_start_pos,
        const vectorized::Block* block, std::vector<BitmapValue>* skip_bitmaps) const {
    auto mutable_full_columns = full_block.mutate_columns();

    // missing_cids are all non sort key columns' cids
    const auto& non_sort_key_cids = rowset_ctx->partial_update_info->missing_cids;
    auto old_value_block = tablet_schema.create_block_by_cids(non_sort_key_cids);
    CHECK_EQ(non_sort_key_cids.size(), old_value_block.columns());

    // cid -> segment pos to write -> rowid to read in old_value_block
    std::map<uint32_t, std::map<uint32_t, uint32_t>> read_index;
    RETURN_IF_ERROR(read_columns_by_plan(tablet_schema, rsid_to_rowset, old_value_block,
                                         &read_index, nullptr));
    // !!!ATTENTION!!!: columns in old_value_block may have different size because every row has different columns to update

    const auto* delete_sign_column_data = BaseTablet::get_delete_sign_column_data(old_value_block);

    // build default value columns
    auto default_value_block = old_value_block.clone_empty();
    if (has_default_or_nullable || delete_sign_column_data != nullptr) {
        RETURN_IF_ERROR(BaseTablet::generate_default_value_block(
                tablet_schema, non_sort_key_cids, rowset_ctx->partial_update_info->default_values,
                old_value_block, default_value_block));
    }

    // fill all non sort key columns from mutable_old_columns, need to consider default value and null value
    for (std::size_t i {0}; i < non_sort_key_cids.size(); i++) {
        auto cid = non_sort_key_cids[i];
        const auto& tablet_column = tablet_schema.column(cid);
        auto col_uid = tablet_column.unique_id();
        auto& cur_col = mutable_full_columns[cid];
        for (auto idx = 0; idx < use_default_or_null_flag.size(); idx++) {
            auto segment_pos = segment_start_pos + idx;
            auto block_pos = block_start_pos + idx;
            if (skip_bitmaps->at(block_pos).contains(col_uid)) {
                DCHECK(cid != tablet_schema.skip_bitmap_col_idx());
                DCHECK(cid != tablet_schema.version_col_idx());
                DCHECK(!tablet_column.is_row_store_column());

                // `use_default_or_null_flag[idx] == false` doesn't mean that we should read values from the old row
                // for the missing columns. For example, if a table has sequence column, the rows with DELETE_SIGN column
                // marked will not be marked in delete bitmap(see https://github.com/apache/doris/pull/24011), so it will
                // be found in Tablet::lookup_row_key() and `use_default_or_null_flag[idx]` will be false. But we should not
                // read values from old rows for missing values in this occasion. So we should read the DELETE_SIGN column
                // to check if a row REALLY exists in the table.
                auto delete_sign_pos = read_index[tablet_schema.delete_sign_idx()][segment_pos];
                if (use_default_or_null_flag[idx] ||
                    (delete_sign_column_data != nullptr &&
                     delete_sign_column_data[delete_sign_pos] != 0)) {
                    if (tablet_column.has_default_value()) {
                        cur_col->insert_from(*default_value_block.get_by_position(i).column, 0);
                    } else if (tablet_column.is_nullable()) {
                        assert_cast<vectorized::ColumnNullable*, TypeCheckOnRelease::DISABLE>(
                                cur_col.get())
                                ->insert_null_elements(1);
                    } else {
                        // If the control flow reaches this branch, the column neither has default value
                        // nor is nullable. It means that the row's delete sign is marked, and the value
                        // columns are useless and won't be read. So we can just put arbitary values in the cells
                        cur_col->insert_default();
                    }
                } else {
                    auto pos_in_old_block = read_index.at(cid).at(segment_pos);
                    cur_col->insert_from(*old_value_block.get_by_position(i).column,
                                         pos_in_old_block);
                }
            } else {
                cur_col->insert_from(*block->get_by_position(cid).column, block_pos);
            }
        }
    }
    full_block.set_columns(std::move(mutable_full_columns));
    return Status::OK();
}

} // namespace doris
