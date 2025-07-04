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

#include "olap/compaction.h"

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <glog/logging.h>

#include <algorithm>
#include <atomic>
#include <cstdint>
#include <cstdlib>
#include <list>
#include <map>
#include <memory>
#include <mutex>
#include <nlohmann/json.hpp>
#include <numeric>
#include <ostream>
#include <set>
#include <shared_mutex>
#include <utility>

#include "cloud/cloud_meta_mgr.h"
#include "cloud/cloud_storage_engine.h"
#include "cloud/cloud_tablet.h"
#include "common/config.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/fs/file_system.h"
#include "io/fs/file_writer.h"
#include "io/fs/remote_file_system.h"
#include "io/io_common.h"
#include "olap/cumulative_compaction.h"
#include "olap/cumulative_compaction_policy.h"
#include "olap/cumulative_compaction_time_series_policy.h"
#include "olap/data_dir.h"
#include "olap/olap_common.h"
#include "olap/olap_define.h"
#include "olap/rowset/beta_rowset.h"
#include "olap/rowset/beta_rowset_reader.h"
#include "olap/rowset/beta_rowset_writer.h"
#include "olap/rowset/rowset.h"
#include "olap/rowset/rowset_fwd.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_writer.h"
#include "olap/rowset/rowset_writer_context.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/index_file_writer.h"
#include "olap/rowset/segment_v2/inverted_index_compaction.h"
#include "olap/rowset/segment_v2/inverted_index_desc.h"
#include "olap/rowset/segment_v2/inverted_index_fs_directory.h"
#include "olap/storage_engine.h"
#include "olap/storage_policy.h"
#include "olap/tablet.h"
#include "olap/tablet_meta.h"
#include "olap/tablet_meta_manager.h"
#include "olap/task/engine_checksum_task.h"
#include "olap/txn_manager.h"
#include "olap/utils.h"
#include "runtime/memory/mem_tracker_limiter.h"
#include "runtime/thread_context.h"
#include "util/doris_metrics.h"
#include "util/time.h"
#include "util/trace.h"

using std::vector;

namespace doris {
using namespace ErrorCode;

namespace {

bool is_rowset_tidy(std::string& pre_max_key, bool& pre_rs_key_bounds_truncated,
                    const RowsetSharedPtr& rhs) {
    size_t min_tidy_size = config::ordered_data_compaction_min_segment_size;
    if (rhs->num_segments() == 0) {
        return true;
    }
    if (rhs->is_segments_overlapping()) {
        return false;
    }
    // check segment size
    auto* beta_rowset = reinterpret_cast<BetaRowset*>(rhs.get());
    std::vector<size_t> segments_size;
    RETURN_FALSE_IF_ERROR(beta_rowset->get_segments_size(&segments_size));
    for (auto segment_size : segments_size) {
        // is segment is too small, need to do compaction
        if (segment_size < min_tidy_size) {
            return false;
        }
    }
    std::string min_key;
    auto ret = rhs->first_key(&min_key);
    if (!ret) {
        return false;
    }
    bool cur_rs_key_bounds_truncated {rhs->is_segments_key_bounds_truncated()};
    if (!Slice::lhs_is_strictly_less_than_rhs(Slice {pre_max_key}, pre_rs_key_bounds_truncated,
                                              Slice {min_key}, cur_rs_key_bounds_truncated)) {
        return false;
    }
    CHECK(rhs->last_key(&pre_max_key));
    pre_rs_key_bounds_truncated = cur_rs_key_bounds_truncated;
    return true;
}

} // namespace

Compaction::Compaction(BaseTabletSPtr tablet, const std::string& label)
        : _mem_tracker(
                  MemTrackerLimiter::create_shared(MemTrackerLimiter::Type::COMPACTION, label)),
          _tablet(std::move(tablet)),
          _is_vertical(config::enable_vertical_compaction),
          _allow_delete_in_cumu_compaction(config::enable_delete_when_cumu_compaction) {
    init_profile(label);
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    _rowid_conversion = std::make_unique<RowIdConversion>();
}

Compaction::~Compaction() {
    SCOPED_SWITCH_THREAD_MEM_TRACKER_LIMITER(_mem_tracker);
    _output_rs_writer.reset();
    _tablet.reset();
    _input_rowsets.clear();
    _output_rowset.reset();
    _cur_tablet_schema.reset();
    _rowid_conversion.reset();
}

void Compaction::init_profile(const std::string& label) {
    _profile = std::make_unique<RuntimeProfile>(label);

    _input_rowsets_data_size_counter =
            ADD_COUNTER(_profile, "input_rowsets_data_size", TUnit::BYTES);
    _input_rowsets_counter = ADD_COUNTER(_profile, "input_rowsets_count", TUnit::UNIT);
    _input_row_num_counter = ADD_COUNTER(_profile, "input_row_num", TUnit::UNIT);
    _input_segments_num_counter = ADD_COUNTER(_profile, "input_segments_num", TUnit::UNIT);
    _merged_rows_counter = ADD_COUNTER(_profile, "merged_rows", TUnit::UNIT);
    _filtered_rows_counter = ADD_COUNTER(_profile, "filtered_rows", TUnit::UNIT);
    _output_rowset_data_size_counter =
            ADD_COUNTER(_profile, "output_rowset_data_size", TUnit::BYTES);
    _output_row_num_counter = ADD_COUNTER(_profile, "output_row_num", TUnit::UNIT);
    _output_segments_num_counter = ADD_COUNTER(_profile, "output_segments_num", TUnit::UNIT);
    _merge_rowsets_latency_timer = ADD_TIMER(_profile, "merge_rowsets_latency");
}

int64_t Compaction::merge_way_num() {
    int64_t way_num = 0;
    for (auto&& rowset : _input_rowsets) {
        way_num += rowset->rowset_meta()->get_merge_way_num();
    }

    return way_num;
}

Status Compaction::merge_input_rowsets() {
    std::vector<RowsetReaderSharedPtr> input_rs_readers;
    input_rs_readers.reserve(_input_rowsets.size());
    for (auto& rowset : _input_rowsets) {
        RowsetReaderSharedPtr rs_reader;
        RETURN_IF_ERROR(rowset->create_reader(&rs_reader));
        input_rs_readers.push_back(std::move(rs_reader));
    }

    RowsetWriterContext ctx;
    RETURN_IF_ERROR(construct_output_rowset_writer(ctx));

    // write merged rows to output rowset
    // The test results show that merger is low-memory-footprint, there is no need to tracker its mem pool
    // if ctx.columns_to_do_index_compaction.size() > 0, it means we need to do inverted index compaction.
    // the row ID conversion matrix needs to be used for inverted index compaction.
    if (!ctx.columns_to_do_index_compaction.empty() ||
        (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
         _tablet->enable_unique_key_merge_on_write())) {
        _stats.rowid_conversion = _rowid_conversion.get();
    }

    int64_t way_num = merge_way_num();

    Status res;
    {
        SCOPED_TIMER(_merge_rowsets_latency_timer);
        // 1. Merge segment files and write bkd inverted index
        if (_is_vertical) {
            if (!_tablet->tablet_schema()->cluster_key_uids().empty()) {
                RETURN_IF_ERROR(update_delete_bitmap());
            }
            res = Merger::vertical_merge_rowsets(_tablet, compaction_type(), *_cur_tablet_schema,
                                                 input_rs_readers, _output_rs_writer.get(),
                                                 get_avg_segment_rows(), way_num, &_stats);
        } else {
            if (!_tablet->tablet_schema()->cluster_key_uids().empty()) {
                return Status::InternalError(
                        "mow table with cluster keys does not support non vertical compaction");
            }
            res = Merger::vmerge_rowsets(_tablet, compaction_type(), *_cur_tablet_schema,
                                         input_rs_readers, _output_rs_writer.get(), &_stats);
        }

        _tablet->last_compaction_status = res;
        if (!res.ok()) {
            return res;
        }
        // 2. Merge the remaining inverted index files of the string type
        RETURN_IF_ERROR(do_inverted_index_compaction());
    }

    COUNTER_UPDATE(_merged_rows_counter, _stats.merged_rows);
    COUNTER_UPDATE(_filtered_rows_counter, _stats.filtered_rows);

    // 3. In the `build`, `_close_file_writers` is called to close the inverted index file writer and write the final compound index file.
    RETURN_NOT_OK_STATUS_WITH_WARN(_output_rs_writer->build(_output_rowset),
                                   fmt::format("rowset writer build failed. output_version: {}",
                                               _output_version.to_string()));

    //RETURN_IF_ERROR(_engine.meta_mgr().commit_rowset(*_output_rowset->rowset_meta().get()));

    // Now we support delete in cumu compaction, to make all data in rowsets whose version
    // is below output_version to be delete in the future base compaction, we should carry
    // all delete predicate in the output rowset.
    // Output start version > 2 means we must set the delete predicate in the output rowset
    if (_allow_delete_in_cumu_compaction && _output_rowset->version().first > 2) {
        DeletePredicatePB delete_predicate;
        std::accumulate(_input_rowsets.begin(), _input_rowsets.end(), &delete_predicate,
                        [](DeletePredicatePB* delete_predicate, const RowsetSharedPtr& rs) {
                            if (rs->rowset_meta()->has_delete_predicate()) {
                                delete_predicate->MergeFrom(rs->rowset_meta()->delete_predicate());
                            }
                            return delete_predicate;
                        });
        // now version in delete_predicate is deprecated
        if (!delete_predicate.in_predicates().empty() ||
            !delete_predicate.sub_predicates_v2().empty() ||
            !delete_predicate.sub_predicates().empty()) {
            _output_rowset->rowset_meta()->set_delete_predicate(std::move(delete_predicate));
        }
    }

    _local_read_bytes_total = _stats.bytes_read_from_local;
    _remote_read_bytes_total = _stats.bytes_read_from_remote;
    DorisMetrics::instance()->local_compaction_read_bytes_total->increment(_local_read_bytes_total);
    DorisMetrics::instance()->remote_compaction_read_bytes_total->increment(
            _remote_read_bytes_total);
    DorisMetrics::instance()->local_compaction_write_bytes_total->increment(
            _stats.cached_bytes_total);

    COUNTER_UPDATE(_output_rowset_data_size_counter, _output_rowset->data_disk_size());
    COUNTER_UPDATE(_output_row_num_counter, _output_rowset->num_rows());
    COUNTER_UPDATE(_output_segments_num_counter, _output_rowset->num_segments());

    return check_correctness();
}

int64_t Compaction::get_avg_segment_rows() {
    // take care of empty rowset
    // input_rowsets_size is total disk_size of input_rowset, this size is the
    // final size after codec and compress, so expect dest segment file size
    // in disk is config::vertical_compaction_max_segment_size
    const auto& meta = _tablet->tablet_meta();
    if (meta->compaction_policy() == CUMULATIVE_TIME_SERIES_POLICY) {
        int64_t compaction_goal_size_mbytes = meta->time_series_compaction_goal_size_mbytes();
        return (compaction_goal_size_mbytes * 1024 * 1024 * 2) /
               (_input_rowsets_data_size / (_input_row_num + 1) + 1);
    }
    return config::vertical_compaction_max_segment_size /
           (_input_rowsets_data_size / (_input_row_num + 1) + 1);
}

CompactionMixin::CompactionMixin(StorageEngine& engine, TabletSharedPtr tablet,
                                 const std::string& label)
        : Compaction(tablet, label), _engine(engine) {}

CompactionMixin::~CompactionMixin() {
    if (_state != CompactionState::SUCCESS && _output_rowset != nullptr) {
        if (!_output_rowset->is_local()) {
            tablet()->record_unused_remote_rowset(_output_rowset->rowset_id(),
                                                  _output_rowset->rowset_meta()->resource_id(),
                                                  _output_rowset->num_segments());
            return;
        }
        _engine.add_unused_rowset(_output_rowset);
    }
}

Tablet* CompactionMixin::tablet() {
    return static_cast<Tablet*>(_tablet.get());
}

Status CompactionMixin::do_compact_ordered_rowsets() {
    build_basic_info();
    RowsetWriterContext ctx;
    RETURN_IF_ERROR(construct_output_rowset_writer(ctx));

    LOG(INFO) << "start to do ordered data compaction, tablet=" << _tablet->tablet_id()
              << ", output_version=" << _output_version;
    // link data to new rowset
    auto seg_id = 0;
    bool segments_key_bounds_truncated {false};
    std::vector<KeyBoundsPB> segment_key_bounds;
    for (auto rowset : _input_rowsets) {
        RETURN_IF_ERROR(rowset->link_files_to(tablet()->tablet_path(),
                                              _output_rs_writer->rowset_id(), seg_id));
        seg_id += rowset->num_segments();
        segments_key_bounds_truncated |= rowset->is_segments_key_bounds_truncated();
        std::vector<KeyBoundsPB> key_bounds;
        RETURN_IF_ERROR(rowset->get_segments_key_bounds(&key_bounds));
        segment_key_bounds.insert(segment_key_bounds.end(), key_bounds.begin(), key_bounds.end());
    }
    // build output rowset
    RowsetMetaSharedPtr rowset_meta = std::make_shared<RowsetMeta>();
    rowset_meta->set_num_rows(_input_row_num);
    rowset_meta->set_total_disk_size(_input_rowsets_data_size + _input_rowsets_index_size);
    rowset_meta->set_data_disk_size(_input_rowsets_data_size);
    rowset_meta->set_index_disk_size(_input_rowsets_index_size);
    rowset_meta->set_empty(_input_row_num == 0);
    rowset_meta->set_num_segments(_input_num_segments);
    rowset_meta->set_segments_overlap(NONOVERLAPPING);
    rowset_meta->set_rowset_state(VISIBLE);
    rowset_meta->set_segments_key_bounds_truncated(segments_key_bounds_truncated);
    rowset_meta->set_segments_key_bounds(segment_key_bounds);
    _output_rowset = _output_rs_writer->manual_build(rowset_meta);
    return Status::OK();
}

void CompactionMixin::build_basic_info() {
    for (auto& rowset : _input_rowsets) {
        const auto& rowset_meta = rowset->rowset_meta();
        auto index_size = rowset_meta->index_disk_size();
        auto total_size = rowset_meta->total_disk_size();
        auto data_size = rowset_meta->data_disk_size();
        // corrupted index size caused by bug before 2.1.5 or 3.0.0 version
        // try to get real index size from disk.
        if (index_size < 0 || index_size > total_size * 2) {
            LOG(ERROR) << "invalid index size:" << index_size << " total size:" << total_size
                       << " data size:" << data_size << " tablet:" << rowset_meta->tablet_id()
                       << " rowset:" << rowset_meta->rowset_id();
            index_size = 0;
            auto st = rowset->get_inverted_index_size(&index_size);
            if (!st.ok()) {
                LOG(ERROR) << "failed to get inverted index size. res=" << st;
            }
        }
        _input_rowsets_data_size += data_size;
        _input_rowsets_index_size += index_size;
        _input_rowsets_total_size += total_size;
        _input_row_num += rowset->num_rows();
        _input_num_segments += rowset->num_segments();
    }
    COUNTER_UPDATE(_input_rowsets_data_size_counter, _input_rowsets_data_size);
    COUNTER_UPDATE(_input_row_num_counter, _input_row_num);
    COUNTER_UPDATE(_input_segments_num_counter, _input_num_segments);

    TEST_SYNC_POINT_RETURN_WITH_VOID("compaction::CompactionMixin::build_basic_info");

    _output_version =
            Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

    _newest_write_timestamp = _input_rowsets.back()->newest_write_timestamp();

    std::vector<RowsetMetaSharedPtr> rowset_metas(_input_rowsets.size());
    std::transform(_input_rowsets.begin(), _input_rowsets.end(), rowset_metas.begin(),
                   [](const RowsetSharedPtr& rowset) { return rowset->rowset_meta(); });
    _cur_tablet_schema = _tablet->tablet_schema_with_merged_max_schema_version(rowset_metas);
}

bool CompactionMixin::handle_ordered_data_compaction() {
    if (!config::enable_ordered_data_compaction) {
        return false;
    }
    if (compaction_type() == ReaderType::READER_COLD_DATA_COMPACTION ||
        compaction_type() == ReaderType::READER_FULL_COMPACTION) {
        // The remote file system and full compaction does not support to link files.
        return false;
    }
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        return false;
    }

    if (_tablet->tablet_meta()->tablet_schema()->skip_write_index_on_load()) {
        // Expected to create index through normal compaction
        return false;
    }

    // check delete version: if compaction type is base compaction and
    // has a delete version, use original compaction
    if (compaction_type() == ReaderType::READER_BASE_COMPACTION ||
        (_allow_delete_in_cumu_compaction &&
         compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION)) {
        for (auto& rowset : _input_rowsets) {
            if (rowset->rowset_meta()->has_delete_predicate()) {
                return false;
            }
        }
    }

    // check if rowsets are tidy so we can just modify meta and do link
    // files to handle compaction
    auto input_size = _input_rowsets.size();
    std::string pre_max_key;
    bool pre_rs_key_bounds_truncated {false};
    for (auto i = 0; i < input_size; ++i) {
        if (!is_rowset_tidy(pre_max_key, pre_rs_key_bounds_truncated, _input_rowsets[i])) {
            if (i <= input_size / 2) {
                return false;
            } else {
                _input_rowsets.resize(i);
                break;
            }
        }
    }
    // most rowset of current compaction is nonoverlapping
    // just handle nonoverlappint rowsets
    auto st = do_compact_ordered_rowsets();
    if (!st.ok()) {
        LOG(WARNING) << "failed to compact ordered rowsets: " << st;
        _pending_rs_guard.drop();
    }

    return st.ok();
}

Status CompactionMixin::execute_compact() {
    uint32_t checksum_before;
    uint32_t checksum_after;
    bool enable_compaction_checksum = config::enable_compaction_checksum;
    if (enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_engine, _tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_before);
        RETURN_IF_ERROR(checksum_task.execute());
    }

    auto* data_dir = tablet()->data_dir();
    int64_t permits = get_compaction_permits();
    data_dir->disks_compaction_score_increment(permits);
    data_dir->disks_compaction_num_increment(1);

    auto record_compaction_stats = [&](const doris::Exception& ex) {
        _tablet->compaction_count.fetch_add(1, std::memory_order_relaxed);
        data_dir->disks_compaction_score_increment(-permits);
        data_dir->disks_compaction_num_increment(-1);
    };

    HANDLE_EXCEPTION_IF_CATCH_EXCEPTION(execute_compact_impl(permits), record_compaction_stats);
    record_compaction_stats(doris::Exception());

    if (enable_compaction_checksum) {
        EngineChecksumTask checksum_task(_engine, _tablet->tablet_id(), _tablet->schema_hash(),
                                         _input_rowsets.back()->end_version(), &checksum_after);
        RETURN_IF_ERROR(checksum_task.execute());
        if (checksum_before != checksum_after) {
            return Status::InternalError(
                    "compaction tablet checksum not consistent, before={}, after={}, tablet_id={}",
                    checksum_before, checksum_after, _tablet->tablet_id());
        }
    }

    DorisMetrics::instance()->local_compaction_read_rows_total->increment(_input_row_num);
    DorisMetrics::instance()->local_compaction_read_bytes_total->increment(
            _input_rowsets_total_size);

    TEST_SYNC_POINT_RETURN_WITH_VALUE("compaction::CompactionMixin::execute_compact", Status::OK());

    DorisMetrics::instance()->local_compaction_write_rows_total->increment(
            _output_rowset->num_rows());
    DorisMetrics::instance()->local_compaction_write_bytes_total->increment(
            _output_rowset->total_disk_size());

    _load_segment_to_cache();
    return Status::OK();
}

Status CompactionMixin::execute_compact_impl(int64_t permits) {
    OlapStopWatch watch;

    if (handle_ordered_data_compaction()) {
        RETURN_IF_ERROR(modify_rowsets());
        LOG(INFO) << "succeed to do ordered data " << compaction_name()
                  << ". tablet=" << _tablet->tablet_id() << ", output_version=" << _output_version
                  << ", disk=" << tablet()->data_dir()->path()
                  << ", segments=" << _input_num_segments << ", input_row_num=" << _input_row_num
                  << ", output_row_num=" << _output_rowset->num_rows()
                  << ", input_rowsets_data_size=" << _input_rowsets_data_size
                  << ", input_rowsets_index_size=" << _input_rowsets_index_size
                  << ", input_rowsets_total_size=" << _input_rowsets_total_size
                  << ", output_rowset_data_size=" << _output_rowset->data_disk_size()
                  << ", output_rowset_index_size=" << _output_rowset->index_disk_size()
                  << ", output_rowset_total_size=" << _output_rowset->total_disk_size()
                  << ". elapsed time=" << watch.get_elapse_second() << "s.";
        _state = CompactionState::SUCCESS;
        return Status::OK();
    }
    build_basic_info();

    TEST_SYNC_POINT_RETURN_WITH_VALUE("compaction::CompactionMixin::execute_compact_impl",
                                      Status::OK());

    VLOG_DEBUG << "dump tablet schema: " << _cur_tablet_schema->dump_structure();

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->tablet_id()
              << ", output_version=" << _output_version << ", permits: " << permits;

    RETURN_IF_ERROR(merge_input_rowsets());

    // Currently, updates are only made in the time_series.
    update_compaction_level();

    RETURN_IF_ERROR(modify_rowsets());

    auto* cumu_policy = tablet()->cumulative_compaction_policy();
    DCHECK(cumu_policy);
    LOG(INFO) << "succeed to do " << compaction_name() << " is_vertical=" << _is_vertical
              << ". tablet=" << _tablet->tablet_id() << ", output_version=" << _output_version
              << ", current_max_version=" << tablet()->max_version().second
              << ", disk=" << tablet()->data_dir()->path() << ", segments=" << _input_num_segments
              << ", input_rowsets_data_size=" << _input_rowsets_data_size
              << ", input_rowsets_index_size=" << _input_rowsets_index_size
              << ", input_rowsets_total_size=" << _input_rowsets_total_size
              << ", output_rowset_data_size=" << _output_rowset->data_disk_size()
              << ", output_rowset_index_size=" << _output_rowset->index_disk_size()
              << ", output_rowset_total_size=" << _output_rowset->total_disk_size()
              << ", input_row_num=" << _input_row_num
              << ", output_row_num=" << _output_rowset->num_rows()
              << ", filtered_row_num=" << _stats.filtered_rows
              << ", merged_row_num=" << _stats.merged_rows
              << ". elapsed time=" << watch.get_elapse_second()
              << "s. cumulative_compaction_policy=" << cumu_policy->name()
              << ", compact_row_per_second=" << int(_input_row_num / watch.get_elapse_second());

    _state = CompactionState::SUCCESS;

    return Status::OK();
}

Status Compaction::do_inverted_index_compaction() {
    const auto& ctx = _output_rs_writer->context();
    if (!config::inverted_index_compaction_enable || _input_row_num <= 0 ||
        ctx.columns_to_do_index_compaction.empty()) {
        return Status::OK();
    }

    auto error_handler = [this](int64_t index_id, int64_t column_uniq_id) {
        LOG(WARNING) << "failed to do index compaction"
                     << ". tablet=" << _tablet->tablet_id() << ". column uniq id=" << column_uniq_id
                     << ". index_id=" << index_id;
        for (auto& rowset : _input_rowsets) {
            rowset->set_skip_index_compaction(column_uniq_id);
            LOG(INFO) << "mark skipping inverted index compaction next time"
                      << ". tablet=" << _tablet->tablet_id() << ", rowset=" << rowset->rowset_id()
                      << ", column uniq id=" << column_uniq_id << ", index_id=" << index_id;
        }
    };

    DBUG_EXECUTE_IF("Compaction::do_inverted_index_compaction_rowid_conversion_null",
                    { _stats.rowid_conversion = nullptr; })
    if (!_stats.rowid_conversion) {
        LOG(WARNING) << "failed to do index compaction, rowid conversion is null"
                     << ". tablet=" << _tablet->tablet_id()
                     << ", input row number=" << _input_row_num;
        mark_skip_index_compaction(ctx, error_handler);

        return Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(
                "failed to do index compaction, rowid conversion is null. tablet={}",
                _tablet->tablet_id());
    }

    OlapStopWatch inverted_watch;

    // translation vec
    // <<dest_idx_num, dest_docId>>
    // the first level vector: index indicates src segment.
    // the second level vector: index indicates row id of source segment,
    // value indicates row id of destination segment.
    // <UINT32_MAX, UINT32_MAX> indicates current row not exist.
    const auto& trans_vec = _stats.rowid_conversion->get_rowid_conversion_map();

    // source rowset,segment -> index_id
    const auto& src_seg_to_id_map = _stats.rowid_conversion->get_src_segment_to_id_map();

    // dest rowset id
    RowsetId dest_rowset_id = _stats.rowid_conversion->get_dst_rowset_id();
    // dest segment id -> num rows
    std::vector<uint32_t> dest_segment_num_rows;
    RETURN_IF_ERROR(_output_rs_writer->get_segment_num_rows(&dest_segment_num_rows));

    auto src_segment_num = src_seg_to_id_map.size();
    auto dest_segment_num = dest_segment_num_rows.size();

    // when all the input rowsets are deleted, the output rowset will be empty and dest_segment_num will be 0.
    if (dest_segment_num <= 0) {
        LOG(INFO) << "skip doing index compaction due to no output segments"
                  << ". tablet=" << _tablet->tablet_id() << ", input row number=" << _input_row_num
                  << ". elapsed time=" << inverted_watch.get_elapse_second() << "s.";
        return Status::OK();
    }

    // Only write info files when debug index compaction is enabled.
    // The files are used to debug index compaction and works with index_tool.
    if (config::debug_inverted_index_compaction) {
        // src index files
        // format: rowsetId_segmentId
        std::vector<std::string> src_index_files(src_segment_num);
        for (const auto& m : src_seg_to_id_map) {
            std::pair<RowsetId, uint32_t> p = m.first;
            src_index_files[m.second] = p.first.to_string() + "_" + std::to_string(p.second);
        }

        // dest index files
        // format: rowsetId_segmentId
        std::vector<std::string> dest_index_files(dest_segment_num);
        for (int i = 0; i < dest_segment_num; ++i) {
            auto prefix = dest_rowset_id.to_string() + "_" + std::to_string(i);
            dest_index_files[i] = prefix;
        }

        auto write_json_to_file = [&](const nlohmann::json& json_obj,
                                      const std::string& file_name) {
            io::FileWriterPtr file_writer;
            std::string file_path =
                    fmt::format("{}/{}.json", std::string(getenv("LOG_DIR")), file_name);
            RETURN_IF_ERROR(io::global_local_filesystem()->create_file(file_path, &file_writer));
            RETURN_IF_ERROR(file_writer->append(json_obj.dump()));
            RETURN_IF_ERROR(file_writer->append("\n"));
            return file_writer->close();
        };

        // Convert trans_vec to JSON and print it
        nlohmann::json trans_vec_json = trans_vec;
        auto output_version =
                _output_version.to_string().substr(1, _output_version.to_string().size() - 2);
        RETURN_IF_ERROR(write_json_to_file(
                trans_vec_json,
                fmt::format("trans_vec_{}_{}", _tablet->tablet_id(), output_version)));

        nlohmann::json src_index_files_json = src_index_files;
        RETURN_IF_ERROR(write_json_to_file(
                src_index_files_json,
                fmt::format("src_idx_dirs_{}_{}", _tablet->tablet_id(), output_version)));

        nlohmann::json dest_index_files_json = dest_index_files;
        RETURN_IF_ERROR(write_json_to_file(
                dest_index_files_json,
                fmt::format("dest_idx_dirs_{}_{}", _tablet->tablet_id(), output_version)));

        nlohmann::json dest_segment_num_rows_json = dest_segment_num_rows;
        RETURN_IF_ERROR(write_json_to_file(
                dest_segment_num_rows_json,
                fmt::format("dest_seg_num_rows_{}_{}", _tablet->tablet_id(), output_version)));
    }

    // create index_writer to compaction indexes
    std::unordered_map<RowsetId, Rowset*> rs_id_to_rowset_map;
    for (auto&& rs : _input_rowsets) {
        rs_id_to_rowset_map.emplace(rs->rowset_id(), rs.get());
    }

    // src index dirs
    std::vector<std::unique_ptr<IndexFileReader>> index_file_readers(src_segment_num);
    for (const auto& m : src_seg_to_id_map) {
        const auto& [rowset_id, seg_id] = m.first;

        auto find_it = rs_id_to_rowset_map.find(rowset_id);
        DBUG_EXECUTE_IF("Compaction::do_inverted_index_compaction_find_rowset_error",
                        { find_it = rs_id_to_rowset_map.end(); })
        if (find_it == rs_id_to_rowset_map.end()) [[unlikely]] {
            LOG(WARNING) << "failed to do index compaction, cannot find rowset. tablet_id="
                         << _tablet->tablet_id() << " rowset_id=" << rowset_id.to_string();
            mark_skip_index_compaction(ctx, error_handler);
            return Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(
                    "failed to do index compaction, cannot find rowset. tablet_id={} rowset_id={}",
                    _tablet->tablet_id(), rowset_id.to_string());
        }

        auto* rowset = find_it->second;
        auto fs = rowset->rowset_meta()->fs();
        DBUG_EXECUTE_IF("Compaction::do_inverted_index_compaction_get_fs_error", { fs = nullptr; })
        if (!fs) {
            LOG(WARNING) << "failed to do index compaction, get fs failed. resource_id="
                         << rowset->rowset_meta()->resource_id();
            mark_skip_index_compaction(ctx, error_handler);
            return Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(
                    "get fs failed, resource_id={}", rowset->rowset_meta()->resource_id());
        }

        auto seg_path = rowset->segment_path(seg_id);
        DBUG_EXECUTE_IF("Compaction::do_inverted_index_compaction_seg_path_nullptr", {
            seg_path = ResultError(Status::Error<ErrorCode::INTERNAL_ERROR>(
                    "do_inverted_index_compaction_seg_path_nullptr"));
        })
        if (!seg_path.has_value()) {
            LOG(WARNING) << "failed to do index compaction, get segment path failed. tablet_id="
                         << _tablet->tablet_id() << " rowset_id=" << rowset_id.to_string()
                         << " seg_id=" << seg_id;
            mark_skip_index_compaction(ctx, error_handler);
            return Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(
                    "get segment path failed. tablet_id={} rowset_id={} seg_id={}",
                    _tablet->tablet_id(), rowset_id.to_string(), seg_id);
        }
        auto index_file_reader = std::make_unique<IndexFileReader>(
                fs,
                std::string {InvertedIndexDescriptor::get_index_file_path_prefix(seg_path.value())},
                _cur_tablet_schema->get_inverted_index_storage_format(),
                rowset->rowset_meta()->inverted_index_file_info(seg_id));
        auto st = index_file_reader->init(config::inverted_index_read_buffer_size);
        DBUG_EXECUTE_IF("Compaction::do_inverted_index_compaction_init_inverted_index_file_reader",
                        {
                            st = Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                                    "debug point: "
                                    "Compaction::do_inverted_index_compaction_init_inverted_index_"
                                    "file_reader error");
                        })
        if (!st.ok()) {
            LOG(WARNING) << "failed to do index compaction, init inverted index file reader "
                            "failed. tablet_id="
                         << _tablet->tablet_id() << " rowset_id=" << rowset_id.to_string()
                         << " seg_id=" << seg_id;
            mark_skip_index_compaction(ctx, error_handler);
            return Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(
                    "init inverted index file reader failed. tablet_id={} rowset_id={} seg_id={}",
                    _tablet->tablet_id(), rowset_id.to_string(), seg_id);
        }
        index_file_readers[m.second] = std::move(index_file_reader);
    }

    // dest index files
    // format: rowsetId_segmentId
    auto& inverted_index_file_writers = dynamic_cast<BaseBetaRowsetWriter*>(_output_rs_writer.get())
                                                ->inverted_index_file_writers();
    DBUG_EXECUTE_IF(
            "Compaction::do_inverted_index_compaction_inverted_index_file_writers_size_error",
            { inverted_index_file_writers.clear(); })
    if (inverted_index_file_writers.size() != dest_segment_num) {
        LOG(WARNING) << "failed to do index compaction, dest segment num not match. tablet_id="
                     << _tablet->tablet_id() << " dest_segment_num=" << dest_segment_num
                     << " inverted_index_file_writers.size()="
                     << inverted_index_file_writers.size();
        mark_skip_index_compaction(ctx, error_handler);
        return Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(
                "dest segment num not match. tablet_id={} dest_segment_num={} "
                "inverted_index_file_writers.size()={}",
                _tablet->tablet_id(), dest_segment_num, inverted_index_file_writers.size());
    }

    // use tmp file dir to store index files
    auto tmp_file_dir = ExecEnv::GetInstance()->get_tmp_file_dirs()->get_tmp_file_dir();
    auto index_tmp_path = tmp_file_dir / dest_rowset_id.to_string();
    LOG(INFO) << "start index compaction"
              << ". tablet=" << _tablet->tablet_id() << ", source index size=" << src_segment_num
              << ", destination index size=" << dest_segment_num << ".";

    Status status = Status::OK();
    for (auto&& column_uniq_id : ctx.columns_to_do_index_compaction) {
        auto col = _cur_tablet_schema->column_by_uid(column_uniq_id);
        const auto* index_meta = _cur_tablet_schema->inverted_index(col);
        DBUG_EXECUTE_IF("Compaction::do_inverted_index_compaction_can_not_find_index_meta",
                        { index_meta = nullptr; })
        if (index_meta == nullptr) {
            status = Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(
                    fmt::format("Can not find index_meta for col {}", col.name()));
            LOG(WARNING) << "failed to do index compaction, can not find index_meta for column"
                         << ". tablet=" << _tablet->tablet_id()
                         << ", column uniq id=" << column_uniq_id;
            error_handler(-1, column_uniq_id);
            break;
        }

        std::vector<lucene::store::Directory*> dest_index_dirs(dest_segment_num);
        try {
            std::vector<std::unique_ptr<DorisCompoundReader>> src_idx_dirs(src_segment_num);
            for (int src_segment_id = 0; src_segment_id < src_segment_num; src_segment_id++) {
                auto res = index_file_readers[src_segment_id]->open(index_meta);
                DBUG_EXECUTE_IF("Compaction::open_inverted_index_file_reader", {
                    res = ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "debug point: Compaction::open_index_file_reader error"));
                })
                if (!res.has_value()) {
                    LOG(WARNING) << "failed to do index compaction, open inverted index file "
                                    "reader failed"
                                 << ". tablet=" << _tablet->tablet_id()
                                 << ", column uniq id=" << column_uniq_id
                                 << ", src_segment_id=" << src_segment_id;
                    throw Exception(ErrorCode::INVERTED_INDEX_COMPACTION_ERROR, res.error().msg());
                }
                src_idx_dirs[src_segment_id] = std::move(res.value());
            }
            for (int dest_segment_id = 0; dest_segment_id < dest_segment_num; dest_segment_id++) {
                auto res = inverted_index_file_writers[dest_segment_id]->open(index_meta);
                DBUG_EXECUTE_IF("Compaction::open_inverted_index_file_writer", {
                    res = ResultError(Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                            "debug point: Compaction::open_inverted_index_file_writer error"));
                })
                if (!res.has_value()) {
                    LOG(WARNING) << "failed to do index compaction, open inverted index file "
                                    "writer failed"
                                 << ". tablet=" << _tablet->tablet_id()
                                 << ", column uniq id=" << column_uniq_id
                                 << ", dest_segment_id=" << dest_segment_id;
                    throw Exception(ErrorCode::INVERTED_INDEX_COMPACTION_ERROR, res.error().msg());
                }
                // Destination directories in dest_index_dirs do not need to be deconstructed,
                // but their lifecycle must be managed by inverted_index_file_writers.
                dest_index_dirs[dest_segment_id] = res.value().get();
            }
            auto st = compact_column(index_meta->index_id(), src_idx_dirs, dest_index_dirs,
                                     index_tmp_path.native(), trans_vec, dest_segment_num_rows);
            if (!st.ok()) {
                error_handler(index_meta->index_id(), column_uniq_id);
                status = Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(st.msg());
            }
        } catch (CLuceneError& e) {
            error_handler(index_meta->index_id(), column_uniq_id);
            status = Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(e.what());
        } catch (const Exception& e) {
            error_handler(index_meta->index_id(), column_uniq_id);
            status = Status::Error<INVERTED_INDEX_COMPACTION_ERROR>(e.what());
        }
    }

    // check index compaction status. If status is not ok, we should return error and end this compaction round.
    if (!status.ok()) {
        return status;
    }
    LOG(INFO) << "succeed to do index compaction"
              << ". tablet=" << _tablet->tablet_id()
              << ". elapsed time=" << inverted_watch.get_elapse_second() << "s.";

    return Status::OK();
}

void Compaction::mark_skip_index_compaction(
        const RowsetWriterContext& context,
        const std::function<void(int64_t, int64_t)>& error_handler) {
    for (auto&& column_uniq_id : context.columns_to_do_index_compaction) {
        auto col = _cur_tablet_schema->column_by_uid(column_uniq_id);
        const auto* index_meta = _cur_tablet_schema->inverted_index(col);
        DBUG_EXECUTE_IF("Compaction::mark_skip_index_compaction_can_not_find_index_meta",
                        { index_meta = nullptr; })
        if (index_meta == nullptr) {
            LOG(WARNING) << "mark skip index compaction, can not find index_meta for column"
                         << ". tablet=" << _tablet->tablet_id()
                         << ", column uniq id=" << column_uniq_id;
            error_handler(-1, column_uniq_id);
            continue;
        }
        error_handler(index_meta->index_id(), column_uniq_id);
    }
}

void Compaction::construct_index_compaction_columns(RowsetWriterContext& ctx) {
    for (const auto& index : _cur_tablet_schema->inverted_indexes()) {
        auto col_unique_ids = index->col_unique_ids();
        // check if column unique ids is empty to avoid crash
        if (col_unique_ids.empty()) {
            LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] index[" << index->index_id()
                         << "] has no column unique id, will skip index compaction."
                         << " tablet_schema=" << _cur_tablet_schema->dump_full_schema();
            continue;
        }
        auto col_unique_id = col_unique_ids[0];
        if (!_cur_tablet_schema->has_column_unique_id(col_unique_id)) {
            LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] column_unique_id["
                         << col_unique_id << "] not found, will skip index compaction";
            continue;
        }
        // Avoid doing inverted index compaction on non-slice type columns
        if (!field_is_slice_type(_cur_tablet_schema->column_by_uid(col_unique_id).type())) {
            continue;
        }

        // if index properties are different, index compaction maybe needs to be skipped.
        bool is_continue = false;
        std::optional<std::map<std::string, std::string>> first_properties;
        for (const auto& rowset : _input_rowsets) {
            const auto* tablet_index = rowset->tablet_schema()->inverted_index(col_unique_id);
            // no inverted index or index id is different from current index id
            if (tablet_index == nullptr || tablet_index->index_id() != index->index_id()) {
                is_continue = true;
                break;
            }
            auto properties = tablet_index->properties();
            if (!first_properties.has_value()) {
                first_properties = properties;
            } else {
                DBUG_EXECUTE_IF(
                        "Compaction::do_inverted_index_compaction_index_properties_different",
                        { properties.emplace("dummy_key", "dummy_value"); })
                if (properties != first_properties.value()) {
                    is_continue = true;
                    break;
                }
            }
        }
        if (is_continue) {
            continue;
        }
        auto has_inverted_index = [&](const RowsetSharedPtr& src_rs) {
            auto* rowset = static_cast<BetaRowset*>(src_rs.get());
            DBUG_EXECUTE_IF("Compaction::construct_skip_inverted_index_is_skip_index_compaction",
                            { rowset->set_skip_index_compaction(col_unique_id); })
            if (rowset->is_skip_index_compaction(col_unique_id)) {
                LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] rowset["
                             << rowset->rowset_id() << "] column_unique_id[" << col_unique_id
                             << "] skip inverted index compaction due to last failure";
                return false;
            }

            auto fs = rowset->rowset_meta()->fs();
            DBUG_EXECUTE_IF("Compaction::construct_skip_inverted_index_get_fs_error",
                            { fs = nullptr; })
            if (!fs) {
                LOG(WARNING) << "get fs failed, resource_id="
                             << rowset->rowset_meta()->resource_id();
                return false;
            }

            const auto* index_meta = rowset->tablet_schema()->inverted_index(col_unique_id);
            DBUG_EXECUTE_IF("Compaction::construct_skip_inverted_index_index_meta_nullptr",
                            { index_meta = nullptr; })
            if (index_meta == nullptr) {
                LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] column_unique_id["
                             << col_unique_id << "] index meta is null, will skip index compaction";
                return false;
            }

            for (auto i = 0; i < rowset->num_segments(); i++) {
                // TODO: inverted_index_path
                auto seg_path = rowset->segment_path(i);
                DBUG_EXECUTE_IF("Compaction::construct_skip_inverted_index_seg_path_nullptr", {
                    seg_path = ResultError(Status::Error<ErrorCode::INTERNAL_ERROR>(
                            "construct_skip_inverted_index_seg_path_nullptr"));
                })
                if (!seg_path) {
                    LOG(WARNING) << seg_path.error();
                    return false;
                }

                std::string index_file_path;
                try {
                    auto index_file_reader = std::make_unique<IndexFileReader>(
                            fs,
                            std::string {InvertedIndexDescriptor::get_index_file_path_prefix(
                                    seg_path.value())},
                            _cur_tablet_schema->get_inverted_index_storage_format(),
                            rowset->rowset_meta()->inverted_index_file_info(i));
                    auto st = index_file_reader->init(config::inverted_index_read_buffer_size);
                    index_file_path = index_file_reader->get_index_file_path(index_meta);
                    DBUG_EXECUTE_IF(
                            "Compaction::construct_skip_inverted_index_index_file_reader_init_"
                            "status_not_ok",
                            {
                                st = Status::Error<ErrorCode::INTERNAL_ERROR>(
                                        "debug point: "
                                        "construct_skip_inverted_index_index_file_reader_init_"
                                        "status_"
                                        "not_ok");
                            })
                    if (!st.ok()) {
                        LOG(WARNING) << "init index " << index_file_path << " error:" << st;
                        return false;
                    }

                    // check index meta
                    auto result = index_file_reader->open(index_meta);
                    DBUG_EXECUTE_IF(
                            "Compaction::construct_skip_inverted_index_index_file_reader_open_"
                            "error",
                            {
                                result = ResultError(
                                        Status::Error<ErrorCode::INVERTED_INDEX_CLUCENE_ERROR>(
                                                "CLuceneError occur when open idx file"));
                            })
                    if (!result.has_value()) {
                        LOG(WARNING)
                                << "open index " << index_file_path << " error:" << result.error();
                        return false;
                    }
                    auto reader = std::move(result.value());
                    std::vector<std::string> files;
                    reader->list(&files);
                    reader->close();
                    DBUG_EXECUTE_IF(
                            "Compaction::construct_skip_inverted_index_index_reader_close_error",
                            { _CLTHROWA(CL_ERR_IO, "debug point: reader close error"); })

                    DBUG_EXECUTE_IF("Compaction::construct_skip_inverted_index_index_files_count",
                                    { files.clear(); })

                    // why is 3?
                    // slice type index file at least has 3 files: null_bitmap, segments_N, segments.gen
                    if (files.size() < 3) {
                        LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] column_unique_id["
                                     << col_unique_id << "]," << index_file_path
                                     << " is corrupted, will skip index compaction";
                        return false;
                    }
                } catch (CLuceneError& err) {
                    LOG(WARNING) << "tablet[" << _tablet->tablet_id() << "] column_unique_id["
                                 << col_unique_id << "] open index[" << index_file_path
                                 << "], will skip index compaction, error:" << err.what();
                    return false;
                }
            }
            return true;
        };

        bool all_have_inverted_index = std::all_of(_input_rowsets.begin(), _input_rowsets.end(),
                                                   std::move(has_inverted_index));

        if (all_have_inverted_index) {
            ctx.columns_to_do_index_compaction.insert(col_unique_id);
        }
    }
}

Status CompactionMixin::update_delete_bitmap() {
    // for mow with cluster keys, compaction read data with delete bitmap
    // if tablet is not ready(such as schema change), we need to update delete bitmap
    {
        std::shared_lock meta_rlock(_tablet->get_header_lock());
        if (_tablet->tablet_state() != TABLET_NOTREADY) {
            return Status::OK();
        }
    }
    OlapStopWatch watch;
    std::vector<RowsetSharedPtr> rowsets;
    for (const auto& rowset : _input_rowsets) {
        std::lock_guard rwlock(tablet()->get_rowset_update_lock());
        std::shared_lock rlock(_tablet->get_header_lock());
        Status st = _tablet->update_delete_bitmap_without_lock(_tablet, rowset, &rowsets);
        if (!st.ok()) {
            LOG(INFO) << "failed update_delete_bitmap_without_lock for tablet_id="
                      << _tablet->tablet_id() << ", st=" << st.to_string();
            return st;
        }
        rowsets.push_back(rowset);
    }
    LOG(INFO) << "finish update delete bitmap for tablet: " << _tablet->tablet_id()
              << ", rowsets: " << _input_rowsets.size() << ", cost: " << watch.get_elapse_time_us()
              << "(us)";
    return Status::OK();
}

Status CloudCompactionMixin::update_delete_bitmap() {
    // for mow with cluster keys, compaction read data with delete bitmap
    // if tablet is not ready(such as schema change), we need to update delete bitmap
    {
        std::shared_lock meta_rlock(_tablet->get_header_lock());
        if (_tablet->tablet_state() != TABLET_NOTREADY) {
            return Status::OK();
        }
    }
    OlapStopWatch watch;
    std::vector<RowsetSharedPtr> rowsets;
    for (const auto& rowset : _input_rowsets) {
        Status st = _tablet->update_delete_bitmap_without_lock(_tablet, rowset, &rowsets);
        if (!st.ok()) {
            LOG(INFO) << "failed update_delete_bitmap_without_lock for tablet_id="
                      << _tablet->tablet_id() << ", st=" << st.to_string();
            return st;
        }
        rowsets.push_back(rowset);
    }
    LOG(INFO) << "finish update delete bitmap for tablet: " << _tablet->tablet_id()
              << ", rowsets: " << _input_rowsets.size() << ", cost: " << watch.get_elapse_time_us()
              << "(us)";
    return Status::OK();
}

Status CompactionMixin::construct_output_rowset_writer(RowsetWriterContext& ctx) {
    // only do index compaction for dup_keys and unique_keys with mow enabled
    if (config::inverted_index_compaction_enable &&
        (((_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
           _tablet->enable_unique_key_merge_on_write()) ||
          _tablet->keys_type() == KeysType::DUP_KEYS))) {
        construct_index_compaction_columns(ctx);
    }
    ctx.version = _output_version;
    ctx.rowset_state = VISIBLE;
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.tablet_schema = _cur_tablet_schema;
    ctx.newest_write_timestamp = _newest_write_timestamp;
    ctx.write_type = DataWriteType::TYPE_COMPACTION;
    ctx.compaction_type = compaction_type();
    _output_rs_writer = DORIS_TRY(_tablet->create_rowset_writer(ctx, _is_vertical));
    _pending_rs_guard = _engine.add_pending_rowset(ctx);
    return Status::OK();
}

Status CompactionMixin::modify_rowsets() {
    std::vector<RowsetSharedPtr> output_rowsets;
    output_rowsets.push_back(_output_rowset);

    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        Version version = tablet()->max_version();
        DeleteBitmap output_rowset_delete_bitmap(_tablet->tablet_id());
        std::unique_ptr<RowLocationSet> missed_rows;
        if ((config::enable_missing_rows_correctness_check ||
             config::enable_mow_compaction_correctness_check_core ||
             config::enable_mow_compaction_correctness_check_fail) &&
            !_allow_delete_in_cumu_compaction &&
            compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) {
            missed_rows = std::make_unique<RowLocationSet>();
            LOG(INFO) << "RowLocation Set inited succ for tablet:" << _tablet->tablet_id();
        }
        std::unique_ptr<std::map<RowsetSharedPtr, RowLocationPairList>> location_map;
        if (config::enable_rowid_conversion_correctness_check &&
            tablet()->tablet_schema()->cluster_key_uids().empty()) {
            location_map = std::make_unique<std::map<RowsetSharedPtr, RowLocationPairList>>();
            LOG(INFO) << "Location Map inited succ for tablet:" << _tablet->tablet_id();
        }
        // Convert the delete bitmap of the input rowsets to output rowset.
        // New loads are not blocked, so some keys of input rowsets might
        // be deleted during the time. We need to deal with delete bitmap
        // of incremental data later.
        // TODO(LiaoXin): check if there are duplicate keys
        std::size_t missed_rows_size = 0;
        tablet()->calc_compaction_output_rowset_delete_bitmap(
                _input_rowsets, *_rowid_conversion, 0, version.second + 1, missed_rows.get(),
                location_map.get(), _tablet->tablet_meta()->delete_bitmap(),
                &output_rowset_delete_bitmap);
        if (missed_rows) {
            missed_rows_size = missed_rows->size();
            std::size_t merged_missed_rows_size = _stats.merged_rows;
            if (!_tablet->tablet_meta()->tablet_schema()->cluster_key_uids().empty()) {
                merged_missed_rows_size += _stats.filtered_rows;
            }

            // Suppose a heavy schema change process on BE converting tablet A to tablet B.
            // 1. during schema change double write, new loads write [X-Y] on tablet B.
            // 2. rowsets with version [a],[a+1],...,[b-1],[b] on tablet B are picked for cumu compaction(X<=a<b<=Y).(cumu compaction
            //    on new tablet during schema change double write is allowed after https://github.com/apache/doris/pull/16470)
            // 3. schema change remove all rowsets on tablet B before version Z(b<=Z<=Y) before it begins to convert historical rowsets.
            // 4. schema change finishes.
            // 5. cumu compation begins on new tablet with version [a],...,[b]. If there are duplicate keys between these rowsets,
            //    the compaction check will fail because these rowsets have skipped to calculate delete bitmap in commit phase and
            //    publish phase because tablet B is in NOT_READY state when writing.

            // Considering that the cumu compaction will fail finally in this situation because `Tablet::modify_rowsets` will check if rowsets in
            // `to_delete`(_input_rowsets) still exist in tablet's `_rs_version_map`, we can just skip to check missed rows here.
            bool need_to_check_missed_rows = true;
            {
                std::shared_lock rlock(_tablet->get_header_lock());
                need_to_check_missed_rows =
                        std::all_of(_input_rowsets.begin(), _input_rowsets.end(),
                                    [&](const RowsetSharedPtr& rowset) {
                                        return tablet()->rowset_exists_unlocked(rowset);
                                    });
            }

            if (_tablet->tablet_state() == TABLET_RUNNING &&
                merged_missed_rows_size != missed_rows_size && need_to_check_missed_rows) {
                std::stringstream ss;
                ss << "cumulative compaction: the merged rows(" << _stats.merged_rows
                   << "), filtered rows(" << _stats.filtered_rows
                   << ") is not equal to missed rows(" << missed_rows_size
                   << ") in rowid conversion, tablet_id: " << _tablet->tablet_id()
                   << ", table_id:" << _tablet->table_id();
                if (missed_rows_size == 0) {
                    ss << ", debug info: ";
                    DeleteBitmap subset_map(_tablet->tablet_id());
                    for (auto rs : _input_rowsets) {
                        _tablet->tablet_meta()->delete_bitmap().subset(
                                {rs->rowset_id(), 0, 0},
                                {rs->rowset_id(), rs->num_segments(), version.second + 1},
                                &subset_map);
                        ss << "(rowset id: " << rs->rowset_id()
                           << ", delete bitmap cardinality: " << subset_map.cardinality() << ")";
                    }
                    ss << ", version[0-" << version.second + 1 << "]";
                }
                std::string err_msg = fmt::format(
                        "cumulative compaction: the merged rows({}), filtered rows({})"
                        " is not equal to missed rows({}) in rowid conversion,"
                        " tablet_id: {}, table_id:{}",
                        _stats.merged_rows, _stats.filtered_rows, missed_rows_size,
                        _tablet->tablet_id(), _tablet->table_id());
                LOG(WARNING) << err_msg;
                if (config::enable_mow_compaction_correctness_check_core) {
                    CHECK(false) << err_msg;
                } else if (config::enable_mow_compaction_correctness_check_fail) {
                    return Status::InternalError<false>(err_msg);
                } else {
                    DCHECK(false) << err_msg;
                }
            }
        }

        if (location_map) {
            RETURN_IF_ERROR(tablet()->check_rowid_conversion(_output_rowset, *location_map));
            location_map->clear();
        }

        {
            std::lock_guard<std::mutex> wrlock_(tablet()->get_rowset_update_lock());
            std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
            SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);

            // Here we will calculate all the rowsets delete bitmaps which are committed but not published to reduce the calculation pressure
            // of publish phase.
            // All rowsets which need to recalculate have been published so we don't need to acquire lock.
            // Step1: collect this tablet's all committed rowsets' delete bitmaps
            CommitTabletTxnInfoVec commit_tablet_txn_info_vec {};
            _engine.txn_manager()->get_all_commit_tablet_txn_info_by_tablet(
                    *tablet(), &commit_tablet_txn_info_vec);

            // Step2: calculate all rowsets' delete bitmaps which are published during compaction.
            for (auto& it : commit_tablet_txn_info_vec) {
                if (!_check_if_includes_input_rowsets(it.rowset_ids)) {
                    // When calculating the delete bitmap of all committed rowsets relative to the compaction,
                    // there may be cases where the compacted rowsets are newer than the committed rowsets.
                    // At this time, row number conversion cannot be performed, otherwise data will be missing.
                    // Therefore, we need to check if every committed rowset has calculated delete bitmap for
                    // all compaction input rowsets.
                    continue;
                }
                DeleteBitmap txn_output_delete_bitmap(_tablet->tablet_id());
                tablet()->calc_compaction_output_rowset_delete_bitmap(
                        _input_rowsets, *_rowid_conversion, 0, UINT64_MAX, missed_rows.get(),
                        location_map.get(), *it.delete_bitmap.get(), &txn_output_delete_bitmap);
                if (config::enable_merge_on_write_correctness_check) {
                    RowsetIdUnorderedSet rowsetids;
                    rowsetids.insert(_output_rowset->rowset_id());
                    _tablet->add_sentinel_mark_to_delete_bitmap(&txn_output_delete_bitmap,
                                                                rowsetids);
                }
                it.delete_bitmap->merge(txn_output_delete_bitmap);
                // Step3: write back updated delete bitmap and tablet info.
                it.rowset_ids.insert(_output_rowset->rowset_id());
                _engine.txn_manager()->set_txn_related_delete_bitmap(
                        it.partition_id, it.transaction_id, _tablet->tablet_id(),
                        tablet()->tablet_uid(), true, it.delete_bitmap, it.rowset_ids,
                        it.partial_update_info);
            }

            // Convert the delete bitmap of the input rowsets to output rowset for
            // incremental data.
            tablet()->calc_compaction_output_rowset_delete_bitmap(
                    _input_rowsets, *_rowid_conversion, version.second, UINT64_MAX,
                    missed_rows.get(), location_map.get(), _tablet->tablet_meta()->delete_bitmap(),
                    &output_rowset_delete_bitmap);

            if (missed_rows) {
                DCHECK_EQ(missed_rows->size(), missed_rows_size);
                if (missed_rows->size() != missed_rows_size) {
                    LOG(WARNING) << "missed rows don't match, before: " << missed_rows_size
                                 << " after: " << missed_rows->size();
                }
            }

            if (location_map) {
                RETURN_IF_ERROR(tablet()->check_rowid_conversion(_output_rowset, *location_map));
            }

            tablet()->merge_delete_bitmap(output_rowset_delete_bitmap);
            RETURN_IF_ERROR(tablet()->modify_rowsets(output_rowsets, _input_rowsets, true));
        }
    } else {
        std::lock_guard<std::shared_mutex> wrlock(_tablet->get_header_lock());
        SCOPED_SIMPLE_TRACE_IF_TIMEOUT(TRACE_TABLET_LOCK_THRESHOLD);
        RETURN_IF_ERROR(tablet()->modify_rowsets(output_rowsets, _input_rowsets, true));
    }

    if (config::tablet_rowset_stale_sweep_by_size &&
        _tablet->tablet_meta()->all_stale_rs_metas().size() >=
                config::tablet_rowset_stale_sweep_threshold_size) {
        tablet()->delete_expired_stale_rowset();
    }

    int64_t cur_max_version = 0;
    {
        std::shared_lock rlock(_tablet->get_header_lock());
        cur_max_version = _tablet->max_version_unlocked();
        tablet()->save_meta();
    }
    if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
        _tablet->enable_unique_key_merge_on_write()) {
        auto st = TabletMetaManager::remove_old_version_delete_bitmap(
                tablet()->data_dir(), _tablet->tablet_id(), cur_max_version);
        if (!st.ok()) {
            LOG(WARNING) << "failed to remove old version delete bitmap, st: " << st;
        }
    }
    DBUG_EXECUTE_IF("CumulativeCompaction.modify_rowsets.delete_expired_stale_rowset",
                    { tablet()->delete_expired_stale_rowset(); });
    return Status::OK();
}

bool CompactionMixin::_check_if_includes_input_rowsets(
        const RowsetIdUnorderedSet& commit_rowset_ids_set) const {
    std::vector<RowsetId> commit_rowset_ids {};
    commit_rowset_ids.insert(commit_rowset_ids.end(), commit_rowset_ids_set.begin(),
                             commit_rowset_ids_set.end());
    std::sort(commit_rowset_ids.begin(), commit_rowset_ids.end());
    std::vector<RowsetId> input_rowset_ids {};
    for (const auto& rowset : _input_rowsets) {
        input_rowset_ids.emplace_back(rowset->rowset_meta()->rowset_id());
    }
    std::sort(input_rowset_ids.begin(), input_rowset_ids.end());
    return std::includes(commit_rowset_ids.begin(), commit_rowset_ids.end(),
                         input_rowset_ids.begin(), input_rowset_ids.end());
}

void CompactionMixin::update_compaction_level() {
    auto* cumu_policy = tablet()->cumulative_compaction_policy();
    if (cumu_policy && cumu_policy->name() == CUMULATIVE_TIME_SERIES_POLICY) {
        int64_t compaction_level =
                cumu_policy->get_compaction_level(tablet(), _input_rowsets, _output_rowset);
        _output_rowset->rowset_meta()->set_compaction_level(compaction_level);
    }
}

Status Compaction::check_correctness() {
    // 1. check row number
    if (_input_row_num != _output_rowset->num_rows() + _stats.merged_rows + _stats.filtered_rows) {
        return Status::Error<CHECK_LINES_ERROR>(
                "row_num does not match between cumulative input and output! tablet={}, "
                "input_row_num={}, merged_row_num={}, filtered_row_num={}, output_row_num={}",
                _tablet->tablet_id(), _input_row_num, _stats.merged_rows, _stats.filtered_rows,
                _output_rowset->num_rows());
    }
    return Status::OK();
}

int64_t CompactionMixin::get_compaction_permits() {
    int64_t permits = 0;
    for (auto&& rowset : _input_rowsets) {
        permits += rowset->rowset_meta()->get_compaction_score();
    }
    return permits;
}

int64_t CompactionMixin::calc_input_rowsets_total_size() const {
    int64_t input_rowsets_total_size = 0;
    for (const auto& rowset : _input_rowsets) {
        const auto& rowset_meta = rowset->rowset_meta();
        auto total_size = rowset_meta->total_disk_size();
        input_rowsets_total_size += total_size;
    }
    return input_rowsets_total_size;
}

int64_t CompactionMixin::calc_input_rowsets_row_num() const {
    int64_t input_rowsets_row_num = 0;
    for (const auto& rowset : _input_rowsets) {
        const auto& rowset_meta = rowset->rowset_meta();
        auto total_size = rowset_meta->total_disk_size();
        input_rowsets_row_num += total_size;
    }
    return input_rowsets_row_num;
}

void Compaction::_load_segment_to_cache() {
    // Load new rowset's segments to cache.
    SegmentCacheHandle handle;
    auto st = SegmentLoader::instance()->load_segments(
            std::static_pointer_cast<BetaRowset>(_output_rowset), &handle, true);
    if (!st.ok()) {
        LOG(WARNING) << "failed to load segment to cache! output rowset version="
                     << _output_rowset->start_version() << "-" << _output_rowset->end_version()
                     << ".";
    }
}

void CloudCompactionMixin::build_basic_info() {
    _output_version =
            Version(_input_rowsets.front()->start_version(), _input_rowsets.back()->end_version());

    _newest_write_timestamp = _input_rowsets.back()->newest_write_timestamp();

    std::vector<RowsetMetaSharedPtr> rowset_metas(_input_rowsets.size());
    std::transform(_input_rowsets.begin(), _input_rowsets.end(), rowset_metas.begin(),
                   [](const RowsetSharedPtr& rowset) { return rowset->rowset_meta(); });
    _cur_tablet_schema = _tablet->tablet_schema_with_merged_max_schema_version(rowset_metas);
}

int64_t CloudCompactionMixin::get_compaction_permits() {
    int64_t permits = 0;
    for (auto&& rowset : _input_rowsets) {
        permits += rowset->rowset_meta()->get_compaction_score();
    }
    return permits;
}

CloudCompactionMixin::CloudCompactionMixin(CloudStorageEngine& engine, CloudTabletSPtr tablet,
                                           const std::string& label)
        : Compaction(tablet, label), _engine(engine) {
    auto uuid = UUIDGenerator::instance()->next_uuid();
    std::stringstream ss;
    ss << uuid;
    _uuid = ss.str();
}

Status CloudCompactionMixin::execute_compact_impl(int64_t permits) {
    OlapStopWatch watch;

    build_basic_info();

    LOG(INFO) << "start " << compaction_name() << ". tablet=" << _tablet->tablet_id()
              << ", output_version=" << _output_version << ", permits: " << permits;

    RETURN_IF_ERROR(merge_input_rowsets());

    DBUG_EXECUTE_IF("CloudFullCompaction::modify_rowsets.wrong_rowset_id", {
        DCHECK(compaction_type() == ReaderType::READER_FULL_COMPACTION);
        RowsetId id;
        id.version = 2;
        id.hi = _output_rowset->rowset_meta()->rowset_id().hi + ((int64_t)(1) << 56);
        id.mi = _output_rowset->rowset_meta()->rowset_id().mi;
        id.lo = _output_rowset->rowset_meta()->rowset_id().lo;
        _output_rowset->rowset_meta()->set_rowset_id(id);
        LOG(INFO) << "[Debug wrong rowset id]:"
                  << _output_rowset->rowset_meta()->rowset_id().to_string();
    })

    // Currently, updates are only made in the time_series.
    update_compaction_level();

    RETURN_IF_ERROR(_engine.meta_mgr().commit_rowset(*_output_rowset->rowset_meta().get(), _uuid));

    // 4. modify rowsets in memory
    RETURN_IF_ERROR(modify_rowsets());

    // update compaction status data
    auto tablet = std::static_pointer_cast<CloudTablet>(_tablet);
    tablet->local_read_time_us.fetch_add(_stats.cloud_local_read_time);
    tablet->remote_read_time_us.fetch_add(_stats.cloud_remote_read_time);
    tablet->exec_compaction_time_us.fetch_add(watch.get_elapse_time_us());

    return Status::OK();
}

int64_t CloudCompactionMixin::initiator() const {
    return HashUtil::hash64(_uuid.data(), _uuid.size(), 0) & std::numeric_limits<int64_t>::max();
}

Status CloudCompactionMixin::execute_compact() {
    TEST_INJECTION_POINT("Compaction::do_compaction");
    int64_t permits = get_compaction_permits();
    HANDLE_EXCEPTION_IF_CATCH_EXCEPTION(
            execute_compact_impl(permits), [&](const doris::Exception& ex) {
                auto st = garbage_collection();
                if (_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
                    _tablet->enable_unique_key_merge_on_write() && !st.ok()) {
                    // if compaction fail, be will try to abort compaction, and delete bitmap lock
                    // will release if abort job successfully, but if abort failed, delete bitmap
                    // lock will not release, in this situation, be need to send this rpc to ms
                    // to try to release delete bitmap lock.
                    _engine.meta_mgr().remove_delete_bitmap_update_lock(
                            _tablet->table_id(), COMPACTION_DELETE_BITMAP_LOCK_ID, initiator(),
                            _tablet->tablet_id());
                }
            });

    DorisMetrics::instance()->remote_compaction_read_rows_total->increment(_input_row_num);
    DorisMetrics::instance()->remote_compaction_write_rows_total->increment(
            _output_rowset->num_rows());
    DorisMetrics::instance()->remote_compaction_write_bytes_total->increment(
            _output_rowset->total_disk_size());

    _load_segment_to_cache();
    return Status::OK();
}

Status CloudCompactionMixin::modify_rowsets() {
    return Status::OK();
}

Status CloudCompactionMixin::construct_output_rowset_writer(RowsetWriterContext& ctx) {
    // only do index compaction for dup_keys and unique_keys with mow enabled
    if (config::inverted_index_compaction_enable &&
        (((_tablet->keys_type() == KeysType::UNIQUE_KEYS &&
           _tablet->enable_unique_key_merge_on_write()) ||
          _tablet->keys_type() == KeysType::DUP_KEYS))) {
        construct_index_compaction_columns(ctx);
    }

    // Use the storage resource of the previous rowset
    ctx.storage_resource =
            *DORIS_TRY(_input_rowsets.back()->rowset_meta()->remote_storage_resource());

    ctx.txn_id = boost::uuids::hash_value(UUIDGenerator::instance()->next_uuid()) &
                 std::numeric_limits<int64_t>::max(); // MUST be positive
    ctx.txn_expiration = _expiration;

    ctx.version = _output_version;
    ctx.rowset_state = VISIBLE;
    ctx.segments_overlap = NONOVERLAPPING;
    ctx.tablet_schema = _cur_tablet_schema;
    ctx.newest_write_timestamp = _newest_write_timestamp;
    ctx.write_type = DataWriteType::TYPE_COMPACTION;
    ctx.compaction_type = compaction_type();

    // We presume that the data involved in cumulative compaction is sufficiently 'hot'
    // and should always be retained in the cache.
    // TODO(gavin): Ensure that the retention of hot data is implemented with precision.
    ctx.write_file_cache = (compaction_type() == ReaderType::READER_CUMULATIVE_COMPACTION) ||
                           (config::enable_file_cache_keep_base_compaction_output &&
                            compaction_type() == ReaderType::READER_BASE_COMPACTION);
    ctx.file_cache_ttl_sec = _tablet->ttl_seconds();
    _output_rs_writer = DORIS_TRY(_tablet->create_rowset_writer(ctx, _is_vertical));
    RETURN_IF_ERROR(
            _engine.meta_mgr().prepare_rowset(*_output_rs_writer->rowset_meta().get(), _uuid));
    return Status::OK();
}

Status CloudCompactionMixin::garbage_collection() {
    if (!config::enable_file_cache) {
        return Status::OK();
    }
    if (_output_rs_writer) {
        auto* beta_rowset_writer = dynamic_cast<BaseBetaRowsetWriter*>(_output_rs_writer.get());
        DCHECK(beta_rowset_writer);
        for (const auto& [_, file_writer] : beta_rowset_writer->get_file_writers()) {
            auto file_key = io::BlockFileCache::hash(file_writer->path().filename().native());
            auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
            file_cache->remove_if_cached_async(file_key);
        }
    }
    return Status::OK();
}

void CloudCompactionMixin::update_compaction_level() {
    auto compaction_policy = _tablet->tablet_meta()->compaction_policy();
    auto cumu_policy = _engine.cumu_compaction_policy(compaction_policy);
    if (cumu_policy && cumu_policy->name() == CUMULATIVE_TIME_SERIES_POLICY) {
        int64_t compaction_level =
                cumu_policy->get_compaction_level(cloud_tablet(), _input_rowsets, _output_rowset);
        _output_rowset->rowset_meta()->set_compaction_level(compaction_level);
    }
}

} // namespace doris
