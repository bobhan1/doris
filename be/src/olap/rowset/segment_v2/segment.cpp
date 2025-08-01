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

#include "olap/rowset/segment_v2/segment.h"

#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/olap_file.pb.h>
#include <gen_cpp/segment_v2.pb.h>

#include <cstring>
#include <memory>
#include <utility>

#include "cloud/config.h"
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "cpp/sync_point.h"
#include "io/cache/block_file_cache.h"
#include "io/cache/block_file_cache_factory.h"
#include "io/cache/cached_remote_file_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/file_system.h"
#include "io/io_common.h"
#include "olap/block_column_predicate.h"
#include "olap/column_predicate.h"
#include "olap/iterators.h"
#include "olap/olap_common.h"
#include "olap/primary_key_index.h"
#include "olap/rowset/rowset_reader_context.h"
#include "olap/rowset/segment_v2/column_reader.h"
#include "olap/rowset/segment_v2/empty_segment_iterator.h"
#include "olap/rowset/segment_v2/hierarchical_data_reader.h"
#include "olap/rowset/segment_v2/index_file_reader.h"
#include "olap/rowset/segment_v2/indexed_column_reader.h"
#include "olap/rowset/segment_v2/page_io.h"
#include "olap/rowset/segment_v2/page_pointer.h"
#include "olap/rowset/segment_v2/segment_iterator.h"
#include "olap/rowset/segment_v2/segment_writer.h" // k_segment_magic_length
#include "olap/rowset/segment_v2/stream_reader.h"
#include "olap/schema.h"
#include "olap/short_key_index.h"
#include "olap/tablet_schema.h"
#include "olap/types.h"
#include "olap/utils.h"
#include "runtime/exec_env.h"
#include "runtime/query_context.h"
#include "runtime/runtime_predicate.h"
#include "runtime/runtime_state.h"
#include "util/coding.h"
#include "util/crc32c.h"
#include "util/slice.h" // Slice
#include "vec/columns/column.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/json/path_in_data.h"
#include "vec/olap/vgeneric_iterators.h"

namespace doris::segment_v2 {
#include "common/compile_check_begin.h"

class InvertedIndexIterator;

Status Segment::open(io::FileSystemSPtr fs, const std::string& path, int64_t tablet_id,
                     uint32_t segment_id, RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
                     const io::FileReaderOptions& reader_options, std::shared_ptr<Segment>* output,
                     InvertedIndexFileInfo idx_file_info, OlapReaderStatistics* stats) {
    auto s = _open(fs, path, segment_id, rowset_id, tablet_schema, reader_options, output,
                   idx_file_info, stats);
    if (!s.ok()) {
        if (!config::is_cloud_mode()) {
            auto res = ExecEnv::get_tablet(tablet_id);
            TabletSharedPtr tablet =
                    res.has_value() ? std::dynamic_pointer_cast<Tablet>(res.value()) : nullptr;
            if (tablet) {
                tablet->report_error(s);
            }
        }
    }

    return s;
}

Status Segment::_open(io::FileSystemSPtr fs, const std::string& path, uint32_t segment_id,
                      RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
                      const io::FileReaderOptions& reader_options, std::shared_ptr<Segment>* output,
                      InvertedIndexFileInfo idx_file_info, OlapReaderStatistics* stats) {
    io::FileReaderSPtr file_reader;
    RETURN_IF_ERROR(fs->open_file(path, &file_reader, &reader_options));
    std::shared_ptr<Segment> segment(
            new Segment(segment_id, rowset_id, std::move(tablet_schema), idx_file_info));
    segment->_fs = fs;
    segment->_file_reader = std::move(file_reader);
    auto st = segment->_open(stats);
    TEST_INJECTION_POINT_CALLBACK("Segment::open:corruption", &st);
    if (st.is<ErrorCode::CORRUPTION>() &&
        reader_options.cache_type == io::FileCachePolicy::FILE_BLOCK_CACHE) {
        LOG(WARNING) << "bad segment file may be read from file cache, try to read remote source "
                        "file directly, file path: "
                     << path << " cache_key: " << file_cache_key_str(path);
        auto file_key = file_cache_key_from_path(path);
        auto* file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
        file_cache->remove_if_cached(file_key);

        RETURN_IF_ERROR(fs->open_file(path, &file_reader, &reader_options));
        segment->_file_reader = std::move(file_reader);
        st = segment->_open(stats);
        TEST_INJECTION_POINT_CALLBACK("Segment::open:corruption1", &st);
        if (st.is<ErrorCode::CORRUPTION>()) { // corrupt again
            LOG(WARNING) << "failed to try to read remote source file again with cache support,"
                         << " try to read from remote directly, "
                         << " file path: " << path << " cache_key: " << file_cache_key_str(path);
            file_cache = io::FileCacheFactory::instance()->get_by_path(file_key);
            file_cache->remove_if_cached(file_key);

            io::FileReaderOptions opt = reader_options;
            opt.cache_type = io::FileCachePolicy::NO_CACHE; // skip cache
            RETURN_IF_ERROR(fs->open_file(path, &file_reader, &opt));
            segment->_file_reader = std::move(file_reader);
            st = segment->_open(stats);
            if (!st.ok()) {
                LOG(WARNING) << "failed to try to read remote source file directly,"
                             << " file path: " << path
                             << " cache_key: " << file_cache_key_str(path);
            }
        }
    }
    RETURN_IF_ERROR(st);
    *output = std::move(segment);
    return Status::OK();
}

Segment::Segment(uint32_t segment_id, RowsetId rowset_id, TabletSchemaSPtr tablet_schema,
                 InvertedIndexFileInfo idx_file_info)
        : _segment_id(segment_id),
          _meta_mem_usage(0),
          _rowset_id(rowset_id),
          _tablet_schema(std::move(tablet_schema)),
          _idx_file_info(std::move(idx_file_info)) {}

Segment::~Segment() {
    g_segment_estimate_mem_bytes << -_tracked_meta_mem_usage;
    // if failed, fix `_tracked_meta_mem_usage` accuracy
    DCHECK(_tracked_meta_mem_usage == meta_mem_usage());
}

io::UInt128Wrapper Segment::file_cache_key(std::string_view rowset_id, uint32_t seg_id) {
    return io::BlockFileCache::hash(fmt::format("{}_{}.dat", rowset_id, seg_id));
}

int64_t Segment::get_metadata_size() const {
    std::shared_ptr<SegmentFooterPB> footer_pb_shared = _footer_pb.lock();
    return sizeof(Segment) + (_pk_index_meta ? _pk_index_meta->ByteSizeLong() : 0) +
           (footer_pb_shared ? footer_pb_shared->ByteSizeLong() : 0);
}

void Segment::update_metadata_size() {
    MetadataAdder::update_metadata_size();
    g_segment_estimate_mem_bytes << _meta_mem_usage - _tracked_meta_mem_usage;
    _tracked_meta_mem_usage = _meta_mem_usage;
}

Status Segment::_open(OlapReaderStatistics* stats) {
    std::shared_ptr<SegmentFooterPB> footer_pb_shared;
    RETURN_IF_ERROR(_get_segment_footer(footer_pb_shared, stats));

    _pk_index_meta.reset(
            footer_pb_shared->has_primary_key_index_meta()
                    ? new PrimaryKeyIndexMetaPB(footer_pb_shared->primary_key_index_meta())
                    : nullptr);
    // delete_bitmap_calculator_test.cpp
    // DCHECK(footer.has_short_key_index_page());
    _sk_index_page = footer_pb_shared->short_key_index_page();
    _num_rows = footer_pb_shared->num_rows();

    // An estimated memory usage of a segment
    _meta_mem_usage += footer_pb_shared->ByteSizeLong();
    if (_pk_index_meta != nullptr) {
        _meta_mem_usage += _pk_index_meta->ByteSizeLong();
    }

    _meta_mem_usage += sizeof(*this);
    _meta_mem_usage += _tablet_schema->num_columns() * config::estimated_mem_per_column_reader;

    // 1024 comes from SegmentWriterOptions
    _meta_mem_usage += (_num_rows + 1023) / 1024 * (36 + 4);
    // 0.01 comes from PrimaryKeyIndexBuilder::init
    _meta_mem_usage += BloomFilter::optimal_bit_num(_num_rows, 0.01) / 8;

    update_metadata_size();

    return Status::OK();
}

Status Segment::_open_index_file_reader() {
    _index_file_reader = std::make_shared<IndexFileReader>(
            _fs,
            std::string {InvertedIndexDescriptor::get_index_file_path_prefix(
                    _file_reader->path().native())},
            _tablet_schema->get_inverted_index_storage_format(), _idx_file_info);
    return Status::OK();
}

Status Segment::new_iterator(SchemaSPtr schema, const StorageReadOptions& read_options,
                             std::unique_ptr<RowwiseIterator>* iter) {
    if (read_options.runtime_state != nullptr) {
        _be_exec_version = read_options.runtime_state->be_exec_version();
    }
    RETURN_IF_ERROR(_create_column_readers_once(read_options.stats));

    read_options.stats->total_segment_number++;
    // trying to prune the current segment by segment-level zone map
    for (auto& entry : read_options.col_id_to_predicates) {
        int32_t column_id = entry.first;
        // schema change
        if (_tablet_schema->num_columns() <= column_id) {
            continue;
        }
        const TabletColumn& col = read_options.tablet_schema->column(column_id);
        ColumnReader* reader = nullptr;
        if (col.is_extracted_column()) {
            auto relative_path = col.path_info_ptr()->copy_pop_front();
            int32_t unique_id = col.unique_id() > 0 ? col.unique_id() : col.parent_unique_id();
            const auto* node = _sub_column_tree[unique_id].find_exact(relative_path);
            reader = node != nullptr ? node->data.reader.get() : nullptr;
        } else {
            reader = _column_readers.contains(col.unique_id())
                             ? _column_readers[col.unique_id()].get()
                             : nullptr;
        }
        if (!reader || !reader->has_zone_map()) {
            continue;
        }
        if (read_options.col_id_to_predicates.contains(column_id) &&
            can_apply_predicate_safely(column_id,
                                       read_options.col_id_to_predicates.at(column_id).get(),
                                       *schema, read_options.io_ctx.reader_type) &&
            !reader->match_condition(entry.second.get())) {
            // any condition not satisfied, return.
            iter->reset(new EmptySegmentIterator(*schema));
            read_options.stats->filtered_segment_number++;
            return Status::OK();
        }
    }

    if (!read_options.topn_filter_source_node_ids.empty()) {
        auto* query_ctx = read_options.runtime_state->get_query_ctx();
        for (int id : read_options.topn_filter_source_node_ids) {
            auto runtime_predicate = query_ctx->get_runtime_predicate(id).get_predicate(
                    read_options.topn_filter_target_node_id);

            int32_t uid =
                    read_options.tablet_schema->column(runtime_predicate->column_id()).unique_id();
            AndBlockColumnPredicate and_predicate;
            and_predicate.add_column_predicate(
                    SingleColumnBlockPredicate::create_unique(runtime_predicate.get()));
            if (_column_readers.contains(uid) &&
                can_apply_predicate_safely(runtime_predicate->column_id(), runtime_predicate.get(),
                                           *schema, read_options.io_ctx.reader_type) &&
                !_column_readers.at(uid)->match_condition(&and_predicate)) {
                // any condition not satisfied, return.
                *iter = std::make_unique<EmptySegmentIterator>(*schema);
                read_options.stats->filtered_segment_number++;
                return Status::OK();
            }
        }
    }

    {
        SCOPED_RAW_TIMER(&read_options.stats->segment_load_index_timer_ns);
        RETURN_IF_ERROR(load_index(read_options.stats));
    }

    if (read_options.delete_condition_predicates->num_of_column_predicate() == 0 &&
        read_options.push_down_agg_type_opt != TPushAggOp::NONE &&
        read_options.push_down_agg_type_opt != TPushAggOp::COUNT_ON_INDEX) {
        iter->reset(vectorized::new_vstatistics_iterator(this->shared_from_this(), *schema));
    } else {
        *iter = std::make_unique<SegmentIterator>(this->shared_from_this(), schema);
    }

    // TODO: Valid the opt not only in ReaderType::READER_QUERY
    if (read_options.io_ctx.reader_type == ReaderType::READER_QUERY &&
        !read_options.column_predicates.empty()) {
        auto pruned_predicates = read_options.column_predicates;
        auto pruned = false;
        for (auto& it : _column_readers) {
            const auto uid = it.first;
            const auto column_id = read_options.tablet_schema->field_index(uid);
            pruned |= it.second->prune_predicates_by_zone_map(pruned_predicates, column_id);
        }

        if (pruned) {
            auto options_with_pruned_predicates = read_options;
            options_with_pruned_predicates.column_predicates = pruned_predicates;
            //because column_predicates is changed, we need to rebuild col_id_to_predicates so that inverted index will not go through it.
            options_with_pruned_predicates.col_id_to_predicates.clear();
            for (auto* pred : options_with_pruned_predicates.column_predicates) {
                if (!options_with_pruned_predicates.col_id_to_predicates.contains(
                            pred->column_id())) {
                    options_with_pruned_predicates.col_id_to_predicates.insert(
                            {pred->column_id(), AndBlockColumnPredicate::create_shared()});
                }
                options_with_pruned_predicates.col_id_to_predicates[pred->column_id()]
                        ->add_column_predicate(SingleColumnBlockPredicate::create_unique(pred));
            }
            return iter->get()->init(options_with_pruned_predicates);
        }
    }
    return iter->get()->init(read_options);
}

Status Segment::_write_error_file(size_t file_size, size_t offset, size_t bytes_read, char* data,
                                  io::IOContext& io_ctx) {
    if (!config::enbale_dump_error_file || !doris::config::is_cloud_mode()) {
        return Status::OK();
    }

    std::string file_name = _rowset_id.to_string() + "_" + std::to_string(_segment_id) + ".dat";
    std::string dir_path = io::FileCacheFactory::instance()->get_base_paths()[0] + "/error_file/";
    Status create_st = io::global_local_filesystem()->create_directory(dir_path, true);
    if (!create_st.ok() && !create_st.is<ErrorCode::ALREADY_EXIST>()) {
        LOG(WARNING) << "failed to create error file dir: " << create_st.to_string();
        return create_st;
    }
    size_t dir_size = 0;
    RETURN_IF_ERROR(io::global_local_filesystem()->directory_size(dir_path, &dir_size));
    if (dir_size > config::file_cache_error_log_limit_bytes) {
        LOG(WARNING) << "error file dir size is too large: " << dir_size;
        return Status::OK();
    }

    std::string error_part;
    error_part.resize(bytes_read);
    std::string part_path = dir_path + file_name + ".part_offset_" + std::to_string(offset);
    LOG(WARNING) << "writer error part to " << part_path;
    bool is_part_exist = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(part_path, &is_part_exist));
    if (is_part_exist) {
        LOG(WARNING) << "error part already exists: " << part_path;
    } else {
        std::unique_ptr<io::FileWriter> part_writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(part_path, &part_writer));
        RETURN_IF_ERROR(part_writer->append(Slice(data, bytes_read)));
        RETURN_IF_ERROR(part_writer->close());
    }

    std::string error_file;
    error_file.resize(file_size);
    auto* cached_reader = dynamic_cast<io::CachedRemoteFileReader*>(_file_reader.get());
    if (cached_reader == nullptr) {
        return Status::InternalError("file reader is not CachedRemoteFileReader");
    }
    size_t error_file_bytes_read = 0;
    RETURN_IF_ERROR(cached_reader->get_remote_reader()->read_at(
            0, Slice(error_file.data(), file_size), &error_file_bytes_read, &io_ctx));
    DCHECK(error_file_bytes_read == file_size);
    //std::string file_path = dir_path + std::to_string(cur_time) + "_" + ss.str();
    std::string file_path = dir_path + file_name;
    LOG(WARNING) << "writer error file to " << file_path;
    bool is_file_exist = false;
    RETURN_IF_ERROR(io::global_local_filesystem()->exists(file_path, &is_file_exist));
    if (is_file_exist) {
        LOG(WARNING) << "error file already exists: " << part_path;
    } else {
        std::unique_ptr<io::FileWriter> writer;
        RETURN_IF_ERROR(io::global_local_filesystem()->create_file(file_path, &writer));
        RETURN_IF_ERROR(writer->append(Slice(error_file.data(), file_size)));
        RETURN_IF_ERROR(writer->close());
    }
    return Status::OK(); // already exists
};

Status Segment::_parse_footer(std::shared_ptr<SegmentFooterPB>& footer,
                              OlapReaderStatistics* stats) {
    // Footer := SegmentFooterPB, FooterPBSize(4), FooterPBChecksum(4), MagicNumber(4)
    auto file_size = _file_reader->size();
    if (file_size < 12) {
        return Status::Corruption("Bad segment file {}: file size {} < 12, cache_key: {}",
                                  _file_reader->path().native(), file_size,
                                  file_cache_key_str(_file_reader->path().native()));
    }

    uint8_t fixed_buf[12];
    size_t bytes_read = 0;
    // TODO(plat1ko): Support session variable `enable_file_cache`
    io::IOContext io_ctx {.is_index_data = true,
                          .file_cache_stats = stats ? &stats->file_cache_stats : nullptr};
    RETURN_IF_ERROR(
            _file_reader->read_at(file_size - 12, Slice(fixed_buf, 12), &bytes_read, &io_ctx));
    DCHECK_EQ(bytes_read, 12);
    TEST_SYNC_POINT_CALLBACK("Segment::parse_footer:magic_number_corruption", fixed_buf);
    TEST_INJECTION_POINT_CALLBACK("Segment::parse_footer:magic_number_corruption_inj", fixed_buf);
    if (memcmp(fixed_buf + 8, k_segment_magic, k_segment_magic_length) != 0) {
        Status st =
                _write_error_file(file_size, file_size - 12, bytes_read, (char*)fixed_buf, io_ctx);
        if (!st.ok()) {
            LOG(WARNING) << "failed to write error file: " << st.to_string();
        }
        return Status::Corruption(
                "Bad segment file {}: file_size: {}, magic number not match, cache_key: {}",
                _file_reader->path().native(), file_size,
                file_cache_key_str(_file_reader->path().native()));
    }

    // read footer PB
    uint32_t footer_length = decode_fixed32_le(fixed_buf);
    if (file_size < 12 + footer_length) {
        Status st =
                _write_error_file(file_size, file_size - 12, bytes_read, (char*)fixed_buf, io_ctx);
        if (!st.ok()) {
            LOG(WARNING) << "failed to write error file: " << st.to_string();
        }
        return Status::Corruption("Bad segment file {}: file size {} < {}, cache_key: {}",
                                  _file_reader->path().native(), file_size, 12 + footer_length,
                                  file_cache_key_str(_file_reader->path().native()));
    }

    std::string footer_buf;
    footer_buf.resize(footer_length);
    RETURN_IF_ERROR(_file_reader->read_at(file_size - 12 - footer_length, footer_buf, &bytes_read,
                                          &io_ctx));
    DCHECK_EQ(bytes_read, footer_length);

    // validate footer PB's checksum
    uint32_t expect_checksum = decode_fixed32_le(fixed_buf + 4);
    uint32_t actual_checksum = crc32c::Value(footer_buf.data(), footer_buf.size());
    if (actual_checksum != expect_checksum) {
        Status st = _write_error_file(file_size, file_size - 12 - footer_length, bytes_read,
                                      footer_buf.data(), io_ctx);
        if (!st.ok()) {
            LOG(WARNING) << "failed to write error file: " << st.to_string();
        }
        return Status::Corruption(
                "Bad segment file {}: file_size = {}, footer checksum not match, actual={} "
                "vs expect={}, cache_key: {}",
                _file_reader->path().native(), file_size, actual_checksum, expect_checksum,
                file_cache_key_str(_file_reader->path().native()));
    }

    // deserialize footer PB
    footer = std::make_shared<SegmentFooterPB>();
    if (!footer->ParseFromString(footer_buf)) {
        Status st = _write_error_file(file_size, file_size - 12 - footer_length, bytes_read,
                                      footer_buf.data(), io_ctx);
        if (!st.ok()) {
            LOG(WARNING) << "failed to write error file: " << st.to_string();
        }
        return Status::Corruption(
                "Bad segment file {}: file_size = {}, failed to parse SegmentFooterPB, cache_key: ",
                _file_reader->path().native(), file_size,
                file_cache_key_str(_file_reader->path().native()));
    }

    VLOG_DEBUG << fmt::format("Loading segment footer from {} finished",
                              _file_reader->path().native());
    return Status::OK();
}

Status Segment::_load_pk_bloom_filter(OlapReaderStatistics* stats) {
#ifdef BE_TEST
    if (_pk_index_meta == nullptr) {
        // for BE UT "segment_cache_test"
        return _load_pk_bf_once.call([this] {
            _meta_mem_usage += 100;
            update_metadata_size();
            return Status::OK();
        });
    }
#endif
    DCHECK(_tablet_schema->keys_type() == UNIQUE_KEYS);
    DCHECK(_pk_index_meta != nullptr);
    DCHECK(_pk_index_reader != nullptr);

    return _load_pk_bf_once.call([this, stats] {
        RETURN_IF_ERROR(_pk_index_reader->parse_bf(_file_reader, *_pk_index_meta, stats));
        // _meta_mem_usage += _pk_index_reader->get_bf_memory_size();
        return Status::OK();
    });
}

Status Segment::load_pk_index_and_bf(OlapReaderStatistics* index_load_stats) {
    // `DorisCallOnce` may catch exception in calling stack A and re-throw it in
    // a different calling stack B which doesn't have catch block. So we add catch block here
    // to prevent coreudmp
    RETURN_IF_CATCH_EXCEPTION({
        RETURN_IF_ERROR(load_index(index_load_stats));
        RETURN_IF_ERROR(_load_pk_bloom_filter(index_load_stats));
    });
    return Status::OK();
}

Status Segment::load_index(OlapReaderStatistics* stats) {
    return _load_index_once.call([this, stats] {
        if (_tablet_schema->keys_type() == UNIQUE_KEYS && _pk_index_meta != nullptr) {
            _pk_index_reader = std::make_unique<PrimaryKeyIndexReader>();
            RETURN_IF_ERROR(_pk_index_reader->parse_index(_file_reader, *_pk_index_meta, stats));
            // _meta_mem_usage += _pk_index_reader->get_memory_size();
            return Status::OK();
        } else {
            // read and parse short key index page
            OlapReaderStatistics tmp_stats;
            OlapReaderStatistics* stats_ptr = stats != nullptr ? stats : &tmp_stats;
            PageReadOptions opts(io::IOContext {.is_index_data = true,
                                                .file_cache_stats = &stats_ptr->file_cache_stats});
            opts.use_page_cache = true;
            opts.type = INDEX_PAGE;
            opts.file_reader = _file_reader.get();
            opts.page_pointer = PagePointer(_sk_index_page);
            // short key index page uses NO_COMPRESSION for now
            opts.codec = nullptr;
            opts.stats = &tmp_stats;

            Slice body;
            PageFooterPB footer;
            RETURN_IF_ERROR(
                    PageIO::read_and_decompress_page(opts, &_sk_index_handle, &body, &footer));
            DCHECK_EQ(footer.type(), SHORT_KEY_PAGE);
            DCHECK(footer.has_short_key_page_footer());

            // _meta_mem_usage += body.get_size();
            _sk_index_decoder = std::make_unique<ShortKeyIndexDecoder>();
            return _sk_index_decoder->parse(body, footer.short_key_page_footer());
        }
    });
}

Status Segment::healthy_status() {
    try {
        if (_load_index_once.has_called()) {
            RETURN_IF_ERROR(_load_index_once.stored_result());
        }
        if (_load_pk_bf_once.has_called()) {
            RETURN_IF_ERROR(_load_pk_bf_once.stored_result());
        }
        if (_create_column_readers_once_call.has_called()) {
            RETURN_IF_ERROR(_create_column_readers_once_call.stored_result());
        }
        if (_index_file_reader_open.has_called()) {
            RETURN_IF_ERROR(_index_file_reader_open.stored_result());
        }
        // This status is set by running time, for example, if there is something wrong during read segment iterator.
        return _healthy_status.status();
    } catch (const doris::Exception& e) {
        // If there is an exception during load_xxx, should not throw exception directly because
        // the caller may not exception safe.
        return e.to_status();
    } catch (const std::exception& e) {
        // The exception is not thrown by doris code.
        return Status::InternalError("Unexcepted error during load segment: {}", e.what());
    }
}

// Return the storage datatype of related column to field.
// Return nullptr meaning no such storage infomation for this column
vectorized::DataTypePtr Segment::get_data_type_of(const ColumnIdentifier& identifier,
                                                  bool read_flat_leaves) const {
    // Path has higher priority
    if (identifier.path != nullptr && !identifier.path->empty()) {
        auto relative_path = identifier.path->copy_pop_front();
        int32_t unique_id =
                identifier.unique_id > 0 ? identifier.unique_id : identifier.parent_unique_id;
        const auto* node = _sub_column_tree.contains(unique_id)
                                   ? _sub_column_tree.at(unique_id).find_leaf(relative_path)
                                   : nullptr;
        const auto* sparse_node =
                _sparse_column_tree.contains(unique_id)
                        ? _sparse_column_tree.at(unique_id).find_exact(relative_path)
                        : nullptr;
        if (node) {
            if (read_flat_leaves || (node->children.empty() && sparse_node == nullptr)) {
                return node->data.file_column_type;
            }
        }
        // missing in storage, treat it using input data type
        if (read_flat_leaves && !node && !sparse_node) {
            return nullptr;
        }
        // it contains children or column missing in storage, so treat it as variant
        return identifier.is_nullable
                       ? vectorized::make_nullable(std::make_shared<vectorized::DataTypeVariant>())
                       : std::make_shared<vectorized::DataTypeVariant>();
    }
    // TODO support normal column type
    return nullptr;
}

Status Segment::_create_column_readers_once(OlapReaderStatistics* stats) {
    SCOPED_RAW_TIMER(&stats->segment_create_column_readers_timer_ns);
    return _create_column_readers_once_call.call([&] {
        std::shared_ptr<SegmentFooterPB> footer_pb_shared;
        RETURN_IF_ERROR(_get_segment_footer(footer_pb_shared, stats));
        return _create_column_readers(*footer_pb_shared);
    });
}

Status Segment::_create_column_readers(const SegmentFooterPB& footer) {
    std::unordered_map<uint32_t, uint32_t> column_id_to_footer_ordinal;
    std::unordered_map<vectorized::PathInData, uint32_t, vectorized::PathInData::Hash>
            column_path_to_footer_ordinal;
    for (uint32_t ordinal = 0; ordinal < footer.columns().size(); ++ordinal) {
        const auto& column_pb = footer.columns(ordinal);
        // column path for accessing subcolumns of variant
        if (column_pb.has_column_path_info()) {
            vectorized::PathInData path;
            path.from_protobuf(column_pb.column_path_info());
            column_path_to_footer_ordinal.emplace(path, ordinal);
        }
        // unique_id is unsigned, -1 meaning no unique id(e.g. an extracted column from variant)
        if (static_cast<int>(column_pb.unique_id()) >= 0) {
            // unique id
            column_id_to_footer_ordinal.emplace(column_pb.unique_id(), ordinal);
        }
    }
    // init by unique_id
    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) {
        const auto& column = _tablet_schema->column(ordinal);
        auto iter = column_id_to_footer_ordinal.find(column.unique_id());
        if (iter == column_id_to_footer_ordinal.end()) {
            continue;
        }

        ColumnReaderOptions opts {
                .kept_in_memory = _tablet_schema->is_in_memory(),
                .be_exec_version = _be_exec_version,
        };
        std::unique_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(ColumnReader::create(opts, footer.columns(iter->second), footer.num_rows(),
                                             _file_reader, &reader));
        _column_readers.emplace(column.unique_id(), std::move(reader));
    }

    // init by column path
    for (uint32_t ordinal = 0; ordinal < _tablet_schema->num_columns(); ++ordinal) {
        const auto& column = _tablet_schema->column(ordinal);
        if (!column.has_path_info()) {
            continue;
        }
        auto path = column.has_path_info() ? *column.path_info_ptr()
                                           : vectorized::PathInData(column.name_lower_case());
        auto iter = column_path_to_footer_ordinal.find(path);
        if (iter == column_path_to_footer_ordinal.end()) {
            continue;
        }
        const ColumnMetaPB& column_pb = footer.columns(iter->second);
        ColumnReaderOptions opts {
                .kept_in_memory = _tablet_schema->is_in_memory(),
                .be_exec_version = _be_exec_version,
        };
        std::unique_ptr<ColumnReader> reader;
        RETURN_IF_ERROR(
                ColumnReader::create(opts, column_pb, footer.num_rows(), _file_reader, &reader));
        // root column use unique id, leaf column use parent_unique_id
        int32_t unique_id =
                column.parent_unique_id() > 0 ? column.parent_unique_id() : column.unique_id();
        auto relative_path = path.copy_pop_front();
        if (relative_path.empty()) {
            // root column
            _sub_column_tree[unique_id].create_root(SubcolumnReader {
                    std::move(reader),
                    vectorized::DataTypeFactory::instance().create_data_type(column_pb)});
        } else {
            // check the root is already a leaf node
            DCHECK(_sub_column_tree[unique_id].get_leaves()[0]->path.empty());
            _sub_column_tree[unique_id].add(
                    relative_path,
                    SubcolumnReader {
                            std::move(reader),
                            vectorized::DataTypeFactory::instance().create_data_type(column_pb)});
        }

        // init sparse columns paths and type info
        for (uint32_t sparse_ordinal = 0; sparse_ordinal < column_pb.sparse_columns().size();
             ++sparse_ordinal) {
            const auto& spase_column_pb = column_pb.sparse_columns(sparse_ordinal);
            if (spase_column_pb.has_column_path_info()) {
                vectorized::PathInData sparse_path;
                sparse_path.from_protobuf(spase_column_pb.column_path_info());
                // Read from root column, so reader is nullptr
                _sparse_column_tree[unique_id].add(
                        sparse_path.copy_pop_front(),
                        SubcolumnReader {nullptr,
                                         vectorized::DataTypeFactory::instance().create_data_type(
                                                 spase_column_pb)});
            }
        }
    }

    return Status::OK();
}

Status Segment::new_default_iterator(const TabletColumn& tablet_column,
                                     std::unique_ptr<ColumnIterator>* iter) {
    if (!tablet_column.has_default_value() && !tablet_column.is_nullable()) {
        return Status::InternalError(
                "invalid nonexistent column without default value. column_uid={}, column_name={}, "
                "column_type={}",
                tablet_column.unique_id(), tablet_column.name(), tablet_column.type());
    }
    auto type_info = get_type_info(&tablet_column);
    std::unique_ptr<DefaultValueColumnIterator> default_value_iter(new DefaultValueColumnIterator(
            tablet_column.has_default_value(), tablet_column.default_value(),
            tablet_column.is_nullable(), std::move(type_info), tablet_column.precision(),
            tablet_column.frac()));
    ColumnIteratorOptions iter_opts;

    RETURN_IF_ERROR(default_value_iter->init(iter_opts));
    *iter = std::move(default_value_iter);
    return Status::OK();
}

Status Segment::_new_iterator_with_variant_root(const TabletColumn& tablet_column,
                                                std::unique_ptr<ColumnIterator>* iter,
                                                const SubcolumnColumnReaders::Node* root,
                                                vectorized::DataTypePtr target_type_hint) {
    ColumnIterator* it;
    RETURN_IF_ERROR(root->data.reader->new_iterator(&it, &tablet_column));
    auto* stream_iter = new ExtractReader(
            tablet_column,
            std::make_unique<SubstreamIterator>(root->data.file_column_type->create_column(),
                                                std::unique_ptr<ColumnIterator>(it),
                                                root->data.file_column_type),
            target_type_hint);
    iter->reset(stream_iter);
    return Status::OK();
}

Status Segment::new_column_iterator_with_path(const TabletColumn& tablet_column,
                                              std::unique_ptr<ColumnIterator>* iter,
                                              const StorageReadOptions* opt) {
    // root column use unique id, leaf column use parent_unique_id
    int32_t unique_id = tablet_column.unique_id() > 0 ? tablet_column.unique_id()
                                                      : tablet_column.parent_unique_id();
    if (!_sub_column_tree.contains(unique_id)) {
        // No such variant column in this segment, get a default one
        RETURN_IF_ERROR(new_default_iterator(tablet_column, iter));
        return Status::OK();
    }
    auto relative_path = tablet_column.path_info_ptr()->copy_pop_front();
    const auto* root = _sub_column_tree[unique_id].get_root();
    const auto* node = tablet_column.has_path_info()
                               ? _sub_column_tree[unique_id].find_exact(relative_path)
                               : nullptr;
    const auto* sparse_node =
            tablet_column.has_path_info() && _sparse_column_tree.contains(unique_id)
                    ? _sparse_column_tree[unique_id].find_exact(relative_path)
                    : nullptr;
    // Currently only compaction and checksum need to read flat leaves
    // They both use tablet_schema_with_merged_max_schema_version as read schema
    auto type_to_read_flat_leaves = [](ReaderType type) {
        return type == ReaderType::READER_BASE_COMPACTION ||
               type == ReaderType::READER_CUMULATIVE_COMPACTION ||
               type == ReaderType::READER_COLD_DATA_COMPACTION ||
               type == ReaderType::READER_SEGMENT_COMPACTION ||
               type == ReaderType::READER_FULL_COMPACTION || type == ReaderType::READER_CHECKSUM;
    };

    // find the sibling of the nested column to fill the target nested column
    auto new_default_iter_with_same_nested = [&](const TabletColumn& tablet_column,
                                                 std::unique_ptr<ColumnIterator>* iter) {
        // We find node that represents the same Nested type as path.
        const auto* parent = _sub_column_tree[unique_id].find_best_match(relative_path);
        VLOG_DEBUG << "find with path " << tablet_column.path_info_ptr()->get_path() << " parent "
                   << (parent ? parent->path.get_path() : "nullptr") << ", type "
                   << ", parent is nested " << (parent ? parent->is_nested() : false) << ", "
                   << TabletColumn::get_string_by_field_type(tablet_column.type());
        // find it's common parent with nested part
        // why not use parent->path->has_nested_part? because parent may not be a leaf node
        // none leaf node may not contain path info
        // Example:
        // {"payload" : {"commits" : [{"issue" : {"id" : 123, "email" : "a@b"}}]}}
        // nested node path          : payload.commits(NESTED)
        // tablet_column path_info   : payload.commits.issue.id(SCALAR)
        // parent path node          : payload.commits.issue(TUPLE)
        // leaf path_info            : payload.commits.issue.email(SCALAR)
        if (parent && SubcolumnColumnReaders::find_parent(
                              parent, [](const auto& node) { return node.is_nested(); })) {
            /// Find any leaf of Nested subcolumn.
            const auto* leaf = SubcolumnColumnReaders::find_leaf(
                    parent, [](const auto& node) { return node.path.has_nested_part(); });
            assert(leaf);
            std::unique_ptr<ColumnIterator> sibling_iter;
            ColumnIterator* sibling_iter_ptr;
            RETURN_IF_ERROR(leaf->data.reader->new_iterator(&sibling_iter_ptr, &tablet_column));
            sibling_iter.reset(sibling_iter_ptr);
            *iter = std::make_unique<DefaultNestedColumnIterator>(std::move(sibling_iter),
                                                                  leaf->data.file_column_type);
        } else {
            *iter = std::make_unique<DefaultNestedColumnIterator>(nullptr, nullptr);
        }
        return Status::OK();
    };

    if (opt != nullptr && type_to_read_flat_leaves(opt->io_ctx.reader_type)) {
        // compaction need to read flat leaves nodes data to prevent from amplification
        const auto* leaf_node = tablet_column.has_path_info()
                                        ? _sub_column_tree[unique_id].find_leaf(relative_path)
                                        : nullptr;
        if (!leaf_node) {
            // sparse_columns have this path, read from root
            if (sparse_node != nullptr && sparse_node->is_leaf_node()) {
                RETURN_IF_ERROR(_new_iterator_with_variant_root(
                        tablet_column, iter, root, sparse_node->data.file_column_type));
            } else {
                if (tablet_column.is_nested_subcolumn()) {
                    // using the sibling of the nested column to fill the target nested column
                    RETURN_IF_ERROR(new_default_iter_with_same_nested(tablet_column, iter));
                } else {
                    RETURN_IF_ERROR(new_default_iterator(tablet_column, iter));
                }
            }
            return Status::OK();
        }
        ColumnIterator* it;
        RETURN_IF_ERROR(leaf_node->data.reader->new_iterator(&it, &tablet_column));
        iter->reset(it);
        return Status::OK();
    }

    if (node != nullptr) {
        if (node->is_leaf_node() && sparse_node == nullptr) {
            // Node contains column without any child sub columns and no corresponding sparse columns
            // Direct read extracted columns
            const auto* leaf_node = _sub_column_tree[unique_id].find_leaf(relative_path);
            ColumnIterator* it;
            RETURN_IF_ERROR(leaf_node->data.reader->new_iterator(&it, &tablet_column));
            iter->reset(it);
        } else {
            // Node contains column with children columns or has correspoding sparse columns
            // Create reader with hirachical data.
            // If sparse column exists or read the full path of variant read in MERGE_SPARSE, otherwise READ_DIRECT
            HierarchicalDataReader::ReadType read_type =
                    (relative_path == root->path) || sparse_node != nullptr
                            ? HierarchicalDataReader::ReadType::MERGE_SPARSE
                            : HierarchicalDataReader::ReadType::READ_DIRECT;
            RETURN_IF_ERROR(
                    HierarchicalDataReader::create(iter, relative_path, node, root, read_type));
        }
    } else {
        // No such node, read from either sparse column or default column
        if (sparse_node != nullptr) {
            // sparse columns have this path, read from root
            RETURN_IF_ERROR(_new_iterator_with_variant_root(tablet_column, iter, root,
                                                            sparse_node->data.file_column_type));
        } else {
            // No such variant column in this segment, get a default one
            RETURN_IF_ERROR(new_default_iterator(tablet_column, iter));
        }
    }

    return Status::OK();
}

// Not use cid anymore, for example original table schema is colA int, then user do following actions
// 1.add column b
// 2. drop column b
// 3. add column c
// in the new schema column c's cid == 2
// but in the old schema column b's cid == 2
// but they are not the same column
Status Segment::new_column_iterator(const TabletColumn& tablet_column,
                                    std::unique_ptr<ColumnIterator>* iter,
                                    const StorageReadOptions* opt) {
    if (opt->runtime_state != nullptr) {
        _be_exec_version = opt->runtime_state->be_exec_version();
    }
    RETURN_IF_ERROR(_create_column_readers_once(opt->stats));

    // init column iterator by path info
    if (tablet_column.has_path_info() || tablet_column.is_variant_type()) {
        return new_column_iterator_with_path(tablet_column, iter, opt);
    }
    // init default iterator
    if (!_column_readers.contains(tablet_column.unique_id())) {
        RETURN_IF_ERROR(new_default_iterator(tablet_column, iter));
        return Status::OK();
    }
    // init iterator by unique id
    ColumnIterator* it;
    RETURN_IF_ERROR(
            _column_readers.at(tablet_column.unique_id())->new_iterator(&it, &tablet_column));
    iter->reset(it);

    if (config::enable_column_type_check && !tablet_column.is_agg_state_type() &&
        tablet_column.type() != _column_readers.at(tablet_column.unique_id())->get_meta_type()) {
        LOG(WARNING) << "different type between schema and column reader,"
                     << " column schema name: " << tablet_column.name()
                     << " column schema type: " << int(tablet_column.type())
                     << " column reader meta type: "
                     << int(_column_readers.at(tablet_column.unique_id())->get_meta_type());
        return Status::InternalError("different type between schema and column reader");
    }
    return Status::OK();
}

Status Segment::new_column_iterator(int32_t unique_id, const StorageReadOptions* opt,
                                    std::unique_ptr<ColumnIterator>* iter) {
    RETURN_IF_ERROR(_create_column_readers_once(opt->stats));
    ColumnIterator* it;
    TabletColumn tablet_column = _tablet_schema->column_by_uid(unique_id);
    RETURN_IF_ERROR(_column_readers.at(unique_id)->new_iterator(&it, &tablet_column));
    iter->reset(it);
    return Status::OK();
}

ColumnReader* Segment::_get_column_reader(const TabletColumn& col) {
    // init column iterator by path info
    if (col.has_path_info() || col.is_variant_type()) {
        auto relative_path = col.path_info_ptr()->copy_pop_front();
        int32_t unique_id = col.unique_id() > 0 ? col.unique_id() : col.parent_unique_id();
        const auto* node = col.has_path_info()
                                   ? _sub_column_tree[unique_id].find_exact(relative_path)
                                   : nullptr;
        if (node != nullptr) {
            return node->data.reader.get();
        }
        return nullptr;
    }
    auto col_unique_id = col.unique_id();
    if (_column_readers.contains(col_unique_id)) {
        return _column_readers[col_unique_id].get();
    }
    return nullptr;
}

Status Segment::new_bitmap_index_iterator(const TabletColumn& tablet_column,
                                          const StorageReadOptions& read_options,
                                          std::unique_ptr<BitmapIndexIterator>* iter) {
    RETURN_IF_ERROR(_create_column_readers_once(read_options.stats));
    ColumnReader* reader = _get_column_reader(tablet_column);
    if (reader != nullptr && reader->has_bitmap_index()) {
        BitmapIndexIterator* it;
        RETURN_IF_ERROR(reader->new_bitmap_index_iterator(&it));
        iter->reset(it);
        return Status::OK();
    }
    return Status::OK();
}

Status Segment::new_index_iterator(const TabletColumn& tablet_column, const TabletIndex* index_meta,
                                   const StorageReadOptions& read_options,
                                   std::unique_ptr<IndexIterator>* iter) {
    if (read_options.runtime_state != nullptr) {
        _be_exec_version = read_options.runtime_state->be_exec_version();
    }
    RETURN_IF_ERROR(_create_column_readers_once(read_options.stats));
    ColumnReader* reader = _get_column_reader(tablet_column);
    if (reader != nullptr && index_meta) {
        // call DorisCallOnce.call without check if _index_file_reader is nullptr
        // to avoid data race during parallel method calls
        RETURN_IF_ERROR(_index_file_reader_open.call([&] { return _open_index_file_reader(); }));
        // after DorisCallOnce.call, _index_file_reader is guaranteed to be not nullptr
        RETURN_IF_ERROR(
                reader->new_index_iterator(_index_file_reader, index_meta, read_options, iter));
        return Status::OK();
    }
    return Status::OK();
}

Status Segment::lookup_row_key(const Slice& key, const TabletSchema* latest_schema,
                               bool with_seq_col, bool with_rowid, RowLocation* row_location,
                               OlapReaderStatistics* stats, std::string* encoded_seq_value) {
    RETURN_IF_ERROR(load_pk_index_and_bf(stats));
    bool has_seq_col = latest_schema->has_sequence_col();
    bool has_rowid = !latest_schema->cluster_key_uids().empty();
    size_t seq_col_length = 0;
    if (has_seq_col) {
        seq_col_length = latest_schema->column(latest_schema->sequence_col_idx()).length() + 1;
    }
    size_t rowid_length = has_rowid ? PrimaryKeyIndexReader::ROW_ID_LENGTH : 0;

    Slice key_without_seq =
            Slice(key.get_data(), key.get_size() - (with_seq_col ? seq_col_length : 0) -
                                          (with_rowid ? rowid_length : 0));

    DCHECK(_pk_index_reader != nullptr);
    if (!_pk_index_reader->check_present(key_without_seq)) {
        return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
    }
    bool exact_match = false;
    std::unique_ptr<segment_v2::IndexedColumnIterator> index_iterator;
    RETURN_IF_ERROR(_pk_index_reader->new_iterator(&index_iterator, stats));
    auto st = index_iterator->seek_at_or_after(&key_without_seq, &exact_match);
    if (!st.ok() && !st.is<ErrorCode::ENTRY_NOT_FOUND>()) {
        return st;
    }
    if (st.is<ErrorCode::ENTRY_NOT_FOUND>() || (!has_seq_col && !has_rowid && !exact_match)) {
        return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
    }
    row_location->row_id = cast_set<uint32_t>(index_iterator->get_current_ordinal());
    row_location->segment_id = _segment_id;
    row_location->rowset_id = _rowset_id;

    size_t num_to_read = 1;
    auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
            _pk_index_reader->type_info()->type(), 1, 0);
    auto index_column = index_type->create_column();
    size_t num_read = num_to_read;
    RETURN_IF_ERROR(index_iterator->next_batch(&num_read, index_column));
    DCHECK(num_to_read == num_read);

    Slice sought_key = Slice(index_column->get_data_at(0).data, index_column->get_data_at(0).size);

    // user may use "ALTER TABLE tbl ENABLE FEATURE "SEQUENCE_LOAD" WITH ..." to add a hidden sequence column
    // for a merge-on-write table which doesn't have sequence column, so `has_seq_col ==  true` doesn't mean
    // data in segment has sequence column value
    bool segment_has_seq_col = _tablet_schema->has_sequence_col();
    Slice sought_key_without_seq = Slice(
            sought_key.get_data(),
            sought_key.get_size() - (segment_has_seq_col ? seq_col_length : 0) - rowid_length);

    if (has_seq_col) {
        // compare key
        if (key_without_seq.compare(sought_key_without_seq) != 0) {
            return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
        }

        if (with_seq_col && segment_has_seq_col) {
            // compare sequence id
            Slice sequence_id =
                    Slice(key.get_data() + key_without_seq.get_size() + 1, seq_col_length - 1);
            Slice previous_sequence_id =
                    Slice(sought_key.get_data() + sought_key_without_seq.get_size() + 1,
                          seq_col_length - 1);
            if (sequence_id.compare(previous_sequence_id) < 0) {
                return Status::Error<ErrorCode::KEY_ALREADY_EXISTS>(
                        "key with higher sequence id exists");
            }
        }
    } else if (has_rowid) {
        Slice sought_key_without_rowid =
                Slice(sought_key.get_data(), sought_key.get_size() - rowid_length);
        // compare key
        if (key_without_seq.compare(sought_key_without_rowid) != 0) {
            return Status::Error<ErrorCode::KEY_NOT_FOUND>("Can't find key in the segment");
        }
    }
    // found the key, use rowid in pk index if necessary.
    if (has_rowid) {
        Slice rowid_slice = Slice(sought_key.get_data() + sought_key_without_seq.get_size() +
                                          (segment_has_seq_col ? seq_col_length : 0) + 1,
                                  rowid_length - 1);
        const auto* type_info = get_scalar_type_info<FieldType::OLAP_FIELD_TYPE_UNSIGNED_INT>();
        const auto* rowid_coder = get_key_coder(type_info->type());
        RETURN_IF_ERROR(rowid_coder->decode_ascending(&rowid_slice, rowid_length,
                                                      (uint8_t*)&row_location->row_id));
    }

    if (encoded_seq_value) {
        if (!segment_has_seq_col) {
            *encoded_seq_value = std::string {};
        } else {
            // include marker
            *encoded_seq_value =
                    Slice(sought_key.get_data() + sought_key_without_seq.get_size(), seq_col_length)
                            .to_string();
        }
    }
    return Status::OK();
}

Status Segment::read_key_by_rowid(uint32_t row_id, std::string* key) {
    OlapReaderStatistics* null_stat = nullptr;
    RETURN_IF_ERROR(load_pk_index_and_bf(null_stat));
    std::unique_ptr<segment_v2::IndexedColumnIterator> iter;
    RETURN_IF_ERROR(_pk_index_reader->new_iterator(&iter, null_stat));

    auto index_type = vectorized::DataTypeFactory::instance().create_data_type(
            _pk_index_reader->type_info()->type(), 1, 0);
    auto index_column = index_type->create_column();
    RETURN_IF_ERROR(iter->seek_to_ordinal(row_id));
    size_t num_read = 1;
    RETURN_IF_ERROR(iter->next_batch(&num_read, index_column));
    CHECK(num_read == 1);
    // trim row id
    if (_tablet_schema->cluster_key_uids().empty()) {
        *key = index_column->get_data_at(0).to_string();
    } else {
        Slice sought_key =
                Slice(index_column->get_data_at(0).data, index_column->get_data_at(0).size);
        Slice sought_key_without_rowid =
                Slice(sought_key.get_data(),
                      sought_key.get_size() - PrimaryKeyIndexReader::ROW_ID_LENGTH);
        *key = sought_key_without_rowid.to_string();
    }
    return Status::OK();
}

bool Segment::same_with_storage_type(int32_t cid, const Schema& schema,
                                     bool read_flat_leaves) const {
    const auto* col = schema.column(cid);
    auto file_column_type =
            get_data_type_of(ColumnIdentifier {.unique_id = col->unique_id(),
                                               .parent_unique_id = col->parent_unique_id(),
                                               .path = col->path(),
                                               .is_nullable = col->is_nullable()},
                             read_flat_leaves);
    auto expected_type = Schema::get_data_type_ptr(*col);
#ifndef NDEBUG
    if (file_column_type && !file_column_type->equals(*expected_type)) {
        VLOG_DEBUG << fmt::format("Get column {}, file column type {}, exepected type {}",
                                  col->name(), file_column_type->get_name(),
                                  expected_type->get_name());
    }
#endif
    bool same =
            (!file_column_type) || (file_column_type && file_column_type->equals(*expected_type));
    return same;
}

Status Segment::seek_and_read_by_rowid(const TabletSchema& schema, SlotDescriptor* slot,
                                       uint32_t row_id, vectorized::MutableColumnPtr& result,
                                       OlapReaderStatistics& stats,
                                       std::unique_ptr<ColumnIterator>& iterator_hint) {
    StorageReadOptions storage_read_opt;
    storage_read_opt.stats = &stats;
    storage_read_opt.io_ctx.reader_type = ReaderType::READER_QUERY;
    segment_v2::ColumnIteratorOptions opt {
            .use_page_cache = !config::disable_storage_page_cache,
            .file_reader = file_reader().get(),
            .stats = &stats,
            .io_ctx = io::IOContext {.reader_type = ReaderType::READER_QUERY,
                                     .file_cache_stats = &stats.file_cache_stats},
    };

    std::vector<segment_v2::rowid_t> single_row_loc {row_id};
    if (!slot->column_paths().empty()) {
        vectorized::PathInDataPtr path = std::make_shared<vectorized::PathInData>(
                schema.column_by_uid(slot->col_unique_id()).name_lower_case(),
                slot->column_paths());

        // here need create column readers to make sure column reader is created before seek_and_read_by_rowid
        // if segment cache miss, column reader will be created to make sure the variant column result not coredump
        RETURN_IF_ERROR(_create_column_readers_once(&stats));
        auto storage_type = get_data_type_of(ColumnIdentifier {.unique_id = slot->col_unique_id(),
                                                               .path = path,
                                                               .is_nullable = slot->is_nullable()},
                                             false);
        vectorized::MutableColumnPtr file_storage_column = storage_type->create_column();
        DCHECK(storage_type != nullptr);
        TabletColumn column = TabletColumn::create_materialized_variant_column(
                schema.column_by_uid(slot->col_unique_id()).name_lower_case(), slot->column_paths(),
                slot->col_unique_id());
        if (iterator_hint == nullptr) {
            RETURN_IF_ERROR(new_column_iterator(column, &iterator_hint, &storage_read_opt));
            RETURN_IF_ERROR(iterator_hint->init(opt));
        }
        RETURN_IF_ERROR(
                iterator_hint->read_by_rowids(single_row_loc.data(), 1, file_storage_column));
        // iterator_hint.reset(nullptr);
        // Get it's inner field, for JSONB case
        vectorized::Field field = remove_nullable(storage_type)->get_default();
        file_storage_column->get(0, field);
        result->insert(field);
    } else {
        int index = (slot->col_unique_id() >= 0) ? schema.field_index(slot->col_unique_id())
                                                 : schema.field_index(slot->col_name());
        if (index < 0) {
            std::stringstream ss;
            ss << "field name is invalid. field=" << slot->col_name()
               << ", field_name_to_index=" << schema.get_all_field_names();
            return Status::InternalError(ss.str());
        }
        storage_read_opt.io_ctx.reader_type = ReaderType::READER_QUERY;
        if (iterator_hint == nullptr) {
            RETURN_IF_ERROR(
                    new_column_iterator(schema.column(index), &iterator_hint, &storage_read_opt));
            RETURN_IF_ERROR(iterator_hint->init(opt));
        }
        RETURN_IF_ERROR(iterator_hint->read_by_rowids(single_row_loc.data(), 1, result));
    }
    return Status::OK();
}

Status Segment::_get_segment_footer(std::shared_ptr<SegmentFooterPB>& footer_pb,
                                    OlapReaderStatistics* stats) {
    std::shared_ptr<SegmentFooterPB> footer_pb_shared = _footer_pb.lock();
    if (footer_pb_shared != nullptr) {
        footer_pb = footer_pb_shared;
        return Status::OK();
    }

    VLOG_DEBUG << fmt::format("Segment footer of {}:{}:{} is missing, try to load it",
                              _file_reader->path().native(), _file_reader->size(),
                              _file_reader->size() - 12);

    StoragePageCache* segment_footer_cache = ExecEnv::GetInstance()->get_storage_page_cache();
    DCHECK(segment_footer_cache != nullptr);

    auto cache_key = get_segment_footer_cache_key();

    PageCacheHandle cache_handle;

    if (!segment_footer_cache->lookup(cache_key, &cache_handle,
                                      segment_v2::PageTypePB::DATA_PAGE)) {
        RETURN_IF_ERROR(_parse_footer(footer_pb_shared, stats));
        segment_footer_cache->insert(cache_key, footer_pb_shared, footer_pb_shared->ByteSizeLong(),
                                     &cache_handle, segment_v2::PageTypePB::DATA_PAGE);
    } else {
        VLOG_DEBUG << fmt::format("Segment footer of {}:{}:{} is found in cache",
                                  _file_reader->path().native(), _file_reader->size(),
                                  _file_reader->size() - 12);
    }
    footer_pb_shared = cache_handle.get<std::shared_ptr<SegmentFooterPB>>();
    _footer_pb = footer_pb_shared;
    footer_pb = footer_pb_shared;
    return Status::OK();
}

StoragePageCache::CacheKey Segment::get_segment_footer_cache_key() const {
    DCHECK(_file_reader != nullptr);
    // The footer is always at the end of the segment file.
    // The size of footer is 12.
    // So we use the size of file minus 12 as the cache key, which is unique for each segment file.
    return StoragePageCache::CacheKey(_file_reader->path().native(), _file_reader->size(),
                                      _file_reader->size() - 12);
}

#include "common/compile_check_end.h"
} // namespace doris::segment_v2
