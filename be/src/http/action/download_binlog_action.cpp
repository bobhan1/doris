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

#include "http/action/download_binlog_action.h"

#include <fmt/format.h>
#include <fmt/ranges.h>

#include <cstdint>
#include <stdexcept>
#include <string_view>
#include <utility>

#include "common/config.h"
#include "common/logging.h"
#include "http/http_channel.h"
#include "http/http_request.h"
#include "http/utils.h"
#include "io/fs/local_file_system.h"
#include "olap/storage_engine.h"
#include "olap/tablet_manager.h"
#include "runtime/exec_env.h"
#include "util/stopwatch.hpp"

namespace doris {

namespace {
const std::string kMethodParameter = "method";
const std::string kTokenParameter = "token";
const std::string kTabletIdParameter = "tablet_id";
const std::string kBinlogVersionParameter = "binlog_version";
const std::string kRowsetIdParameter = "rowset_id";
const std::string kSegmentIndexParameter = "segment_index";
const std::string kSegmentIndexIdParameter = "segment_index_id";
const std::string kAcquireMD5Parameter = "acquire_md5";

bvar::LatencyRecorder g_get_binlog_info_latency("doris_download_binlog", "get_binlog_info");
bvar::LatencyRecorder g_get_segment_file_latency("doris_download_binlog", "get_segment_file");
bvar::LatencyRecorder g_get_segment_index_file_latency("doris_download_binlog",
                                                       "get_segment_index_file");
bvar::LatencyRecorder g_get_rowset_meta_latency("doris_download_binlog", "get_rowset_meta");

// get http param, if no value throw exception
const auto& get_http_param(HttpRequest* req, const std::string& param_name) {
    const auto& param = req->param(param_name);
    if (param.empty()) {
        auto error_msg = fmt::format("parameter {} not specified in url.", param_name);
        throw std::runtime_error(error_msg);
    }
    return param;
}

auto get_tablet(StorageEngine& engine, const std::string& tablet_id_str) {
    int64_t tablet_id = std::atoll(tablet_id_str.data());

    TabletSharedPtr tablet = engine.tablet_manager()->get_tablet(tablet_id);
    if (tablet == nullptr) {
        auto error = fmt::format("tablet is not exist, tablet_id={}", tablet_id);
        LOG(WARNING) << error;
        throw std::runtime_error(error);
    }

    return tablet;
}

// need binlog_version, tablet_id
void handle_get_binlog_info(StorageEngine& engine, HttpRequest* req) {
    try {
        const auto& binlog_version = get_http_param(req, kBinlogVersionParameter);
        const auto& tablet_id = get_http_param(req, kTabletIdParameter);
        auto tablet = get_tablet(engine, tablet_id);

        const auto& [rowset_id, num_segments] = tablet->get_binlog_info(binlog_version);
        if (rowset_id.empty()) {
            HttpChannel::send_reply(
                    req, HttpStatus::NOT_FOUND,
                    fmt::format("get binlog info failed, binlog_version={}", binlog_version));
        } else if (num_segments < 0) {
            HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                    fmt::format("invalid num_segments: {}", num_segments));
        } else {
            auto binlog_info_msg = fmt::format("{}:{}", rowset_id, num_segments);
            HttpChannel::send_reply(req, binlog_info_msg);
        }
    } catch (const std::exception& e) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, e.what());
        LOG(WARNING) << "get binlog info failed, error: " << e.what();
        return;
    }
}

/// handle get segment file, need tablet_id, rowset_id && index
void handle_get_segment_file(StorageEngine& engine, HttpRequest* req,
                             bufferevent_rate_limit_group* rate_limit_group) {
    // Step 1: get download file path
    std::string segment_file_path;
    bool is_acquire_md5 = false;
    try {
        const auto& tablet_id = get_http_param(req, kTabletIdParameter);
        auto tablet = get_tablet(engine, tablet_id);
        const auto& rowset_id = get_http_param(req, kRowsetIdParameter);
        const auto& segment_index = get_http_param(req, kSegmentIndexParameter);
        segment_file_path = tablet->get_segment_filepath(rowset_id, segment_index);
        is_acquire_md5 = !req->param(kAcquireMD5Parameter).empty();
    } catch (const std::exception& e) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, e.what());
        LOG(WARNING) << "get download file path failed, error: " << e.what();
        return;
    }

    // Step 2: handle download
    // check file exists
    bool exists = false;
    Status status = io::global_local_filesystem()->exists(segment_file_path, &exists);
    if (!status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status.to_string());
        LOG(WARNING) << "check file exists failed, error: " << status.to_string();
        return;
    }
    if (!exists) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "file not exist.");
        LOG(WARNING) << "file not exist, file path: " << segment_file_path;
        return;
    }
    do_file_response(segment_file_path, req, rate_limit_group, is_acquire_md5);
}

/// handle get segment index file, need tablet_id, rowset_id, segment_index && segment_index_id
void handle_get_segment_index_file(StorageEngine& engine, HttpRequest* req,
                                   bufferevent_rate_limit_group* rate_limit_group) {
    // Step 1: get download file path
    std::string segment_index_file_path;
    bool is_acquire_md5 = false;
    try {
        const auto& tablet_id = get_http_param(req, kTabletIdParameter);
        auto tablet = get_tablet(engine, tablet_id);
        const auto& rowset_id = get_http_param(req, kRowsetIdParameter);
        const auto& segment_index = get_http_param(req, kSegmentIndexParameter);
        const auto& segment_index_id = req->param(kSegmentIndexIdParameter);
        auto segment_file_path = tablet->get_segment_filepath(rowset_id, segment_index);
        if (tablet->tablet_schema()->get_inverted_index_storage_format() ==
            InvertedIndexStorageFormatPB::V1) {
            // now CCR not support for variant + index v1
            constexpr std::string_view index_suffix = "";
            segment_index_file_path = InvertedIndexDescriptor::get_index_file_path_v1(
                    InvertedIndexDescriptor::get_index_file_path_prefix(segment_file_path),
                    std::stoll(segment_index_id), index_suffix);
        } else {
            DCHECK(segment_index_id == "-1");
            segment_index_file_path = InvertedIndexDescriptor::get_index_file_path_v2(
                    InvertedIndexDescriptor::get_index_file_path_prefix(segment_file_path));
        }
        is_acquire_md5 = !req->param(kAcquireMD5Parameter).empty();
    } catch (const std::exception& e) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, e.what());
        LOG(WARNING) << "get download file path failed, error: " << e.what();
        return;
    }

    // Step 2: handle download
    // check file exists
    bool exists = false;
    Status status = io::global_local_filesystem()->exists(segment_index_file_path, &exists);
    if (!status.ok()) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, status.to_string());
        LOG(WARNING) << "check file exists failed, error: " << status.to_string();
        return;
    }
    if (!exists) {
        HttpChannel::send_reply(req, HttpStatus::NOT_FOUND, "file not exist.");
        LOG(WARNING) << "file not exist, file path: " << segment_index_file_path;
        return;
    }
    do_file_response(segment_index_file_path, req, rate_limit_group, is_acquire_md5);
}

void handle_get_rowset_meta(StorageEngine& engine, HttpRequest* req) {
    try {
        const auto& tablet_id = get_http_param(req, kTabletIdParameter);
        auto tablet = get_tablet(engine, tablet_id);
        const auto& rowset_id = get_http_param(req, kRowsetIdParameter);
        const auto& binlog_version = get_http_param(req, kBinlogVersionParameter);
        auto rowset_meta = tablet->get_rowset_binlog_meta(binlog_version, rowset_id);
        if (rowset_meta.empty()) {
            HttpChannel::send_reply(req, HttpStatus::NOT_FOUND,
                                    fmt::format("get rowset meta failed, rowset_id={}", rowset_id));
        } else {
            HttpChannel::send_reply(req, rowset_meta);
        }
    } catch (const std::exception& e) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR, e.what());
        LOG(WARNING) << "get download file path failed, error: " << e.what();
    }
}

} // namespace

DownloadBinlogAction::DownloadBinlogAction(
        ExecEnv* exec_env, StorageEngine& engine,
        std::shared_ptr<bufferevent_rate_limit_group> rate_limit_group)
        : HttpHandlerWithAuth(exec_env),
          _engine(engine),
          _rate_limit_group(std::move(rate_limit_group)) {}

void DownloadBinlogAction::handle(HttpRequest* req) {
    VLOG_CRITICAL << "accept one download binlog request " << req->debug_string();

    if (!config::enable_feature_binlog) {
        HttpChannel::send_reply(req, HttpStatus::INTERNAL_SERVER_ERROR,
                                "binlog feature is not enabled.");
        return;
    }

    // Step 1: check token
    Status status;
    if (config::enable_token_check) {
        // FIXME(Drogon): support check token
        // status = _check_token(req);
        if (!status.ok()) {
            HttpChannel::send_reply(req, HttpStatus::UNAUTHORIZED, status.to_string());
            return;
        }
    }

    // Step 2: get method
    const std::string& method = req->param(kMethodParameter);

    // Step 3: dispatch
    MonotonicStopWatch watch;
    watch.start();
    if (method == "get_binlog_info") {
        handle_get_binlog_info(_engine, req);
        g_get_binlog_info_latency << watch.elapsed_time_microseconds();
    } else if (method == "get_segment_file") {
        handle_get_segment_file(_engine, req, _rate_limit_group.get());
        g_get_segment_file_latency << watch.elapsed_time_microseconds();
    } else if (method == "get_segment_index_file") {
        handle_get_segment_index_file(_engine, req, _rate_limit_group.get());
        g_get_segment_index_file_latency << watch.elapsed_time_microseconds();
    } else if (method == "get_rowset_meta") {
        handle_get_rowset_meta(_engine, req);
        g_get_rowset_meta_latency << watch.elapsed_time_microseconds();
    } else {
        auto error_msg = fmt::format("invalid method: {}", method);
        LOG(WARNING) << error_msg;
        HttpChannel::send_reply(req, HttpStatus::NOT_IMPLEMENTED, error_msg);
    }
}

Status DownloadBinlogAction::_check_token(HttpRequest* req) {
    const std::string& token_str = req->param(kTokenParameter);
    if (token_str.empty()) {
        return Status::InternalError("token is not specified.");
    }

    const std::string& local_token = _exec_env->token();
    if (token_str != local_token) {
        LOG(WARNING) << "invalid download token: " << token_str << ", local token: " << local_token;
        return Status::NotAuthorized("invalid token {}", token_str);
    }

    return Status::OK();
}

} // end namespace doris
