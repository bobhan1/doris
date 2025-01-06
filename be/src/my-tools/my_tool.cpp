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

#include <fmt/format.h>
#include <gen_cpp/olap_file.pb.h>
#include <gflags/gflags.h>

#include <cstdint>
#include <string>

#include "common/status.h"
#include "olap/olap_meta.h"
#include "olap/rowset/rowset_meta.h"
#include "olap/rowset/rowset_meta_manager.h"
#include "olap/tablet_meta_manager.h"

DEFINE_string(operation, "", "valid operation: show");
DEFINE_string(root_path, "", "root path");

std::string get_usage(const std::string& progname) {
    std::stringstream ss;
    ss << progname << " is the Doris inverted index file tool.\n";
    ss << "Usage:\n";
    ss << "./my_tool";
    return ss.str();
}

void print_meta_detail(doris::OlapMeta& meta) {
    using namespace doris;

    // rowset meta
    int64_t rowset_meta_size_sum {};
    int64_t rowset_meta_count {};
    auto load_rowset_func = [&](TabletUid tablet_uid, RowsetId rowset_id,
                                const std::string& meta_str) -> bool {
        rowset_meta_size_sum += meta_str.size();
        rowset_meta_count++;
        std::cout << fmt::format("collect rowset meta: rowset_id={}, size={}\n",
                                 rowset_id.to_string(), meta_str.size());
        return true;
    };

    Status res = RowsetMetaManager::traverse_rowset_metas(&meta, load_rowset_func);
    if (!res.ok()) {
        std::cout << fmt::format("failed to traverse rowset meta.\n");
    }

    std::cout << fmt::format("finish collect rowset meta\n");

    // tablet_meta
    int64_t tablet_meta_size_sum {};
    int64_t delete_bitmap_size_sum {};
    int64_t tablet_meta_count {};
    auto load_tablet_func = [&](int64_t tablet_id, int32_t schema_hash,
                                const std::string& value) -> bool {
        tablet_meta_size_sum += value.size();
        ++tablet_meta_count;
        std::cout << fmt::format("collect tablet meta: tablet_id={}, size={}\n", tablet_id,
                                 value.size());
        TabletMetaPB tablet_meta_pb;
        bool parsed = tablet_meta_pb.ParseFromString(value);
        if (!parsed) {
            std::cout << fmt::format("failed to parse tablet mete. tablet_id={}\n", tablet_id);
        }

        if (tablet_meta_pb.has_delete_bitmap()) {
            int rst_ids_size = tablet_meta_pb.delete_bitmap().rowset_ids_size();
            int seg_ids_size = tablet_meta_pb.delete_bitmap().segment_ids_size();
            int versions_size = tablet_meta_pb.delete_bitmap().versions_size();
            int seg_maps_size = tablet_meta_pb.delete_bitmap().segment_delete_bitmaps_size();
            CHECK(rst_ids_size == seg_ids_size && seg_ids_size == seg_maps_size &&
                  seg_maps_size == versions_size);
            int64_t sz {};
            int64_t max_ver {-1};
            for (size_t i = 0; i < rst_ids_size; ++i) {
                RowsetId rst_id;
                rst_id.init(tablet_meta_pb.delete_bitmap().rowset_ids(i));
                auto seg_id = tablet_meta_pb.delete_bitmap().segment_ids(i);
                uint32_t ver = tablet_meta_pb.delete_bitmap().versions(i);
                max_ver = std::max<int64_t>(max_ver, ver);
                const auto& delete_bitmap =
                        tablet_meta_pb.delete_bitmap().segment_delete_bitmaps(i);
                sz += delete_bitmap.size();
                std::cout << fmt::format(
                        "xx   collect delete bitmap: rowset_id={}, seg={}, ver={}, sz={}\n",
                        rst_id.to_string(), seg_id, ver, delete_bitmap.size());
            }
            delete_bitmap_size_sum += sz;
            std::cout << fmt::format(
                    "tablet's delete bitmap: tablet_id={}, delete_bitmap_size={}, kvs={}, "
                    "max_ver={}, cumu_point={}\n",
                    tablet_id, sz, seg_maps_size, max_ver, tablet_meta_pb.cumulative_layer_point());
        }

        return true;
    };

    res = TabletMetaManager::traverse_headers(&meta, load_tablet_func);
    if (!res.ok()) {
        std::cout << fmt::format("failed to traverse tablet meta.\n");
    }

    // pending delete bitmaps
    std::vector<DeleteBitmapPB> delete_bitmap_pbs;
    int64_t pending_delete_bitmap_size_sum {};
    auto collect_delete_bitmap_func = [&](int64_t tablet_id, int64_t version,
                                          const std::string& val) {
        DeleteBitmapPB delete_bitmap_pb;
        delete_bitmap_pb.ParseFromString(val);
        delete_bitmap_pbs.emplace_back(std::move(delete_bitmap_pb));
        pending_delete_bitmap_size_sum += val.size();

        std::cout << fmt::format(
                "collect pending delete bitmap: tablet_id={}, version={} val.size={}\n", tablet_id,
                version, val.size());

        return true;
    };

    res = TabletMetaManager::traverse_delete_bitmap(&meta, collect_delete_bitmap_func);
    if (!res.ok()) {
        std::cout << fmt::format("failed to traverse delete bitmap.\n");
    }

    std::cout << fmt::format(
            " ======================== Summary: ======================== "
            "\ntablet_meta.size={}, rowset_meta_count={}\nrowset_meta.size={} "
            "B\ntablet_meta_size={} "
            "B\ndelete_bitmap_size={} B\npending_delete_bitmap_size={} B\n",
            tablet_meta_count, rowset_meta_count, rowset_meta_size_sum, tablet_meta_size_sum,
            delete_bitmap_size_sum, pending_delete_bitmap_size_sum);
}

int main(int argc, char** argv) {
    using namespace doris;

    std::string usage = get_usage(argv[0]);
    gflags::SetUsageMessage(usage);
    google::ParseCommandLineFlags(&argc, &argv, true);

    if (FLAGS_operation == "show") {
        std::string root_path {FLAGS_root_path};
        std::cout << fmt::format("get root_path={}\n", root_path);
        OlapMeta meta {root_path};
        std::cout << fmt::format("begin to init OlapMeta, root_path={}\n", root_path);
        Status res = meta.init();
        if (!res.ok()) {
            std::cout << fmt::format("init OlapMeta failed, open rocksdb failed, path={}",
                                     root_path);
        }
        std::cout << fmt::format("successfully open RocksDB, path={}\n", meta.get_root_path());

        print_meta_detail(meta);
    }
    gflags::ShutDownCommandLineFlags();
    return 0;
}