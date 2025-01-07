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

#include <algorithm>
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

void print_rowset_meta(std::vector<doris::RowsetMetaSharedPtr>& rs_metas, std::string prefix,
                       std::map<doris::RowsetId, int64_t>& kvs) {
    using namespace doris;
    std::sort(rs_metas.begin(), rs_metas.end(),
              [](const RowsetMetaSharedPtr& x, const RowsetMetaSharedPtr& y) {
                  return x->start_version() < y->start_version();
              });
    std::cout << fmt::format("    {} rowsets:\n", prefix);
    int64_t marks_sum {};
    for (const auto& rs_meta : rs_metas) {
        std::cout << fmt::format(
                "        tablet_id={}, rowset_id={}, version={}, delete bitmap kvs={}\n",
                rs_meta->tablet_id(), rs_meta->rowset_id().to_string(),
                rs_meta->version().to_string(), kvs[rs_meta->rowset_id()]);
        marks_sum += kvs[rs_meta->rowset_id()];
    }
    std::cout << fmt::format("    {} rowsets summary: count={}, kvs={}\n", prefix, rs_metas.size(),
                             marks_sum);
}

void print_meta_detail(doris::OlapMeta& meta) {
    using namespace doris;

    // rowset meta
    int64_t rowset_meta_size_sum {};
    int64_t rowset_meta_count {};
    std::map<int64_t, std::vector<Version>> tablet_rowsets;
    auto load_rowset_func = [&](TabletUid tablet_uid, RowsetId rowset_id,
                                const std::string& meta_str) -> bool {
        rowset_meta_size_sum += meta_str.size();
        rowset_meta_count++;
        RowsetMetaPB rowset_meta_pb;
        if (!rowset_meta_pb.ParseFromString(meta_str)) {
            std::cout << fmt::format("failed to parse rowset meta pb. rowset_id={}",
                                     rowset_id.to_string());
            return true;
        }

        RowsetStatePB state = rowset_meta_pb.rowset_state();
        int64_t tablet_id = rowset_meta_pb.tablet_id();
        int64_t start_ver = rowset_meta_pb.start_version();
        int64_t end_ver = rowset_meta_pb.end_version();

        std::cout << fmt::format(
                "collect rowset meta: tablet_id={}, rowset_id={}, size={}, state={}, "
                "version=[{}-{}]\n",
                tablet_id, rowset_id.to_string(), meta_str.size(), RowsetStatePB_Name(state),
                start_ver, end_ver);

        if (rowset_meta_pb.rowset_state() == RowsetStatePB::VISIBLE) {
            tablet_rowsets[tablet_id].emplace_back(start_ver, end_ver);
        }

        return true;
    };

    // for (auto& [tablet_id, versions] : tablet_rowsets) {
    //     std::sort(versions.begin(), versions.end(), [](const Version& left, const Version& right) {
    //         return left.first < right.first;
    //     });
    //     std::cout << fmt::format("xxx tablet_id={} visible rowsets count={}\n", tablet_id,
    //                              versions.size());
    //     for (const auto ver : versions) {
    //         std::cout << fmt::format("    {}\n", ver.to_string());
    //     }
    // }

    Status res = RowsetMetaManager::traverse_rowset_metas(&meta, load_rowset_func);
    if (!res.ok()) {
        std::cout << fmt::format("failed to traverse rowset meta.\n");
    }

    std::cout << fmt::format("finish collect rowset meta\n");

    // tablet_meta
    int64_t tablet_meta_size_sum {};
    int64_t delete_bitmap_size_sum {};
    int64_t delete_bitmap_kvs {};
    int64_t tablet_meta_count {};

    struct rowset_digest {
        bool init {false};
        int64_t count {};
        int64_t max_seg_id {};
        int64_t min_ver {};
        int64_t max_ver {};
        int64_t sz {};
        std::set<int64_t> distinct_versions;

        void update(uint32_t seg_id, uint32_t ver, int64_t size) {
            ++count;
            sz += size;
            distinct_versions.insert(ver);
            if (!init) {
                init = true;
                min_ver = ver;
                max_ver = ver;
                max_seg_id = seg_id;
                return;
            }
            min_ver = std::min<int64_t>(min_ver, ver);
            max_ver = std::max<int64_t>(max_ver, ver);
            max_seg_id = std::max<int64_t>(max_seg_id, seg_id);
        }

        std::string str() const {
            return fmt::format(
                    "kvs={}, sz={}, max_seg_id={}, min_ver={}, max_ver={}, version_gap={}, "
                    "version_distinct_count={}",
                    count, sz, max_seg_id, min_ver, max_ver, max_ver - min_ver,
                    distinct_versions.size());
        }
    };

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
            std::map<RowsetId, rowset_digest> digest;
            std::map<RowsetId, int64_t> kvs;

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
                ++kvs[rst_id];
                digest[rst_id].update(seg_id, ver, delete_bitmap.size());
                // std::cout << fmt::format(
                //         "xx   collect delete bitmap: rowset_id={}, seg={}, ver={}, sz={}\n",
                //         rst_id.to_string(), seg_id, ver, delete_bitmap.size());
            }
            delete_bitmap_size_sum += sz;
            delete_bitmap_kvs += seg_maps_size;
            std::vector<Version> versions;
            std::vector<RowsetMetaSharedPtr> rs_metas;
            std::vector<RowsetMetaSharedPtr> stale_rs_metas;
            for (auto& rs_meta : *tablet_meta_pb.mutable_rs_metas()) {
                if (rs_meta.rowset_state() == RowsetStatePB::VISIBLE) {
                    int64_t start_ver = rs_meta.start_version();
                    int64_t end_ver = rs_meta.end_version();
                    versions.emplace_back(start_ver, end_ver);

                    RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
                    rs_meta.clear_tablet_schema();
                    bool parsed = rowset_meta->init_from_pb(rs_meta);
                    if (!parsed) {
                        std::cout << fmt::format("failed to init_from_pb for rowset_meta.");
                        continue;
                    }
                    rs_metas.push_back(std::move(rowset_meta));
                }
            }
            for (auto& stale_rs_meta : *tablet_meta_pb.mutable_stale_rs_metas()) {
                RowsetMetaSharedPtr rowset_meta(new RowsetMeta());
                stale_rs_meta.clear_tablet_schema();
                bool parsed = rowset_meta->init_from_pb(stale_rs_meta);
                if (!parsed) {
                    std::cout << fmt::format("failed to init_from_pb for rowset_meta.");
                    continue;
                }
                stale_rs_metas.push_back(std::move(rowset_meta));
            }

            std::cout << fmt::format(
                    "tablet's delete bitmap: tablet_id={}, delete_bitmap_size={}, kvs={}, "
                    "max_ver={}, cumu_point={}, visible rowset count={}, stale rowset count={}\n",
                    tablet_id, sz, seg_maps_size, max_ver, tablet_meta_pb.cumulative_layer_point(),
                    rs_metas.size(), stale_rs_metas.size());
            print_rowset_meta(rs_metas, "visible", kvs);
            print_rowset_meta(stale_rs_metas, "stale", kvs);

            std::vector<RowsetId> ids;
            for (const auto& [rowset_id, _] : digest) {
                ids.push_back(rowset_id);
            }
            std::sort(ids.begin(), ids.end(), [&](const RowsetId& x, const RowsetId& y) {
                return digest[x].count > digest[y].count;
            });
            for (const auto& rowset_id : ids) {
                std::cout << fmt::format("xx    collect dbm rowset digest: rowset_id={}, {}\n",
                                         rowset_id.to_string(), digest[rowset_id].str());
            }
            // for (const auto& [rowset_id, stats] : digest) {
            //     std::cout << fmt::format("xx    collect dbm rowset digest: rowset_id={}, {}\n",
            //                              rowset_id.to_string(), stats.str());
            // }
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
            " ======================== Summary: ======================== \n"
            "tablet_meta_count={}, tablet_meta_size={} B\n"
            "rowset_meta_count={}, rowset_meta.size={} B\n"
            "delete_bitmap_kvs={}, delete_bitmap_size={} B\n"
            "pending_delete_bitmap_size={} B\n",
            tablet_meta_count, tablet_meta_size_sum, rowset_meta_count, rowset_meta_size_sum,
            delete_bitmap_kvs, delete_bitmap_size_sum, pending_delete_bitmap_size_sum);
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