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

#include "meta-store/mem_txn_kv.h"

#include <gtest/gtest-death-test.h>
#include <gtest/gtest.h>

#include <memory>

#include "common/config.h"
#include "common/util.h"
#include "meta-service/doris_txn.h"
#include "meta-store/codec.h"
#include "meta-store/txn_kv.h"
#include "meta-store/txn_kv_error.h"

using namespace doris;

std::shared_ptr<cloud::TxnKv> fdb_txn_kv;

int main(int argc, char** argv) {
    cloud::config::init(nullptr, true);
    cloud::config::fdb_cluster_file_path = "fdb.cluster";
    fdb_txn_kv = std::dynamic_pointer_cast<cloud::TxnKv>(std::make_shared<cloud::FdbTxnKv>());
    if (!fdb_txn_kv.get()) {
        std::cout << "exit get FdbTxnKv error" << std::endl;
        return -1;
    }
    if (fdb_txn_kv->init() != 0) {
        std::cout << "exit inti FdbTxnKv error" << std::endl;
        return -1;
    }

    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}

static void put_and_get_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::string txn_kv_class = dynamic_cast<MemTxnKv*>(txn_kv.get()) != nullptr ? " memkv" : " fdb";
    std::unique_ptr<Transaction> txn;
    std::string key = "testkey1";
    std::string val = "testvalue1";
    {
        // put
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put(key, val);
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
        int64_t ver1;
        ASSERT_EQ(txn->get_committed_version(&ver1), TxnErrorCode::TXN_OK);

        // get
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get(key, &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        int64_t ver2 = 0;
        ASSERT_EQ(txn->get_read_version(&ver2), TxnErrorCode::TXN_OK);
        ASSERT_GE(ver2, ver1) << txn_kv_class;
        ASSERT_EQ(val, get_val) << txn_kv_class;
        std::cout << "val:" << get_val << std::endl;

        // get not exist key
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get("NotExistKey", &get_val), TxnErrorCode::TXN_KEY_NOT_FOUND)
                << txn_kv_class;
    }
}

TEST(TxnMemKvTest, PutAndGetTest) {
    using namespace doris::cloud;

    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    put_and_get_test(mem_txn_kv);
    put_and_get_test(fdb_txn_kv);
}

static void range_get_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::string txn_kv_class = dynamic_cast<MemTxnKv*>(txn_kv.get()) != nullptr ? " memkv" : " fdb";
    std::unique_ptr<Transaction> txn;
    std::vector<std::pair<std::string, std::string>> put_kv = {
            std::make_pair("key1", "val1"), std::make_pair("key2", "val2"),
            std::make_pair("key3", "val3"), std::make_pair("key4", "val4"),
            std::make_pair("key5", "val5"),
    };

    // put some kvs before test
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (const auto& [key, val] : put_kv) {
        txn->put(key, val);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // normal range get
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        ASSERT_EQ(txn->get("key1", "key4", &iter), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(iter->size(), 3) << txn_kv_class;
        ASSERT_EQ(iter->more(), false) << txn_kv_class;
        int i = 0;
        while (iter->has_next()) {
            auto [key, val] = iter->next();
            ASSERT_EQ(key, put_kv[i].first) << txn_kv_class;
            ASSERT_EQ(val, put_kv[i].second) << txn_kv_class;
            ++i;
            std::cout << "key:" << key << " val:" << val << std::endl;
        }
    }

    // range get with not exist end key
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        ASSERT_EQ(txn->get("key2", "key6", &iter), TxnErrorCode::TXN_OK);
        ASSERT_EQ(iter->size(), 4) << txn_kv_class;
        ASSERT_EQ(iter->more(), false) << txn_kv_class;
        int i = 1;
        while (iter->has_next()) {
            auto [key, val] = iter->next();
            ASSERT_EQ(key, put_kv[i].first) << txn_kv_class;
            ASSERT_EQ(val, put_kv[i].second) << txn_kv_class;
            ++i;
            std::cout << "key:" << key << " val:" << val << std::endl;
        }
    }

    // range get with limit
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        ASSERT_EQ(txn->get("key1", "key4", &iter, false, 1), TxnErrorCode::TXN_OK);
        ASSERT_EQ(iter->size(), 1) << txn_kv_class;
        ASSERT_EQ(iter->more(), true) << txn_kv_class;

        auto [key, val] = iter->next();
        ASSERT_EQ(key, put_kv[0].first) << txn_kv_class;
        ASSERT_EQ(val, put_kv[0].second) << txn_kv_class;
    }

    // range get with begin key larger than end key
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        ASSERT_EQ(txn->get("key4", "key1", &iter), TxnErrorCode::TXN_OK);
        ASSERT_EQ(iter->size(), 0) << txn_kv_class;
        ASSERT_EQ(txn->get("key1", "key1", &iter), TxnErrorCode::TXN_OK);
        ASSERT_EQ(iter->size(), 0) << txn_kv_class;
    }
}

TEST(TxnMemKvTest, RangeGetTest) {
    using namespace doris::cloud;

    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    range_get_test(mem_txn_kv);
    range_get_test(fdb_txn_kv);
}

static void remove_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::unique_ptr<Transaction> txn;
    std::string txn_kv_class = dynamic_cast<MemTxnKv*>(txn_kv.get()) != nullptr ? " memkv" : " fdb";
    std::vector<std::pair<std::string, std::string>> put_kv = {
            std::make_pair("key1", "val1"), std::make_pair("key2", "val2"),
            std::make_pair("key3", "val3"), std::make_pair("key4", "val4"),
            std::make_pair("key5", "val5"),
    };

    // put some kvs before test
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    for (const auto& [key, val] : put_kv) {
        txn->put(key, val);
    }
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // remove single key
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->remove("key1");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string get_val;
        ASSERT_EQ(txn->get("key1", &get_val), TxnErrorCode::TXN_KEY_NOT_FOUND) << txn_kv_class;
    }

    // range remove with begin key larger than end key
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->remove("key5", "key1");
        ASSERT_NE(txn->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        ASSERT_EQ(txn->get("key2", "key6", &iter), TxnErrorCode::TXN_OK);
        ASSERT_EQ(iter->size(), 4) << txn_kv_class;
    }

    // range remove
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);

        txn->remove("key2", "key6");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        ASSERT_EQ(txn->get("key2", "key6", &iter), TxnErrorCode::TXN_OK);
        while (iter->has_next()) {
            auto [key, value] = iter->next();
            std::cout << "key: " << key << ", value: " << value << std::endl;
        }
        ASSERT_EQ(iter->size(), 0) << txn_kv_class;
    }
}
TEST(TxnMemKvTest, RemoveTest) {
    using namespace doris::cloud;

    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);
    remove_test(mem_txn_kv);
    remove_test(fdb_txn_kv);
}

static void atomic_set_ver_value_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::unique_ptr<Transaction> txn;
    std::string txn_kv_class = dynamic_cast<MemTxnKv*>(txn_kv.get()) != nullptr ? " memkv" : " fdb";
    // txn_kv_test.cpp
    {
        std::string key;
        std::string val;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        key.push_back('\xfe');
        key.append(" unit_test_prefix ");
        key.append(" GetVersionTest ");
        txn->atomic_set_ver_value(key, "");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        int64_t ver0 = 0;
        ASSERT_EQ(txn->get_committed_version(&ver0), TxnErrorCode::TXN_OK);
        ASSERT_GT(ver0, 0) << txn_kv_class;

        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
        err = txn->get(key, &val);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK) << txn_kv_class;
        int64_t ver1 = 0;
        ASSERT_EQ(txn->get_read_version(&ver1), TxnErrorCode::TXN_OK);
        ASSERT_GE(ver1, ver0) << txn_kv_class;

        int64_t ver2;
        int64_t txn_id;
        int ret = get_txn_id_from_fdb_ts(val, &txn_id);
        ASSERT_EQ(ret, 0) << txn_kv_class;
        ver2 = txn_id >> 10;
        std::cout << "ver0=" << ver0 << " ver1=" << ver1 << " ver2=" << ver2 << std::endl;

        // ASSERT_EQ(ver0, ver2);
    }
}

TEST(TxnMemKvTest, AtomicSetVerValueTest) {
    using namespace doris::cloud;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    atomic_set_ver_value_test(mem_txn_kv);
    atomic_set_ver_value_test(fdb_txn_kv);
}

static void atomic_set_ver_key_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::string key_prefix = "key_1";

    std::string versionstamp_1;
    {
        // write key_1
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->atomic_set_ver_key(key_prefix, "1");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // read key_1
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string end_key = key_prefix + "\xFF";
        std::unique_ptr<RangeGetIterator> it;
        ASSERT_EQ(txn->get(key_prefix, end_key, &it), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(it->has_next());
        auto&& [key_1, _1] = it->next();
        ASSERT_EQ(key_1.length(), key_prefix.size() + 10); // versionstamp = 10bytes
        key_1.remove_prefix(key_prefix.size());
        versionstamp_1 = key_1;
    }

    std::string versionstamp_2;
    {
        // write key_2
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        key_prefix = "key_2";
        txn->atomic_set_ver_key(key_prefix, "2");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        // read key_2
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string end_key = key_prefix + "\xFF";
        std::unique_ptr<RangeGetIterator> it;
        ASSERT_EQ(txn->get(key_prefix, end_key, &it), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(it->has_next());
        auto&& [key_2, _2] = it->next();
        ASSERT_EQ(key_2.length(), key_prefix.size() + 10); // versionstamp = 10bytes
        key_2.remove_prefix(key_prefix.size());
        versionstamp_2 = key_2;
    }

    ASSERT_LT(versionstamp_1, versionstamp_2);

    std::string versionstamp_3;
    {
        // write key_3, with offset
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string suffix = "_suffix";
        std::string prefix = "key_3_";
        std::string k(prefix);
        uint32_t offset = k.size();
        k.append(10, '\0'); // reserve 10 bytes for versionstamp
        k += suffix;
        ASSERT_TRUE(txn->atomic_set_ver_key(k, offset, "3"));
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        // read key_3
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string end_key = prefix + "\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF\xFF";
        std::unique_ptr<RangeGetIterator> it;
        ASSERT_EQ(txn->get(prefix, end_key, &it), TxnErrorCode::TXN_OK);
        ASSERT_TRUE(it->has_next());
        auto&& [key_3, _3] = it->next();
        ASSERT_EQ(key_3.length(), k.size());
        key_3.remove_suffix(suffix.size());
        key_3.remove_prefix(prefix.size());
        versionstamp_3 = key_3;
    }

    ASSERT_LT(versionstamp_2, versionstamp_3);

    {
        // write key, but offset is invalid
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        std::string prefix = "key_4_";
        std::string k(prefix);
        k.append(10, '\0');             // reserve 10 bytes for versionstamp
        uint32_t offset = k.size() + 1; // invalid offset
        ASSERT_FALSE(txn->atomic_set_ver_key(k, offset, "4"));

        k = "fake";
        ASSERT_FALSE(txn->atomic_set_ver_key(k, 0, "4"));
    }
}

TEST(TxnMemKvTest, AtomicSetVerKeyTest) {
    using namespace doris::cloud;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    atomic_set_ver_key_test(mem_txn_kv);
    atomic_set_ver_key_test(fdb_txn_kv);
}

static void atomic_add_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::unique_ptr<Transaction> txn;
    std::string txn_kv_class = dynamic_cast<MemTxnKv*>(txn_kv.get()) != nullptr ? " memkv" : " fdb";
    // clear counter
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->remove("counter");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    // add to uninitialized kv
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add("counter", 123);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    std::string val;
    ASSERT_EQ(txn->get("counter", &val), TxnErrorCode::TXN_OK);
    int64_t val_int = *reinterpret_cast<const int64_t*>(val.data());
    ASSERT_EQ(val_int, 123) << txn_kv_class;

    txn->put("counter", "1");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // add
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add("counter", 10);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get("counter", &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    std::cout << "atomic add: " << val_int << std::endl;
    ASSERT_EQ(val_int, 59) << txn_kv_class; // "1" + 10 = ASCII("1") + 10 = 49 + 10 = 59

    // sub
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->atomic_add("counter", -5);
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get("counter", &val), TxnErrorCode::TXN_OK);
    val_int = *reinterpret_cast<const int64_t*>(val.data());
    std::cout << "atomic sub: " << val_int << std::endl;
    ASSERT_EQ(val_int, 54) << txn_kv_class;
}

TEST(TxnMemKvTest, AtomicAddTest) {
    using namespace doris::cloud;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    atomic_add_test(mem_txn_kv);
    atomic_add_test(fdb_txn_kv);
}

// modify identical key in one transcation
static void modify_identical_key_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::unique_ptr<Transaction> txn;
    std::string txn_kv_class = dynamic_cast<MemTxnKv*>(txn_kv.get()) != nullptr ? " memkv" : " fdb";
    // put after remove
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put("test", "1");
        txn->remove("test");
        txn->put("test", "2");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get("test", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "2") << txn_kv_class;
    }

    // remove after put
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put("test", "1");
        txn->remove("test");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn->get("test", &get_val), TxnErrorCode::TXN_KEY_NOT_FOUND) << txn_kv_class;
    }
}

TEST(TxnMemKvTest, ModifyIdenticalKeyTest) {
    using namespace doris::cloud;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    modify_identical_key_test(mem_txn_kv);
    modify_identical_key_test(fdb_txn_kv);
}

static void modify_snapshot_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::unique_ptr<Transaction> txn_1;
    std::unique_ptr<Transaction> txn_2;
    std::string txn_kv_class = dynamic_cast<MemTxnKv*>(txn_kv.get()) != nullptr ? " memkv" : " fdb";
    {
        std::string get_val;
        // txn_1: put <test, version1> and commit
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("test", "version1");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        // txn_2: get the snapshot of database, will see <test, version1>
        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("test", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "version1") << txn_kv_class;

        // txn_1: modify <test, version1> to <test, version2> and commit
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("test", "version2");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        // txn_2: should still see the <test, version1>
        ASSERT_EQ(txn_2->get("test", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "version1") << txn_kv_class;

        // txn_2: modify <test, version1> to <test, version3> but not commit,
        // txn_2 should get <test, version3>
        txn_2->put("test", "version3");
        ASSERT_EQ(txn_2->get("test", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "version3") << txn_kv_class;

        // txn_2: remove <test, version3> bu not commit,
        // txn_2 should not get <test, version3>
        txn_2->remove("test");
        ASSERT_EQ(txn_2->get("test", &get_val), TxnErrorCode::TXN_KEY_NOT_FOUND) << txn_kv_class;

        // txn_1: will still see <test, version2>
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_1->get("test", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "version2") << txn_kv_class;

        // txn_2: commit all changes, should conflict
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;

        // txn_1: should not get <test, version2>
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_1->get("test", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "version2") << txn_kv_class;
    }

    {
        std::string get_val;

        // txn_1: put <test, version1> and commit
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("test", "version1");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        // txn_2: read the key set by atomic_set_xxx before commit
        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        txn_2->atomic_set_ver_value("test", "");
        TxnErrorCode err = txn_2->get("test", &get_val);
        // can not read the unreadable key
        ASSERT_TRUE(err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND)
                << txn_kv_class;
        // after read the unreadable key, can not commit
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK);

        // txn_1: still see the <test version1>
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_1->get("test", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "version1") << txn_kv_class;
    }
}

TEST(TxnMemKvTest, ModifySnapshotTest) {
    using namespace doris::cloud;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    modify_snapshot_test(mem_txn_kv);
    modify_snapshot_test(fdb_txn_kv);
}

static void check_conflicts_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::string txn_kv_class = dynamic_cast<MemTxnKv*>(txn_kv.get()) != nullptr ? " memkv" : " fdb";
    std::unique_ptr<Transaction> txn_1;
    std::unique_ptr<Transaction> txn_2;

    // txn1 change "key" after txn2 get "key", txn2 should conflict when change "key".
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key", "txn1_1");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("key", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "txn1_1");

        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key", "txn1_2");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        txn_2->put("key", "txn2_1");
        ASSERT_EQ(txn_2->get("key", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "txn2_1");

        // conflicts
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }

    // txn1 add "key" after txn2 get "key", txn2 should conflict when add "key2".
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->remove("key");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("key", &get_val), TxnErrorCode::TXN_KEY_NOT_FOUND) << txn_kv_class;

        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key", "txn1_1");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        txn_2->put("key2", "txn2_1");

        // conflicts
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }

    // txn1 change "key" after txn2 get "key",
    // txn2 can read "key2" before commit, but commit conflict.
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key", "txn1_1");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("key", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "txn1_1");

        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key", "txn1_2");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        txn_2->put("key2", "txn2_2");
        ASSERT_EQ(txn_2->get("key2", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "txn2_2") << txn_kv_class;

        // conflicts
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }

    // txn1 change "key" after txn2 get "key", txn2 should conflict when atomic_set "key".
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key", "txn1_1");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("key", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "txn1_1");

        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key", "txn1_2");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        txn_2->atomic_set_ver_value("key", "txn2_2");

        // conflicts
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }

    // txn1 change "key1" after txn2 range get "key1~key5", txn2 should conflict when change "key2"
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key1", "v1");
        txn_1->put("key2", "v2");
        txn_1->put("key3", "v3");
        txn_1->put("key4", "v4");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        ASSERT_EQ(txn_2->get("key1", "key5", &iter), TxnErrorCode::TXN_OK);
        ASSERT_EQ(iter->size(), 4) << txn_kv_class;

        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key1", "v11");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        txn_2->put("key2", "v22");
        ASSERT_EQ(txn_2->get("key2", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "v22") << txn_kv_class;

        // conflicts
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }

    // txn1 change "key3" after txn2 limit range get "key1~key5", txn2 do not conflict when change "key4"
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key1", "v1");
        txn_1->put("key2", "v2");
        txn_1->put("key3", "v3");
        txn_1->put("key4", "v4");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        std::unique_ptr<RangeGetIterator> iter;
        ASSERT_EQ(txn_2->get("key1", "key5", &iter, false, 1), TxnErrorCode::TXN_OK);
        ASSERT_EQ(iter->size(), 1) << txn_kv_class;

        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key3", "v33");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        txn_2->put("key4", "v44");
        ASSERT_EQ(txn_2->get("key4", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "v44") << txn_kv_class;

        // not conflicts
        ASSERT_EQ(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }

    // txn1 remove "key1" after txn2 get "key1", txn2 should conflict when change "key5".
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key1", "v1");
        txn_1->put("key2", "v2");
        txn_1->put("key3", "v3");
        txn_1->put("key4", "v4");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("key1", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "v1") << txn_kv_class;

        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->remove("key1");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_2->get("key1", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "v1") << txn_kv_class;

        txn_2->put("key5", "v5");
        ASSERT_EQ(txn_2->get("key5", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "v5") << txn_kv_class;

        // conflicts
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }

    // txn1 range remove "key1~key4" after txn2 get "key1", txn2 should conflict when change "key1".
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key1", "v1");
        txn_1->put("key2", "v2");
        txn_1->put("key3", "v3");
        txn_1->put("key4", "v4");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("key1", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "v1");

        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->remove("key1", "key4");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_2->get("key1", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "v1");
        txn_2->put("key1", "v11");
        // conflicts
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }
}

TEST(TxnMemKvTest, CheckConflictsTest) {
    using namespace doris::cloud;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    check_conflicts_test(mem_txn_kv);
    check_conflicts_test(fdb_txn_kv);
}

// ConflictTest of txn_kv_test.cpp
TEST(TxnMemKvTest, ConflictTest) {
    using namespace doris::cloud;
    auto txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(txn_kv.get(), nullptr);
    std::unique_ptr<Transaction> txn, txn1, txn2;
    std::string key = "unit_test";
    std::string val, val1, val2;

    // Historical data
    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    txn->put("unit_test", "xxxxxxxxxxxxx");
    ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

    // txn1 begin
    ASSERT_EQ(txn_kv->create_txn(&txn1), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn1->get(key, &val), TxnErrorCode::TXN_OK);
    std::cout << "val1=" << val1 << std::endl;

    // txn2 begin
    ASSERT_EQ(txn_kv->create_txn(&txn2), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn2->get(key, &val), TxnErrorCode::TXN_OK);
    std::cout << "val2=" << val2 << std::endl;

    // txn2 commit
    val2 = "zzzzzzzzzzzzzzz";
    txn2->put(key, val2);
    ASSERT_EQ(txn2->commit(), TxnErrorCode::TXN_OK);

    // txn1 commit, intend to fail
    val1 = "yyyyyyyyyyyyyyy";
    txn1->put(key, val1);
    ASSERT_NE(txn1->commit(), TxnErrorCode::TXN_OK);

    ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
    ASSERT_EQ(txn->get(key, &val), TxnErrorCode::TXN_OK);
    ASSERT_EQ(val, val2); // First wins
    std::cout << "final val=" << val << std::endl;
}

static void txn_behavior_test(std::shared_ptr<cloud::TxnKv> txn_kv) {
    using namespace doris::cloud;
    std::string txn_kv_class = dynamic_cast<MemTxnKv*>(txn_kv.get()) != nullptr ? " memkv" : " fdb";
    std::unique_ptr<Transaction> txn_1;
    std::unique_ptr<Transaction> txn_2;
    // av: atomic_set_ver_value
    // ak: atomic_set_ver_key
    // ad: atomic_add
    // c : commit

    // txn_1: --- put<key, v1> -- av<key, v1> --------------- c
    // txn_2: ------------------------------- ad<key, v1> ----- put<key, v2> --- c
    // result: <key v2>
    {
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key", "v1");
        txn_1->atomic_set_ver_value("key", "v1");

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        txn_2->atomic_add("key", 1);

        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        txn_2->put("key", "v2");
        ASSERT_EQ(txn_2->commit(), TxnErrorCode::TXN_OK);

        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("key", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "v2");
    }

    // txn_1: --- ad<"key",1> --- av<"key", "v1"> ------ c
    // txn_2: ------------------- av<"key", "v2"> ---- c
    // result: <"key", "version"+"v1">
    {
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->atomic_add("key", 1);
        txn_1->atomic_set_ver_value("key", "v1");

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        txn_2->atomic_set_ver_value("key", "v2");

        ASSERT_EQ(txn_2->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("key", &get_val), TxnErrorCode::TXN_OK);
        std::cout << get_val << std::endl;
    }

    // txn_1: --- put<"key", "1"> --- get<"key"> --- c
    // result: can get "1" and commit success
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->put("key", "1");
        ASSERT_EQ(txn_1->get("key", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(get_val, "1") << txn_kv_class;
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);
    }

    // txn_1: --- ad<"key",1> --- get<"key"> --- c
    // result: commit success
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->atomic_add("key", 1);
        ASSERT_EQ(txn_1->get("key", &get_val), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }

    // txn_1: --- av<"key", "1"> --- get<"key"> --- c
    // result: can not read the unreadable key and commit error
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->atomic_set_ver_value("key", "1");
        TxnErrorCode err = txn_1->get("key", &get_val);
        // can not read the unreadable key
        ASSERT_TRUE(err != TxnErrorCode::TXN_OK && err != TxnErrorCode::TXN_KEY_NOT_FOUND)
                << txn_kv_class;
        ASSERT_NE(txn_1->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }

    // txn_1: --- get<"keyNotExit"> --- put<"keyNotExit", "1"> --- get<"keyNotExit"> --- c
    // txn_2: --- get<"keyNotExit"> --- put<"keyNotExit", "1"> --- get<"keyNotExit"> ------ c
    // result: txn_2 commit conflict
    {
        std::string get_val;
        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        txn_1->remove("keyNotExit");
        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK);

        ASSERT_EQ(txn_kv->create_txn(&txn_1), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_1->get("keyNotExit", &get_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
        txn_1->put("keyNotExit", "1");
        ASSERT_EQ(txn_1->get("keyNotExit", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "1");

        ASSERT_EQ(txn_kv->create_txn(&txn_2), TxnErrorCode::TXN_OK);
        ASSERT_EQ(txn_2->get("keyNotExit", &get_val), TxnErrorCode::TXN_KEY_NOT_FOUND);
        txn_2->put("keyNotExit", "1");
        ASSERT_EQ(txn_2->get("keyNotExit", &get_val), TxnErrorCode::TXN_OK);
        ASSERT_EQ(get_val, "1");

        ASSERT_EQ(txn_1->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
        ASSERT_NE(txn_2->commit(), TxnErrorCode::TXN_OK) << txn_kv_class;
    }
}

TEST(TxnMemKvTest, TxnBehaviorTest) {
    using namespace doris::cloud;
    auto mem_txn_kv = std::dynamic_pointer_cast<TxnKv>(std::make_shared<MemTxnKv>());
    ASSERT_NE(mem_txn_kv.get(), nullptr);

    txn_behavior_test(mem_txn_kv);
    txn_behavior_test(fdb_txn_kv);
}

TEST(TxnMemKvTest, MaybeUnusedFunctionTest) {
    using namespace doris::cloud;
    auto mem_txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(mem_txn_kv->init(), 0);

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(mem_txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->put("key", "v1");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
        ASSERT_EQ(mem_txn_kv->get_last_commited_version(), 1);
        ASSERT_EQ(mem_txn_kv->get_last_read_version(), 1);
    }

    {
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(mem_txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        auto t = dynamic_cast<memkv::Transaction*>(txn.get());
        ASSERT_EQ(t->init(), 0);
        ASSERT_EQ(t->abort(), TxnErrorCode::TXN_OK);
    }

    {
        auto new_mem_txn_kv = std::make_shared<MemTxnKv>();
        std::unique_ptr<Transaction> txn;
        ASSERT_EQ(new_mem_txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        txn->atomic_set_ver_key("", "v2");
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);

        std::unique_ptr<Transaction> txn2;
        ASSERT_EQ(new_mem_txn_kv->create_txn(&txn2), TxnErrorCode::TXN_OK);
        for (auto& t : new_mem_txn_kv->mem_kv_) {
            int64_t txn_id;
            ASSERT_EQ(get_txn_id_from_fdb_ts(t.first, &txn_id), 0);
            auto ver = txn_id >> 10;
            std::cout << "version: " << ver << std::endl;
            ASSERT_EQ(ver, 1);
        }
    }
}

TEST(TxnMemKvTest, RangeGetKeySelector) {
    using namespace doris::cloud;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    constexpr std::string_view prefix = "range_get_key_selector_";

    {
        // Remove the existing keys and insert some new keys.
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::string last_key = fmt::format("{}{}", prefix, 9);
        encode_int64(INT64_MAX, &last_key);
        txn->remove(prefix, last_key);
        for (int i = 0; i < 5; ++i) {
            std::string key = fmt::format("{}{}", prefix, i);
            txn->put(key, std::to_string(i));
        }
        err = txn->commit();
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    struct TestCase {
        RangeKeySelector begin_key_selector, end_key_selector;
        std::vector<std::string> expected_keys;
    };

    std::string range_begin = fmt::format("{}{}", prefix, 1);
    std::string range_end = fmt::format("{}{}", prefix, 3);
    std::vector<TestCase> test_case {
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 1), fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                            fmt::format("{}{}", prefix, 3),
                    },
            },
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 1), fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::LAST_LESS_THAN,
                    {fmt::format("{}{}", prefix, 1)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {fmt::format("{}{}", prefix, 2), fmt::format("{}{}", prefix, 3)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::LAST_LESS_THAN,
                    {},
            },
            {
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 1), fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                            fmt::format("{}{}", prefix, 3),
                    },
            },
            {
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 1), fmt::format("{}{}", prefix, 2)},
            },
            {
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    RangeKeySelector::LAST_LESS_THAN,
                    {fmt::format("{}{}", prefix, 1)},
            },
            {
                    RangeKeySelector::LAST_LESS_THAN,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {
                            fmt::format("{}{}", prefix, 0),
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                    },
            },
            {
                    RangeKeySelector::LAST_LESS_THAN,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {
                            fmt::format("{}{}", prefix, 0),
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                            fmt::format("{}{}", prefix, 3),
                    },
            },
            {
                    RangeKeySelector::LAST_LESS_THAN,
                    RangeKeySelector::LAST_LESS_OR_EQUAL,
                    {
                            fmt::format("{}{}", prefix, 0),
                            fmt::format("{}{}", prefix, 1),
                            fmt::format("{}{}", prefix, 2),
                    },
            },
            {
                    RangeKeySelector::LAST_LESS_THAN,
                    RangeKeySelector::LAST_LESS_THAN,
                    {fmt::format("{}{}", prefix, 0), fmt::format("{}{}", prefix, 1)},
            },
    };

    // Scan range with different key selectors
    for (const auto& tc : test_case) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RangeGetOptions options;
        options.batch_limit = 1000;
        options.begin_key_selector = tc.begin_key_selector;
        options.end_key_selector = tc.end_key_selector;
        std::unique_ptr<RangeGetIterator> it;
        err = txn->get(range_begin, range_end, &it, options);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::vector<std::string> actual_keys;
        while (it->has_next()) {
            auto [k, v] = it->next();
            actual_keys.emplace_back(k);
        }
        EXPECT_EQ(actual_keys, tc.expected_keys)
                << "Failed for begin_key_selector=" << static_cast<int>(tc.begin_key_selector)
                << ", end_key_selector=" << static_cast<int>(tc.end_key_selector);
    }
}

TEST(TxnMemKvTest, ReverseRangeGet) {
    using namespace doris::cloud;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    constexpr std::string_view prefix = "reverse_range_get_";

    {
        // Remove the existing keys and insert some new keys.
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::string last_key = fmt::format("{}{}", prefix, 9);
        encode_int64(INT64_MAX, &last_key);
        txn->remove(prefix, last_key);
        for (int i = 0; i < 5; ++i) {
            std::string key = fmt::format("{}{}", prefix, i);
            txn->put(key, std::to_string(i));
        }
        err = txn->commit();
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    std::string range_begin = fmt::format("{}{}", prefix, 1);
    std::string range_end = fmt::format("{}{}", prefix, 3);

    struct TestCase {
        RangeKeySelector begin_key_selector, end_key_selector;
        std::vector<std::string> expected_keys;
    };

    std::vector<TestCase> test_case {
            // 1. [begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 2), fmt::format("{}{}", prefix, 1)},
            },
            // 2. [begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {
                            fmt::format("{}{}", prefix, 3),
                            fmt::format("{}{}", prefix, 2),
                            fmt::format("{}{}", prefix, 1),
                    },
            },
            // 3. (begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    {fmt::format("{}{}", prefix, 2)},
            },
            // 4. (begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_THAN,
                    {fmt::format("{}{}", prefix, 3), fmt::format("{}{}", prefix, 2)},
            },
    };
    for (const auto& tc : test_case) {
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        RangeGetOptions options;
        options.batch_limit = 1000;
        options.begin_key_selector = tc.begin_key_selector;
        options.end_key_selector = tc.end_key_selector;
        options.reverse = true; // Reserve range get
        std::unique_ptr<RangeGetIterator> it;
        err = txn->get(range_begin, range_end, &it, options);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::vector<std::string> actual_keys;
        while (it->has_next()) {
            auto [k, v] = it->next();
            actual_keys.emplace_back(k);
        }
        EXPECT_EQ(actual_keys, tc.expected_keys)
                << "Failed for begin_key_selector=" << static_cast<int>(tc.begin_key_selector)
                << ", end_key_selector=" << static_cast<int>(tc.end_key_selector);
    }
}

TEST(TxnMemKvTest, ReverseFullRangeGet) {
    using namespace doris::cloud;
    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_EQ(txn_kv->init(), 0);

    constexpr std::string_view prefix = "reverse_full_range_get_";

    {
        // Remove the existing keys and insert some new keys.
        std::unique_ptr<Transaction> txn;
        TxnErrorCode err = txn_kv->create_txn(&txn);
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);

        std::string last_key = fmt::format("{}{:03}", prefix, 99);
        encode_int64(INT64_MAX, &last_key);
        txn->remove(prefix, last_key);
        for (int i = 0; i < 100; ++i) {
            std::string key = fmt::format("{}{:03}", prefix, i);
            txn->put(key, std::to_string(i));
        }
        err = txn->commit();
        ASSERT_EQ(err, TxnErrorCode::TXN_OK);
    }

    std::string range_begin = fmt::format("{}{:03}", prefix, 1);
    std::string range_end = fmt::format("{}{:03}", prefix, 98);

    struct TestCase {
        RangeKeySelector begin_key_selector, end_key_selector;
    };

    std::vector<TestCase> test_case {
            // 1. [begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
            },
            // 2. [begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
                    RangeKeySelector::FIRST_GREATER_THAN,
            },
            // 3. (begin, end)
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_OR_EQUAL,
            },
            // 4. (begin, end]
            {
                    RangeKeySelector::FIRST_GREATER_THAN,
                    RangeKeySelector::FIRST_GREATER_THAN,
            },
    };

    for (const auto& tc : test_case) {
        std::vector<std::string> expected_keys;
        {
            // Read the expected keys via range_get
            std::unique_ptr<Transaction> txn;
            TxnErrorCode err = txn_kv->create_txn(&txn);
            ASSERT_EQ(err, TxnErrorCode::TXN_OK);

            RangeGetOptions options;
            options.batch_limit = 11;
            options.begin_key_selector = tc.begin_key_selector;
            options.end_key_selector = tc.end_key_selector;
            options.reverse = true; // Reserve range get
            std::string begin = range_begin, end = range_end;

            std::unique_ptr<RangeGetIterator> it;
            do {
                err = txn->get(begin, end, &it, options);
                ASSERT_EQ(err, TxnErrorCode::TXN_OK);

                while (it->has_next()) {
                    auto [k, v] = it->next();
                    expected_keys.emplace_back(k);
                }
                // Get next begin key for reverse range get
                end = it->last_key();
                options.end_key_selector = RangeKeySelector::FIRST_GREATER_OR_EQUAL;
            } while (it->more());
        }

        std::vector<std::string> actual_keys;
        {
            // Read the actual keys via full_range_get
            FullRangeGetOptions opts(txn_kv);
            opts.batch_limit = 11;
            opts.begin_key_selector = tc.begin_key_selector;
            opts.end_key_selector = tc.end_key_selector;
            opts.reverse = true; // Reserve full range get

            auto it = txn_kv->full_range_get(range_begin, range_end, opts);
            ASSERT_TRUE(it->is_valid());

            while (it->has_next()) {
                auto kvp = it->next();
                ASSERT_TRUE(kvp.has_value());
                auto [k, v] = *kvp;
                actual_keys.emplace_back(k);
            }
        }

        EXPECT_EQ(actual_keys, expected_keys)
                << "Failed for begin_key_selector=" << static_cast<int>(tc.begin_key_selector)
                << ", end_key_selector=" << static_cast<int>(tc.end_key_selector);
    }
}

TEST(TxnMemKvTest, BatchScan) {
    using namespace doris::cloud;

    auto txn_kv = std::make_shared<MemTxnKv>();
    ASSERT_NE(txn_kv.get(), nullptr);

    std::vector<std::pair<std::string, std::string>> test_data = {
            {"different_key", "different_value"},
            {"prefix1", "value1"},
            {"prefix1_sub1", "sub_value1"},
            {"prefix1_sub2", "sub_value2"},
            {"prefix2", "value2"},
            {"prefix2_sub1", "sub_value3"},
            {"prefix3", "value3"},
    };

    std::unique_ptr<Transaction> txn;
    {
        ASSERT_EQ(txn_kv->create_txn(&txn), TxnErrorCode::TXN_OK);
        for (const auto& [key, val] : test_data) {
            txn->put(key, val);
        }
        ASSERT_EQ(txn->commit(), TxnErrorCode::TXN_OK);
    }

    struct TestCase {
        bool reverse;
        std::vector<std::string> scan_keys;
        std::vector<std::optional<std::string>> expected_keys;
    };

    std::vector<TestCase> test_cases = {
            {false,
             {"prefix1", "prefix2", "prefix3", "different_key"},
             {"prefix1", "prefix2", "prefix3", "different_key"}},
            {false, {"prefix1_", "prefix2_"}, {"prefix1_sub1", "prefix2_sub1"}},
            {false, {"prefix5_"}, {std::nullopt}},
            {true, {"a"}, {std::nullopt}},
            {true, {"prefix1_", "prefix2_"}, {"prefix1", "prefix2"}},
            {true,
             {"prefix1", "prefix2", "prefix3", "different_key"},
             {"prefix1", "prefix2", "prefix3", "different_key"}},
    };

    size_t count = 0;
    for (auto& tc : test_cases) {
        auto ret = txn_kv->create_txn(&txn);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        std::vector<std::optional<std::pair<std::string, std::string>>> results;
        Transaction::BatchGetOptions opts;
        opts.reverse = tc.reverse; // Reverse order
        opts.snapshot = false;

        ret = txn->batch_scan(&results, tc.scan_keys, opts);
        ASSERT_EQ(ret, TxnErrorCode::TXN_OK);
        ASSERT_EQ(results.size(), tc.scan_keys.size());

        for (size_t i = 0; i < results.size(); ++i) {
            if (tc.expected_keys[i].has_value()) {
                ASSERT_TRUE(results[i].has_value())
                        << "count: " << count << ", expected key at index " << i
                        << " for scan key: " << tc.scan_keys[i]
                        << ", expected: " << tc.expected_keys[i].value() << ", got: empty";
                std::string& key = results[i].value().first;
                std::string& expected_key = tc.expected_keys[i].value();
                ASSERT_EQ(key, expected_key) << "count: " << count << ", mismatch at index " << i
                                             << " for scan key: " << tc.scan_keys[i]
                                             << ", expected: " << expected_key << ", got: " << key;
            } else {
                ASSERT_FALSE(results[i].has_value());
            }
        }
        count += 1;
    }
}
