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

// OPENSOURCE-272 Case 3: Inserting the same \0-key multiple times across rowsets should dedup correctly

suite("test_same_key_dedup") {

    def mowTable = "test_same_key_dedup_mow"
    def morTable = "test_same_key_dedup_mor"

    sql """ DROP TABLE IF EXISTS ${mowTable} """
    sql """ DROP TABLE IF EXISTS ${morTable} """

    sql """
        CREATE TABLE ${mowTable} (
            `id` INT NOT NULL,
            `key1` VARCHAR(50) NOT NULL,
            `key2` VARCHAR(50) NOT NULL,
            `key3` VARCHAR(255) NOT NULL,
            `value` INT NOT NULL
        )
        UNIQUE KEY(`id`, `key1`, `key2`, `key3`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "true"
        );
    """

    sql """
        CREATE TABLE ${morTable} (
            `id` INT NOT NULL,
            `key1` VARCHAR(50) NOT NULL,
            `key2` VARCHAR(50) NOT NULL,
            `key3` VARCHAR(255) NOT NULL,
            `value` INT NOT NULL
        )
        UNIQUE KEY(`id`, `key1`, `key2`, `key3`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 1
        PROPERTIES (
            "replication_num" = "1",
            "disable_auto_compaction" = "true",
            "enable_unique_key_merge_on_write" = "false"
        );
    """

    // First rowset: insert a \0-key
    sql """ INSERT INTO ${mowTable} VALUES (3, concat('foo', char(0)), 'bar', 'baz', 500) """
    sql """ INSERT INTO ${morTable} VALUES (3, concat('foo', char(0)), 'bar', 'baz', 500) """

    // Second rowset: insert the same \0-key again - should update
    sql """ INSERT INTO ${mowTable} VALUES (3, concat('foo', char(0)), 'bar', 'baz', 600) """
    sql """ INSERT INTO ${morTable} VALUES (3, concat('foo', char(0)), 'bar', 'baz', 600) """

    // Should have 1 row after dedup
    def mow_count = sql """ SELECT count(*) FROM ${mowTable} """
    def mor_count = sql """ SELECT count(*) FROM ${morTable} """
    logger.info("MOW count: ${mow_count[0][0]}, MOR count: ${mor_count[0][0]}")
    assertEquals(1, mow_count[0][0], "MOW should have 1 row after dedup of same \\0-key")
    assertEquals(1, mor_count[0][0], "MOR should have 1 row after dedup of same \\0-key")

    // No duplicate keys
    def mow_dup = sql """ SELECT `id`, `key1`, `key2`, `key3`, count(*) cnt
                          FROM ${mowTable}
                          GROUP BY `id`, `key1`, `key2`, `key3`
                          HAVING cnt > 1 """
    def mor_dup = sql """ SELECT `id`, `key1`, `key2`, `key3`, count(*) cnt
                          FROM ${morTable}
                          GROUP BY `id`, `key1`, `key2`, `key3`
                          HAVING cnt > 1 """
    logger.info("MOW duplicates: ${mow_dup}")
    logger.info("MOR duplicates: ${mor_dup}")
    assertEquals(0, mow_dup.size(), "MOW should have no duplicate \\0-keys")
    assertEquals(0, mor_dup.size(), "MOR should have no duplicate \\0-keys")

    sql """ DROP TABLE IF EXISTS ${mowTable} """
    sql """ DROP TABLE IF EXISTS ${morTable} """
}
