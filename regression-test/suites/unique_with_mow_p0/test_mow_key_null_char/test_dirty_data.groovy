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

// OPENSOURCE-272 Case 2: Dirty data (\0 keys) should not affect dedup of normal data.
// Insert normal key in multiple rowsets while \0 dirty data exists - should still dedup correctly.

suite("test_dirty_data") {

    def mowTable = "test_dirty_data_mow"
    def morTable = "test_dirty_data_mor"

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

    // First rowset: insert a normal key
    sql """ INSERT INTO ${mowTable} VALUES (2, 'hello', 'world', 'test', 300) """
    sql """ INSERT INTO ${morTable} VALUES (2, 'hello', 'world', 'test', 300) """

    // Second rowset: insert a key with trailing \0 (dirty data, different key)
    sql """ INSERT INTO ${mowTable} VALUES (2, concat('hello', char(0)), 'world', 'test', 301) """
    sql """ INSERT INTO ${morTable} VALUES (2, concat('hello', char(0)), 'world', 'test', 301) """

    // Third rowset: insert the same normal key again (should update, not create duplicate)
    sql """ INSERT INTO ${mowTable} VALUES (2, 'hello', 'world', 'test', 400) """
    sql """ INSERT INTO ${morTable} VALUES (2, 'hello', 'world', 'test', 400) """

    // Should have 2 rows: one for 'hello' (updated to 400) and one for 'hello\0' (301)
    def mow_count = sql """ SELECT count(*) FROM ${mowTable} """
    def mor_count = sql """ SELECT count(*) FROM ${morTable} """
    logger.info("MOW count: ${mow_count[0][0]}, MOR count: ${mor_count[0][0]}")
    assertEquals(2, mow_count[0][0], "MOW should have 2 rows: normal key dedup should work even with dirty data present")
    assertEquals(2, mor_count[0][0], "MOR should have 2 rows")

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
    assertEquals(0, mow_dup.size(), "MOW should have no duplicate keys even with dirty \\0 data present")
    assertEquals(0, mor_dup.size(), "MOR should have no duplicate keys")

    // Verify the normal key was updated correctly
    def mow_val = sql """ SELECT `value` FROM ${mowTable} WHERE `id` = 2 AND `key1` = 'hello' """
    def mor_val = sql """ SELECT `value` FROM ${morTable} WHERE `id` = 2 AND `key1` = 'hello' """
    assertEquals(1, mow_val.size(), "MOW should have exactly 1 row for normal key 'hello'")
    assertEquals(1, mor_val.size(), "MOR should have exactly 1 row for normal key 'hello'")
    assertEquals(400, mow_val[0][0], "MOW normal key value should be updated to 400")
    assertEquals(400, mor_val[0][0], "MOR normal key value should be updated to 400")

    sql """ DROP TABLE IF EXISTS ${mowTable} """
    sql """ DROP TABLE IF EXISTS ${morTable} """
}
