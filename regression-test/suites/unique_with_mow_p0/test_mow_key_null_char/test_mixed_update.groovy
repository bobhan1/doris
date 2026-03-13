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

// OPENSOURCE-272 Case 4: Mixed scenario - multiple normal keys and \0 keys coexist,
// updates to both types should work correctly without producing duplicates.

suite("test_mixed_update") {

    def mowTable = "test_mixed_update_mow"
    def morTable = "test_mixed_update_mor"

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

    // First rowset: 3 normal keys
    sql """ INSERT INTO ${mowTable} VALUES
            (4, 'aaa', 'bbb', 'ccc', 1000),
            (4, 'aaa', 'bbb', 'ddd', 1001),
            (5, 'aaa', 'bbb', 'ccc', 1002)
        """
    sql """ INSERT INTO ${morTable} VALUES
            (4, 'aaa', 'bbb', 'ccc', 1000),
            (4, 'aaa', 'bbb', 'ddd', 1001),
            (5, 'aaa', 'bbb', 'ccc', 1002)
        """

    // Second rowset: add \0 dirty data (different keys)
    sql """ INSERT INTO ${mowTable} VALUES
            (4, concat('aaa', char(0)), 'bbb', 'ccc', 1003),
            (5, 'aaa', concat('bbb', char(0)), 'ccc', 1004)
        """
    sql """ INSERT INTO ${morTable} VALUES
            (4, concat('aaa', char(0)), 'bbb', 'ccc', 1003),
            (5, 'aaa', concat('bbb', char(0)), 'ccc', 1004)
        """

    // Should have 5 rows: 3 original normal + 2 new \0-key rows
    def mow_count = sql """ SELECT count(*) FROM ${mowTable} """
    def mor_count = sql """ SELECT count(*) FROM ${morTable} """
    logger.info("MOW count: ${mow_count[0][0]}, MOR count: ${mor_count[0][0]}")
    assertEquals(5, mow_count[0][0], "MOW should have 5 rows: 3 normal + 2 with \\0")
    assertEquals(5, mor_count[0][0], "MOR should have 5 rows: 3 normal + 2 with \\0")

    // Third rowset: update a normal key - should not create duplicates
    sql """ INSERT INTO ${mowTable} VALUES (4, 'aaa', 'bbb', 'ccc', 2000) """
    sql """ INSERT INTO ${morTable} VALUES (4, 'aaa', 'bbb', 'ccc', 2000) """

    mow_count = sql """ SELECT count(*) FROM ${mowTable} """
    mor_count = sql """ SELECT count(*) FROM ${morTable} """
    logger.info("MOW after update count: ${mow_count[0][0]}, MOR: ${mor_count[0][0]}")
    assertEquals(5, mow_count[0][0], "MOW should still have 5 rows after updating normal key")
    assertEquals(5, mor_count[0][0], "MOR should still have 5 rows after updating normal key")

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
    assertEquals(0, mow_dup.size(), "MOW should have no duplicate keys in mixed scenario")
    assertEquals(0, mor_dup.size(), "MOR should have no duplicate keys in mixed scenario")

    sql """ DROP TABLE IF EXISTS ${mowTable} """
    sql """ DROP TABLE IF EXISTS ${morTable} """
}
