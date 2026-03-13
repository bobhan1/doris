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

// OPENSOURCE-272 Case 5: Multiple trailing \0 characters of different lengths.
// "foo", "foo\0", "foo\0\0", "foo\0\0\0" should all be different keys.
// Updating each key across rowsets should not produce duplicates.

suite("test_multi_length") {

    def mowTable = "test_multi_length_mow"
    def morTable = "test_multi_length_mor"

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

    // Insert 4 different keys with different \0 lengths (each in separate rowset)
    sql """ INSERT INTO ${mowTable} VALUES (6, 'foo', 'bar', 'baz', 100) """
    sql """ INSERT INTO ${mowTable} VALUES (6, concat('foo', char(0)), 'bar', 'baz', 200) """
    sql """ INSERT INTO ${mowTable} VALUES (6, concat('foo', char(0), char(0)), 'bar', 'baz', 300) """
    sql """ INSERT INTO ${mowTable} VALUES (6, concat('foo', char(0), char(0), char(0)), 'bar', 'baz', 400) """

    sql """ INSERT INTO ${morTable} VALUES (6, 'foo', 'bar', 'baz', 100) """
    sql """ INSERT INTO ${morTable} VALUES (6, concat('foo', char(0)), 'bar', 'baz', 200) """
    sql """ INSERT INTO ${morTable} VALUES (6, concat('foo', char(0), char(0)), 'bar', 'baz', 300) """
    sql """ INSERT INTO ${morTable} VALUES (6, concat('foo', char(0), char(0), char(0)), 'bar', 'baz', 400) """

    // Should have 4 rows: different \0 lengths are different keys
    def mow_count = sql """ SELECT count(*) FROM ${mowTable} """
    def mor_count = sql """ SELECT count(*) FROM ${morTable} """
    logger.info("MOW count: ${mow_count[0][0]}, MOR count: ${mor_count[0][0]}")
    assertEquals(4, mow_count[0][0], "MOW should have 4 rows: different \\0 lengths are different keys")
    assertEquals(4, mor_count[0][0], "MOR should have 4 rows: different \\0 lengths are different keys")

    def mow_dup = sql """ SELECT `id`, `key1`, `key2`, `key3`, count(*) cnt
                          FROM ${mowTable}
                          GROUP BY `id`, `key1`, `key2`, `key3`
                          HAVING cnt > 1 """
    def mor_dup = sql """ SELECT `id`, `key1`, `key2`, `key3`, count(*) cnt
                          FROM ${morTable}
                          GROUP BY `id`, `key1`, `key2`, `key3`
                          HAVING cnt > 1 """
    logger.info("MOW duplicates: ${mow_dup}")
    assertEquals(0, mow_dup.size(), "MOW should have no duplicate keys with different \\0 lengths")
    assertEquals(0, mor_dup.size(), "MOR should have no duplicate keys with different \\0 lengths")

    // Now update each key in new rowsets - should not produce duplicates
    sql """ INSERT INTO ${mowTable} VALUES (6, 'foo', 'bar', 'baz', 101) """
    sql """ INSERT INTO ${mowTable} VALUES (6, concat('foo', char(0)), 'bar', 'baz', 201) """
    sql """ INSERT INTO ${mowTable} VALUES (6, concat('foo', char(0), char(0)), 'bar', 'baz', 301) """
    sql """ INSERT INTO ${mowTable} VALUES (6, concat('foo', char(0), char(0), char(0)), 'bar', 'baz', 401) """

    mow_count = sql """ SELECT count(*) FROM ${mowTable} """
    logger.info("MOW after update count: ${mow_count[0][0]}")
    assertEquals(4, mow_count[0][0], "MOW should still have 4 rows after updating all keys")

    mow_dup = sql """ SELECT `id`, `key1`, `key2`, `key3`, count(*) cnt
                      FROM ${mowTable}
                      GROUP BY `id`, `key1`, `key2`, `key3`
                      HAVING cnt > 1 """
    logger.info("MOW after update duplicates: ${mow_dup}")
    assertEquals(0, mow_dup.size(), "MOW should have no duplicate keys after updating all \\0 variants")

    sql """ DROP TABLE IF EXISTS ${mowTable} """
    sql """ DROP TABLE IF EXISTS ${morTable} """
}
