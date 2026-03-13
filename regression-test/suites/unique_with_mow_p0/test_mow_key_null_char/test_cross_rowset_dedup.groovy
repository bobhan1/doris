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

// OPENSOURCE-272 Case 7: Reproduces the exact scenario from user report.
// Old rowset has dirty key (varchar padded with \0 to full length),
// new rowset has both normal key and dirty key in the same batch.
// The dirty key across rowsets should be properly deduped.

suite("test_cross_rowset_dedup") {

    def mowTable = "test_cross_rowset_dedup_mow"
    def morTable = "test_cross_rowset_dedup_mor"

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

    // Build a dirty key: 'ATE' padded with \0 to length 50 (like the user report)
    def dirtyKeyExpr = "concat('ATE'" + ", char(0)" * 47 + ")"

    // Rowset 1 (old version): dirty key only
    sql """ INSERT INTO ${mowTable} VALUES (1, ${dirtyKeyExpr}, 'Normal', 'D26100373800', 16) """
    sql """ INSERT INTO ${morTable} VALUES (1, ${dirtyKeyExpr}, 'Normal', 'D26100373800', 16) """

    // Rowset 2 (new version): normal key + dirty key in same batch
    sql """ INSERT INTO ${mowTable} VALUES
            (1, 'ATE', 'Normal', 'D26100373800', 18),
            (1, ${dirtyKeyExpr}, 'Normal', 'D26100373800', 18)
        """
    sql """ INSERT INTO ${morTable} VALUES
            (1, 'ATE', 'Normal', 'D26100373800', 18),
            (1, ${dirtyKeyExpr}, 'Normal', 'D26100373800', 18)
        """

    // Expected: 2 rows (normal 'ATE' + dirty 'ATE\0...\0'), dirty key deduped across rowsets
    def mow_count = sql """ SELECT count(*) FROM ${mowTable} """
    def mor_count = sql """ SELECT count(*) FROM ${morTable} """
    logger.info("MOW count: ${mow_count[0][0]}, MOR count: ${mor_count[0][0]}")
    assertEquals(2, mow_count[0][0], "MOW should have 2 rows: dirty key from old rowset should be deduped")
    assertEquals(2, mor_count[0][0], "MOR should have 2 rows")

    // Verify with hex to inspect actual key content (like the user report)
    def mow_detail = sql """ SELECT `key1`, length(`key1`), hex(`key1`), `value`
                             FROM ${mowTable} ORDER BY length(`key1`) """
    logger.info("MOW detail: ${mow_detail}")
    assertEquals(2, mow_detail.size(), "MOW should have exactly 2 rows")
    // Row 1: normal key
    assertEquals(3, mow_detail[0][1], "First row key1 length should be 3")
    assertEquals("415445", mow_detail[0][2], "First row hex should be 415445 (ATE)")
    assertEquals(18, mow_detail[0][3], "First row value should be 18 (latest)")
    // Row 2: dirty key (latest version)
    assertEquals(50, mow_detail[1][1], "Second row key1 length should be 50")
    assertEquals(18, mow_detail[1][3], "Second row value should be 18 (latest)")

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
    assertEquals(0, mow_dup.size(), "MOW should have no duplicate keys")
    assertEquals(0, mor_dup.size(), "MOR should have no duplicate keys")

    sql """ DROP TABLE IF EXISTS ${mowTable} """
    sql """ DROP TABLE IF EXISTS ${morTable} """
}
