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

// OPENSOURCE-272 Case 10: Multi-segment version of case 8 (reverse).
// Old rowset has normal key, new rowset has normal key + dirty key in DIFFERENT segments.
// Uses debug point to force multi-segment write. Padding rows are added between
// the normal key and dirty key to ensure they land in separate segments.

suite("test_cross_rowset_dedup_rev_mseg", "nonConcurrent") {

    GetDebugPoint().clearDebugPointsForAllBEs()

    def mowTable = "test_cross_rs_rev_mseg_mow"
    def morTable = "test_cross_rs_rev_mseg_mor"

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

    // Rowset 1 (old version): normal key only, via SQL INSERT
    sql """ INSERT INTO ${mowTable} VALUES (1, 'ATE', 'Normal', 'D26100373800', 16) """
    sql """ INSERT INTO ${morTable} VALUES (1, 'ATE', 'Normal', 'D26100373800', 16) """
    sql "sync"

    // Build stream load content:
    //   - row 1: normal key (ATE, len=3)
    //   - rows 2..4097: padding rows with distinct keys to fill segments
    //   - row 4098: dirty key (ATE + 47 \0, len=50)
    // This ensures the normal key and dirty key end up in different segments.
    def paddingRows = 4096
    def totalRows = paddingRows + 2
    def baos = new ByteArrayOutputStream()
    // Normal key row
    baos.write("1,ATE,Normal,D26100373800,18\n".getBytes())
    // Padding rows (unique keys: id=100+i, key1=PAD_{i})
    for (int i = 1; i <= paddingRows; i++) {
        baos.write("${i + 100},PAD_${i},Normal,D26100373800,0\n".getBytes())
    }
    // Dirty key row
    baos.write("1,".getBytes())
    baos.write("ATE".getBytes())
    baos.write(new byte[47]) // 47 \0 bytes
    baos.write(",Normal,D26100373800,18\n".getBytes())
    def contentBytes = baos.toByteArray()

    // Helper to check segment count of the last rowset
    def checkLastRowsetSegNum = { tableName, expectedSegNum ->
        def tablets = sql_return_maparray """ show tablets from ${tableName}; """
        String compactionUrl = tablets[0]["CompactionStatus"]
        def (code, out, err) = curl("GET", compactionUrl)
        assertEquals(0, code)
        def tabletJson = parseJson(out.trim())
        assert tabletJson.rowsets instanceof List
        def rowset = tabletJson.rowsets.get(tabletJson.rowsets.size() - 1)
        logger.info("last rowset: ${rowset}")
        int startIdx = rowset.indexOf("]")
        int endIdx = rowset.indexOf("DATA")
        def segNum = Integer.parseInt(rowset.substring(startIdx + 1, endIdx).trim())
        logger.info("segment count of last rowset: ${segNum}")
        assert segNum >= expectedSegNum : "Expected at least ${expectedSegNum} segments, got ${segNum}"
    }

    // Helper for stream load
    def doStreamLoad = { tableName ->
        streamLoad {
            table "${tableName}"
            set 'column_separator', ','
            inputStream new ByteArrayInputStream(contentBytes)
            time 30000
            check { result, exception, startTime, endTime ->
                if (exception != null) throw exception
                def json = parseJson(result)
                assertEquals("success", json.Status.toLowerCase())
                assertEquals(totalRows, json.NumberTotalRows)
            }
        }
    }

    // Rowset 2 (new version): multi-segment - normal key + dirty key in different segments
    def customBeConfig = [doris_scanner_row_bytes: 1]
    setBeConfigTemporary(customBeConfig) {
        try {
            GetDebugPoint().enableDebugPointForAllBEs("MemTable.need_flush")
            Thread.sleep(1000)

            doStreamLoad(mowTable)
            doStreamLoad(morTable)
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }

    Thread.sleep(2000)

    // Verify multi-segments were created (at least 2 segments in last rowset)
    checkLastRowsetSegNum(mowTable, 2)

    // Expected: totalRows rows total (normal ATE + dirty ATE\0 + 4096 padding),
    // normal key deduped across rowsets
    def mow_count = sql """ SELECT count(*) FROM ${mowTable} """
    def mor_count = sql """ SELECT count(*) FROM ${morTable} """
    logger.info("MOW count: ${mow_count[0][0]}, MOR count: ${mor_count[0][0]}")
    assertEquals(totalRows, mow_count[0][0], "MOW should have ${totalRows} rows")
    assertEquals(totalRows, mor_count[0][0], "MOR should have ${totalRows} rows")

    // Verify the 2 ATE rows with hex
    def mow_detail = sql """ SELECT `key1`, length(`key1`), hex(`key1`), `value`
                             FROM ${mowTable} WHERE `id` = 1 ORDER BY length(`key1`) """
    logger.info("MOW ATE detail: ${mow_detail}")
    assertEquals(2, mow_detail.size(), "MOW should have exactly 2 ATE rows (normal + dirty)")
    // Row 1: normal key (latest version, deduped from old rowset)
    assertEquals(3, mow_detail[0][1], "Normal key length should be 3")
    assertEquals("415445", mow_detail[0][2], "Normal key hex should be 415445 (ATE)")
    assertEquals(18, mow_detail[0][3], "Normal key value should be 18 (latest)")
    // Row 2: dirty key
    assertEquals(50, mow_detail[1][1], "Dirty key length should be 50")
    assertEquals(18, mow_detail[1][3], "Dirty key value should be 18")

    // No duplicate keys in the whole table
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
    assertEquals(0, mow_dup.size(), "MOW should have no duplicate keys: dirty data should not affect normal key dedup (multi-segment)")
    assertEquals(0, mor_dup.size(), "MOR should have no duplicate keys")
}
