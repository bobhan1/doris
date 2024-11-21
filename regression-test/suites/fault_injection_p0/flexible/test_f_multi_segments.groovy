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

suite('test_f_multi_segments') {
    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    try {
        GetDebugPoint().enableDebugPointForAllBEs("NewJsonReader::get_next_block.set_batch_size", [batch_size: 10])
        GetDebugPoint().enableDebugPointForAllBEs("VNodeChannel::init.set_batch_size", [batch_size: 10])
        GetDebugPoint().enableDebugPointForAllBEs("MemTable::need_flush.force_flush")
        update_all_be_config("doris_scanner_row_num", 10)
        update_all_be_config("doris_scanner_row_bytes", 100)

        def tableName = "test_f_multi_segments"
        sql """ DROP TABLE IF EXISTS ${tableName} """
        sql """ CREATE TABLE ${tableName} (
            `k` int(11) NULL, 
            `v1` BIGINT NULL,
            `v2` BIGINT NULL DEFAULT "9876",
            `v3` BIGINT NOT NULL,
            `v4` BIGINT NOT NULL DEFAULT "1234",
            `v5` BIGINT NULL
            ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "true",
            "light_schema_change" = "true",
            "enable_unique_key_skip_bitmap_column" = "true"); """

        sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "6"); """
        qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k;"

        int n = 10
        String text1 = ""
        (0..n-1).each { idx -> 
            text1 += """{"k":${idx},"v1":20,"v2":123,"v3":45678}\n""";
        }
        (0..n-1).each { idx -> 
            text1 += """{"k":${idx},"v3":40,"v4":9876}\n""";
        }
        def load1 = new ByteArrayInputStream(text1.getBytes())
        streamLoad {
            table "${tableName}"
            set 'format', 'json'
            set 'read_json_by_line', 'true'
            set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
            inputStream load1
            time 20000
        }
        // qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k;"

    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()
        update_all_be_config("doris_scanner_row_num", 16384)
        update_all_be_config("doris_scanner_row_bytes", 10485760)
    }
}