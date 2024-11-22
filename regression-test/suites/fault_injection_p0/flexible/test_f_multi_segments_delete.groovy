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

import org.apache.doris.regression.suite.ClusterOptions
import org.apache.doris.regression.util.NodeType

suite('test_f_multi_segments_delete') {
    def options = new ClusterOptions()
    options.enableDebugPoints()
    options.setFeNum(1)
    options.setBeNum(1)
    options.beConfigs += ['doris_scanner_row_num=7']
    docker(options) {

        GetDebugPoint().clearDebugPointsForAllFEs()
        GetDebugPoint().clearDebugPointsForAllBEs()

        try {
            GetDebugPoint().enableDebugPointForAllBEs("NewJsonReader::get_next_block.set_batch_size", [batch_size: 7])
            GetDebugPoint().enableDebugPointForAllBEs("VNodeChannel::init.set_batch_size", [batch_size: 7])
            GetDebugPoint().enableDebugPointForAllBEs("MemTable::need_flush.force_flush")
            update_all_be_config("doris_scanner_row_num", 7)
            update_all_be_config("doris_scanner_row_bytes", 10)

            def tableName = "test_f_multi_segments_delete"
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
            sql """insert into ${tableName} select number, number, number, number, number, number from numbers("number" = "11"); """

            String text1 = ""

            // segment 1
            text1 += """{"k":1,"v1":999}\n""";
            text1 += """{"k":2,"__DORIS_DELETE_SIGN__":1}\n""";
            text1 += """{"k":3,"v1":999,"v2":888,"v3":777}\n""";
            text1 += """{"k":4,"v1":999,"v2":888,"v3":777}\n""";
            text1 += """{"k":5,"__DORIS_DELETE_SIGN__":1}\n""";
            text1 += """{"k":6,"v1":999,"v2":888}\n""";
            text1 += """{"k":7,"__DORIS_DELETE_SIGN__":1}\n""";


            // segment 2
            text1 += """{"k":1,"v3":123,"v4":456}\n""";
            text1 += """{"k":2,"v3":123,"v5":789}\n""";
            text1 += """{"k":3,"__DORIS_DELETE_SIGN__":1}\n""";
            text1 += """{"k":4,"v1":123,"v3":456,"v5":789}\n""";
            text1 += """{"k":5,"v1":999,"v2":888}\n""";
            text1 += """{"k":6,"__DORIS_DELETE_SIGN__":1}\n""";
            text1 += """{"k":7,"__DORIS_DELETE_SIGN__":1}\n""";


            // segment 3
            text1 += """{"k":1,"v5":777}\n""";
            text1 += """{"k":2,"v1":999,"v4":8888}\n""";
            text1 += """{"k":3,"__DORIS_DELETE_SIGN__":1}\n""";
            text1 += """{"k":4,"v1":123,"v3":456,"v5":789}\n""";
            text1 += """{"k":5,"__DORIS_DELETE_SIGN__":1}\n""";
            text1 += """{"k":6,"v3":777}\n""";
            text1 += """{"k":7,"v3":777,"v4":999,"v5":888}\n""";

            streamLoad {
                table "${tableName}"
                set 'format', 'json'
                set 'read_json_by_line', 'true'
                set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
                inputStream new ByteArrayInputStream(text1.getBytes())
                time 20000
            }
            
            qt_sql "select k,v1,v2,v3,v4,v5,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName} order by k;"

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
}