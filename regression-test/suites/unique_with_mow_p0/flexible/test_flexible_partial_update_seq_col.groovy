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

suite('test_flexible_partial_update_seq_col') {

    // 1. sequence map col(without default value)
    def tableName = "test_flexible_partial_update_seq_map_col1"
    sql """ DROP TABLE IF EXISTS ${tableName} force;"""
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
        "function_column.sequence_col" = "v5",
        "store_row_column" = "false"); """

    sql """insert into ${tableName} select number, number, number, number, number, number * 10 from numbers("number" = "6"); """
    order_qt_seq_map_no_default_val1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

    // update rows(1,5) with lower seq map col
    // update rows(2,4) with higher seq map col
    // update rows(3) wihout seq map col, should use original seq map col
    // insert new row(6) without seq map col, should be filled with null
    // insert new row(7) with seq map col 
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test2.json"
        time 20000 // limit inflight 10s
    }
    order_qt_seq_map_no_default_val2 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"


    // 2. sequence map col(with default value)
    tableName = "test_flexible_partial_update_seq_map_col2"
    sql """ DROP TABLE IF EXISTS ${tableName} force;"""
    sql """ CREATE TABLE ${tableName} (
    `k` int(11) NULL, 
    `v1` BIGINT NULL,
    `v2` BIGINT NULL DEFAULT "9876",
    `v3` BIGINT NOT NULL,
    `v4` BIGINT NOT NULL DEFAULT "1234",
    `v5` BIGINT NULL DEFAULT "31"
    ) UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
    PROPERTIES(
    "replication_num" = "1",
    "enable_unique_key_merge_on_write" = "true",
    "light_schema_change" = "true",
    "function_column.sequence_col" = "v5",
    "store_row_column" = "false"); """
    sql """insert into ${tableName} select number, number, number, number, number, number * 10 from numbers("number" = "6"); """
    order_qt_seq_map_has_default_val1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
    // update rows(1,5) with lower seq map col
    // update rows(2,4) with higher seq map col
    // update rows(3) wihout seq map col, should use original seq map col
    // insert new row(6) without seq map col, should be filled with default value
    // insert new row(7) with seq map col 
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test2.json"
        time 20000 // limit inflight 10s
    }
    // TODO(bobhan): FIXME! result is incorrect, wait for https://github.com/apache/doris/pull/40272 to be merged
    // and process for flexible partial update
    order_qt_seq_map_has_default_val2 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"


    // 3. sequence type col
    tableName = "test_flexible_partial_update_seq_type_col1"
    sql """ DROP TABLE IF EXISTS ${tableName} force;"""
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
    "function_column.sequence_type" = "int",
    "store_row_column" = "false"); """
    sql """insert into ${tableName}(k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__) select number, number, number, number, number, number, number * 10 from numbers("number" = "6"); """
    order_qt_seq_type_col1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
    // update rows(1,5) with lower seq type col
    // update rows(2,4) with higher seq type col
    // update rows(3) wihout seq type col, should use original seq type col
    // insert new row(6) without seq map col, should be filled null value
    // insert new row(7) with seq map col
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test3.json"
        time 20000 // limit inflight 10s
    }
    // TODO(bobhan1): behavior here may be changed, maybe we need to force user to specify __DORIS_SEQUENCE_COL__ for tables
    // with sequence type col and discard rows without it in XXXReader
    order_qt_seq_type_col2 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"


    // ==============================================================================================================================
    // the below cases will have many rows with same keys in one load

    // 4. sequence type col
    tableName = "test_flexible_partial_update_seq_type_col2"
    sql """ DROP TABLE IF EXISTS ${tableName} force;"""
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
    "function_column.sequence_type" = "int",
    "store_row_column" = "false"); """
    sql """insert into ${tableName}(k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__) select number, number, number, number, number, number, number * 10 from numbers("number" = "6"); """
    order_qt_seq_type_col_multi_rows_1 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
    // rows with same keys are neighbers
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test4.json"
        time 20000 // limit inflight 10s
    }
    order_qt_seq_type_col_multi_rows_2 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"
    // rows with same keys are interleaved
    streamLoad {
        table "${tableName}"
        set 'format', 'json'
        set 'read_json_by_line', 'true'
        set 'strict_mode', 'false'
        set 'unique_key_update_mode', 'UPDATE_FLEXIBLE_COLUMNS'
        file "test5.json"
        time 20000 // limit inflight 10s
    }
    order_qt_seq_type_col_multi_rows_3 "select k,v1,v2,v3,v4,v5,__DORIS_SEQUENCE_COL__,BITMAP_TO_STRING(__DORIS_SKIP_BITMAP_COL__) from ${tableName};"

    // TODO(bobhan1): add cases that has multiple rows with same key in one memtable flush
    // some rows have seq map col, some rows don't
}