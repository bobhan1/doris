
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

suite("test_partial_update_new_key_policy", "p0") {

    def tableName = "test_partial_update_new_key_policy"
    sql """ DROP TABLE IF EXISTS ${tableName} force"""
    sql """ CREATE TABLE ${tableName} (
            `k` BIGINT NOT NULL,
            `c1` int,
            `c2` int,
            `c3` int)
            UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
            PROPERTIES(
                "replication_num" = "1",
                "enable_unique_key_merge_on_write" = "true"); """
    sql """insert into ${tableName} select number,number,number,number from numbers("number"="3");"""
    qt_sql """select * from ${tableName} order by k;"""

    def checkVariable = { expected -> 
        def res = sql_return_maparray """show variables where Variable_name="partial_update_new_key_policy";""";
        assertTrue(res[0].Value.equalsIgnoreCase(expected));
    }

    // 1. test insert stmt
    // 1.1
    sql "set enable_unique_key_partial_update=true;"
    sql "sync"

    sql """set partial_update_new_key_policy="APPEND";"""
    sql "sync;"
    checkVariable("APPEND")
    explain {
        sql "insert into ${tableName}(k,c1) values(0,10),(3,10),(4,10),(5,10);"
        contains "PARTIAL_UPDATE_NEW_KEY_POLICY: APPEND" 
    }
    sql "insert into ${tableName}(k,c1) values(0,10),(3,10),(4,10),(5,10);"
    qt_insert_append """select * from ${tableName} order by k;"""


    sql """set partial_update_new_key_policy="ERROR";"""
    sql "sync;"
    checkVariable("ERROR")
    explain {
        sql "insert into ${tableName}(k,c2) values(1,30),(2,30);"
        contains "PARTIAL_UPDATE_NEW_KEY_POLICY: ERROR"
    }
    sql "insert into ${tableName}(k,c2) values(1,30),(2,30);"
    qt_insert_error1 """select * from ${tableName} order by k;"""
    test {
        sql "insert into ${tableName}(k,c2) values(1,30),(10,999),(11,999);"
        exception "[E-7003]Can't append new rows in partial update when partial_update_new_key_policy is ERROR"
    }
    qt_insert_error2 """select * from ${tableName} order by k;"""


    sql """set partial_update_new_key_policy=default;"""
    sql "sync;"
    checkVariable("APPEND")
    test {
        sql """set partial_update_new_key_policy="invalid";"""
        exception "partial_update_new_key_policy should be one of {'APPEND', 'ERROR'}, but found invalid"
    }
    checkVariable("APPEND")

    // 1.2 partial_update_new_key_policy will not take effect when enable_unique_key_partial_update is false
    sql "set enable_unique_key_partial_update=false;"
    sql "sync;"

    sql "insert into ${tableName} values(1,9,9,9),(2,9,9,9),(100,9,9,9),(200,9,9,9);"
    qt_insert3 """select * from ${tableName} order by k;"""


    // 2. test stream load
    // 2.1
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k,c3'
        set 'partial_columns', 'true'
        set 'partial_update_new_key_policy', 'append'
        file 'row_policy1.csv'
        time 10000
    }
    qt_stream_load_append """select * from ${tableName} order by k;"""

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k,c3'
        set 'partial_columns', 'true'
        set 'partial_update_new_key_policy', 'error'
        file 'row_policy2.csv'
        time 10000
        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
            assertTrue(json.Message.toString().contains("[E-7003]Can't append new rows in partial update when partial_update_new_key_policy is ERROR"))
        }
    }
    qt_stream_load_error """select * from ${tableName} order by k;"""


    // 2.2 partial_update_new_key_policy will not take effect when enable_unique_key_partial_update is false
    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k,c1,c2,c3'
        set 'partial_columns', 'false'
        set 'partial_update_new_key_policy', 'error'
        file 'row_policy3.csv'
        time 10000
        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("Fail", json.Status)
            assertTrue(json.Message.toString().contains("[INVALID_ARGUMENT]partial_update_new_key_policy can only be set when partial_colums is true."))
        }
    }


    // 3. test this config will not affect non MOW tables
    tableName = "test_partial_update_new_key_policy2"
    sql """ DROP TABLE IF EXISTS ${tableName} force"""
    sql """ CREATE TABLE ${tableName} (
        `k` BIGINT NOT NULL,
        `c1` int,
        `c2` int default "123",
        `c3` int default "456")
        UNIQUE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES(
            "replication_num" = "1",
            "enable_unique_key_merge_on_write" = "false"); """
    sql """insert into ${tableName} select number,number,number,number from numbers("number"="5");"""
    sql " insert into ${tableName}(k,c1) values(0,20),(1,20),(10,20),(11,20);"
    sql "insert into ${tableName}(k,c2) values(0,30),(2,30),(10,30),(12,30);"
    qt_sql """select * from ${tableName} order by k;"""

    tableName = "test_partial_update_new_key_policy3"
    sql """ DROP TABLE IF EXISTS ${tableName} force"""
    sql """ CREATE TABLE ${tableName} (
        `k` BIGINT NOT NULL,
        `c1` int,
        `c2` int default "123",
        `c3` int default "456")
        DUPLICATE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES("replication_num" = "1"); """
    sql """insert into ${tableName} select number,number,number,number from numbers("number"="5");"""
    sql " insert into ${tableName}(k,c1) values(0,20),(1,20),(10,20),(11,20);"
    sql "insert into ${tableName}(k,c2) values(0,30),(2,30),(10,30),(12,30);"
    qt_sql """select * from ${tableName} order by k;"""

    tableName = "test_partial_update_new_key_policy4"
    sql """ DROP TABLE IF EXISTS ${tableName} force"""
    sql """ CREATE TABLE ${tableName} (
        `k` BIGINT NOT NULL,
        `c1` int max,
        `c2` int min,
        `c3` int sum)
        AGGREGATE KEY(`k`) DISTRIBUTED BY HASH(`k`) BUCKETS 1
        PROPERTIES("replication_num" = "1"); """
    sql """insert into ${tableName} select number,number,number,number from numbers("number"="5");"""
    sql " insert into ${tableName}(k,c1) values(0,20),(1,20),(10,20),(11,20);"
    sql "insert into ${tableName}(k,c2) values(0,30),(2,30),(10,30),(12,30);"
    qt_sql """select * from ${tableName} order by k;"""
}
