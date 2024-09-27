
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

suite("test_partial_update_new_row_policy", "p0") {

    def tableName = "test_partial_update_only_keys"
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
    qt_append """select * from ${tableName} order by k;"""


    sql """set partial_update_new_key_policy="ignore";"""
    sql "sync;"
    checkVariable("IGNORE")
    explain {
        sql "insert into ${tableName}(k,c2) values(1,20),(3,80),(6,80),(7,80);"
        contains "PARTIAL_UPDATE_NEW_KEY_POLICY: IGNORE" 
    }
    sql "insert into ${tableName}(k,c2) values(1,20),(3,80),(6,80),(7,80);"
    qt_ignore """select * from ${tableName} order by k;"""


    sql """set partial_update_new_key_policy="ERROR";"""
    sql "sync;"
    checkVariable("ERROR")
    explain {
        sql "insert into ${tableName}(k,c2) values(1,30),(2,30);"
        contains "PARTIAL_UPDATE_NEW_KEY_POLICY: ERROR"
    }
    sql "insert into ${tableName}(k,c2) values(1,30),(2,30);"
    qt_error1 """select * from ${tableName} order by k;"""
    test {
        sql "insert into ${tableName}(k,c2) values(1,30),(10,999),(11,999);"
        exception "[E-7003]Can't append new rows in partial update when partial_update_new_key_policy is ERROR"
    }
    qt_error2 """select * from ${tableName} order by k;"""


    sql """set partial_update_new_key_policy=default;"""
    sql "sync;"
    checkVariable("APPEND")
    test {
        sql """set partial_update_new_key_policy="invalid";"""
        exception "partial_update_new_row_policyshould be one of {'APPEND', 'IGNORE', 'ERROR'}, but found invalid"
    }
    checkVariable("APPEND")

    sql "set enable_unique_key_partial_update=false;"
    sql "sync;"

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
    qt_append """select * from ${tableName} order by k;"""

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k,c3'
        set 'partial_columns', 'true'
        set 'partial_update_new_key_policy', 'IGNORE'
        file 'row_policy2.csv'
        time 10000
    }
    qt_ignore """select * from ${tableName} order by k;"""

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k,c3'
        set 'partial_columns', 'true'
        set 'partial_update_new_key_policy', 'IGNORE'
        file 'row_policy3.csv'
        time 10000
    }
    qt_error1 """select * from ${tableName} order by k;"""

    streamLoad {
        table "${tableName}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'columns', 'k,c3'
        set 'partial_columns', 'true'
        set 'partial_update_new_key_policy', 'IGNORE'
        file 'row_policy4.csv'
        time 10000
    }
    qt_error2 """select * from ${tableName} order by k;"""
}
