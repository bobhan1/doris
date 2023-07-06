
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

suite("test_primary_key_partial_update_default_value", "p0") {
    def tableName = "test_primary_key_partial_update_default_value"

    // create table
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL DEFAULT "yixiu" COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL DEFAULT "4321" COMMENT  "test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    // insert 2 lines
    sql """
        insert into ${tableName} values(2, "doris2", 2000, 223, 1)
    """

    sql """
        insert into ${tableName} values(1, "doris", 1000, 123, 1)
    """

    // stream load with key not exit before
    streamLoad {
        table "${tableName}"

        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score'

        file 'default.csv'
        time 10000 // limit inflight 10s
    }

    sql "sync"

    qt_select_default """
        select * from ${tableName} order by id;
    """

    // drop drop
    sql """ DROP TABLE IF EXISTS ${tableName} """


    def tableName2 = "test_primary_key_partial_update_default_value2"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
            CREATE TABLE ${tableName2} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    sql """insert into ${tableName2} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1)"""
    qt_sql """ select * from ${tableName2} order by id; """
    streamLoad {
        table "${tableName2}"
        set 'column_separator', ','
        set 'format', 'csv'
        set 'partial_columns', 'true'
        set 'columns', 'id,score'
        file 'default.csv'
        time 10000 // limit inflight 10s

        // the partial update stream load wiil fail if the unmentioned column is neither nullable or have default value
        check {result, exception, startTime, endTime ->
            assertTrue(exception == null)
            def json = parseJson(result)
            assertEquals("fail", json.Status.toLowerCase())
        }
    }
    qt_sql """ select * from ${tableName2} order by id; """
    sql """ DROP TABLE IF EXISTS ${tableName2} """
}
