
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

suite("test_partial_update_native_insert_stmt", "p0") {

    def tableName = "test_partial_update_native_insert_stmt"
    sql "set enable_insert_strict = false;"
    sql """ DROP TABLE IF EXISTS ${tableName} """
    sql """
            CREATE TABLE ${tableName} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL DEFAULT "yixiu" COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    sql """insert into ${tableName} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1)"""
    sql "set enable_unique_key_partial_update=true;"
    sql "sync;"
    sql """insert into ${tableName}(id,score) values(2,400),(1,200),(4,400)"""
    sql "set enable_unique_key_partial_update=false;"
    sql "sync;"
    qt_select_default """ select * from ${tableName} order by id; """
    sql """ DROP TABLE IF EXISTS ${tableName} """


    def tableName2 = "test_partial_update_native_insert_stmt2"
    sql "set enable_insert_strict = false;"
    sql """ DROP TABLE IF EXISTS ${tableName2} """
    sql """
            CREATE TABLE ${tableName2} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL, 
                `last_access_time` datetime NULL 
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) 
            BUCKETS AUTO PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );
    """
    sql """insert into ${tableName2} values(1,"kevin",18,"shenzhen",400,"2023-07-01 12:00:00");"""
    qt_sql """select * from ${tableName2} order by id;"""
    sql "set enable_unique_key_partial_update=true;"
    sql "sync;"
    sql """ insert into ${tableName2}(id,balance,last_access_time) values(1,500,"2023-07-03 12:00:01"),(3,23,"2023-07-03 12:00:02"),(18,9999999,"2023-07-03 12:00:03"); """
    sql "set enable_unique_key_partial_update=false;"
    sql "sync;"
    qt_sql """select * from ${tableName2} order by id;"""
    sql """ DROP TABLE IF EXISTS ${tableName2} """

    def tableName3 = "test_partial_update_native_insert_stmt3"
    sql """ DROP TABLE IF EXISTS ${tableName3} """
    sql """
            CREATE TABLE ${tableName3} (
                `id` int(11) NOT NULL COMMENT "用户 ID",
                `name` varchar(65533) NOT NULL COMMENT "用户姓名",
                `score` int(11) NOT NULL COMMENT "用户得分",
                `test` int(11) NULL COMMENT "null test",
                `dft` int(11) DEFAULT "4321")
                UNIQUE KEY(`id`) DISTRIBUTED BY HASH(`id`) BUCKETS 1
                PROPERTIES("replication_num" = "1", "enable_unique_key_merge_on_write" = "true")
    """
    sql """insert into ${tableName3} values(2, "doris2", 2000, 223, 1),(1, "doris", 1000, 123, 1)"""
    qt_sql """ select * from ${tableName3} order by id; """
    test {
        sql """insert into ${tableName3}(id,score) values(2,400),(1,200),(4,400)"""
        exception "must be explicitly mentioned in column permutation"
    }
    qt_sql """ select * from ${tableName3} order by id; """
    sql """ DROP TABLE IF EXISTS ${tableName3} """


    def tableName4 = "test_partial_update_native_insert_stmt4"
    sql "set enable_insert_strict = false;"
    sql """ DROP TABLE IF EXISTS ${tableName4} """
    sql """
            CREATE TABLE ${tableName4} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL, 
                `last_access_time` datetime NULL 
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) 
            BUCKETS AUTO PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );
    """
    sql """insert into ${tableName4} values(1,"kevin",18,"shenzhen",400,"2023-07-01 12:00:00");"""
    qt_sql """select * from ${tableName4} order by id;"""
    sql "set enable_unique_key_partial_update=true;"
    sql "sync;"
    sql """ insert into ${tableName4}(id,balance,last_access_time) values(1,500,"2023-07-03 12:00:01"),(3,23,"2023-07-03 12:00:02"),(18,9999999,"2023-07-03 12:00:03"); """
    sql "set enable_unique_key_partial_update=false;"
    sql "sync;"
    qt_sql """select * from ${tableName4} order by id;"""
    sql """ DROP TABLE IF EXISTS ${tableName4} """


    def tableName5 = "test_partial_update_native_insert_stmt5"
    sql """ DROP TABLE IF EXISTS ${tableName5} """
    sql """
            CREATE TABLE ${tableName5} ( 
                `id` int(11) NULL, 
                `name` varchar(10) NULL,
                `age` int(11) NULL DEFAULT "20", 
                `city` varchar(10) NOT NULL DEFAULT "beijing", 
                `balance` decimalv3(9, 0) NULL, 
                `last_access_time` datetime NULL 
            ) ENGINE = OLAP UNIQUE KEY(`id`) 
            COMMENT 'OLAP' DISTRIBUTED BY HASH(`id`) 
            BUCKETS AUTO PROPERTIES ( 
                "replication_allocation" = "tag.location.default: 1", 
                "storage_format" = "V2", 
                "enable_unique_key_merge_on_write" = "true", 
                "light_schema_change" = "true", 
                "disable_auto_compaction" = "false", 
                "enable_single_replica_compaction" = "false" 
            );
    """
    sql """insert into ${tableName5} values(1,"kevin",18,"shenzhen",400,"2023-07-01 12:00:00");"""
    qt_sql """select * from ${tableName5} order by id;"""
    sql "set enable_insert_strict = true;"
    sql "set enable_unique_key_partial_update=true;"
    sql "sync;"
    test {
        sql """ insert into ${tableName5}(id,balance,last_access_time) values(1,500,"2023-07-03 12:00:01"),(3,23,"2023-07-03 12:00:02"),(18,9999999,"2023-07-03 12:00:03"); """
        exception "Insert has filtered data in strict mode"
    }
    sql "set enable_unique_key_partial_update=false;"
    sql "sync;"
    qt_sql """select * from ${tableName5} order by id;"""
    sql """ DROP TABLE IF EXISTS ${tableName5} """
}
