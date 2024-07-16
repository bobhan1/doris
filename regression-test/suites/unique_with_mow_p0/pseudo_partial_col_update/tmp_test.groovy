
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

suite("test_pseduo_partial_col_update") {

    def tbl1 = "test_pseduo_partial_col_update1"
    sql """ DROP TABLE IF EXISTS ${tbl1} """
    sql """CREATE TABLE `${tbl1}` (
        c1 int, c2 int, c3 int
        ) ENGINE=OLAP
        UNIQUE KEY(c1)
        COMMENT 'OLAP'
        DISTRIBUTED BY HASH(c1) BUCKETS 1
        PROPERTIES ("replication_num" = "1"); """
    sql "insert into ${tbl1} values(1,1,1),(2,2,2),(3,3,3);"
    qt_sql "select * from ${tbl1} order by c1,c2,c3;"

    sql "set enable_insert_strict=false;"
    sql """insert into ${tbl1}(c1,c2,c3,__DORIS_UPDATE_COLS__) values(4,4,4,"test");"""
    qt_sql "select * from ${tbl1} order by c1,c2,c3;"
    sql """insert into ${tbl1}(c1,c2,__DORIS_UPDATE_COLS__) values(1,10,"test");"""
    qt_sql "select * from ${tbl1} order by c1,c2,c3;"
}
