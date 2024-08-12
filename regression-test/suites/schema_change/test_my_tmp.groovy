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
import java.nio.file.Files
import java.nio.file.Paths
import java.net.URL
import java.io.File
import java.util.concurrent.atomic.AtomicBoolean

suite("test_my_tmp") {
    def tableName = "test_my_tmp"

    def checkDupKeys = { keys ->
        for(def idx:[0,1,2]) {
            sql "set use_fix_replica=${idx};"
            sql "sync;"
            def res = sql_return_maparray("select c0, count(*) as a from ${tableName} group by ${keys} having count(*) > 1;")
            assertEquals(res.size(), 0)
        }
    }

    def myWait = {
        waitForSchemaChangeDone {
            sql """ SHOW ALTER TABLE COLUMN WHERE tablename='${tableName}' ORDER BY createtime DESC LIMIT 1 """
            time 1600
        }
        qt_sql "select count(*) from ${tableName};"
        return true
    }

    def dbgenDir = "/mnt/disk1/baohan/tests/doris-dbgen-dir"
    String jdbcUrl = context.config.jdbcUrl
    String urlWithoutSchema = jdbcUrl.substring(jdbcUrl.indexOf("://") + 3)
    def sql_ip = urlWithoutSchema.substring(0, urlWithoutSchema.indexOf(":"))
    def sql_port
    if (urlWithoutSchema.indexOf("/") >= 0) {
        // e.g: jdbc:mysql://locahost:8080/?a=b
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1, urlWithoutSchema.indexOf("/"))
    } else {
        // e.g: jdbc:mysql://locahost:8080
        sql_port = urlWithoutSchema.substring(urlWithoutSchema.indexOf(":") + 1)
    }
    String feHttpAddress = context.config.feHttpAddress
    def http_port = feHttpAddress.substring(feHttpAddress.indexOf(":") + 1)

    String realDb = context.config.getDbNameByFile(context.file)
    String user = context.config.jdbcUser
    String password = context.config.jdbcPassword

    def genData = {
        def curRows = 0;
        def bulkSize = 10000
        def sucess = new AtomicBoolean()
        sucess.set(true)
        // while (sucess.get()) {
            def rows = 2 * bulkSize;
            if (password) {
                cm = """${dbgenDir}/doris-dbgen gen  --no-progress
                        --host ${sql_ip} --sql-port ${sql_port} --user ${user} --pass ${password} --database ${realDb}
                        --table ${tableName} --rows ${rows} --bulk-size ${bulkSize}
                        --http-port ${http_port} --max-threads 2
                    """
            } else {
                cm = """${dbgenDir}/doris-dbgen gen  --no-progress
                        --host ${sql_ip} --sql-port ${sql_port} --user ${user} --database ${realDb}
                        --table ${tableName} --rows ${rows} --bulk-size ${bulkSize}
                        --http-port ${http_port} --max-threads 2
                    """
            }
            logger.info("command is: " + cm)
            def proc = cm.execute()
            def sout = new StringBuilder(), serr = new StringBuilder()
            proc.consumeProcessOutput(sout, serr)
            proc.waitForOrKill(600*1000) // millisecond
            logger.info("std out: " + sout + "std err: " + serr)
            curRows += rows;
            logger.info("total load rows: " + curRows)
        // }
    }

    try {
        sql "DROP TABLE IF EXISTS ${tableName} force;"
        sql """
        CREATE TABLE `${tableName}` (
                `c0` varchar(100) NOT NULL,
                `c1` int NULL,
                `c2` text NULL ,
                `c3` int NULL,
                `c4` text NULL,
                `c5` text NULL,
                `c6` varchar(20) NULL,
                `c7` varchar(50) NULL,
                `c8` varchar(50) NULL,
                `c9` date NULL,
                `c10` varchar(50) NULL,
                `c11` date NULL,
                `c12` text NULL,
                `c13` text NULL,
                `c14` bitmap NOT NULL,
                `c15` map<int, int> NULL,
                `c16` varchar(10) NULL,
                `c17` tinyint(4) NULL,
                `c18` struct<s_id:int(11), s_date:datetime> NULL,
                `c19` varchar(50) NULL,
                `c20` varchar(50) NULL,
                `c21` datetime NULL,
                `c22` datetime NULL)
                ENGINE=OLAP
                UNIQUE KEY(`c0`,`c1`)
                COMMENT 'OLAP'
                DISTRIBUTED BY HASH(`c0`) BUCKETS 8
                PROPERTIES (
                    "replication_num" = "3",
                    "enable_unique_key_merge_on_write" = "true"
                );
                    """

        // sql "insert into ${tableName}(c0,c1,c3) values(1,1,1);"
        // sql "insert into ${tableName}(c0,c1,c3) values(2,2,2);"
        // sql "insert into ${tableName}(c0,c1,c3) values(3,3,3);"
        // // order_qt_sql0 "select c0,c1,c3,__DORIS_VERSION_COL__ from ${tableName};"

        genData()

        sql "ALTER TABLE ${tableName} modify COLUMN c3 varchar(20)"
        myWait()
        checkDupKeys("c0,c1")
        // order_qt_sql1 "select c0,c1,c3,__DORIS_VERSION_COL__ from ${tableName};"

        genData()
        sql "ALTER TABLE ${tableName} ADD COLUMN new_val_column int;"
        myWait()
        checkDupKeys("c0,c1")
        // order_qt_sql2 "select c0,c1,c3,__DORIS_VERSION_COL__ from ${tableName};"

        genData()
        sql "ALTER TABLE ${tableName} DROP COLUMN new_val_column;"
        myWait()
        checkDupKeys("c0,c1")
        // order_qt_sql3 "select c0,c1,c3,__DORIS_VERSION_COL__ from ${tableName};"

        genData()
        sql "ALTER TABLE ${tableName} RENAME COLUMN c21 c21_rename;" 
        myWait()
        checkDupKeys("c0,c1")
        // order_qt_sql4 "select c0,c1,c3,__DORIS_VERSION_COL__ from ${tableName};"

        genData()
        sql "ALTER TABLE ${tableName} RENAME COLUMN c21_rename c21;"
        myWait()
        checkDupKeys("c0,c1")
        // order_qt_sql5 "select c0,c1,c3,__DORIS_VERSION_COL__ from ${tableName};"

        genData()
        sql "ALTER TABLE ${tableName} RENAME COLUMN c1 c1_rename;"
        myWait()
        checkDupKeys("c0,c1_rename")
        // order_qt_sql6 "select c0,c1_rename,c3,__DORIS_VERSION_COL__ from ${tableName};"

        genData()
        sql "ALTER TABLE ${tableName} RENAME COLUMN c1_rename c1;"
        myWait()
        checkDupKeys("c0,c1")
        // order_qt_sql7 "select c0,c1,c3,__DORIS_VERSION_COL__ from ${tableName};"

        genData()
        sql "ALTER TABLE ${tableName} modify COLUMN c1 bigint key;"
        myWait()
        checkDupKeys("c0,c1")
        // order_qt_sql8 "select c0,c1,c3,__DORIS_VERSION_COL__ from ${tableName};"

        genData()
        sql "ALTER TABLE ${tableName} ORDER BY(c1, c0, c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16, c17, c18, c20, c19, c21, c22); "
        myWait()
        checkDupKeys("c1,c0")
        // order_qt_sql9 "select c0,c1,c3,__DORIS_VERSION_COL__ from ${tableName};"

    } finally {
        // drop table
        // sql """ DROP TABLE  ${tableName} force;"""
    }
}
