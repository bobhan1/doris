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

suite("test_mow_schema_change", "db_gen") {
    def getJobState = { tableName ->
         def jobStateResult = sql """  SHOW ALTER TABLE COLUMN WHERE IndexName='${tableName}' ORDER BY createtime DESC LIMIT 1 """
         return jobStateResult[0][9]
    }

    def getBuildIndexState = { tableName ->
         def buildIndexRes = sql """ show build index where TableName = '${tableName}' ORDER BY createtime DESC LIMIT 1 """
         log.info("build index state: " + buildIndexRes)
         return buildIndexRes[0][7]
    }


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

    def tableName = "mow_table_schema_change"

    def create_table_with_seq = """
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
                    "enable_unique_key_merge_on_write" = "true",
                    "function_column.sequence_col" = "c11"
                ); """ 

    def create_table_without_seq = """
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

    sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
    sql create_table_without_seq

    def sucess = new AtomicBoolean()
    sucess.set(true)


    // count(*)
    Thread.startDaemon {
        while (sucess.get()) {
            try {
                def total = sql """ select count(*)  from ${tableName} """
                logger.info("total:" + total)
            } catch (Throwable t) {
                log.error("failed to select count *", t)
                context.reconnectFe()
            }
            Thread.sleep(5000)
        }
    }

    // delete predicate
    Thread.startDaemon {
        while (true) {
            Thread.sleep(600000) // delete predicate every 10m
            try {
                sql """ delete  from ${tableName} where c0 < 'abcd'"""
            } catch (Throwable t) {
                log.error("faild to delete", t)
                context.reconnectFe()
            }
        }
    }

    // check duplicate keys
    def checkDupKey = { table, keys, count ->
        // while (count < 0 || count-- > 0) {
            try {
                def res = sql """ select ${keys}, count(*) a from ${table} group by ${keys} having a > 1 """
                logger.info("dup key result:" + res)
                // result should be empty
                if (res[0]) {
                     sucess.set(false)
                     logger.error("duplicate key occur")
                     assertTrue(false)
                }
            } catch (Throwable t) {
                log.error("select duplicate key failed", t)
                context.reconnectFe()
            }
            // Thread.sleep(5000)
        // }
    }

    def waitSchemaChagneFinish = { table, keys ->
        while (true) {
            String result = getJobState(table)
            if (result == "FINISHED") {
                 break;
            } else if (result == "CANCELLED") {
                 logger.error("schema change was cancelled")
                 break;
            } else {
                 sleep(2000)
            }

            try {
                def res = sql """ select ${keys}, count(*) a from ${table} group by ${keys} having a > 1 """
                logger.info("result:" + res)
                if (res[0]) {
                    result = getJobState(table)
                    if (result == "FINISHED") {
                         break;
                    }
                    sucess.set(false)
                    logger.error("duplicate key occur")
                    assertTrue(flase)
                }
            } catch (Throwable t) {
                log.error("select duplicate key failed", t)
                context.reconnectFe()
            }
        }
    }

    def waitBuildIndexFinish = { table, keys ->
        while (true) {
            String result = getBuildIndexState(table)
            logger.info("getBuildIndexState: " + result)
            if (result == "FINISHED") {
                 break;
            } else if (result == "CANCELLED") {
                 logger.error("build index was cancelled")
                 break;
            } else {
                 sleep(2000)
            }

            try {
                def res = sql """ select ${keys}, count(*) a from ${table} group by ${keys} having a > 1 """
                logger.info("result:" + res)
                if (res[0]) {
                    sucess.set(false)
                    logger.error("duplicate key occur")
                    assertTrue(flase)
                }
            } catch (Throwable t) {
                log.error("select duplicate key failed", t)
                context.reconnectFe()
            }
        }
    }

    def i = 0
    Thread.startDaemon {
        while (sucess.get()) {
            logger.info("start " + i + " round")
            def keyCols = "c0, c1"
            checkDupKey(tableName, keyCols, 10)

            logger.info("schema change modify value col start")
            sql """ ALTER TABLE ${tableName} modify COLUMN c3 varchar(20)"""
            waitSchemaChagneFinish(tableName, keyCols)
            checkDupKey(tableName, keyCols, 10)
            logger.info("schema change modify value col finished")

            logger.info("schema change add new value col start")
            sql """ ALTER TABLE ${tableName} ADD COLUMN new_val_column int """
            waitSchemaChagneFinish(tableName, keyCols)
            checkDupKey(tableName, keyCols, 10)
            logger.info("schema change add new value finished")

            logger.info("schema change drop col start")
            sql """ ALTER TABLE ${tableName} DROP COLUMN new_val_column """
            waitSchemaChagneFinish(tableName, keyCols)
            checkDupKey(tableName, keyCols, 10)
            logger.info("schema change drop col finished")

            logger.info("schema change add inverted index start")
            sql """ CREATE INDEX idx_c20 ON ${tableName}(c20) USING INVERTED """
            sql """ BUILD INDEX idx_c20 ON ${tableName} """
            waitBuildIndexFinish(tableName, keyCols)
            checkDupKey(tableName, keyCols, 10)
            logger.info("schema change add inverted index finished")

            logger.info("schema change drop inverted index start")
            sql """ DROP INDEX IF EXISTS idx_c20 ON ${tableName} """
            waitBuildIndexFinish(tableName, keyCols)
            checkDupKey(tableName, keyCols, 10)
            logger.info("schema change drop inverted index finished")

            logger.info("schema change rename val col start")
            sql """ ALTER TABLE ${tableName} RENAME COLUMN c21 c21_rename """
            checkDupKey(tableName, keyCols, 10)
            sql """ ALTER TABLE ${tableName} RENAME COLUMN c21_rename c21 """
            logger.info("schema change rename val col finished")

            logger.info("schema change rename key col start")
            sql """ ALTER TABLE ${tableName} RENAME COLUMN c1 c1_rename """
            keyCols = "c0, c1_rename"
            checkDupKey(tableName, keyCols, 10)
            sql """ ALTER TABLE ${tableName} RENAME COLUMN c1_rename c1 """
            keyCols = "c0, c1"
            logger.info("schema change rename key col finished")

            logger.info("schema change modify new key col start")
            sql """ ALTER TABLE ${tableName} modify COLUMN c1 bigint key """
            waitSchemaChagneFinish(tableName, keyCols)
            checkDupKey(tableName, keyCols, 10)
            logger.info("schema change modify new key col finished")

            logger.info("schema change reorder col start")
            sql """ ALTER TABLE ${tableName} ORDER BY(c1, c0,
                c2, c3, c4, c5, c6, c7, c8, c9, c10, c11, c12, c13, c14, c15, c16,
                c17, c18, c20, c19, c21, c22) 
            """
            waitSchemaChagneFinish(tableName, keyCols)
            keyCols = "c1, c0"
            checkDupKey(tableName, keyCols, 10)
            logger.info("schema change reorder col finished")

            logger.info("schema change add new key col start")
            sql """ ALTER TABLE ${tableName} ADD COLUMN new_key_column int key """
            waitSchemaChagneFinish(tableName, keyCols)
            keyCols = "c1, c0, new_key_column"
            checkDupKey(tableName, keyCols, 10)
            logger.info("schema change add new key col finished")

            sql """ DROP TABLE IF EXISTS ${tableName} FORCE"""
            if (i % 2 == 0) {
                sql create_table_with_seq
            } else {
                sql create_table_without_seq
            }
            i++
        }
    }

    // delete sign
    def dbgenDir = "/mnt/disk1/baohan/tests/doris-dbgen-dir" 
    Thread.startDaemon {
        while (sucess.get()) {
            Thread.sleep(600000) // delete sign every 10m
            def cm
            def rows = 4 * bulkSize;
            if (password) {
                cm = """${dbgenDir}/doris-dbgen gen  --no-progress
                        --host ${sql_ip} --sql-port ${sql_port} --user ${user} --pass ${password} --database ${realDb}
                        --table ${tableName} --rows ${rows} --bulk-size ${bulkSize}
                        --http-port ${http_port} --max-threads 2 --merge-type delete
                     """
            } else { 
                cm = """${dbgenDir}/doris-dbgen gen  --no-progress
                        --host ${sql_ip} --sql-port ${sql_port} --user ${user} --database ${realDb}
                        --table ${tableName} --rows ${rows} --bulk-size ${bulkSize}
                        --http-port ${http_port} --max-threads 2 --merge-type delete
                     """
            }
            logger.info("command is: " + cm)
            def proc = cm.execute()
            def sout = new StringBuilder(), serr = new StringBuilder()
            proc.consumeProcessOutput(sout, serr)
            proc.waitForOrKill(1000*1000) // millisecond
            logger.info("std out: " + sout + "std err: " + serr)
        }
    }

    // doris-dbgen load data
    def curRows = 0;
    def bulkSize = 10000
    while (sucess.get()) {
        def rows = 2 * bulkSize;
        def cm
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
    }

    log.error("shouldn't come to here")
    assertTrue(false)
}
