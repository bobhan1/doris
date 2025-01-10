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

import java.util.concurrent.TimeUnit
import org.awaitility.Awaitility
import org.apache.doris.regression.suite.ClusterOptions

suite("test_cloud_sc_wrong_pending_delete_bitmap", "p0,docker") {

    logger.info(" ==== test =====")
    def token = "greedisgood9999"

    def enable_ms_inject_api = { msHttpPort, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=enable"
            check check_func
        }
    }

    def disable_ms_inject_api = { msHttpPort, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=disable"
            check check_func
        }
    }

    def inject_suite_to_ms = { msHttpPort, suite_name, check_func ->
        httpTest {
            op "get"
            endpoint msHttpPort
            uri "/MetaService/http/v1/injection_point?token=${token}&op=apply_suite&name=${suite_name}"
            check check_func
        }
    }

    def options = new ClusterOptions()
    options.feConfigs += [
        'cloud_cluster_check_interval_second=1',
        'sys_log_verbose_modules=org',
        'heartbeat_interval_second=1',
        'enable_debug_points=true'
    ]
    options.beConfigs += [
        'enable_debug_points=true'
    ]
    options.setFeNum(1)
    options.setBeNum(1)
    options.cloudMode = true

    logger.info(" ==== test 2 =====")
    docker(options) {
        def ms = cluster.getAllMetaservices().get(0)
        def msHttpPort = ms.host + ":" + ms.httpPort
        logger.info("ms1 addr={}, port={}, ms endpoint={}", ms.host, ms.httpPort, msHttpPort)

        def table1 = "test_cloud_sc_wrong_pending_delete_bitmap"
        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k1` int,
                `c1` int,
                `c2` int
                )UNIQUE KEY(k1)
            DISTRIBUTED BY HASH(k1) BUCKETS 1
            PROPERTIES (
                "enable_unique_key_merge_on_write" = "true",
                "disable_auto_compaction" = "true",
                "replication_num" = "1"); """

        sql "insert into ${table1} values(1,1,1);"
        sql "insert into ${table1} values(2,2,2);"
        sql "insert into ${table1} values(3,3,3);"
        sql "sync;"
        order_qt_sql "select * from ${table1};"

        try {
            GetDebugPoint().clearDebugPointsForAllFEs()
            GetDebugPoint().clearDebugPointsForAllBEs()


            // let the schema change fail finally
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.injected_err")
            // block the schema change process before it change the shadow index to base index
            // and after the BE's tablet state has been changed to NORMAL
            GetDebugPoint().enableDebugPointForAllBEs("CloudSchemaChangeJob::_convert_historical_rowsets.leave.sleep")

            sql "alter table ${table1} modify column c1 bigint;"
            Thread.sleep(600)


            // we use sync point 'commit_txn_immediately::before_commit' to simulate that
            // the first load failed to remove its pending delete bitmap when commit_txn on MS
            inject_suite_to_ms(msHttpPort, "commit_txn_immediately::before_commit") {
                respCode, body ->
                    log.info("inject resource_manager::set_safe_drop_time resp: ${body} ${respCode}".toString()) 
            }
            enable_ms_inject_api(msHttpPort) {
                respCode, body -> log.info("enable inject resp: ${body} ${respCode}".toString()) 
            }

            Thread.sleep(200)
            try {
                sql "insert into ${table1} values(1,999,999);"
            } catch (Exception e) {
                logger.info(e.getMessage())
            }

            disable_ms_inject_api(msHttpPort) {
                respCode, body -> log.info("disable inject resp: ${body} ${respCode}".toString())
            }

            Thread.sleep(2000)

            // let the second load block before publish before the schema change drop shadow index
            // and let it publish after the schema change drop shadow index. In this case, it will not
            // send calculate delete bitmap agent task to tablets under shadow index becuase they have been dropped.
            // So the pending delete bitmap's lock_id on these tablets will still be the first load's lock_id
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.enable_spin_wait")
            GetDebugPoint().enableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")

            Thread.sleep(600)
            def t1 = Thread.start {
                // wait util the schema change failed, then let the second load publish
                Awaitility.await().atMost(20, TimeUnit.SECONDS).pollDelay(1000, TimeUnit.MILLISECONDS).pollInterval(1000, TimeUnit.MILLISECONDS).until(() -> {
                    def res = sql_return_maparray """ SHOW ALTER TABLE COLUMN WHERE TableName='${table1}' ORDER BY createtime DESC LIMIT 1 """
                    logger.info("state: ${res.State}")
                    if (res.State.equals("CANCELLED")) {
                        return true;
                    }
                    return false;
                });
                GetDebugPoint().disableDebugPointForAllFEs("CloudGlobalTransactionMgr.getDeleteBitmapUpdateLock.block")
            }
            
            sql "insert into ${table1} values(2,888,888);"
            
            t1.join()        

            qt_sql "select * from ${table1} order by k1,c1,c2;"
        } catch(Exception e) {
            logger.info(e.getMessage())
            throw e
        } finally {
            GetDebugPoint().clearDebugPointsForAllBEs()
        }
    }
}
