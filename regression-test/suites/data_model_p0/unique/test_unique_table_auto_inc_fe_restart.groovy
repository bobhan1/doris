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

suite("test_unique_table_auto_inc_fe_restart") {
    
    def options = new ClusterOptions()
    options.setFeNum(2)
    options.feConfigs.add('sys_log_verbose_modules=org.apache.doris')
    options.beConfigs.add('enable_java_support=false')
    docker(options) {
        def table1 = "test_unique_table_auto_inc_fe_restart"
        sql "drop table if exists ${table1}"
        sql """
            CREATE TABLE IF NOT EXISTS `${table1}` (
            `id` BIGINT NOT NULL AUTO_INCREMENT,
            `value` int(11) NOT NULL
            ) ENGINE=OLAP
            UNIQUE KEY(`id`)
            COMMENT "OLAP"
            DISTRIBUTED BY HASH(`id`) BUCKETS 1
            PROPERTIES (
            "enable_unique_key_merge_on_write" = "true"
            )
        """
        
        def run_load = {thread_num, rows, iters -> 
            def threads = []
            (1..thread_num).each { id1 -> 
                threads.add(Thread.start {
                    (1..iters).each { id2 -> 
                        sql """insert into ${table1}(value) select number from numbers("number" = "${rows}");"""
                    }
                })
            }

            threads.each { thread -> thread.join() }
            sql "sync"
        }

        def restart_nums = 3

        (1..restart_nums).each { id3 -> 
            run_load(3, 50000, 3)

            cluster.restartFrontends()
            sleep(8000)
            context.reconnectFe()
        }

        qt_sql "select id, count(*) from ${table1} group by id having count(*) > 1;"
        sql "drop table if exists ${table1};"
    }
}

