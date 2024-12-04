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

import org.junit.Assert
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

suite("prepare_missing_version", "nonConcurrent") {
    def backends = sql_return_maparray('show backends')
    def replicaNum = 0
    for (def be : backends) {
        def alive = be.Alive.toBoolean()
        def decommissioned = be.SystemDecommissioned.toBoolean()
        if (alive && !decommissioned) {
            replicaNum++
        }
    }
    if (replicaNum < 3) {
        logger.info("replicaNum=${replicaNum}, should >= 3")
    }

    GetDebugPoint().clearDebugPointsForAllFEs()
    GetDebugPoint().clearDebugPointsForAllBEs()

    try {
        def table1 = "test_mow_missing_version"
        sql "DROP TABLE IF EXISTS ${table1} FORCE;"
        sql """ CREATE TABLE IF NOT EXISTS ${table1} (
                `k` int,
                `v` int
                )UNIQUE KEY(k)
            DISTRIBUTED BY HASH(k) BUCKETS 400
            PROPERTIES (
                "disable_auto_compaction" = "true",
                "replication_num" = "${replicaNum}",
                "min_load_replica_num" = "1"); """

        GetDebugPoint().enableDebugPointForAllBEs("SnapshotManager::make_snapshot.inject_failure")


    } catch(Exception e) {
        logger.info(e.getMessage())
        throw e
    } finally {
        // GetDebugPoint().clearDebugPointsForAllFEs()
        // GetDebugPoint().clearDebugPointsForAllBEs()
    }
}
