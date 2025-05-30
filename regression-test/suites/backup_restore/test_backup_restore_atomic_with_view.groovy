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

suite("test_backup_restore_atomic_with_view", "backup_restore") {
    String suiteName = "backup_restore_atomic_with_view"
    String dbName = "${suiteName}_db"
    String dbName1 = "${suiteName}_db_1"
    String repoName = "${suiteName}_repo_" + UUID.randomUUID().toString().replace("-", "")
    String snapshotName = "${suiteName}_snapshot"
    String tableName = "${suiteName}_table"
    String viewName = "${suiteName}_view"

    def syncer = getSyncer()
    syncer.createS3Repository(repoName)
    sql "CREATE DATABASE IF NOT EXISTS ${dbName}"
    sql "CREATE DATABASE IF NOT EXISTS ${dbName1}"

    int numRows = 10;
    sql "DROP TABLE IF EXISTS ${dbName}.${tableName} FORCE"
    sql "DROP VIEW IF EXISTS ${dbName}.${viewName}"
    sql """
        CREATE TABLE ${dbName}.${tableName} (
            `id` LARGEINT NOT NULL,
            `count` LARGEINT SUM DEFAULT "0"
        )
        AGGREGATE KEY(`id`)
        DISTRIBUTED BY HASH(`id`) BUCKETS 2
        PROPERTIES
        (
            "replication_num" = "1"
        )
        """
    List<String> values = []
    for (int j = 1; j <= numRows; ++j) {
        values.add("(${j}, ${j})")
    }
    sql "INSERT INTO ${dbName}.${tableName} VALUES ${values.join(",")}"

    sql """CREATE VIEW ${dbName}.${viewName} (id, count)
        AS
        SELECT * FROM ${dbName}.${tableName} WHERE count > 5
        """

    sql "sync"
    qt_sql "SELECT * FROM ${dbName}.${tableName} ORDER BY id ASC"
    qt_sql "SELECT * FROM ${dbName}.${viewName} ORDER BY id ASC"

    sql """
        BACKUP SNAPSHOT ${dbName}.${snapshotName}
        TO `${repoName}`
    """

    syncer.waitSnapshotFinish(dbName)

    def snapshot = syncer.getSnapshotTimestamp(repoName, snapshotName)
    assertTrue(snapshot != null)

    // restore new view
    sql "DROP TABLE IF EXISTS ${dbName1}.${tableName} FORCE"
    sql "DROP VIEW IF EXISTS ${dbName1}.${viewName}"

    sql """
        RESTORE SNAPSHOT ${dbName1}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "atomic_restore" = "true",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName1)

    sql "sync"
    qt_sql "SELECT * FROM ${dbName1}.${tableName} ORDER BY id ASC"
    qt_sql "SELECT * FROM ${dbName1}.${viewName} ORDER BY id ASC"
    def show_view_result = sql_return_maparray "SHOW VIEW FROM ${tableName} FROM ${dbName1}"
    logger.info("show view result: ${show_view_result}")
    assertTrue(show_view_result.size() == 1);
    def show_view = show_view_result[0]['Create View']
    assertTrue(show_view.contains("${dbName1}"))
    assertTrue(show_view.contains("${tableName}"))

    // restore an exists view
    sql """
        RESTORE SNAPSHOT ${dbName}.${snapshotName}
        FROM `${repoName}`
        PROPERTIES
        (
            "backup_timestamp" = "${snapshot}",
            "atomic_restore" = "true",
            "reserve_replica" = "true"
        )
    """

    syncer.waitAllRestoreFinish(dbName)
    def restore_result = sql_return_maparray """ SHOW RESTORE FROM ${dbName} WHERE Label ="${snapshotName}" """
    restore_result.last()
    logger.info("show restore result: ${restore_result}")
    assertTrue(restore_result.last().State == "FINISHED")

    // View could read the incremental data.
    sql "INSERT INTO ${dbName}.${tableName} VALUES (11, 11)"

    sql "sync"
    qt_sql "SELECT * FROM ${dbName}.${tableName} ORDER BY id ASC"
    qt_sql "SELECT * FROM ${dbName}.${viewName} ORDER BY id ASC"

    sql "DROP REPOSITORY `${repoName}`"
}


