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

suite("regression_test_variant_nested", "p0"){
    def backendId_to_backendIP = [:]
    def backendId_to_backendHttpPort = [:]
    getBackendIpHttpPort(backendId_to_backendIP, backendId_to_backendHttpPort);

    try {

        def table_name = "var_nested"
        sql "DROP TABLE IF EXISTS ${table_name}"
        sql "set disable_variant_flatten_nested = false"
        sql """
                CREATE TABLE IF NOT EXISTS ${table_name} (
                    k bigint,
                    v variant
                )
                DUPLICATE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 4
                properties("replication_num" = "1", "disable_auto_compaction" = "false", "variant_enable_flatten_nested" = "true");
            """
        sql """
            insert into var_nested values (1, '{"xx" : 10}');
            insert into var_nested values (2, '{"nested": [{"ba" : "11111"},{"a" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (3, '{"xx" : 10}');
            insert into var_nested values (4, '{"nested": [{"baaa" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (5, '{"nested": [{"ba" : "11111"},{"a" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (6, '{"nested": [{"mmm" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (7, '{"nested": [{"ba" : "11111"},{"a" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (8, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (9, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (10, '{"xx" : 10}');
            insert into var_nested values (11, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (12, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (13, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (14, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (15, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (16, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (17, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (18, '{"xx" : 10}');
            insert into var_nested values (19, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (20, '{"nested": [{"yyy" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (21, '{"nested": [{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
            insert into var_nested values (22, '{"nested": [{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}, {"zzz11" : "123333"}]}');
            insert into var_nested values (23, '{"nested": [{"yyyxxxx" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}');
        """

        sql """
            insert into var_nested values (24, '{"xx" : 10}');
            insert into var_nested values (25, '{"nested":{"nested": [{"ba" : "11111"},{"a" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}}');
            insert into var_nested values (26, '{"xx" : 10}');
            insert into var_nested values (27, '{"nested" : {"nested": [{"yyyxxxx" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}}');
            insert into var_nested values (28, '{"nested" : {"nested": [{"yyyxxxx" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}, "not nested" : 1024, "not nested2" : {"llll" : 123}}');
        """
        sql """select * from var_nested limit 1"""
        sql """set describe_extend_variant_column = true"""
        qt_sql """DESC var_nested"""
        qt_sql """
            select * from var_nested order by k limit 101
        """
        for (int i = 101; i < 121; ++i) {
            sql """insert into var_nested values (${i}, '{"nested${i}" : {"nested": [{"yyyxxxx" : "11111"},{"ax1111" : "1111"},{"axxxb": 100, "xxxy111": 111}, {"ddsss":1024, "aaa" : "11"}, {"xx" : 10}]}, "not nested" : 1024, "not nested2" : {"llll" : 123}}');"""
        }

        trigger_and_wait_compaction("var_nested", "full")

        qt_sql """
            select * from var_nested order by k limit 101
        """
        sql """INSERT INTO var_nested SELECT *, '{"k1":1, "k2": "some", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]], "nested1" : {"nested2" : [{"a" : 10, "b" : 1.1, "c" : "1111"}]}}' FROM numbers("number" = "1000") where number > 200 limit 100;"""
        sql """INSERT INTO var_nested SELECT *, '{"k2":1, "k3": "nice", "k4" : [1234], "k5" : 1.10000, "k6" : [[123]], "nested2" : {"nested1" : [{"a" : 10, "b" : 1.1, "c" : "1111"}]}}' FROM numbers("number" = "5013") where number >= 400 limit 1024;"""
        trigger_and_wait_compaction("var_nested", "full")

        qt_sql """select  /*+SET_VAR(batch_size=1024,broker_load_batch_size=16352,disable_streaming_preaggregations=true,enable_distinct_streaming_aggregation=true,parallel_fragment_exec_
parallel_pipeline_task_num=7,profile_level=1,enable_pipeline_engine=true,enable_parallel_scan=false,parallel_scan_max_scanners_count=16
,parallel_scan_min_rows_per_scanner=128,enable_fold_constant_by_be=false,enable_rewrite_element_at_to_slot=true,runtime_filter_type=2,enable_parallel_result_sink=true,sort_phase_num=0,enable_nereids_planner=true,rewrite_or_to_in_predicate_threshold=2,enable_function_pushdown=true,enable_common_expr_pushdown=false,enable_local_exchange=true,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=false,enable_two_phase_read_opt=true,enable_common_expr_pushdown_for_inverted_index=true,spill_min_revocable_mem=1,fetch_remote_schema_timeout_seconds=120,max_fetch_remote_schema_tablet_count=512,enable_spill=true,enable_force_spill=false,data_queue_max_blocks=1,spill_streaming_agg_mem_limit=268435456,spill_aggregation_partition_count=5) */ * from var_nested where v['k2'] = 'some' order by k limit 10"""
        qt_sql """select * from var_nested where v['k2'] = 'some'  and array_contains(cast(v['nested1']['nested2']['a'] as array<tinyint>), 10) order by k limit 1;"""

        sql """INSERT INTO var_nested SELECT *, '{"k1":1, "k2": "some", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]], "nested1" : {"nested2" : [{"a" : 10, "b" : 1.1, "c" : "1111"}]}}' FROM numbers("number" = "4096") where number > 1024 limit 1024;"""
        sql """INSERT INTO var_nested SELECT *, '{"k1":1, "k2": "what", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]], "nested1" : {"nested2" : [{"a" : 10, "b" : 1.1, "c" : "1111"}]}}' FROM numbers("number" = "4096") where number > 2048 limit 1024;"""
        sql """INSERT INTO var_nested SELECT *, '{"k1":1, "k2": "about", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]], "nested1" : {"nested2" : [{"a" : 10, "b" : 1.1, "c" : "1111"}]}}' FROM numbers("number" = "4096") where number > 3072 limit 1024;"""
        sql """INSERT INTO var_nested SELECT *, '{"k1":1, "k2": "nested", "k3" : [1234], "k4" : 1.10000, "k5" : [[123]], "nested1" : {"nested2" : [{"a" : 10, "b" : 1.1, "c" : "1111"}]}}' FROM numbers("number" = "6000") where number > 4096 limit 1024;"""
        qt_sql """select * from var_nested where v['k2'] = 'what' order by k limit 10"""
        qt_sql """select * from var_nested where v['k2'] = 'some'  and array_contains(cast(v['nested1']['nested2']['a'] as array<tinyint>), 10) order by k limit 1;"""

        // type change case
        sql """INSERT INTO var_nested SELECT *, '{"k1":"1", "k2": 1.1, "k3" : [1234.0], "k4" : 1.10000, "k5" : [["123"]], "nested1" : {"nested2" : [{"a" : "10", "b" : "1.1", "c" : 1111.111}]}}' FROM numbers("number" = "8000") where number > 7000 limit 100;"""
        qt_sql """select * from var_nested where v['k2'] = 'what'  and array_contains(cast(v['nested1']['nested2']['a'] as array<tinyint>), 10) order by k limit 1;"""
        trigger_and_wait_compaction("var_nested", "full")
        qt_sql """select * from var_nested where v['k2'] = 'nested'  and array_contains(cast(v['nested1']['nested2']['a'] as array<tinyint>), 10) order by k limit 1;"""
        sql """select * from var_nested where v['k2'] = 'some' or v['k3'] = 'nice' limit 100;"""

        // insert into select
        sql "DROP TABLE IF EXISTS var_nested2"
        sql """
                CREATE TABLE IF NOT EXISTS var_nested2 (
                    k bigint,
                    v variant
                )
                UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                properties("replication_num" = "1", "disable_auto_compaction" = "false", "enable_unique_key_merge_on_write" = "true", "variant_enable_flatten_nested" = "true");
            """
        sql """insert into var_nested2 select * from var_nested order by k limit 1024"""
        qt_sql """select  /*+SET_VAR(batch_size=4064,broker_load_batch_size=16352,disable_streaming_preaggregations=true,enable_distinct_streaming_aggregation=true,parallel_pipeline_task_num=1,profile_level=1,enable_pipeline_engine=false,enable_parallel_scan=true,parallel_scan_max_scanners_count=48,parallel_scan_min_rows_per_scanner=16384,enable_fold_constant_by_be=true,enable_rewrite_element_at_to_slot=true,runtime_filter_type=12,enable_parallel_result_sink=false,enable_nereids_planner=true,rewrite_or_to_in_predicate_threshold=2,enable_function_pushdown=true,enable_common_expr_pushdown=false,enable_local_exchange=false,partition_pruning_expand_threshold=10,enable_share_hash_table_for_broadcast_join=false,enable_two_phase_read_opt=true,enable_common_expr_pushdown_for_inverted_index=true,spill_min_revocable_mem=33554432,fetch_remote_schema_timeout_seconds=120,max_fetch_remote_schema_tablet_count=512,enable_spill=false,enable_force_spill=false,data_queue_max_blocks=1,spill_streaming_agg_mem_limit=268435456,spill_aggregation_partition_count=5) */  * from var_nested2 order by k limit 10;"""
        qt_sql """select v['nested'] from var_nested2 where k < 10 order by k limit 10;"""
        // 0. nomal explode variant array
        order_qt_explode_sql """select count(),cast(vv['xx'] as int) from var_nested lateral view explode_variant_array(v['nested']) tmp as vv where vv['xx'] = 10 group by cast(vv['xx'] as int)"""
        sql """truncate table var_nested2"""
        sql """insert into var_nested2 values(1119111, '{"eventId":1,"firstName":"Name1","lastName":"Surname1","body":{"phoneNumbers":[{"number":"5550219210","type":"GSM","callLimit":5},{"number":"02124713252","type":"HOME","callLimit":3},{"number":"05550219211","callLimit":2,"type":"WORK"}]}}
')"""
        order_qt_explode_sql """select v['eventId'], phone_numbers from var_nested2 lateral view explode_variant_array(v['body']['phoneNumbers']) tmp1 as phone_numbers
where phone_numbers['type'] = 'GSM' OR phone_numbers['type'] = 'HOME' and phone_numbers['callLimit'] > 2;"""

        // test array_function
        sql "DROP TABLE IF EXISTS var_nested_array_agg"
        sql """
                CREATE TABLE IF NOT EXISTS var_nested_array_agg(
                    k bigint,
                    v variant
                )
                UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                properties("replication_num" = "1", "disable_auto_compaction" = "false", "enable_unique_key_merge_on_write" = "true", "variant_enable_flatten_nested" = "true");
            """
        sql "insert into var_nested_array_agg select * from var_nested"
        // 1. array_contains
        qt_sql "select * from var_nested_array_agg where array_contains(cast(v['nested']['xx'] as array<int>), 10) order by k limit 10"
        // 2. array_agg scalar
        sql "select k, array_agg(cast(v['nested'] as text))  from var_nested_array_agg group by k limit 10"

        // test explode_variant_array with abonomal case
        sql "DROP TABLE IF EXISTS var_nested_explode_variant_with_abnomal"
        sql """
                CREATE TABLE IF NOT EXISTS var_nested_explode_variant_with_abnomal(
                    k bigint,
                    v variant
                )
                UNIQUE KEY(`k`)
                DISTRIBUTED BY HASH(k) BUCKETS 1
                properties("replication_num" = "1", "disable_auto_compaction" = "false", "enable_unique_key_merge_on_write" = "true", "variant_enable_flatten_nested" = "true");
            """
        sql "insert into var_nested_explode_variant_with_abnomal select * from var_nested"
        // 1. v['nested']['x'] is null root
        order_qt_explode_sql """select count(),cast(vv as int) from var_nested_explode_variant_with_abnomal lateral view explode_variant_array(v['nested']['x']) tmp as vv where vv = 10 group by cast(vv as int)"""
        // 2. v['nested']['xx'] is normal array
        order_qt_explode_sql """select count(),cast(vv as int) from var_nested_explode_variant_with_abnomal lateral view explode_variant_array(v['nested']['xx']) tmp as vv where vv = 10 group by cast(vv as int)"""
        // 3. v['xx'] is none array scalar type
        test {
            sql """select count(),cast(vv as int) from var_nested_explode_variant_with_abnomal lateral view explode_variant_array(v['xx']) tmp as vv where vv = 10 group by cast(vv as int)"""
            exception("explode not support none array type")
        }
        // 4. v['k1'] is json scalar type
        test {
            sql """select count(),cast(vv as int) from var_nested_explode_variant_with_abnomal lateral view explode_variant_array(v['k1']) tmp as vv where vv = 10 group by cast(vv as int)"""
            exception("explode not support none array type")
        }
        // 5. toplevel nested array
        sql "truncate table var_nested_explode_variant_with_abnomal"
        sql """insert into var_nested_explode_variant_with_abnomal values(1, '[{"a" : 10}, {"b" : "20", "c" :1024, "a" : 11}]')"""
        sql """insert into var_nested_explode_variant_with_abnomal values(2, '[{"a" : 10}, {"b" : "20", "a" : 150}]')"""
        order_qt_explode_sql """select count(),cast(vv as int) from var_nested_explode_variant_with_abnomal lateral view explode_variant_array(v['a']) tmp as vv where vv = 10 group by cast(vv as int)"""
        // FIXME after refator
        // order_qt_explode_sql """select count(),cast(vv as int) from var_nested_explode_variant_with_abnomal lateral view explode_variant_array(v) tmp as vv where vv['a'] = 10 group by cast(vv as int)"""
    } finally {
        // reset flags
    }

}
