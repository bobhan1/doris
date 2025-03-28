package mv.same_name
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

suite("sync_async_same_name") {
    String db = context.config.getDbNameByFile(context.file)
    sql "use ${db}"
    sql "set runtime_filter_mode=OFF";
    sql "SET ignore_shape_nodes='PhysicalDistribute,PhysicalProject'"

    sql """
    drop table if exists orders
    """

    sql """
    CREATE TABLE IF NOT EXISTS orders  (
      o_orderkey       INTEGER NOT NULL,
      o_custkey        INTEGER NOT NULL,
      o_orderstatus    CHAR(1) NOT NULL,
      o_totalprice     DECIMALV3(15,2) NOT NULL,
      o_orderdate      DATE NOT NULL,
      o_orderpriority  CHAR(15) NOT NULL,  
      o_clerk          CHAR(15) NOT NULL, 
      o_shippriority   INTEGER NOT NULL,
      O_COMMENT        VARCHAR(79) NOT NULL
    )
    DUPLICATE KEY(o_orderkey, o_custkey)
    DISTRIBUTED BY HASH(o_orderkey) BUCKETS 3
    PROPERTIES (
      "replication_num" = "1"
    );
    """

    sql """
    insert into orders values
    (1, 1, 'o', 9.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (1, 1, 'o', 10.5, '2023-12-08', 'a', 'b', 1, 'yy'),
    (2, 1, 'o', 11.5, '2023-12-09', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 12.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (3, 1, 'o', 33.5, '2023-12-10', 'a', 'b', 1, 'yy'),
    (4, 2, 'o', 43.2, '2023-12-11', 'c','d',2, 'mm'),
    (5, 2, 'o', 56.2, '2023-12-12', 'c','d',2, 'mi'),
    (5, 2, 'o', 1.2, '2023-12-12', 'c','d',2, 'mi');  
    """

    sql """alter table orders modify column O_COMMENT set stats ('row_count'='8');"""

    sql """analyze table orders with sync;"""

    def common_mv_name = 'common_mv_name'

    def mtmv_sql = """
            select o_orderdate, o_shippriority, o_comment,
            sum(o_totalprice) as sum_total,
            max(o_totalprice) as max_total,
            min(o_totalprice) as min_total,
            count(*) as count_all,
            bitmap_union(to_bitmap(case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end)) cnt_1,
            bitmap_union(to_bitmap(case when o_shippriority > 2 and o_orderkey IN (2) then o_custkey else null end)) as cnt_2
            from orders
            group by
            o_orderdate,
            o_shippriority,
            o_comment;
    """
    def mtmv_query = """
            select o_shippriority, o_comment,
            count(distinct case when o_shippriority > 1 and o_orderkey IN (1, 3) then o_custkey else null end) as cnt_1,
            count(distinct case when O_SHIPPRIORITY > 2 and o_orderkey IN (2) then o_custkey else null end) as cnt_2,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
            from orders
            where o_orderdate = '2023-12-09'
            group by
            o_shippriority,
            o_comment;
            """

    def mv_query = """
            select o_shippriority, o_comment,
            sum(o_totalprice),
            max(o_totalprice),
            min(o_totalprice),
            count(*)
            from orders
            group by
            o_shippriority,
            o_comment;
            """


    order_qt_query_mv_before "${mv_query}"
    order_qt_query_mtmv_before "${mtmv_query}"


    // create sync mv
    sql """drop materialized view if exists ${common_mv_name} on orders;"""
    createMV ("create materialized view ${common_mv_name} as ${mv_query};")

    // create async mv
    create_async_mv(db, common_mv_name, mtmv_sql)


    // only async mv rewrite successfully
    mv_rewrite_success_without_check_chosen(mtmv_query, common_mv_name)

    // both sync and async mv rewrite successfully
    mv_rewrite_all_success_without_check_chosen(mv_query, [common_mv_name, "orders.${common_mv_name}"])


    order_qt_query_mv_after "${mv_query}"
    order_qt_query_mtmv_after "${mtmv_query}"

    sql """DROP MATERIALIZED VIEW IF EXISTS ${common_mv_name}"""
    sql """drop materialized view if exists ${common_mv_name} on orders;"""
}
