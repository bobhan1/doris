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


suite("test_cast_to_double_from_decimal32_9_8") {

    // This test case is generated from the correspoinding be UT test case,
    // update this case if the correspoinding be UT test case is updated,
    // e.g.: ../run-be-ut.sh --run --filter=FunctionCastToDecimalTest.* --gen_regression_case
    sql "drop table if exists test_cast_to_double_from_decimal32_9_8_0_nullable;"
    sql "create table test_cast_to_double_from_decimal32_9_8_0_nullable(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_double_from_decimal32_9_8_0_nullable values (0, "0.00000000"),(1, "0.00000000"),(2, "0.00000001"),(3, "-0.00000001"),(4, "0.00000009"),(5, "-0.00000009"),(6, "0.09999999"),(7, "-0.09999999"),(8, "0.90000000"),(9, "-0.90000000"),(10, "0.90000001"),(11, "-0.90000001"),(12, "0.99999998"),(13, "-0.99999998"),(14, "0.99999999"),(15, "-0.99999999"),(16, "1.00000000"),(17, "-1.00000000"),(18, "1.00000001"),(19, "-1.00000001"),
      (20, "1.00000009"),(21, "-1.00000009"),(22, "1.09999999"),(23, "-1.09999999"),(24, "1.90000000"),(25, "-1.90000000"),(26, "1.90000001"),(27, "-1.90000001"),(28, "1.99999998"),(29, "-1.99999998"),(30, "1.99999999"),(31, "-1.99999999"),(32, "8.00000000"),(33, "-8.00000000"),(34, "8.00000001"),(35, "-8.00000001"),(36, "8.00000009"),(37, "-8.00000009"),(38, "8.09999999"),(39, "-8.09999999"),
      (40, "8.90000000"),(41, "-8.90000000"),(42, "8.90000001"),(43, "-8.90000001"),(44, "8.99999998"),(45, "-8.99999998"),(46, "8.99999999"),(47, "-8.99999999"),(48, "9.00000000"),(49, "-9.00000000"),(50, "9.00000001"),(51, "-9.00000001"),(52, "9.00000009"),(53, "-9.00000009"),(54, "9.09999999"),(55, "-9.09999999"),(56, "9.90000000"),(57, "-9.90000000"),(58, "9.90000001"),(59, "-9.90000001"),
      (60, "9.99999998"),(61, "-9.99999998"),(62, "9.99999999"),(63, "-9.99999999")
      ,(64, null);
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as double) from test_cast_to_double_from_decimal32_9_8_0_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as double) from test_cast_to_double_from_decimal32_9_8_0_nullable order by 1;'

    sql "drop table if exists test_cast_to_double_from_decimal32_9_8_0_not_nullable;"
    sql "create table test_cast_to_double_from_decimal32_9_8_0_not_nullable(f1 int, f2 decimalv3(9, 8)) properties('replication_num'='1');"
    sql """insert into test_cast_to_double_from_decimal32_9_8_0_not_nullable values (0, "0.00000000"),(1, "0.00000000"),(2, "0.00000001"),(3, "-0.00000001"),(4, "0.00000009"),(5, "-0.00000009"),(6, "0.09999999"),(7, "-0.09999999"),(8, "0.90000000"),(9, "-0.90000000"),(10, "0.90000001"),(11, "-0.90000001"),(12, "0.99999998"),(13, "-0.99999998"),(14, "0.99999999"),(15, "-0.99999999"),(16, "1.00000000"),(17, "-1.00000000"),(18, "1.00000001"),(19, "-1.00000001"),
      (20, "1.00000009"),(21, "-1.00000009"),(22, "1.09999999"),(23, "-1.09999999"),(24, "1.90000000"),(25, "-1.90000000"),(26, "1.90000001"),(27, "-1.90000001"),(28, "1.99999998"),(29, "-1.99999998"),(30, "1.99999999"),(31, "-1.99999999"),(32, "8.00000000"),(33, "-8.00000000"),(34, "8.00000001"),(35, "-8.00000001"),(36, "8.00000009"),(37, "-8.00000009"),(38, "8.09999999"),(39, "-8.09999999"),
      (40, "8.90000000"),(41, "-8.90000000"),(42, "8.90000001"),(43, "-8.90000001"),(44, "8.99999998"),(45, "-8.99999998"),(46, "8.99999999"),(47, "-8.99999999"),(48, "9.00000000"),(49, "-9.00000000"),(50, "9.00000001"),(51, "-9.00000001"),(52, "9.00000009"),(53, "-9.00000009"),(54, "9.09999999"),(55, "-9.09999999"),(56, "9.90000000"),(57, "-9.90000000"),(58, "9.90000001"),(59, "-9.90000001"),
      (60, "9.99999998"),(61, "-9.99999998"),(62, "9.99999999"),(63, "-9.99999999");
    """

    sql "set enable_strict_cast=true;"
    qt_sql_0_strict 'select f1, cast(f2 as double) from test_cast_to_double_from_decimal32_9_8_0_not_nullable order by 1;'

    sql "set enable_strict_cast=false;"
    qt_sql_0_non_strict 'select f1, cast(f2 as double) from test_cast_to_double_from_decimal32_9_8_0_not_nullable order by 1;'

}