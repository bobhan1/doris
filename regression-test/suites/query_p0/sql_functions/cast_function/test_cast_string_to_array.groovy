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

suite("test_cast_string_to_array", "nonConcurrent") {
    // cast string to array<int>
    qt_sql """ select cast ("[1,2,3]" as array<int>) """

    // cast string to array<string>
    qt_sql """ select cast ("['a','b','c']" as array<string>) """

    // cast string to array<double>
    qt_sql """ select cast ("[1.34,2.001]" as array<double>) """

    // cast string to array<decimal>
    qt_sql """ select cast ("[1.34,2.001]" as array<decimalv3(10, 3)>) """

    // cast string to array<date>
    qt_sql """ select cast ("[2022-09-01]" as array<date>) """

    // cast string to array<datev2>
    qt_sql """ select cast ("[2022-09-01]" as array<datev2>) """

    // cast string to array<datetimev2>
    qt_sql """ select cast ("[2022-09-01]" as array<datetimev2>) """

    // cast empty value
    qt_sql """ select cast ("[1,2,3,,,]" as array<int>) """
    qt_sql """ select cast ("[a,b,c,,,]" as array<string>) """
    qt_sql """ select cast ("[1.34,2.01,,,]" as array<decimalv3(10, 3)>) """

    sql """ ADMIN SET FRONTEND CONFIG ("enable_date_conversion" = "false"); """
    qt_sql """ select cast ("[2022-09-01,,]" as array<date>) """
    qt_sql """ select cast ("[2022-09-01,,]" as array<string>) """
    qt_sql """ select cast(cast ("[2022-09-01,,]" as array<string>) as array<date>) """

    sql """ ADMIN SET FRONTEND CONFIG ("enable_date_conversion" = "true"); """
    qt_sql """ select cast ("[2022-09-01,,]" as array<date>) """
    qt_sql """ select cast ("[2022-09-01,,]" as array<string>) """
    qt_sql """ select cast(cast ("[2022-09-01,,]" as array<string>) as array<date>) """
}
