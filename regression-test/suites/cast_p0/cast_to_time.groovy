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

suite("cast_to_time") {
    sql "set debug_skip_fold_constant = true"
    sql "set enable_strict_cast = false"

qt_sql1 """ select cast("0" as time(6)) """
qt_sql2 """ select cast("1" as time(6)) """
qt_sql3 """ select cast("123" as time(6)) """
qt_sql4 """ select cast("2005959.12" as time(6)) """
qt_sql5 """ select cast("0.12" as time(6)) """
qt_sql6 """ select cast("00:00:00.12" as time(6)) """
qt_sql7 """ select cast("123." as time(6)) """
qt_sql8 """ select cast("123.0" as time(6)) """
qt_sql9 """ select cast("123.123" as time(6)) """
qt_sql10 """ select cast("-1" as time(6)) """
qt_sql11 """ select cast("-800:05:05" as time(6)) """
qt_sql12 """ select cast("-991213.56" as time(6)) """
qt_sql13 """ select cast("80302.9999999" as time(6)) """
qt_sql14 """ select cast("5656.3000000009" as time(6)) """
qt_sql15 """ select cast("5656.3000007001" as time(6)) """
qt_sql16 """ select cast("   1   " as time(6)) """
qt_sql17 """ select cast(".123" as time(6)) """
qt_sql18 """ select cast(":12:34" as time(6)) """
qt_sql19 """ select cast("12-34:56.1" as time(6)) """
qt_sql20 """ select cast("12 : 34 : 56" as time(6)) """
qt_sql21 """ select cast("76" as time(6)) """
qt_sql22 """ select cast("200595912" as time(6)) """
qt_sql23 """ select cast("8385959.9999999" as time(6)) """

qt_sql24 """ select cast("0" as time(3)) """
qt_sql25 """ select cast("1" as time(3)) """
qt_sql26 """ select cast("123" as time(3)) """
qt_sql27 """ select cast("2005959.12" as time(3)) """
qt_sql28 """ select cast("0.12" as time(3)) """
qt_sql29 """ select cast("00:00:00.12" as time(3)) """
qt_sql30 """ select cast("123." as time(3)) """
qt_sql31 """ select cast("123.0" as time(3)) """
qt_sql32 """ select cast("123.123" as time(3)) """
qt_sql33 """ select cast("-1" as time(3)) """
qt_sql34 """ select cast("-800:05:05" as time(3)) """
qt_sql35 """ select cast("-991213.56" as time(3)) """
qt_sql36 """ select cast("80302.9999999" as time(3)) """
qt_sql37 """ select cast("5656.3000000009" as time(3)) """
qt_sql38 """ select cast("5656.3000007001" as time(3)) """
qt_sql39 """ select cast("   1   " as time(3)) """
qt_sql40 """ select cast(".123" as time(3)) """
qt_sql41 """ select cast(":12:34" as time(3)) """
qt_sql42 """ select cast("12-34:56.1" as time(3)) """
qt_sql43 """ select cast("12 : 34 : 56" as time(3)) """
qt_sql44 """ select cast("76" as time(3)) """
qt_sql45 """ select cast("200595912" as time(3)) """
qt_sql46 """ select cast("8385959.9999999" as time(3)) """

qt_sql47 """ select cast("0" as time(0)) """
qt_sql48 """ select cast("1" as time(0)) """
qt_sql49 """ select cast("123" as time(0)) """
qt_sql50 """ select cast("2005959.12" as time(0)) """
qt_sql51 """ select cast("0.12" as time(0)) """
qt_sql52 """ select cast("00:00:00.12" as time(0)) """
qt_sql53 """ select cast("123." as time(0)) """
qt_sql54 """ select cast("123.0" as time(0)) """
qt_sql55 """ select cast("123.123" as time(0)) """
qt_sql56 """ select cast("-1" as time(0)) """
qt_sql57 """ select cast("-800:05:05" as time(0)) """
qt_sql58 """ select cast("-991213.56" as time(0)) """
qt_sql59 """ select cast("80302.9999999" as time(0)) """
qt_sql60 """ select cast("5656.3000000009" as time(0)) """
qt_sql61 """ select cast("5656.3000007001" as time(0)) """
qt_sql62 """ select cast("   1   " as time(0)) """
qt_sql63 """ select cast(".123" as time(0)) """
qt_sql64 """ select cast(":12:34" as time(0)) """
qt_sql65 """ select cast("12-34:56.1" as time(0)) """
qt_sql66 """ select cast("12 : 34 : 56" as time(0)) """
qt_sql67 """ select cast("76" as time(0)) """
qt_sql68 """ select cast("200595912" as time(0)) """
qt_sql69 """ select cast("8385959.9999999" as time(0)) """

qt_sql70 """ select cast(cast(0 as int) as time(3)) """
qt_sql71 """ select cast(cast(123456 as int) as time(3)) """
qt_sql72 """ select cast(cast(-123456 as int) as time(3)) """
qt_sql73 """ select cast(cast(123 as int) as time(3)) """
qt_sql74 """ select cast(cast(6.99999 as int) as time(3)) """
qt_sql75 """ select cast(cast(-0.99 as int) as time(3)) """
qt_sql76 """ select cast(cast(8501212 as int) as time(3)) """
qt_sql77 """ select cast(cast(20001212 as int) as time(3)) """
qt_sql78 """ select cast(cast(9000000 as int) as time(3)) """
qt_sql79 """ select cast(cast(67 as int) as time(3)) """
qt_sql """ select cast(1986 as time(6)) """

qt_sql80 """ select cast(cast(0 as double) as time(3)) """
qt_sql81 """ select cast(cast(123456 as double) as time(3)) """
qt_sql82 """ select cast(cast(-123456 as double) as time(3)) """
qt_sql83 """ select cast(cast(123 as double) as time(3)) """
qt_sql84 """ select cast(cast(6.99999 as double) as time(3)) """
qt_sql85 """ select cast(cast(-0.99 as double) as time(3)) """
qt_sql86 """ select cast(cast(8501212 as double) as time(3)) """
qt_sql87 """ select cast(cast(20001212 as double) as time(3)) """
qt_sql88 """ select cast(cast(9000000 as double) as time(3)) """
qt_sql89 """ select cast(cast(67 as double) as time(3)) """

qt_sql90 """ select cast(cast(0 as decimal(27, 9)) as time(3)) """
qt_sql91 """ select cast(cast(123456 as decimal(27, 9)) as time(3)) """
qt_sql92 """ select cast(cast(-123456 as decimal(27, 9)) as time(3)) """
qt_sql93 """ select cast(cast(123 as decimal(27, 9)) as time(3)) """
qt_sql94 """ select cast(cast(6.99999 as decimal(27, 9)) as time(3)) """
qt_sql95 """ select cast(cast(-0.99 as decimal(27, 9)) as time(3)) """
qt_sql96 """ select cast(cast(8501212 as decimal(27, 9)) as time(3)) """
qt_sql97 """ select cast(cast(20001212 as decimal(27, 9)) as time(3)) """
qt_sql98 """ select cast(cast(9000000 as decimal(27, 9)) as time(3)) """
qt_sql99 """ select cast(cast(67 as decimal(27, 9)) as time(3)) """

qt_sql100 """ select cast(cast("2012-02-05 12:12:12.123456" as datetime(6)) as time(4)) """

qt_sql101 "select cast('11:12:13.123456' as time) = cast('11:12:13.12' as time)"
qt_sql102 "select cast('11:12:13.123456' as time(3)) = cast('11:12:13.12' as time(3))"
}
