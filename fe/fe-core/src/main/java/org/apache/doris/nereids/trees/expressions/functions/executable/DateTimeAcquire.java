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

package org.apache.doris.nereids.trees.expressions.functions.executable;

import org.apache.doris.nereids.trees.expressions.ExecFunction;
import org.apache.doris.nereids.trees.expressions.Expression;
import org.apache.doris.nereids.trees.expressions.literal.DateLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeLiteral;
import org.apache.doris.nereids.trees.expressions.literal.DateTimeV2Literal;
import org.apache.doris.nereids.trees.expressions.literal.IntegerLiteral;
import org.apache.doris.nereids.trees.expressions.literal.TimeV2Literal;
import org.apache.doris.nereids.util.DateUtils;

import java.time.LocalDateTime;
import java.time.ZoneId;

/**
 * executable functions:
 * unclassified date function
 */
public class DateTimeAcquire {
    /**
     * date acquire function: now
     */
    @ExecFunction(name = "now")
    public static Expression now() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), 0);
    }

    @ExecFunction(name = "now")
    public static Expression now(IntegerLiteral precision) {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()),
                precision.getValue());
    }

    /**
     * date acquire function: current_timestamp
     */
    @ExecFunction(name = "current_timestamp")
    public static Expression currentTimestamp() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), 0);
    }

    @ExecFunction(name = "current_timestamp")
    public static Expression currentTimestamp(IntegerLiteral precision) {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), precision.getValue());
    }

    /**
     * date acquire function: localtime/localtimestamp
     */
    @ExecFunction(name = "localtime")
    public static Expression localTime() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), 0);
    }

    @ExecFunction(name = "localtimestamp")
    public static Expression localTimestamp() {
        return DateTimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()), 0);
    }

    /**
     * date acquire function: current_date
     */
    @ExecFunction(name = "curdate")
    public static Expression curDate() {
        return DateLiteral.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    @ExecFunction(name = "current_date")
    public static Expression currentDate() {
        return DateLiteral.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    /**
     * date acquire function: current_time
     */
    @ExecFunction(name = "curtime")
    public static Expression curTime() {
        return TimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    @ExecFunction(name = "current_time")
    public static Expression currentTime() {
        return TimeV2Literal.fromJavaDateType(LocalDateTime.now(DateUtils.getTimeZone()));
    }

    /**
     * date transformation function: unix_timestamp
     */
    @ExecFunction(name = "unix_timestamp")
    public static Expression unixTimestamp() {
        return new IntegerLiteral((int) (System.currentTimeMillis() / 1000L));
    }

    /**
     * date transformation function: utc_timestamp
     */
    @ExecFunction(name = "utc_timestamp")
    public static Expression utcTimestamp() {
        return DateTimeLiteral.fromJavaDateType(LocalDateTime.now(ZoneId.of("UTC+0")));
    }
}
