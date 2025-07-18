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

#pragma once

#include <cstdint>
#include <type_traits>

#include "runtime/decimalv2_value.h"
#include "util/types.h"
#include "vec/core/extended_types.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
union TypeConverter {
    uint64_t u64;
    int64_t i64;
    uint32_t u32[2];
    int32_t i32[2];
    uint8_t u8[8];
    float flt[2];
    double dbl;
};

template <typename C0, typename C1, typename T0, typename T1>
constexpr bool match_v = std::is_same_v<C0, C1> && std::is_same_v<T0, T1>;

union DecimalInt128Union {
    DecimalV2Value decimal;
    PackedInt128 packed128;
    __int128_t i128;
};

static_assert(sizeof(DecimalV2Value) == sizeof(PackedInt128));
static_assert(sizeof(DecimalV2Value) == sizeof(__int128_t));

union VecDateTimeInt64Union {
    doris::VecDateTimeValue dt;
    __int64_t i64;
    ~VecDateTimeInt64Union() {}
};

union DateV2UInt32Union {
    DateV2Value<DateV2ValueType> dt;
    uint32_t ui32;
    ~DateV2UInt32Union() {}
};

union DateTimeV2UInt64Union {
    DateV2Value<DateTimeV2ValueType> dt;
    uint64_t ui64;
    ~DateTimeV2UInt64Union() {}
};

// similar to reinterpret_cast but won't break strict-aliasing rules. you can treat it as std::bit_cast with type checking
template <typename From, typename To>
constexpr PURE To binary_cast(const From& from) {
    constexpr bool from_u64_to_db = match_v<From, uint64_t, To, double>;
    constexpr bool from_i64_to_db = match_v<From, int64_t, To, double>;
    constexpr bool from_db_to_i64 = match_v<From, double, To, int64_t>;
    constexpr bool from_db_to_u64 = match_v<From, double, To, uint64_t>;
    constexpr bool from_i64_to_vec_dt = match_v<From, __int64_t, To, doris::VecDateTimeValue>;
    constexpr bool from_vec_dt_to_i64 = match_v<From, doris::VecDateTimeValue, To, __int64_t>;
    constexpr bool from_i128_to_decv2 = match_v<From, __int128_t, To, DecimalV2Value>;
    constexpr bool from_decv2_to_i128 = match_v<From, DecimalV2Value, To, __int128_t>;
    constexpr bool from_decv2_to_i256 = match_v<From, DecimalV2Value, To, wide::Int256>;

    constexpr bool from_ui32_to_date_v2 = match_v<From, uint32_t, To, DateV2Value<DateV2ValueType>>;

    constexpr bool from_date_v2_to_ui32 = match_v<From, DateV2Value<DateV2ValueType>, To, uint32_t>;

    constexpr bool from_ui64_to_datetime_v2 =
            match_v<From, uint64_t, To, DateV2Value<DateTimeV2ValueType>>;

    constexpr bool from_datetime_v2_to_ui64 =
            match_v<From, DateV2Value<DateTimeV2ValueType>, To, uint64_t>;

    static_assert(from_u64_to_db || from_i64_to_db || from_db_to_i64 || from_db_to_u64 ||
                  from_i64_to_vec_dt || from_vec_dt_to_i64 || from_i128_to_decv2 ||
                  from_decv2_to_i128 || from_decv2_to_i256 || from_ui32_to_date_v2 ||
                  from_date_v2_to_ui32 || from_ui64_to_datetime_v2 || from_datetime_v2_to_ui64);

    return std::bit_cast<To>(from);
}

} // namespace doris
