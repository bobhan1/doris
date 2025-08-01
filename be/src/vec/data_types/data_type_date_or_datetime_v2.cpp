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

#include "vec/data_types/data_type_date_or_datetime_v2.h"

#include <gen_cpp/data.pb.h>

#include <memory>
#include <ostream>
#include <string>
#include <typeinfo>
#include <utility>

#include "util/binary_cast.hpp"
#include "vec/columns/column_const.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/string_buffer.hpp"
#include "vec/core/types.h"
#include "vec/io/io_helper.h"
#include "vec/io/reader_buffer.h"
#include "vec/runtime/vdatetime_value.h"

namespace doris {
#include "common/compile_check_begin.h"
namespace vectorized {
class IColumn;
} // namespace vectorized
} // namespace doris

// FIXME: This file contains widespread UB due to unsafe type-punning casts.
//        These must be properly refactored to eliminate reliance on reinterpret-style behavior.
//
// Temporarily suppress GCC 15+ warnings on user-defined type casts to allow build to proceed.
#if defined(__GNUC__) && (__GNUC__ >= 15)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wcast-user-defined"
#endif

namespace doris::vectorized {
bool DataTypeDateV2::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

size_t DataTypeDateV2::number_length() const {
    //2024-01-01
    return 10;
}
void DataTypeDateV2::push_number(ColumnString::Chars& chars, const UInt32& num) const {
    DateV2Value<DateV2ValueType> val = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(num);

    char buf[64];
    char* pos = val.to_string(buf);
    // DateTime to_string the end is /0
    chars.insert(buf, pos - 1);
}

std::string DataTypeDateV2::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    UInt32 int_val = assert_cast<const ColumnDateV2&>(*ptr).get_element(row_num);
    DateV2Value<DateV2ValueType> val = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(int_val);

    char buf[64];
    val.to_string(buf); // DateTime to_string the end is /0
    return std::string {buf};
}

std::string DataTypeDateV2::to_string(UInt32 int_val) const {
    DateV2Value<DateV2ValueType> val = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(int_val);

    char buf[64];
    val.to_string(buf); // DateTime to_string the end is /0
    return std::string {buf};
}

void DataTypeDateV2::to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    UInt32 int_val = assert_cast<const ColumnDateV2&>(*ptr).get_element(row_num);
    DateV2Value<DateV2ValueType> val = binary_cast<UInt32, DateV2Value<DateV2ValueType>>(int_val);

    char buf[64];
    char* pos = val.to_string(buf);
    // DateTime to_string the end is /0
    ostr.write(buf, pos - buf - 1);
}

Status DataTypeDateV2::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = assert_cast<ColumnDateV2*>(column);
    UInt32 val = 0;
    if (!read_date_v2_text_impl<UInt32>(val, rb)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data->insert_value(val);
    return Status::OK();
}

MutableColumnPtr DataTypeDateV2::create_column() const {
    return DataTypeNumberBase<PrimitiveType::TYPE_DATEV2>::create_column();
}

void DataTypeDateV2::cast_to_date_time(const UInt32 from, Int64& to) {
    auto& to_value = (VecDateTimeValue&)to;
    auto& from_value = (DateV2Value<DateV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATETIME);
}

void DataTypeDateV2::cast_to_date(const UInt32 from, Int64& to) {
    auto& to_value = (VecDateTimeValue&)(to);
    auto& from_value = (DateV2Value<DateV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATE);
}

void DataTypeDateV2::cast_to_date_time_v2(const UInt32 from, UInt64& to) {
    to = ((UInt64)from) << TIME_PART_LENGTH;
}

void DataTypeDateV2::cast_from_date(const Int64 from, UInt32& to) {
    auto& to_value = (DateV2Value<DateV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.unchecked_set_time(from_value.year(), from_value.month(), from_value.day(), 0, 0, 0,
                                0);
}

void DataTypeDateV2::cast_from_date_time(const Int64 from, UInt32& to) {
    auto& to_value = (DateV2Value<DateV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.unchecked_set_time(from_value.year(), from_value.month(), from_value.day(), 0, 0, 0,
                                0);
}

bool DataTypeDateTimeV2::equals(const IDataType& rhs) const {
    return typeid(rhs) == typeid(*this);
}

std::string DataTypeDateTimeV2::to_string(const IColumn& column, size_t row_num) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    UInt64 int_val = assert_cast<const ColumnDateTimeV2&>(*ptr).get_element(row_num);
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);

    char buf[64];
    val.to_string(buf, _scale);
    return buf; // DateTime to_string the end is /0
}

std::string DataTypeDateTimeV2::to_string(UInt64 int_val) const {
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);

    char buf[64];
    val.to_string(buf, _scale);
    return buf; // DateTime to_string the end is /0
}

size_t DataTypeDateTimeV2::number_length() const {
    //2024-01-01 00:00:00-000000
    return 32;
}
void DataTypeDateTimeV2::push_number(ColumnString::Chars& chars, const UInt64& num) const {
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(num);
    char buf[64];
    char* pos = val.to_string(buf, _scale);
    chars.insert(buf, pos - 1);
}

void DataTypeDateTimeV2::to_string(const IColumn& column, size_t row_num,
                                   BufferWritable& ostr) const {
    auto result = check_column_const_set_readability(column, row_num);
    ColumnPtr ptr = result.first;
    row_num = result.second;

    UInt64 int_val = assert_cast<const ColumnDateTimeV2&>(*ptr).get_element(row_num);
    DateV2Value<DateTimeV2ValueType> val =
            binary_cast<UInt64, DateV2Value<DateTimeV2ValueType>>(int_val);

    char buf[64];
    char* pos = val.to_string(buf, _scale);
    ostr.write(buf, pos - buf - 1);
}

Status DataTypeDateTimeV2::from_string(ReadBuffer& rb, IColumn* column) const {
    auto* column_data = assert_cast<ColumnDateTimeV2*>(column);
    UInt64 val = 0;
    if (!read_datetime_v2_text_impl<UInt64>(val, rb, _scale)) {
        return Status::InvalidArgument("parse date fail, string: '{}'",
                                       std::string(rb.position(), rb.count()).c_str());
    }
    column_data->insert_value(val);
    return Status::OK();
}

void DataTypeDateTimeV2::to_pb_column_meta(PColumnMeta* col_meta) const {
    IDataType::to_pb_column_meta(col_meta);
    col_meta->mutable_decimal_param()->set_scale(_scale);
}

MutableColumnPtr DataTypeDateTimeV2::create_column() const {
    return DataTypeNumberBase<PrimitiveType::TYPE_DATETIMEV2>::create_column();
}

void DataTypeDateTimeV2::cast_to_date_time(const UInt64 from, Int64& to) {
    auto& to_value = (VecDateTimeValue&)to;
    auto& from_value = (DateV2Value<DateTimeV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATETIME);
}

void DataTypeDateTimeV2::cast_to_date(const UInt64 from, Int64& to) {
    auto& to_value = (VecDateTimeValue&)(to);
    auto& from_value = (DateV2Value<DateTimeV2ValueType>&)from;
    to_value.create_from_date_v2(from_value, TimeType::TIME_DATE);
}

void DataTypeDateTimeV2::cast_from_date(const Int64 from, UInt64& to) {
    auto& to_value = (DateV2Value<DateTimeV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.unchecked_set_time(from_value.year(), from_value.month(), from_value.day(),
                                from_value.hour(), from_value.minute(), from_value.second(), 0);
}

void DataTypeDateTimeV2::cast_from_date_time(const Int64 from, UInt64& to) {
    auto& to_value = (DateV2Value<DateTimeV2ValueType>&)(to);
    auto from_value = binary_cast<Int64, VecDateTimeValue>(from);
    to_value.unchecked_set_time(from_value.year(), from_value.month(), from_value.day(),
                                from_value.hour(), from_value.minute(), from_value.second(), 0);
}

FieldWithDataType DataTypeDateTimeV2::get_field_with_data_type(const IColumn& column,
                                                               size_t row_num) const {
    const auto& column_data =
            assert_cast<const ColumnDateTimeV2&, TypeCheckOnRelease::DISABLE>(column);
    Field field;
    column_data.get(row_num, field);
    return FieldWithDataType {.field = std::move(field),
                              .base_scalar_type_id = get_primitive_type(),
                              .precision = -1,
                              .scale = static_cast<int>(get_scale())};
}

void DataTypeDateTimeV2::cast_to_date_v2(const UInt64 from, UInt32& to) {
    to = from >> TIME_PART_LENGTH;
}

DataTypePtr create_datetimev2(UInt64 scale_value) {
    if (scale_value > 6) {
        throw doris::Exception(doris::ErrorCode::NOT_IMPLEMENTED_ERROR, "scale_value {} > 6",
                               scale_value);
    }
    return std::make_shared<DataTypeDateTimeV2>(scale_value);
}

} // namespace doris::vectorized

#if defined(__GNUC__) && (__GNUC__ >= 15)
#pragma GCC diagnostic pop
#endif
