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

#include <gen_cpp/parquet_types.h>

#include "common/cast_set.h"
#include "vec/core/extended_types.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/exec/format/column_type_convert.h"
#include "vec/exec/format/format_common.h"
#include "vec/exec/format/parquet/decoder.h"
#include "vec/exec/format/parquet/parquet_common.h"
#include "vec/exec/format/parquet/schema_desc.h"

namespace doris::vectorized::parquet {
#include "common/compile_check_begin.h"
struct ConvertParams {
    // schema.logicalType.TIMESTAMP.isAdjustedToUTC == false
    static const cctz::time_zone utc0;
    // schema.logicalType.TIMESTAMP.isAdjustedToUTC == true, we should set local time zone
    cctz::time_zone* ctz = nullptr;
    size_t offset_days = 0;
    int64_t second_mask = 1;
    int64_t scale_to_nano_factor = 1;
    FieldSchema* field_schema = nullptr;

    //For UInt8 -> Int16,UInt16 -> Int32,UInt32 -> Int64,UInt64 -> Int128.
    bool is_type_compatibility = false;

    /**
     * Some frameworks like paimon maybe writes non-standard parquet files. Timestamp field doesn't have
     * logicalType or converted_type to indicates its precision. We have to reset the time mask.
     */
    void reset_time_scale_if_missing(int scale) {
        const auto& schema = field_schema->parquet_schema;
        if (!schema.__isset.logicalType && !schema.__isset.converted_type) {
            int ts_scale = 9;
            if (scale <= 3) {
                ts_scale = 3;
            } else if (scale <= 6) {
                ts_scale = 6;
            }
            second_mask = common::exp10_i64(ts_scale);
            scale_to_nano_factor = common::exp10_i64(9 - ts_scale);

            // The missing parque metadata makes it impossible for us to know the time zone information,
            // so we default to UTC here.
            if (ctz == nullptr) {
                ctz = const_cast<cctz::time_zone*>(&utc0);
            }
        }
    }

    void init(FieldSchema* field_schema_, cctz::time_zone* ctz_) {
        field_schema = field_schema_;
        if (ctz_ != nullptr) {
            ctz = ctz_;
        }
        const auto& schema = field_schema->parquet_schema;
        if (schema.__isset.logicalType && schema.logicalType.__isset.TIMESTAMP) {
            const auto& timestamp_info = schema.logicalType.TIMESTAMP;
            if (!timestamp_info.isAdjustedToUTC) {
                // should set timezone to utc+0
                // Reference: https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#instant-semantics-timestamps-normalized-to-utc
                // If isAdjustedToUTC = false, the reader should display the same value no mater what local time zone is. For example:
                // When a timestamp is stored as `1970-01-03 12:00:00`,
                // if isAdjustedToUTC = true, UTC8 should read as `1970-01-03 20:00:00`, UTC6 should read as `1970-01-03 18:00:00`
                // if isAdjustedToUTC = false, UTC8 and UTC6 should read as `1970-01-03 12:00:00`, which is the same as `1970-01-03 12:00:00` in UTC0
                ctz = const_cast<cctz::time_zone*>(&utc0);
            }
            const auto& time_unit = timestamp_info.unit;
            if (time_unit.__isset.MILLIS) {
                second_mask = 1000;
                scale_to_nano_factor = 1000000;
            } else if (time_unit.__isset.MICROS) {
                second_mask = 1000000;
                scale_to_nano_factor = 1000;
            } else if (time_unit.__isset.NANOS) {
                second_mask = 1000000000;
                scale_to_nano_factor = 1;
            }
        } else if (schema.__isset.converted_type) {
            const auto& converted_type = schema.converted_type;
            if (converted_type == tparquet::ConvertedType::TIMESTAMP_MILLIS) {
                second_mask = 1000;
                scale_to_nano_factor = 1000000;
            } else if (converted_type == tparquet::ConvertedType::TIMESTAMP_MICROS) {
                second_mask = 1000000;
                scale_to_nano_factor = 1000;
            }
        }

        if (ctz) {
            VecDateTimeValue t;
            t.from_unixtime(0, *ctz);
            offset_days = t.day() == 31 ? -1 : 0;
        }
        is_type_compatibility = field_schema_->is_type_compatibility;
    }
};

/**
 * Convert parquet physical column to logical column
 * In parquet document(https://github.com/apache/parquet-format/blob/master/LogicalTypes.md),
 * Logical or converted type is the data type of column, physical type is the stored type of column chunk.
 * eg, decimal type can be stored as INT32, INT64, BYTE_ARRAY, FIXED_LENGTH_BYTE_ARRAY.
 * So there is a convert process from physical type to logical type.
 * In addition, Schema change will bring about a change in logical type.
 *
 * `PhysicalToLogicalConverter` strips away the conversion of logical type, and reuse `ColumnTypeConverter`
 * to resolve schema change, allowing parquet reader to only focus on the conversion of physical types.
 *
 * Therefore, tow layers converters are designed:
 * First, read parquet data with the physical type
 * Second, convert physical type to logical type
 * Third, convert logical type to the final type planned by FE(schema change)
 *
 * Ultimate performance optimization:
 * 1. If process of (First => Second) is consistent, eg. from BYTE_ARRAY to string, no additional copies and conversions will be introduced;
 * 2. If process of (Second => Third) is consistent, no additional copies and conversions will be introduced;
 * 3. Null map is share among all processes, no additional copies and conversions will be introduced in null map;
 * 4. Only create one physical column in physical conversion, and reused in each loop;
 * 5. Only create one logical column in logical conversion, and reused in each loop;
 * 6. FIXED_LENGTH_BYTE_ARRAY is read as ColumnUInt8 instead of ColumnString, so the underlying decoder has no process to decode string
 *    and use memory copy to read the data as a whole, and the conversion has no need to resolve the Offsets in ColumnString.
 */
class PhysicalToLogicalConverter {
protected:
    ColumnPtr _cached_src_physical_column = nullptr;
    DataTypePtr _cached_src_physical_type = nullptr;
    std::unique_ptr<converter::ColumnTypeConverter> _logical_converter = nullptr;

    std::string _error_msg;

    std::unique_ptr<ConvertParams> _convert_params;

public:
    static std::unique_ptr<PhysicalToLogicalConverter> get_converter(
            FieldSchema* field_schema, DataTypePtr src_logical_type,
            const DataTypePtr& dst_logical_type, cctz::time_zone* ctz, bool is_dict_filter);

    static bool is_parquet_native_type(PrimitiveType type);

    static bool is_decimal_type(PrimitiveType type);

    PhysicalToLogicalConverter() = default;
    virtual ~PhysicalToLogicalConverter() = default;

    virtual Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) {
        return Status::OK();
    }

    Status convert(ColumnPtr& src_physical_col, DataTypePtr src_logical_type,
                   const DataTypePtr& dst_logical_type, ColumnPtr& dst_logical_col,
                   bool is_dict_filter) {
        if (is_dict_filter) {
            src_logical_type = DataTypeFactory::instance().create_data_type(
                    PrimitiveType::TYPE_INT, dst_logical_type->is_nullable());
        }
        if (is_consistent() && _logical_converter->is_consistent()) {
            return Status::OK();
        }
        ColumnPtr src_logical_column;
        if (is_consistent()) {
            if (dst_logical_type->is_nullable()) {
                auto doris_nullable_column = const_cast<ColumnNullable*>(
                        assert_cast<const ColumnNullable*>(dst_logical_col.get()));
                src_logical_column =
                        ColumnNullable::create(_cached_src_physical_column,
                                               doris_nullable_column->get_null_map_column_ptr());
            } else {
                src_logical_column = _cached_src_physical_column;
            }
        } else {
            src_logical_column = _logical_converter->get_column(src_logical_type, dst_logical_col,
                                                                dst_logical_type);
        }
        RETURN_IF_ERROR(physical_convert(src_physical_col, src_logical_column));
        auto converted_column = dst_logical_col->assume_mutable();
        return _logical_converter->convert(src_logical_column, converted_column);
    }

    virtual ColumnPtr get_physical_column(tparquet::Type::type src_physical_type,
                                          DataTypePtr src_logical_type,
                                          ColumnPtr& dst_logical_column,
                                          const DataTypePtr& dst_logical_type, bool is_dict_filter);

    DataTypePtr& get_physical_type() { return _cached_src_physical_type; }

    virtual bool is_consistent() { return false; }

    virtual bool support() { return true; }

    std::string get_error_msg() { return _error_msg; }
};

class ConsistentPhysicalConverter : public PhysicalToLogicalConverter {
    bool is_consistent() override { return true; }
};

class UnsupportedConverter : public PhysicalToLogicalConverter {
public:
    UnsupportedConverter(std::string error_msg) { _error_msg = error_msg; }

    UnsupportedConverter(tparquet::Type::type src_physical_type,
                         const DataTypePtr& src_logical_type) {
        std::string src_physical_str = tparquet::to_string(src_physical_type);
        std::string src_logical_str = src_logical_type->get_name();
        _error_msg = src_physical_str + " => " + src_logical_str;
    }

    bool support() override { return false; }

    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        return Status::InternalError("Unsupported physical to logical type: {}", _error_msg);
    }
};

// for tinyint, smallint
template <PrimitiveType IntPrimitiveType>
class LittleIntPhysicalConverter : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        using DstCppType = typename PrimitiveTypeTraits<IntPrimitiveType>::CppType;
        using DstColumnType = typename PrimitiveTypeTraits<IntPrimitiveType>::ColumnType;
        ColumnPtr from_col = remove_nullable(src_physical_col);
        MutableColumnPtr to_col = remove_nullable(src_logical_column)->assume_mutable();

        size_t rows = from_col->size();
        // always comes from tparquet::Type::INT32
        auto& src_data = assert_cast<const ColumnInt32*>(from_col.get())->get_data();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = assert_cast<DstColumnType&>(*to_col.get()).get_data();
        for (int i = 0; i < rows; ++i) {
            data[start_idx + i] = static_cast<DstCppType>(src_data[i]);
        }

        return Status::OK();
    }
};

template <PrimitiveType type>
struct UnsignedTypeTraits;

template <>
struct UnsignedTypeTraits<TYPE_SMALLINT> {
    using UnsignedCppType = UInt8;
    //https://github.com/apache/parquet-format/blob/master/LogicalTypes.md#unsigned-integers
    //INT(8, false), INT(16, false), and INT(32, false) must annotate an int32 primitive type and INT(64, false)
    //must annotate an int64 primitive type.
    using StorageCppType = Int32;
    using StorageColumnType = vectorized::ColumnInt32;
};

template <>
struct UnsignedTypeTraits<TYPE_INT> {
    using UnsignedCppType = UInt16;
    using StorageCppType = Int32;
    using StorageColumnType = vectorized::ColumnInt32;
};

template <>
struct UnsignedTypeTraits<TYPE_BIGINT> {
    using UnsignedCppType = UInt32;
    using StorageCppType = Int32;
    using StorageColumnType = vectorized::ColumnInt32;
};

template <>
struct UnsignedTypeTraits<TYPE_LARGEINT> {
    using UnsignedCppType = UInt64;
    using StorageCppType = Int64;
    using StorageColumnType = vectorized::ColumnInt64;
};

template <PrimitiveType IntPrimitiveType>
class UnsignedIntegerConverter : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        using UnsignedCppType = typename UnsignedTypeTraits<IntPrimitiveType>::UnsignedCppType;
        using StorageCppType = typename UnsignedTypeTraits<IntPrimitiveType>::StorageCppType;
        using StorageColumnType = typename UnsignedTypeTraits<IntPrimitiveType>::StorageColumnType;
        using DstColumnType = typename PrimitiveTypeTraits<IntPrimitiveType>::ColumnType;

        ColumnPtr from_col = remove_nullable(src_physical_col);
        MutableColumnPtr to_col = remove_nullable(src_logical_column)->assume_mutable();
        auto& src_data = assert_cast<const StorageColumnType*>(from_col.get())->get_data();

        size_t rows = src_data.size();
        size_t start_idx = to_col->size();
        to_col->resize(start_idx + rows);
        auto& data = assert_cast<DstColumnType&>(*to_col.get()).get_data();

        for (int i = 0; i < rows; i++) {
            StorageCppType src_value = src_data[i];
            auto unsigned_value = static_cast<UnsignedCppType>(src_value);
            data[start_idx + i] = unsigned_value;
        }

        return Status::OK();
    }
};

class FixedSizeBinaryConverter : public PhysicalToLogicalConverter {
private:
    int _type_length;

public:
    FixedSizeBinaryConverter(int type_length) : _type_length(type_length) {}

    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr from_col = remove_nullable(src_physical_col);
        MutableColumnPtr to_col = remove_nullable(src_logical_column)->assume_mutable();

        auto* src_data = assert_cast<const ColumnUInt8*>(from_col.get());
        size_t length = src_data->size();
        size_t num_values = length / _type_length;
        auto& string_col = static_cast<ColumnString&>(*to_col.get());
        auto& offsets = string_col.get_offsets();
        auto& chars = string_col.get_chars();

        size_t origin_size = chars.size();
        chars.resize(origin_size + length);
        memcpy(chars.data() + origin_size, src_data->get_data().data(), length);

        origin_size = offsets.size();
        offsets.resize(origin_size + num_values);
        auto end_offset = offsets[origin_size - 1];
        for (int i = 0; i < num_values; ++i) {
            end_offset += _type_length;
            offsets[origin_size + i] = end_offset;
        }

        return Status::OK();
    }
};

template <PrimitiveType DecimalPType>
class FixedSizeToDecimal : public PhysicalToLogicalConverter {
public:
    using DecimalType = typename PrimitiveTypeTraits<DecimalPType>::ColumnItemType;
    FixedSizeToDecimal(int32_t type_length) : _type_length(type_length) {}

    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        MutableColumnPtr dst_col = remove_nullable(src_logical_column)->assume_mutable();

#define M(FixedTypeLength, ValueCopyType) \
    case FixedTypeLength:                 \
        return _convert_internal<FixedTypeLength, ValueCopyType>(src_col, dst_col);

#define APPLY_FOR_DECIMALS() \
    M(1, int64_t)            \
    M(2, int64_t)            \
    M(3, int64_t)            \
    M(4, int64_t)            \
    M(5, int64_t)            \
    M(6, int64_t)            \
    M(7, int64_t)            \
    M(8, int64_t)            \
    M(9, int128_t)           \
    M(10, int128_t)          \
    M(11, int128_t)          \
    M(12, int128_t)          \
    M(13, int128_t)          \
    M(14, int128_t)          \
    M(15, int128_t)          \
    M(16, int128_t)          \
    M(17, wide::Int256)      \
    M(18, wide::Int256)      \
    M(19, wide::Int256)      \
    M(20, wide::Int256)      \
    M(21, wide::Int256)      \
    M(22, wide::Int256)      \
    M(23, wide::Int256)      \
    M(24, wide::Int256)      \
    M(25, wide::Int256)      \
    M(26, wide::Int256)      \
    M(27, wide::Int256)      \
    M(28, wide::Int256)      \
    M(29, wide::Int256)      \
    M(30, wide::Int256)      \
    M(31, wide::Int256)      \
    M(32, wide::Int256)

        switch (_type_length) {
            APPLY_FOR_DECIMALS()
        default:
            throw Exception(Status::FatalError("__builtin_unreachable"));
        }
        return Status::OK();
#undef APPLY_FOR_DECIMALS
#undef M
    }

    template <int fixed_type_length, typename ValueCopyType>
    Status _convert_internal(ColumnPtr& src_col, MutableColumnPtr& dst_col) {
        size_t rows = src_col->size() / fixed_type_length;
        auto* buf = static_cast<const ColumnUInt8*>(src_col.get())->get_data().data();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);

        auto& data = static_cast<ColumnDecimal<DecimalPType>*>(dst_col.get())->get_data();
        size_t offset = 0;
        for (int i = 0; i < rows; i++) {
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.
            ValueCopyType value = 0;
            memcpy(reinterpret_cast<char*>(&value), buf + offset, sizeof(value));
            offset += fixed_type_length;
            value = to_endian<std::endian::big>(value);
            value = value >> ((sizeof(value) - fixed_type_length) * 8);
            auto& v = reinterpret_cast<DecimalType&>(data[start_idx + i]);
            v = (DecimalType)value;
        }

        return Status::OK();
    }

private:
    int32_t _type_length;
};

template <PrimitiveType DecimalPType>
class StringToDecimal : public PhysicalToLogicalConverter {
    using DecimalType = typename PrimitiveTypeTraits<DecimalPType>::ColumnItemType;
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        using ValueCopyType = DecimalType::NativeType;
        ColumnPtr src_col = remove_nullable(src_physical_col);
        MutableColumnPtr dst_col = remove_nullable(src_logical_column)->assume_mutable();

        size_t rows = src_col->size();
        auto buf = static_cast<const ColumnString*>(src_col.get())->get_chars().data();
        auto& offset = static_cast<const ColumnString*>(src_col.get())->get_offsets();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);

        auto& data = static_cast<ColumnDecimal<DecimalPType>*>(dst_col.get())->get_data();
        for (int i = 0; i < rows; i++) {
            size_t len = offset[i] - offset[i - 1];
            // When Decimal in parquet is stored in byte arrays, binary and fixed,
            // the unscaled number must be encoded as two's complement using big-endian byte order.
            ValueCopyType value = 0;
            if (len > 0) {
                memcpy(reinterpret_cast<char*>(&value), buf + offset[i - 1], len);
                value = to_endian<std::endian::big>(value);
                value = value >> ((sizeof(value) - len) * 8);
            }
            auto& v = reinterpret_cast<DecimalType&>(data[start_idx + i]);
            v = (DecimalType)value;
        }

        return Status::OK();
    }
};

template <PrimitiveType NumberType, PrimitiveType DecimalPType>
class NumberToDecimal : public PhysicalToLogicalConverter {
    using DecimalType = typename PrimitiveTypeTraits<DecimalPType>::ColumnItemType;
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        using ValueCopyType = typename DecimalType::NativeType;
        ColumnPtr src_col = remove_nullable(src_physical_col);
        MutableColumnPtr dst_col = remove_nullable(src_logical_column)->assume_mutable();

        size_t rows = src_col->size();
        auto* src_data =
                static_cast<const ColumnVector<NumberType>*>(src_col.get())->get_data().data();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);

        auto* data = static_cast<ColumnDecimal<DecimalPType>*>(dst_col.get())->get_data().data();

        for (int i = 0; i < rows; i++) {
            ValueCopyType value;
            if constexpr (std::is_same_v<DecimalType, Decimal256>) {
                value = src_data[i];
            } else {
                value = cast_set<ValueCopyType, typename PrimitiveTypeTraits<NumberType>::CppType,
                                 false>(src_data[i]);
            }

            data[start_idx + i] = (DecimalType)value;
        }
        return Status::OK();
    }
};

class Int32ToDate : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        MutableColumnPtr dst_col = remove_nullable(src_logical_column)->assume_mutable();

        size_t rows = src_col->size();
        size_t start_idx = dst_col->size();
        dst_col->reserve(start_idx + rows);

        auto& src_data = static_cast<const ColumnInt32*>(src_col.get())->get_data();
        auto& data = static_cast<ColumnDateV2*>(dst_col.get())->get_data();
        date_day_offset_dict& date_dict = date_day_offset_dict::get();

        for (int i = 0; i < rows; i++) {
            int64_t date_value = (int64_t)src_data[i] + _convert_params->offset_days;
            data.push_back_without_reserve(
                    date_dict[cast_set<int32_t>(date_value)].to_date_int_val());
        }

        return Status::OK();
    }
};

struct Int64ToTimestamp : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        MutableColumnPtr dst_col = remove_nullable(src_logical_column)->assume_mutable();

        size_t rows = src_col->size();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);

        auto src_data = static_cast<const ColumnInt64*>(src_col.get())->get_data().data();
        auto& data = static_cast<ColumnDateTimeV2*>(dst_col.get())->get_data();

        for (int i = 0; i < rows; i++) {
            int64_t x = src_data[i];
            auto& num = data[start_idx + i];
            auto& value = reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(num);
            value.from_unixtime(x / _convert_params->second_mask, *_convert_params->ctz);
            value.set_microsecond((x % _convert_params->second_mask) *
                                  (_convert_params->scale_to_nano_factor / 1000));
        }
        return Status::OK();
    }
};

struct Int96toTimestamp : public PhysicalToLogicalConverter {
    Status physical_convert(ColumnPtr& src_physical_col, ColumnPtr& src_logical_column) override {
        ColumnPtr src_col = remove_nullable(src_physical_col);
        MutableColumnPtr dst_col = remove_nullable(src_logical_column)->assume_mutable();

        size_t rows = src_col->size() / sizeof(ParquetInt96);
        auto& src_data = static_cast<const ColumnInt8*>(src_col.get())->get_data();
        auto ParquetInt96_data = (ParquetInt96*)src_data.data();
        size_t start_idx = dst_col->size();
        dst_col->resize(start_idx + rows);
        auto& data = static_cast<ColumnDateTimeV2*>(dst_col.get())->get_data();

        for (int i = 0; i < rows; i++) {
            ParquetInt96 src_cell_data = ParquetInt96_data[i];
            auto& dst_value =
                    reinterpret_cast<DateV2Value<DateTimeV2ValueType>&>(data[start_idx + i]);

            int64_t timestamp_with_micros = src_cell_data.to_timestamp_micros();
            dst_value.from_unixtime(timestamp_with_micros / 1000000, *_convert_params->ctz);
            dst_value.set_microsecond(timestamp_with_micros % 1000000);
        }
        return Status::OK();
    }
};
#include "common/compile_check_end.h"

} // namespace doris::vectorized::parquet
