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
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <stddef.h>
#include <stdint.h>

#include <memory>
#include <ostream>
#include <string>
#include <typeinfo>

#include "runtime/define_primitive_type.h"
#include "serde/data_type_bitmap_serde.h"
#include "util/bitmap_value.h"
#include "vec/columns/column_complex.h"
#include "vec/columns/column_const.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/serde/data_type_serde.h"

namespace doris::vectorized {
class BufferReadable;
class BufferWritable;
class IColumn;

class DataTypeBitMap : public IDataType {
public:
    DataTypeBitMap() = default;
    ~DataTypeBitMap() override = default;

    using ColumnType = ColumnBitmap;
    using FieldType = BitmapValue;
    static constexpr PrimitiveType PType = TYPE_BITMAP;

    std::string do_get_name() const override { return get_family_name(); }
    const std::string get_family_name() const override { return "BitMap"; }
    PrimitiveType get_primitive_type() const override { return PrimitiveType::TYPE_BITMAP; }

    doris::FieldType get_storage_field_type() const override {
        return doris::FieldType::OLAP_FIELD_TYPE_BITMAP;
    }

    int64_t get_uncompressed_serialized_bytes(const IColumn& column,
                                              int be_exec_version) const override;
    char* serialize(const IColumn& column, char* buf, int be_exec_version) const override;
    const char* deserialize(const char* buf, MutableColumnPtr* column,
                            int be_exec_version) const override;
    MutableColumnPtr create_column() const override;
    Status check_column(const IColumn& column) const override;

    bool have_subtypes() const override { return false; }
    bool should_align_right_in_pretty_formats() const override { return false; }
    bool text_can_contain_only_valid_utf8() const override { return true; }
    bool is_comparable() const override { return false; }
    // TODO:
    bool is_value_unambiguously_represented_in_contiguous_memory_region() const override {
        return true;
    }

    bool equals(const IDataType& rhs) const override { return typeid(rhs) == typeid(*this); }

    bool can_be_inside_low_cardinality() const override { return false; }

    std::string to_string(const IColumn& column, size_t row_num) const override {
        auto result = check_column_const_set_readability(column, row_num);
        ColumnPtr ptr = result.first;
        row_num = result.second;

        const auto& data = assert_cast<const ColumnBitmap&>(*ptr).get_element(row_num);
        return data.to_string();
    }
    void to_string(const IColumn& column, size_t row_num, BufferWritable& ostr) const override;
    Status from_string(ReadBuffer& rb, IColumn* column) const override;

    Field get_default() const override {
        return Field::create_field<TYPE_BITMAP>(BitmapValue::empty_bitmap());
    }

    [[noreturn]] Field get_field(const TExprNode& node) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Unimplemented get_field for BitMap");
    }

    FieldWithDataType get_field_with_data_type(const IColumn& column,
                                               size_t row_num) const override {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "Unimplemented get_field_with_data_type for BitMap");
    }

    static void serialize_as_stream(const BitmapValue& value, BufferWritable& buf);

    static void deserialize_as_stream(BitmapValue& value, BufferReadable& buf);

    using SerDeType = DataTypeBitMapSerDe;
    DataTypeSerDeSPtr get_serde(int nesting_level = 1) const override {
        return std::make_shared<SerDeType>(nesting_level);
    };
};

} // namespace doris::vectorized
