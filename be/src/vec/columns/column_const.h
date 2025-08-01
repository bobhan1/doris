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
// This file is copied from
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnConst.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <sys/types.h>

#include <cstddef>
#include <cstdint>
#include <functional>
#include <initializer_list>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_nullable.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/column_numbers.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"

class SipHash;

namespace doris::vectorized {
#include "common/compile_check_begin.h"

class Arena;
class Block;

/*
 * @return first : pointer to column itself if it's not ColumnConst, else to column's data column.
 *         second : zero if column is ColumnConst, else itself.
*/
std::pair<ColumnPtr, size_t> check_column_const_set_readability(const IColumn& column,
                                                                size_t row_num) noexcept;

/*
 * @warning use this function sometimes cause performance problem in GCC.
*/
template <typename T>
    requires std::is_integral_v<T>
T index_check_const(T arg, bool constancy) noexcept {
    return constancy ? 0 : arg;
}
template <bool is_const, typename T>
    requires std::is_integral_v<T>
constexpr T index_check_const(T arg) noexcept {
    if constexpr (is_const) {
        return 0;
    } else {
        return arg;
    }
}

/*
 * @return first : data_column_ptr for ColumnConst, itself otherwise.
 *         second : whether it's ColumnConst.
*/
std::pair<const ColumnPtr&, bool> unpack_if_const(const ColumnPtr&) noexcept;

/*
 * For the functions that some columns of arguments are almost but not completely always const, we use this function to preprocessing its parameter columns
 * (which are not data columns). When we have two or more columns which only provide parameter, use this to deal with corner case. So you can specialize you
 * implementations for all const or all parameters const, without considering some of parameters are const.
 
 * Do the transformation only for the columns whose arg_indexes in parameters.
*/
void default_preprocess_parameter_columns(ColumnPtr* columns, const bool* col_const,
                                          const std::initializer_list<size_t>& parameters,
                                          Block& block, const ColumnNumbers& arg_indexes);

/** ColumnConst contains another column with single element,
  *  but looks like a column with arbitrary amount of same elements.
  */
class ColumnConst final : public COWHelper<IColumn, ColumnConst> {
private:
    friend class COWHelper<IColumn, ColumnConst>;
    using Self = ColumnConst;
    WrappedPtr data;
    size_t s;

    ColumnConst(const ColumnPtr& data, size_t s_);
    ColumnConst(const ColumnPtr& data, size_t s_, bool create_with_empty);
    ColumnConst(const ColumnConst& src) = default;

public:
    ColumnPtr convert_to_full_column() const;

    ColumnPtr convert_to_full_column_if_const() const override {
        return convert_to_full_column()->convert_to_full_column_if_const();
    }

    bool is_variable_length() const override { return data->is_variable_length(); }

    std::string get_name() const override { return "Const(" + data->get_name() + ")"; }

    void resize(size_t new_size) override { s = new_size; }

    MutableColumnPtr clone_resized(size_t new_size) const override {
        return ColumnConst::create(data->clone_resized(1), new_size);
    }

    size_t size() const override { return s; }

    Field operator[](size_t) const override { return (*data)[0]; }

    void get(size_t, Field& res) const override { data->get(0, res); }

    StringRef get_data_at(size_t) const override { return data->get_data_at(0); }

    Int64 get_int(size_t) const override { return data->get_int(0); }

    bool get_bool(size_t) const override { return data->get_bool(0); }

    bool is_null_at(size_t) const override { return data->is_null_at(0); }

    void insert_range_from(const IColumn& src, size_t /*start*/, size_t length) override {
        if (!is_column_const(src) || compare_at(0, 0, src, 0) != 0) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "ColumnConst::insert_indices_from: src is not const or not equal to dst");
        }
        s += length;
    }

    void insert_many_from(const IColumn& src, size_t position, size_t length) override {
        if (!is_column_const(src) || compare_at(0, 0, src, 0) != 0) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "ColumnConst::insert_indices_from: src is not const or not equal to dst");
        }
        s += length;
    }

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override {
        if (!is_column_const(src) || compare_at(0, 0, src, 0) != 0) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "ColumnConst::insert_indices_from: src is not const or not equal to dst");
        }
        s += (indices_end - indices_begin);
    }

    void insert(const Field&) override { ++s; }

    void insert_data(const char*, size_t) override { ++s; }

    void insert_from(const IColumn& src, size_t) override {
        if (!is_column_const(src) || compare_at(0, 0, src, 0) != 0) {
            throw Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "ColumnConst::insert_indices_from: src is not const or not equal to dst");
        }
        ++s;
    }

    void clear() override { s = 0; }

    void insert_default() override { ++s; }

    void pop_back(size_t n) override { s -= n; }

    StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                         char const*& begin) const override {
        DCHECK_EQ(data->size(), 1);
        auto* pos = arena.alloc_continue(data->serialize_size_at(0), begin);
        return {pos, serialize_impl(pos, n)};
    }

    const char* deserialize_and_insert_from_arena(const char* pos) override {
        return pos + deserialize_impl(pos);
    }

    size_t get_max_row_byte_size() const override { return data->get_max_row_byte_size(); }

    void serialize_vec(StringRef* keys, size_t num_rows) const override {
        DCHECK_EQ(data->size(), 1);
        for (size_t i = 0; i < num_rows; i++) {
            serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
        }
    }

    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override {
        auto real_data = data->get_data_at(0);
        if (real_data.data == nullptr) {
            hash = HashUtil::xxHash64NullWithSeed(hash);
        } else {
            hash = HashUtil::xxHash64WithSeed(real_data.data, real_data.size, hash);
        }
    }

    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override {
        get_data_column_ptr()->update_crc_with_value(start, end, hash, nullptr);
    }

    void update_hash_with_value(size_t, SipHash& hash) const override {
        data->update_hash_with_value(0, hash);
    }

    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const Filter& filter) override;

    MutableColumnPtr permute(const Permutation& perm, size_t limit) const override;
    // ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         Permutation& res) const override;

    size_t byte_size() const override { return s > 0 ? data->byte_size() + sizeof(s) : 0; }

    size_t allocated_bytes() const override { return data->allocated_bytes() + sizeof(s); }

    bool has_enough_capacity(const IColumn& src) const override { return true; }

    int compare_at(size_t, size_t, const IColumn& rhs, int nan_direction_hint) const override {
        auto rhs_const_column = assert_cast<const ColumnConst&, TypeCheckOnRelease::DISABLE>(rhs);

        const auto* this_nullable = check_and_get_column<ColumnNullable>(data.get());
        const auto* rhs_nullable =
                check_and_get_column<ColumnNullable>(rhs_const_column.data.get());
        if (this_nullable && rhs_nullable) {
            return data->compare_at(0, 0, *rhs_const_column.data, nan_direction_hint);
        } else if (this_nullable) {
            auto rhs_nullable_column = make_nullable(rhs_const_column.data, false);
            return this_nullable->compare_at(0, 0, *rhs_nullable_column, nan_direction_hint);
        } else if (rhs_nullable) {
            auto this_nullable_column = make_nullable(data, false);
            return this_nullable_column->compare_at(0, 0, *rhs_const_column.data,
                                                    nan_direction_hint);
        } else {
            return data->compare_at(0, 0, *rhs_const_column.data, nan_direction_hint);
        }
    }

    void for_each_subcolumn(ColumnCallback callback) override { callback(data); }

    bool structure_equals(const IColumn& rhs) const override {
        if (const auto* rhs_concrete = typeid_cast<const ColumnConst*>(&rhs)) {
            return data->structure_equals(*rhs_concrete->data);
        }
        return false;
    }

    // ColumnConst is not nullable, but may be concrete nullable.
    bool is_concrete_nullable() const override { return is_column_nullable(*data); }
    bool only_null() const override { return data->is_null_at(0); }
    StringRef get_raw_data() const override { return data->get_raw_data(); }

    /// Not part of the common interface.

    IColumn& get_data_column() { return *data; }
    const IColumn& get_data_column() const { return *data; }
    const ColumnPtr& get_data_column_ptr() const { return data; }

    Field get_field() const { return get_data_column()[0]; }

    template <typename T>
    T get_value() const {
        // Here the cast is correct, relevant code is rather tricky.
        return static_cast<T>(get_field().get<NearestFieldType<T>>());
    }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data->replace_column_data(rhs, row, self_row);
    }

    void finalize() override { data->finalize(); }

    void erase(size_t start, size_t length) override {
        if (start >= s || length == 0) {
            return;
        }
        length = std::min(length, s - start);
        s = s - length;
    }

    size_t serialize_impl(char* pos, const size_t row) const override {
        return data->serialize_impl(pos, 0);
    }

    size_t serialize_size_at(size_t row) const override { return data->serialize_size_at(0); }

    size_t deserialize_impl(const char* pos) override {
        ++s;
        return data->deserialize_impl(pos);
    }
};

// For example, DataType may not correspond to a type and const,
// so it is necessary to make a special judgment of ColumnConst.
template <typename Type>
const Type* check_and_get_column_with_const(const IColumn& column) {
    if (const auto* col_const = check_and_get_column<ColumnConst>(&column)) {
        return check_and_get_column<Type>(col_const->get_data_column());
    }
    return check_and_get_column<Type>(column);
}

} // namespace doris::vectorized
#include "common/compile_check_end.h"
