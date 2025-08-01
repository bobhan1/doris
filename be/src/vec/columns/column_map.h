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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnMap.cpp
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <stdint.h>
#include <sys/types.h>

#include <functional>
#include <ostream>
#include <string>
#include <type_traits>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_struct.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/sip_hash.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

class SipHash;

namespace doris::vectorized {

class Arena;
/** A column of map values.
  */
class ColumnMap final : public COWHelper<IColumn, ColumnMap> {
public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnMap>;
    using COffsets = ColumnArray::ColumnOffsets;

    static MutablePtr create(const ColumnPtr& keys, const ColumnPtr& values,
                             const ColumnPtr& offsets) {
        return ColumnMap::create(keys->assume_mutable(), values->assume_mutable(),
                                 offsets->assume_mutable());
    }

    template <typename... Args,
              typename = typename std::enable_if<IsMutableColumns<Args...>::value>::type>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }

    std::string get_name() const override;

    void for_each_subcolumn(ColumnCallback callback) override {
        callback(keys_column);
        callback(values_column);
        callback(offsets_column);
    }

    void sanity_check() const override {
        keys_column->sanity_check();
        values_column->sanity_check();
        offsets_column->sanity_check();
    }

    void clear() override {
        keys_column->clear();
        values_column->clear();
        offsets_column->clear();
    }

    ColumnPtr convert_to_full_column_if_const() const override;

    MutableColumnPtr clone_resized(size_t size) const override;
    bool is_variable_length() const override { return true; }

    bool is_exclusive() const override {
        return IColumn::is_exclusive() && keys_column->is_exclusive() &&
               values_column->is_exclusive() && offsets_column->is_exclusive();
    }

    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    void insert_range_from(const IColumn& src, size_t start, size_t length) override;
    void insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                           size_t length) override;
    void insert_from(const IColumn& src_, size_t n) override;
    void insert(const Field& x) override;
    void insert_default() override;

    void pop_back(size_t n) override;
    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void shrink_padding_chars() override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const Filter& filter) override;
    MutableColumnPtr permute(const Permutation& perm, size_t limit) const override;

    int compare_at(size_t n, size_t m, const IColumn& rhs_, int nan_direction_hint) const override;

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;

    void insert_many_from(const IColumn& src, size_t position, size_t length) override;

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Method replace_column_data is not supported for " + get_name());
    }

    ColumnArray::Offsets64& ALWAYS_INLINE get_offsets() {
        return assert_cast<COffsets&, TypeCheckOnRelease::DISABLE>(*offsets_column).get_data();
    }
    const ColumnArray::Offsets64& ALWAYS_INLINE get_offsets() const {
        return assert_cast<const COffsets&, TypeCheckOnRelease::DISABLE>(*offsets_column)
                .get_data();
    }
    IColumn& get_offsets_column() { return *offsets_column; }
    const IColumn& get_offsets_column() const { return *offsets_column; }

    const ColumnPtr& get_offsets_ptr() const { return offsets_column; }
    ColumnPtr& get_offsets_ptr() { return offsets_column; }

    size_t ALWAYS_INLINE offset_at(ssize_t i) const { return get_offsets()[i - 1]; }

    size_t size() const override { return get_offsets().size(); }
    void reserve(size_t n) override;
    void resize(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    bool has_enough_capacity(const IColumn& src) const override;

    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data = nullptr) const override;

    void update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                uint32_t offset = 0,
                                const uint8_t* __restrict null_data = nullptr) const override;

    /******************** keys and values ***************/
    const ColumnPtr& get_keys_ptr() const { return keys_column; }
    ColumnPtr& get_keys_ptr() { return keys_column; }

    const IColumn& get_keys() const { return *keys_column; }
    IColumn& get_keys() { return *keys_column; }

    const ColumnPtr get_keys_array_ptr() const {
        return ColumnArray::create(keys_column, offsets_column);
    }
    ColumnPtr get_keys_array_ptr() { return ColumnArray::create(keys_column, offsets_column); }

    const ColumnPtr& get_values_ptr() const { return values_column; }
    ColumnPtr& get_values_ptr() { return values_column; }

    const IColumn& get_values() const { return *values_column; }
    IColumn& get_values() { return *values_column; }

    const ColumnPtr get_values_array_ptr() const {
        return ColumnArray::create(values_column, offsets_column);
    }
    ColumnPtr get_values_array_ptr() { return ColumnArray::create(values_column, offsets_column); }

    size_t ALWAYS_INLINE size_at(ssize_t i) const {
        return get_offsets()[i] - get_offsets()[i - 1];
    }

    ColumnPtr convert_column_if_overflow() override {
        keys_column = keys_column->convert_column_if_overflow();
        values_column = values_column->convert_column_if_overflow();
        return IColumn::convert_column_if_overflow();
    }

    void erase(size_t start, size_t length) override;
    size_t serialize_impl(char* pos, const size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
    size_t serialize_size_at(size_t row) const override;
    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override;
    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;
    void deserialize_vec(StringRef* keys, const size_t num_rows) override;
    void serialize_vec(StringRef* keys, size_t num_rows) const override;
    size_t get_max_row_byte_size() const override;

    template <bool positive>
    struct less;

private:
    friend class COWHelper<IColumn, ColumnMap>;

    WrappedPtr keys_column;    // nullable
    WrappedPtr values_column;  // nullable
    WrappedPtr offsets_column; // offset

    ColumnMap(MutableColumnPtr&& keys, MutableColumnPtr&& values, MutableColumnPtr&& offsets);

    ColumnMap(const ColumnMap&) = default;
};

} // namespace doris::vectorized
