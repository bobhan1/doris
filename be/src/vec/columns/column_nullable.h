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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnNullable.h
// and modified by Doris

#pragma once

#include <functional>
#include <string>
#include <type_traits>
#include <utility>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/status.h"
#include "olap/olap_common.h"
#include "runtime/define_primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/string_ref.h"
#include "vec/common/typeid_cast.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

class SipHash;

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;
class ColumnSorter;

using NullMap = ColumnUInt8::Container;
using ConstNullMapPtr = const NullMap*;

/// use this to avoid directly access null_map forgetting modify _need_update_has_null. see more in inner comments
class NullMapProvider {
public:
    NullMapProvider() = default;
    NullMapProvider(MutableColumnPtr&& null_map) : _null_map(std::move(null_map)) {}
    void reset_null_map(MutableColumnPtr&& null_map) { _null_map = std::move(null_map); }

    // return the column that represents the byte map. if want use null_map, just call this.
    const ColumnPtr& get_null_map_column_ptr() const { return _null_map; }
    // for functions getting nullmap, we assume it will modify it. so set `_need_update_has_null` to true. if you know it wouldn't,
    // call with arg false. but for the ops which will set _has_null themselves, call `update_has_null()`
    MutableColumnPtr get_null_map_column_ptr(bool may_change = true) {
        if (may_change) {
            _need_update_has_null = true;
        }
        return _null_map->assume_mutable();
    }
    ColumnUInt8::WrappedPtr& get_null_map(bool may_change = true) {
        if (may_change) {
            _need_update_has_null = true;
        }
        return _null_map;
    }

    ColumnUInt8& get_null_map_column(bool may_change = true) {
        if (may_change) {
            _need_update_has_null = true;
        }
        return assert_cast<ColumnUInt8&, TypeCheckOnRelease::DISABLE>(*_null_map);
    }
    const ColumnUInt8& get_null_map_column() const {
        return assert_cast<const ColumnUInt8&, TypeCheckOnRelease::DISABLE>(*_null_map);
    }

    NullMap& get_null_map_data(bool may_change = true) {
        return get_null_map_column(may_change).get_data();
    }
    const NullMap& get_null_map_data() const { return get_null_map_column().get_data(); }

    void clear_null_map() { assert_cast<ColumnUInt8*>(_null_map.get())->clear(); }

    void update_has_null(bool new_value) {
        _has_null = new_value;
        _need_update_has_null = false;
    }

protected:
    /**
    * Here we have three variables which serve for `has_null()` judgement. If we have known the nullity of object, no need
    *  to check through the `null_map` to get the answer until the next time we modify it. Here `_has_null` is just the answer
    *  we cached. `_need_update_has_null` indicates there's modification or not since we got `_has_null()` last time. So in 
    *  `_has_null()` we can check the two vars to know if there's need to update `has_null` or not.
    * If you just want QUERY BUT NOT MODIFY, make sure the caller is const. There will be no perf overhead for const overload.
    *  Otherwise, this class, as the base class, will make it no possible to directly visit `null_map` forgetting to change the
    *  protected flags. Just call the interface is ok.
    */
    bool _need_update_has_null = true;
    bool _has_null = true;

private:
    IColumn::WrappedPtr _null_map;
};

/// Class that specifies nullable columns. A nullable column represents
/// a column, which may have any type, provided with the possibility of
/// storing NULL values. For this purpose, a ColumnNullable object stores
/// an ordinary column along with a special column, namely a byte map,
/// whose type is ColumnUInt8. The latter column indicates whether the
/// value of a given row is a NULL or not. Such a design is preferred
/// over a bitmap because columns are usually stored on disk as compressed
/// files. In this regard, using a bitmap instead of a byte map would
/// greatly complicate the implementation with little to no benefits.
class ColumnNullable final : public COWHelper<IColumn, ColumnNullable>, public NullMapProvider {
private:
    friend class COWHelper<IColumn, ColumnNullable>;

    ColumnNullable(MutableColumnPtr&& nested_column_, MutableColumnPtr&& null_map_);
    ColumnNullable(const ColumnNullable&) = default;

public:
    /** Create immutable column using immutable arguments. This arguments may be shared with other columns.
      * Use IColumn::mutate in order to make mutable column and mutate shared nested columns.
      */
    using Base = COWHelper<IColumn, ColumnNullable>;
    static MutablePtr create(const ColumnPtr& nested_column_, const ColumnPtr& null_map_) {
        return ColumnNullable::create(nested_column_->assume_mutable(),
                                      null_map_->assume_mutable());
    }

    template <typename... Args, typename = std::enable_if_t<IsMutableColumns<Args...>::value>>
    static MutablePtr create(Args&&... args) {
        return Base::create(std::forward<Args>(args)...);
    }

    void sanity_check() const override {
        if (nested_column->size() != get_null_map_data().size()) {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "Size of nested column {} with size {} is not equal to size of null map {}",
                    nested_column->get_name(), nested_column->size(), get_null_map_data().size());
        }
        nested_column->sanity_check();
    }

    void shrink_padding_chars() override;

    bool is_variable_length() const override { return nested_column->is_variable_length(); }

    std::string get_name() const override { return "Nullable(" + nested_column->get_name() + ")"; }
    MutableColumnPtr clone_resized(size_t size) const override;
    size_t size() const override {
        return assert_cast<const ColumnUInt8&, TypeCheckOnRelease::DISABLE>(get_null_map_column())
                .size();
    }
    PURE bool is_null_at(size_t n) const override {
        return assert_cast<const ColumnUInt8&, TypeCheckOnRelease::DISABLE>(get_null_map_column())
                       .get_data()[n] != 0;
    }
    Field operator[](size_t n) const override;
    void get(size_t n, Field& res) const override;
    bool get_bool(size_t n) const override {
        return is_null_at(n) ? false : nested_column->get_bool(n);
    }
    // column must be nullable(uint8)
    bool get_bool_inline(size_t n) const {
        return is_null_at(n) ? false
                             : assert_cast<const ColumnUInt8*, TypeCheckOnRelease::DISABLE>(
                                       nested_column.get())
                                       ->get_bool(n);
    }
    StringRef get_data_at(size_t n) const override;

    /// Will insert null value if pos=nullptr
    void insert_data(const char* pos, size_t length) override;

    void insert_many_strings(const StringRef* strings, size_t num) override;

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;
    const char* deserialize_and_insert_from_arena(const char* pos) override;
    size_t get_max_row_byte_size() const override;

    void serialize_vec(StringRef* keys, size_t num_rows) const override;

    void deserialize_vec(StringRef* keys, size_t num_rows) override;

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                           size_t length) override;

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;
    void insert_indices_from_not_has_null(const IColumn& src, const uint32_t* indices_begin,
                                          const uint32_t* indices_end);

    void insert(const Field& x) override;
    void insert_from(const IColumn& src, size_t n) override;

    void insert_many_from(const IColumn& src, size_t position, size_t length) override;

    void append_data_by_selector(IColumn::MutablePtr& res,
                                 const IColumn::Selector& selector) const override;

    void append_data_by_selector(IColumn::MutablePtr& res, const IColumn::Selector& selector,
                                 size_t begin, size_t end) const override;

    template <typename ColumnType>
    void insert_from_with_type(const IColumn& src, size_t n) {
        const auto& src_concrete =
                assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(src);
        assert_cast<ColumnType*, TypeCheckOnRelease::DISABLE>(nested_column.get())
                ->insert_from(src_concrete.get_nested_column(), n);
        auto is_null = src_concrete.get_null_map_data()[n];
        if (is_null) {
            get_null_map_data().push_back(1);
            _has_null = true;
            _need_update_has_null = false;
        } else {
            _push_false_to_nullmap(1);
        }
    }

    void insert_range_from_not_nullable(const IColumn& src, size_t start, size_t length);

    void insert_many_fix_len_data(const char* pos, size_t num) override {
        _push_false_to_nullmap(num);
        get_nested_column().insert_many_fix_len_data(pos, num);
    }

    void insert_many_raw_data(const char* pos, size_t num) override {
        DCHECK(pos);
        _push_false_to_nullmap(num);
        get_nested_column().insert_many_raw_data(pos, num);
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t data_num, uint32_t dict_num) override {
        _push_false_to_nullmap(data_num);
        get_nested_column().insert_many_dict_data(data_array, start_index, dict, data_num,
                                                  dict_num);
    }

    void insert_many_continuous_binary_data(const char* data, const uint32_t* offsets,
                                            const size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }
        _push_false_to_nullmap(num);
        get_nested_column().insert_many_continuous_binary_data(data, offsets, num);
    }

    // Default value in `ColumnNullable` is null
    void insert_default() override {
        get_nested_column().insert_default();
        get_null_map_data().push_back(1);
        _has_null = true;
        _need_update_has_null = false;
    }

    void insert_many_defaults(size_t length) override {
        get_nested_column().insert_many_defaults(length);
        get_null_map_data().resize_fill(get_null_map_data().size() + length, 1);
        _has_null = true;
        _need_update_has_null = false;
    }

    void insert_not_null_elements(size_t num) {
        get_nested_column().insert_many_defaults(num);
        _push_false_to_nullmap(num);
    }

    void pop_back(size_t n) override;
    ColumnPtr filter(const Filter& filt, ssize_t result_size_hint) const override;

    size_t filter(const Filter& filter) override;

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override;
    MutableColumnPtr permute(const Permutation& perm, size_t limit) const override;
    //    ColumnPtr index(const IColumn & indexes, size_t limit) const override;
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int null_direction_hint) const override;

    void compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                          int direction, std::vector<uint8_t>& cmp_res,
                          uint8_t* __restrict filter) const override;
    void get_permutation(bool reverse, size_t limit, int null_direction_hint,
                         Permutation& res) const override;
    void reserve(size_t n) override;
    void resize(size_t n) override;
    size_t byte_size() const override;
    size_t allocated_bytes() const override;
    bool has_enough_capacity(const IColumn& src) const override;
    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override;
    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override;

    void update_hash_with_value(size_t n, SipHash& hash) const override;
    void update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                uint32_t offset,
                                const uint8_t* __restrict null_data) const override;
    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override;

    ColumnPtr convert_column_if_overflow() override {
        nested_column = nested_column->convert_column_if_overflow();
        return get_ptr();
    }

    void for_each_subcolumn(ColumnCallback callback) override {
        callback(nested_column);
        callback(get_null_map());
    }

    bool structure_equals(const IColumn& rhs) const override {
        if (const auto* rhs_nullable = typeid_cast<const ColumnNullable*>(&rhs)) {
            return nested_column->structure_equals(*rhs_nullable->nested_column);
        }
        return false;
    }

    bool is_nullable() const override { return true; }
    bool is_concrete_nullable() const override { return true; }
    bool is_column_string() const override { return get_nested_column().is_column_string(); }

    bool is_exclusive() const override {
        return IColumn::is_exclusive() && nested_column->is_exclusive() &&
               get_null_map_column().is_exclusive();
    }

    bool only_null() const override { return size() == 1 && is_null_at(0); }

    // used in schema change
    void change_nested_column(ColumnPtr& other) { ((ColumnPtr&)nested_column) = other; }

    /// Return the column that represents values.
    IColumn& get_nested_column() { return *nested_column; }
    const IColumn& get_nested_column() const { return *nested_column; }

    const ColumnPtr& get_nested_column_ptr() const { return nested_column; }

    MutableColumnPtr get_nested_column_ptr() { return nested_column->assume_mutable(); }

    void clear() override {
        clear_null_map();
        nested_column->clear();
        _has_null = false;
    }

    /// Apply the null byte map of a specified nullable column onto the
    /// null byte map of the current column by performing an element-wise OR
    /// between both byte maps. This method is used to determine the null byte
    /// map of the result column of a function taking one or more nullable
    /// columns.
    void apply_null_map(const ColumnNullable& other);
    void apply_null_map(const ColumnUInt8& map);
    void apply_negated_null_map(const ColumnUInt8& map);

    /// Check that size of null map equals to size of nested column.
    void check_consistency() const;

    bool has_null() const override {
        if (UNLIKELY(_need_update_has_null)) {
            const_cast<ColumnNullable*>(this)->_update_has_null();
        }
        return _has_null;
    }

    bool has_null(size_t size) const override;

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        const auto& nullable_rhs =
                assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(rhs);
        get_null_map_column().replace_column_data(nullable_rhs.get_null_map_column(), row,
                                                  self_row);

        if (!nullable_rhs.is_null_at(row)) {
            nested_column->replace_column_data(*nullable_rhs.nested_column, row, self_row);
        }
    }

    MutableColumnPtr convert_to_predicate_column_if_dictionary() override {
        nested_column = get_nested_column().convert_to_predicate_column_if_dictionary();
        return get_ptr();
    }

    double get_ratio_of_default_rows(double sample_ratio) const override {
        if (sample_ratio <= 0.0 || sample_ratio > 1.0) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "invalid sample_ratio {}",
                                   sample_ratio);
        }
        static constexpr auto MAX_NUMBER_OF_ROWS_FOR_FULL_SEARCH = 1000;
        size_t num_rows = size();
        size_t num_sampled_rows = std::min(
                static_cast<size_t>(static_cast<double>(num_rows) * sample_ratio), num_rows);
        size_t num_checked_rows = 0;
        size_t res = 0;
        if (num_sampled_rows == num_rows || num_rows <= MAX_NUMBER_OF_ROWS_FOR_FULL_SEARCH) {
            for (size_t i = 0; i < num_rows; ++i) {
                res += is_null_at(i);
            }
            num_checked_rows = num_rows;
        } else if (num_sampled_rows != 0) {
            for (size_t i = 0; i < num_rows; ++i) {
                if (num_checked_rows * num_rows <= i * num_sampled_rows) {
                    res += is_null_at(i);
                    ++num_checked_rows;
                }
            }
        }
        if (num_checked_rows == 0) {
            return 0.0;
        }
        return static_cast<double>(res) / static_cast<double>(num_checked_rows);
    }

    void convert_dict_codes_if_necessary() override {
        get_nested_column().convert_dict_codes_if_necessary();
    }

    void initialize_hash_values_for_runtime_filter() override {
        get_nested_column().initialize_hash_values_for_runtime_filter();
    }

    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;

    void set_rowset_segment_id(std::pair<RowsetId, uint32_t> rowset_segment_id) override {
        nested_column->set_rowset_segment_id(rowset_segment_id);
    }

    std::pair<RowsetId, uint32_t> get_rowset_segment_id() const override {
        return nested_column->get_rowset_segment_id();
    }

    void finalize() override { get_nested_column().finalize(); }

    void erase(size_t start, size_t length) override {
        get_nested_column().erase(start, length);
        get_null_map_column().erase(start, length);
    }

    size_t serialize_impl(char* pos, const size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
    size_t serialize_size_at(size_t row) const override {
        return sizeof(NullMap::value_type) +
               (is_null_at(row) ? 0 : nested_column->serialize_size_at(row));
    }

private:
    void _update_has_null();

    template <bool negative>
    void apply_null_map_impl(const ColumnUInt8& map);

    // push not null value wouldn't change the nullity. no need to update _has_null
    void _push_false_to_nullmap(size_t num) { get_null_map_column(false).insert_many_vals(0, num); }

    WrappedPtr nested_column;
};

ColumnPtr make_nullable(const ColumnPtr& column, bool is_nullable = false);
ColumnPtr remove_nullable(const ColumnPtr& column);
} // namespace doris::vectorized
#include "common/compile_check_end.h"
