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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnNullable.cpp
// and modified by Doris

#include "vec/columns/column_nullable.h"

#include "vec/columns/column_const.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/sip_hash.h"
#include "vec/core/sort_block.h"
#include "vec/data_types/data_type.h"
#include "vec/utils/util.hpp"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

ColumnNullable::ColumnNullable(MutableColumnPtr&& nested_column_, MutableColumnPtr&& null_map_)
        : NullMapProvider(std::move(null_map_)), nested_column(std::move(nested_column_)) {
    /// ColumnNullable cannot have constant nested column. But constant argument could be passed. Materialize it.
    nested_column = get_nested_column().convert_to_full_column_if_const();

    // after convert const column to full column, it may be a nullable column
    if (nested_column->is_nullable()) {
        assert_cast<ColumnNullable&>(*nested_column)
                .apply_null_map(static_cast<const ColumnUInt8&>(get_null_map_column()));
        reset_null_map(assert_cast<ColumnNullable&>(*nested_column).get_null_map_column_ptr());
        nested_column = assert_cast<ColumnNullable&>(*nested_column).get_nested_column_ptr();
    }

    if (is_column_const(get_null_map_column())) [[unlikely]] {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "ColumnNullable cannot have constant null map");
        __builtin_unreachable();
    }
    _need_update_has_null = true;
}

void ColumnNullable::shrink_padding_chars() {
    get_nested_column_ptr()->shrink_padding_chars();
}

void ColumnNullable::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                              const uint8_t* __restrict null_data) const {
    if (!has_null()) {
        nested_column->update_xxHash_with_value(start, end, hash, nullptr);
    } else {
        const auto* __restrict real_null_data =
                assert_cast<const ColumnUInt8&>(get_null_map_column()).get_data().data();
        for (size_t i = start; i < end; ++i) {
            if (real_null_data[i] != 0) {
                hash = HashUtil::xxHash64NullWithSeed(hash);
            }
        }
        nested_column->update_xxHash_with_value(start, end, hash, real_null_data);
    }
}

void ColumnNullable::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                           const uint8_t* __restrict null_data) const {
    if (!has_null()) {
        nested_column->update_crc_with_value(start, end, hash, nullptr);
    } else {
        const auto* __restrict real_null_data =
                assert_cast<const ColumnUInt8&>(get_null_map_column()).get_data().data();
        for (size_t i = start; i < end; ++i) {
            if (real_null_data[i] != 0) {
                hash = HashUtil::zlib_crc_hash_null(hash);
            }
        }
        nested_column->update_crc_with_value(start, end, hash, real_null_data);
    }
}

void ColumnNullable::update_hash_with_value(size_t n, SipHash& hash) const {
    if (is_null_at(n)) {
        hash.update(0);
    } else {
        get_nested_column().update_hash_with_value(n, hash);
    }
}

void ColumnNullable::update_crcs_with_value(uint32_t* __restrict hashes, doris::PrimitiveType type,
                                            uint32_t rows, uint32_t offset,
                                            const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    auto s = rows;
    DCHECK(s == size());
    const auto* __restrict real_null_data =
            assert_cast<const ColumnUInt8&>(get_null_map_column()).get_data().data();
    if (!has_null()) {
        nested_column->update_crcs_with_value(hashes, type, rows, offset, nullptr);
    } else {
        for (int i = 0; i < s; ++i) {
            if (real_null_data[i] != 0) {
                hashes[i] = HashUtil::zlib_crc_hash_null(hashes[i]);
            }
        }
        nested_column->update_crcs_with_value(hashes, type, rows, offset, real_null_data);
    }
}

void ColumnNullable::update_hashes_with_value(uint64_t* __restrict hashes,
                                              const uint8_t* __restrict null_data) const {
    DCHECK(null_data == nullptr);
    auto s = size();
    const auto* __restrict real_null_data =
            assert_cast<const ColumnUInt8&>(get_null_map_column()).get_data().data();
    if (!has_null()) {
        nested_column->update_hashes_with_value(hashes, nullptr);
    } else {
        for (int i = 0; i < s; ++i) {
            if (real_null_data[i] != 0) {
                hashes[i] = HashUtil::xxHash64NullWithSeed(hashes[i]);
            }
        }
        nested_column->update_hashes_with_value(hashes, real_null_data);
    }
}

MutableColumnPtr ColumnNullable::clone_resized(size_t new_size) const {
    MutableColumnPtr new_nested_col = get_nested_column().clone_resized(new_size);
    auto new_null_map = ColumnUInt8::create();

    if (new_size > 0) {
        new_null_map->get_data().resize(new_size);

        size_t count = std::min(size(), new_size);
        memcpy(new_null_map->get_data().data(), get_null_map_data().data(),
               count * sizeof(get_null_map_data()[0]));

        /// If resizing to bigger one, set all new values to NULLs.
        if (new_size > count) {
            memset(&new_null_map->get_data()[count], 1, new_size - count);
        }
    }

    return ColumnNullable::create(std::move(new_nested_col), std::move(new_null_map));
}

Field ColumnNullable::operator[](size_t n) const {
    return is_null_at(n) ? Field::create_field<TYPE_NULL>(Null()) : get_nested_column()[n];
}

void ColumnNullable::get(size_t n, Field& res) const {
    if (is_null_at(n)) {
        res = Field();
    } else {
        get_nested_column().get(n, res);
    }
}

StringRef ColumnNullable::get_data_at(size_t n) const {
    if (is_null_at(n)) {
        return {(const char*)nullptr, 0};
    }
    return get_nested_column().get_data_at(n);
}

void ColumnNullable::insert_data(const char* pos, size_t length) {
    if (pos == nullptr) {
        get_nested_column().insert_default();
        get_null_map_data().push_back(1);
        _has_null = true;
        _need_update_has_null = false;
    } else {
        get_nested_column().insert_data(pos, length);
        _push_false_to_nullmap(1);
    }
}

void ColumnNullable::insert_many_strings(const StringRef* strings, size_t num) {
    auto not_null_count = 0;
    for (size_t i = 0; i != num; ++i) {
        if (strings[i].data == nullptr) {
            _push_false_to_nullmap(not_null_count);
            not_null_count = 0;
            get_null_map_data().push_back(1);
            _has_null = true;
        } else {
            not_null_count++;
        }
    }
    if (not_null_count) {
        _push_false_to_nullmap(not_null_count);
    }
    nested_column->insert_many_strings(strings, num);
}

void ColumnNullable::insert_many_from(const IColumn& src, size_t position, size_t length) {
    const auto& nullable_col = assert_cast<const ColumnNullable&>(src);
    get_null_map_column().insert_many_from(nullable_col.get_null_map_column(), position, length);
    get_nested_column().insert_many_from(*nullable_col.nested_column, position, length);
}

StringRef ColumnNullable::serialize_value_into_arena(size_t n, Arena& arena,
                                                     char const*& begin) const {
    auto* pos = arena.alloc_continue(serialize_size_at(n), begin);
    return {pos, serialize_impl(pos, n)};
}

const char* ColumnNullable::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

size_t ColumnNullable::deserialize_impl(const char* pos) {
    size_t sz = 0;
    UInt8 val = *reinterpret_cast<const UInt8*>(pos);
    sz += sizeof(val);

    get_null_map_data().push_back(val);

    if (val == 0) {
        sz += get_nested_column().deserialize_impl(pos + sz);
    } else {
        get_nested_column().insert_default();
        _has_null = true;
        _need_update_has_null = false;
    }
    return sz;
}

size_t ColumnNullable::get_max_row_byte_size() const {
    constexpr auto flag_size = sizeof(NullMap::value_type);
    return flag_size + get_nested_column().get_max_row_byte_size();
}

size_t ColumnNullable::serialize_impl(char* pos, const size_t row) const {
    const auto& arr = get_null_map_data();
    memcpy_fixed<NullMap::value_type>(pos, (char*)&arr[row]);
    if (arr[row]) {
        return sizeof(NullMap::value_type);
    }
    return sizeof(NullMap::value_type) +
           get_nested_column().serialize_impl(pos + sizeof(NullMap::value_type), row);
}

void ColumnNullable::serialize_vec(StringRef* keys, size_t num_rows) const {
    const bool has_null = simd::contain_byte(get_null_map_data().data(), num_rows, 1);
    if (has_null) {
        for (size_t i = 0; i < num_rows; ++i) {
            keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
        }
    } else {
        const auto& arr = get_null_map_data();
        for (size_t i = 0; i < num_rows; ++i) {
            memcpy_fixed<NullMap::value_type>(const_cast<char*>(keys[i].data + keys[i].size),
                                              (char*)&arr[i]);
            keys[i].size += sizeof(NullMap::value_type);
        }
        nested_column->serialize_vec(keys, num_rows);
    }
}

void ColumnNullable::deserialize_vec(StringRef* keys, const size_t num_rows) {
    auto& arr = get_null_map_data();
    const size_t old_size = arr.size();
    arr.reserve(old_size + num_rows);

    for (size_t i = 0; i != num_rows; ++i) {
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

void ColumnNullable::insert_range_from_ignore_overflow(const doris::vectorized::IColumn& src,
                                                       size_t start, size_t length) {
    const auto& nullable_col = assert_cast<const ColumnNullable&>(src);
    get_null_map_column().insert_range_from(nullable_col.get_null_map_column(), start, length);
    get_nested_column().insert_range_from_ignore_overflow(*nullable_col.nested_column, start,
                                                          length);
}

void ColumnNullable::insert_range_from(const IColumn& src, size_t start, size_t length) {
    const auto& nullable_col = assert_cast<const ColumnNullable&>(src);
    get_null_map_column().insert_range_from(nullable_col.get_null_map_column(), start, length);
    get_nested_column().insert_range_from(*nullable_col.nested_column, start, length);
}

void ColumnNullable::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                         const uint32_t* indices_end) {
    const auto& src_concrete = assert_cast<const ColumnNullable&>(src);
    get_nested_column().insert_indices_from(src_concrete.get_nested_column(), indices_begin,
                                            indices_end);
    get_null_map_column().insert_indices_from(src_concrete.get_null_map_column(), indices_begin,
                                              indices_end);
}

void ColumnNullable::insert_indices_from_not_has_null(const IColumn& src,
                                                      const uint32_t* indices_begin,
                                                      const uint32_t* indices_end) {
    const auto& src_concrete = assert_cast<const ColumnNullable&>(src);
    get_nested_column().insert_indices_from(src_concrete.get_nested_column(), indices_begin,
                                            indices_end);
    _push_false_to_nullmap(indices_end - indices_begin);
}

void ColumnNullable::insert(const Field& x) {
    if (x.is_null()) {
        get_nested_column().insert_default();
        get_null_map_data().push_back(1);
        _has_null = true;
        _need_update_has_null = false;
    } else {
        get_nested_column().insert(x);
        _push_false_to_nullmap(1);
    }
}

void ColumnNullable::insert_from(const IColumn& src, size_t n) {
    const auto& src_concrete = assert_cast<const ColumnNullable&>(src);
    get_nested_column().insert_from(src_concrete.get_nested_column(), n);
    auto is_null = src_concrete.get_null_map_data()[n];
    get_null_map_data().push_back(is_null);
}

void ColumnNullable::append_data_by_selector(IColumn::MutablePtr& res,
                                             const IColumn::Selector& selector) const {
    append_data_by_selector(res, selector, 0, selector.size());
}

void ColumnNullable::append_data_by_selector(IColumn::MutablePtr& res,
                                             const IColumn::Selector& selector, size_t begin,
                                             size_t end) const {
    auto& res_column = assert_cast<ColumnNullable&>(*res);
    auto res_nested_column = res_column.get_nested_column_ptr();
    this->get_nested_column().append_data_by_selector(res_nested_column, selector, begin, end);
    auto res_null_map = res_column.get_null_map_column_ptr();
    this->get_null_map_column().append_data_by_selector(res_null_map, selector, begin, end);
}

void ColumnNullable::insert_range_from_not_nullable(const IColumn& src, size_t start,
                                                    size_t length) {
    get_nested_column().insert_range_from(src, start, length);
    _push_false_to_nullmap(length);
}

void ColumnNullable::pop_back(size_t n) {
    get_nested_column().pop_back(n);
    get_null_map_column().pop_back(n);
}

ColumnPtr ColumnNullable::filter(const Filter& filt, ssize_t result_size_hint) const {
    ColumnPtr filtered_data = get_nested_column().filter(filt, result_size_hint);
    ColumnPtr filtered_null_map = get_null_map_column().filter(filt, result_size_hint);
    return ColumnNullable::create(filtered_data, filtered_null_map);
}

size_t ColumnNullable::filter(const Filter& filter) {
    const auto data_result_size = get_nested_column().filter(filter);
    const auto map_result_size = get_null_map_column().filter(filter);
    CHECK_EQ(data_result_size, map_result_size);
    return data_result_size;
}

Status ColumnNullable::filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) {
    auto* nullable_col_ptr = assert_cast<ColumnNullable*>(col_ptr);
    ColumnPtr nest_col_ptr = nullable_col_ptr->nested_column;

    /// `get_null_map_data` will set `_need_update_has_null` to true
    auto& res_nullmap = nullable_col_ptr->get_null_map_data();

    RETURN_IF_ERROR(get_nested_column().filter_by_selector(
            sel, sel_size, const_cast<doris::vectorized::IColumn*>(nest_col_ptr.get())));
    DCHECK(res_nullmap.empty());
    res_nullmap.resize(sel_size);
    auto& cur_nullmap = get_null_map_column().get_data();
    for (size_t i = 0; i < sel_size; i++) {
        res_nullmap[i] = cur_nullmap[sel[i]];
    }
    return Status::OK();
}

MutableColumnPtr ColumnNullable::permute(const Permutation& perm, size_t limit) const {
    MutableColumnPtr permuted_data = get_nested_column().permute(perm, limit);
    MutableColumnPtr permuted_null_map = get_null_map_column().permute(perm, limit);
    return ColumnNullable::create(std::move(permuted_data), std::move(permuted_null_map));
}

int ColumnNullable::compare_at(size_t n, size_t m, const IColumn& rhs_,
                               int null_direction_hint) const {
    /// NULL values share the properties of NaN values.
    /// Here the last parameter of compare_at is called null_direction_hint
    /// instead of the usual nan_direction_hint and is used to implement
    /// the ordering specified by either NULLS FIRST or NULLS LAST in the
    /// ORDER BY construction.
    const auto& nullable_rhs = assert_cast<const ColumnNullable&>(rhs_);

    if (is_null_at(n)) {
        return nullable_rhs.is_null_at(m) ? 0 : null_direction_hint;
    }
    if (nullable_rhs.is_null_at(m)) {
        return -null_direction_hint;
    }

    return get_nested_column().compare_at(n, m, nullable_rhs.get_nested_column(),
                                          null_direction_hint);
}

void ColumnNullable::compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                                      int direction, std::vector<uint8_t>& cmp_res,
                                      uint8_t* __restrict filter) const {
    const auto& rhs_null_column = assert_cast<const ColumnNullable&>(rhs);
    const bool right_is_null = rhs.is_null_at(rhs_row_id);
    const bool left_contains_null = has_null();
    if (!left_contains_null && !right_is_null) {
        get_nested_column().compare_internal(rhs_row_id, rhs_null_column.get_nested_column(),
                                             nan_direction_hint, direction, cmp_res, filter);
    } else {
        auto sz = this->size();
        DCHECK(cmp_res.size() == sz);

        size_t begin = simd::find_zero(cmp_res, 0);
        while (begin < sz) {
            size_t end = simd::find_one(cmp_res, begin + 1);
            if (right_is_null) {
                for (size_t row_id = begin; row_id < end; row_id++) {
                    if (!is_null_at(row_id)) {
                        if ((-nan_direction_hint * direction) < 0) {
                            filter[row_id] = 1;
                            cmp_res[row_id] = 1;
                        } else if ((-nan_direction_hint * direction) > 0) {
                            cmp_res[row_id] = 1;
                        }
                    }
                }
            } else {
                for (size_t row_id = begin; row_id < end; row_id++) {
                    if (is_null_at(row_id)) {
                        if (nan_direction_hint * direction < 0) {
                            filter[row_id] = 1;
                            cmp_res[row_id] = 1;
                        } else if (nan_direction_hint * direction > 0) {
                            cmp_res[row_id] = 1;
                        }
                    }
                }
            }
            begin = simd::find_zero(cmp_res, end + 1);
        }
        if (!right_is_null) {
            get_nested_column().compare_internal(rhs_row_id, rhs_null_column.get_nested_column(),
                                                 nan_direction_hint, direction, cmp_res, filter);
        }
    }
}

void ColumnNullable::get_permutation(bool reverse, size_t limit, int null_direction_hint,
                                     Permutation& res) const {
    /// Cannot pass limit because of unknown amount of NULLs.
    get_nested_column().get_permutation(reverse, 0, null_direction_hint, res);

    if ((null_direction_hint > 0) != reverse) {
        /// Shift all NULL values to the end.

        size_t read_idx = 0;
        size_t write_idx = 0;
        size_t end_idx = res.size();

        if (!limit) {
            limit = end_idx;
        } else {
            limit = std::min(end_idx, limit);
        }

        while (read_idx < limit && !is_null_at(res[read_idx])) {
            ++read_idx;
            ++write_idx;
        }

        ++read_idx;

        /// Invariants:
        ///  write_idx < read_idx
        ///  write_idx points to NULL
        ///  read_idx will be incremented to position of next not-NULL
        ///  there are range of NULLs between write_idx and read_idx - 1,
        /// We are moving elements from end to begin of this range,
        ///  so range will "bubble" towards the end.
        /// Relative order of NULL elements could be changed,
        ///  but relative order of non-NULLs is preserved.

        while (read_idx < end_idx && write_idx < limit) {
            if (!is_null_at(res[read_idx])) {
                std::swap(res[read_idx], res[write_idx]);
                ++write_idx;
            }
            ++read_idx;
        }
    } else {
        /// Shift all NULL values to the beginning.

        ssize_t read_idx = res.size() - 1;
        ssize_t write_idx = res.size() - 1;

        while (read_idx >= 0 && !is_null_at(res[read_idx])) {
            --read_idx;
            --write_idx;
        }

        --read_idx;

        while (read_idx >= 0 && write_idx >= 0) {
            if (!is_null_at(res[read_idx])) {
                std::swap(res[read_idx], res[write_idx]);
                --write_idx;
            }
            --read_idx;
        }
    }
}

void ColumnNullable::reserve(size_t n) {
    get_nested_column().reserve(n);
    get_null_map_data(false).reserve(n);
}

void ColumnNullable::resize(size_t n) {
    auto& null_map_data = get_null_map_data();
    get_nested_column().resize(n);
    null_map_data.resize(n);
}

size_t ColumnNullable::byte_size() const {
    return get_nested_column().byte_size() + get_null_map_column().byte_size();
}

size_t ColumnNullable::allocated_bytes() const {
    return get_nested_column().allocated_bytes() + get_null_map_column().allocated_bytes();
}

bool ColumnNullable::has_enough_capacity(const IColumn& src) const {
    const auto& src_concrete = assert_cast<const ColumnNullable&>(src);
    return get_nested_column().has_enough_capacity(src_concrete.get_nested_column()) &&
           get_null_map_column().has_enough_capacity(src_concrete.get_null_map_column());
}

template <bool negative>
void ColumnNullable::apply_null_map_impl(const ColumnUInt8& map) {
    NullMap& arr1 = get_null_map_data();
    const NullMap& arr2 = map.get_data();

    if (arr1.size() != arr2.size()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Inconsistent sizes of ColumnNullable objects. Self: {}. Expect: {}",
                               arr1.size(), arr2.size());
    }

    for (size_t i = 0, size = arr1.size(); i < size; ++i) {
        arr1[i] |= negative ^ arr2[i];
    }
}

void ColumnNullable::apply_null_map(const ColumnUInt8& map) {
    apply_null_map_impl<false>(map);
}

void ColumnNullable::apply_negated_null_map(const ColumnUInt8& map) {
    apply_null_map_impl<true>(map);
}

void ColumnNullable::apply_null_map(const ColumnNullable& other) {
    apply_null_map(other.get_null_map_column());
}

void ColumnNullable::check_consistency() const {
    if (get_null_map_column().size() != get_nested_column().size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "Sizes of nested column and null map of Nullable column are not equal");
    }
}

void ColumnNullable::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                                 IColumn::Permutation& perms, EqualRange& range,
                                 bool last_column) const {
    sorter->sort_column(static_cast<const ColumnNullable&>(*this), flags, perms, range,
                        last_column);
}

void ColumnNullable::_update_has_null() {
    const UInt8* null_pos = get_null_map_data().data();
    _has_null = simd::contain_byte(null_pos, get_null_map_data().size(), 1);
    _need_update_has_null = false;
}

bool ColumnNullable::has_null(size_t size) const {
    if (!_has_null && !_need_update_has_null) {
        return false;
    }
    const UInt8* null_pos = get_null_map_data().data();
    return simd::contain_byte(null_pos, size, 1);
}

ColumnPtr make_nullable(const ColumnPtr& column, bool is_nullable) {
    if (is_column_nullable(*column)) {
        return column;
    }

    if (is_column_const(*column)) {
        return ColumnConst::create(
                make_nullable(assert_cast<const ColumnConst&>(*column).get_data_column_ptr(),
                              is_nullable),
                column->size());
    }

    return ColumnNullable::create(column, ColumnUInt8::create(column->size(), is_nullable ? 1 : 0));
}

ColumnPtr remove_nullable(const ColumnPtr& column) {
    if (is_column_nullable(*column)) {
        return assert_cast<const ColumnNullable*, TypeCheckOnRelease::DISABLE>(column.get())
                ->get_nested_column_ptr();
    }

    if (is_column_const(*column)) {
        const auto& column_nested =
                assert_cast<const ColumnConst&, TypeCheckOnRelease::DISABLE>(*column)
                        .get_data_column_ptr();
        if (is_column_nullable(*column_nested)) {
            return ColumnConst::create(
                    assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*column_nested)
                            .get_nested_column_ptr(),
                    column->size());
        }
    }

    return column;
}

} // namespace doris::vectorized
