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

#include "vec/columns/column_map.h"

#include <string.h>

#include <algorithm>
#include <boost/iterator/iterator_facade.hpp>
#include <limits>
#include <memory>
#include <vector>

#include "common/status.h"
#include "pdqsort.h"
#include "runtime/primitive_type.h"
#include "vec/common/arena.h"
#include "vec/common/typeid_cast.h"
#include "vec/common/unaligned.h"
#include "vec/core/sort_block.h"

class SipHash;

namespace doris::vectorized {

/** A column of map values.
  */
std::string ColumnMap::get_name() const {
    return "Map(" + keys_column->get_name() + ", " + values_column->get_name() + ")";
}

ColumnMap::ColumnMap(MutableColumnPtr&& keys, MutableColumnPtr&& values, MutableColumnPtr&& offsets)
        : keys_column(std::move(keys)),
          values_column(std::move(values)),
          offsets_column(std::move(offsets)) {
    const auto* offsets_concrete = assert_cast<const COffsets*>(offsets_column.get());

    if (!offsets_concrete->empty() && keys_column && values_column) {
        auto last_offset = offsets_concrete->get_data().back();

        /// This will also prevent possible overflow in offset.
        if (keys_column->size() != last_offset) {
            throw doris::Exception(
                    doris::ErrorCode::INTERNAL_ERROR,
                    "offsets_column size {} has data inconsistent with key_column {}", last_offset,
                    keys_column->size());
        }
        if (values_column->size() != last_offset) {
            throw doris::Exception(
                    doris::ErrorCode::INTERNAL_ERROR,
                    "offsets_column size {} has data inconsistent with value_column {}",
                    last_offset, values_column->size());
        }
    }
}

// todo. here to resize every row map
MutableColumnPtr ColumnMap::clone_resized(size_t to_size) const {
    auto res = ColumnMap::create(get_keys().clone_empty(), get_values().clone_empty(),
                                 COffsets::create());
    if (to_size == 0) {
        return res;
    }

    size_t from_size = size();

    if (to_size <= from_size) {
        res->get_offsets().assign(get_offsets().begin(), get_offsets().begin() + to_size);
        res->get_keys().insert_range_from(get_keys(), 0, get_offsets()[to_size - 1]);
        res->get_values().insert_range_from(get_values(), 0, get_offsets()[to_size - 1]);
    } else {
        /// Copy column and append empty arrays for extra elements.
        Offset64 offset = 0;
        if (from_size > 0) {
            res->get_offsets().assign(get_offsets().begin(), get_offsets().end());
            res->get_keys().insert_range_from(get_keys(), 0, get_keys().size());
            res->get_values().insert_range_from(get_values(), 0, get_values().size());
            offset = get_offsets().back();
        }
        res->get_offsets().resize(to_size);
        for (size_t i = from_size; i < to_size; ++i) {
            res->get_offsets()[i] = offset;
        }
    }
    return res;
}

// to support field functions
Field ColumnMap::operator[](size_t n) const {
    size_t start_offset = offset_at(n);
    size_t element_size = size_at(n);

    if (element_size > max_array_size_as_field) {
        throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                               "element size {} is too large to be manipulated as single map "
                               "field, maximum size {}",
                               element_size, max_array_size_as_field);
    }

    Array k(element_size), v(element_size);

    for (size_t i = 0; i < element_size; ++i) {
        k[i] = get_keys()[start_offset + i];
        v[i] = get_values()[start_offset + i];
    }

    return Field::create_field<TYPE_MAP>(
            Map {Field::create_field<TYPE_ARRAY>(k), Field::create_field<TYPE_ARRAY>(v)});
}

// here to compare to below
void ColumnMap::get(size_t n, Field& res) const {
    res = operator[](n);
}

void ColumnMap::insert(const Field& x) {
    DCHECK_EQ(x.get_type(), PrimitiveType::TYPE_MAP);
    const auto& map = doris::vectorized::get<const Map&>(x);
    CHECK_EQ(map.size(), 2);
    const auto& k_f = doris::vectorized::get<const Array&>(map[0]);
    const auto& v_f = doris::vectorized::get<const Array&>(map[1]);

    size_t element_size = k_f.size();

    for (size_t i = 0; i < element_size; ++i) {
        keys_column->insert(k_f[i]);
        values_column->insert(v_f[i]);
    }
    get_offsets().push_back(get_offsets().back() + element_size);
}

void ColumnMap::insert_default() {
    auto last_offset = get_offsets().back();
    get_offsets().push_back(last_offset);
}

void ColumnMap::pop_back(size_t n) {
    auto& offsets_data = get_offsets();
    DCHECK(n <= offsets_data.size());
    size_t elems_size = offsets_data.back() - offset_at(offsets_data.size() - n);

    DCHECK_EQ(keys_column->size(), values_column->size());
    if (elems_size) {
        keys_column->pop_back(elems_size);
        values_column->pop_back(elems_size);
    }

    offsets_data.resize_assume_reserved(offsets_data.size() - n);
}

void ColumnMap::insert_from(const IColumn& src_, size_t n) {
    DCHECK(n < src_.size());
    const ColumnMap& src = assert_cast<const ColumnMap&>(src_);
    size_t size = src.size_at(n);
    size_t offset = src.offset_at(n);

    if ((!get_keys().is_nullable() && src.get_keys().is_nullable()) ||
        (!get_values().is_nullable() && src.get_values().is_nullable())) {
        DCHECK(false);
    } else if ((get_keys().is_nullable() && !src.get_keys().is_nullable()) ||
               (get_values().is_nullable() && !src.get_values().is_nullable())) {
        DCHECK(false);
    } else {
        keys_column->insert_range_from(src.get_keys(), offset, size);
        values_column->insert_range_from(src.get_values(), offset, size);
    }

    get_offsets().push_back(get_offsets().back() + size);
}

void ColumnMap::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                    const uint32_t* indices_end) {
    for (const auto* x = indices_begin; x != indices_end; ++x) {
        ColumnMap::insert_from(src, *x);
    }
}

void ColumnMap::insert_many_from(const IColumn& src, size_t position, size_t length) {
    for (auto x = 0; x != length; ++x) {
        ColumnMap::insert_from(src, position);
    }
}

StringRef ColumnMap::serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const {
    char* pos = arena.alloc_continue(serialize_size_at(n), begin);
    return {pos, serialize_impl(pos, n)};
}

size_t ColumnMap::serialize_impl(char* pos, const size_t row) const {
    size_t array_size = size_at(row);
    size_t offset = offset_at(row);

    memcpy_fixed<size_t>(pos, (char*)&array_size);
    size_t sz = sizeof(array_size);
    for (size_t i = 0; i < array_size; ++i) {
        sz += get_keys().serialize_impl(pos + sz, offset + i);
    }

    for (size_t i = 0; i < array_size; ++i) {
        sz += get_values().serialize_impl(pos + sz, offset + i);
    }

    DCHECK_EQ(sz, serialize_size_at(row));
    return sz;
}

size_t ColumnMap::serialize_size_at(size_t row) const {
    size_t array_size = size_at(row);
    size_t offset = offset_at(row);

    size_t sz = 0;

    for (size_t i = 0; i < array_size; ++i) {
        sz += get_keys().serialize_size_at(offset + i);
    }

    for (size_t i = 0; i < array_size; ++i) {
        sz += get_values().serialize_size_at(offset + i);
    }

    return sz + sizeof(size_t);
}

size_t ColumnMap::deserialize_impl(const char* pos) {
    size_t sz = 0;
    size_t array_size = unaligned_load<size_t>(pos);
    sz += sizeof(array_size);

    for (size_t i = 0; i < array_size; ++i) {
        sz += get_keys().deserialize_impl(pos + sz);
    }

    for (size_t i = 0; i < array_size; ++i) {
        sz += get_values().deserialize_impl(pos + sz);
    }

    get_offsets().push_back(get_offsets().back() + array_size);
    return sz;
}

const char* ColumnMap::deserialize_and_insert_from_arena(const char* pos) {
    return pos + deserialize_impl(pos);
}

int ColumnMap::compare_at(size_t n, size_t m, const IColumn& rhs_, int nan_direction_hint) const {
    const auto& rhs = assert_cast<const ColumnMap&, TypeCheckOnRelease::DISABLE>(rhs_);

    size_t lhs_size = size_at(n);
    size_t rhs_size = rhs.size_at(m);

    size_t lhs_offset = offset_at(n);
    size_t rhs_offset = rhs.offset_at(m);

    size_t min_size = std::min(lhs_size, rhs_size);

    for (size_t i = 0; i < min_size; ++i) {
        // if any value in key not equal, just return
        if (int res = get_keys().compare_at(lhs_offset + i, rhs_offset + i, rhs.get_keys(),
                                            nan_direction_hint);
            res) {
            return res;
        }
        // // if any value in value not equal, just return
        if (int res = get_values().compare_at(lhs_offset + i, rhs_offset + i, rhs.get_values(),
                                              nan_direction_hint);
            res) {
            return res;
        }
    }

    return lhs_size < rhs_size ? -1 : (lhs_size == rhs_size ? 0 : 1);
}

void ColumnMap::update_hash_with_value(size_t n, SipHash& hash) const {
    size_t kv_size = size_at(n);
    size_t offset = offset_at(n);

    hash.update(reinterpret_cast<const char*>(&kv_size), sizeof(kv_size));
    for (size_t i = 0; i < kv_size; ++i) {
        get_keys().update_hash_with_value(offset + i, hash);
        get_values().update_hash_with_value(offset + i, hash);
    }
}

void ColumnMap::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                         const uint8_t* __restrict null_data) const {
    auto& offsets = get_offsets();
    if (null_data) {
        for (size_t i = start; i < end; ++i) {
            if (null_data[i] == 0) {
                size_t kv_size = offsets[i] - offsets[i - 1];
                if (kv_size == 0) {
                    hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&kv_size),
                                                      sizeof(kv_size), hash);
                } else {
                    get_keys().update_xxHash_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                    get_values().update_xxHash_with_value(offsets[i - 1], offsets[i], hash,
                                                          nullptr);
                }
            }
        }
    } else {
        for (size_t i = start; i < end; ++i) {
            size_t kv_size = offsets[i] - offsets[i - 1];
            if (kv_size == 0) {
                hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&kv_size),
                                                  sizeof(kv_size), hash);
            } else {
                get_keys().update_xxHash_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                get_values().update_xxHash_with_value(offsets[i - 1], offsets[i], hash, nullptr);
            }
        }
    }
}

void ColumnMap::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                      const uint8_t* __restrict null_data) const {
    auto& offsets = get_offsets();
    if (null_data) {
        for (size_t i = start; i < end; ++i) {
            if (null_data[i] == 0) {
                size_t kv_size = offsets[i] - offsets[i - 1];
                if (kv_size == 0) {
                    hash = HashUtil::zlib_crc_hash(reinterpret_cast<const char*>(&kv_size),
                                                   sizeof(kv_size), hash);
                } else {
                    get_keys().update_crc_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                    get_values().update_crc_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                }
            }
        }
    } else {
        for (size_t i = start; i < end; ++i) {
            size_t kv_size = offsets[i] - offsets[i - 1];
            if (kv_size == 0) {
                hash = HashUtil::zlib_crc_hash(reinterpret_cast<const char*>(&kv_size),
                                               sizeof(kv_size), hash);
            } else {
                get_keys().update_crc_with_value(offsets[i - 1], offsets[i], hash, nullptr);
                get_values().update_crc_with_value(offsets[i - 1], offsets[i], hash, nullptr);
            }
        }
    }
}

void ColumnMap::update_hashes_with_value(uint64_t* hashes, const uint8_t* null_data) const {
    size_t s = size();
    if (null_data) {
        for (size_t i = 0; i < s; ++i) {
            // every row
            if (null_data[i] == 0) {
                update_xxHash_with_value(i, i + 1, hashes[i], nullptr);
            }
        }
    } else {
        for (size_t i = 0; i < s; ++i) {
            update_xxHash_with_value(i, i + 1, hashes[i], nullptr);
        }
    }
}

void ColumnMap::update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type, uint32_t rows,
                                       uint32_t offset, const uint8_t* __restrict null_data) const {
    auto s = rows;
    DCHECK(s == size());

    if (null_data) {
        for (size_t i = 0; i < s; ++i) {
            // every row
            if (null_data[i] == 0) {
                update_crc_with_value(i, i + 1, hash[i], nullptr);
            }
        }
    } else {
        for (size_t i = 0; i < s; ++i) {
            update_crc_with_value(i, i + 1, hash[i], nullptr);
        }
    }
}

void ColumnMap::insert_range_from(const IColumn& src, size_t start, size_t length) {
    if (length == 0) {
        return;
    }

    const ColumnMap& src_concrete = assert_cast<const ColumnMap&>(src);

    if (start + length > src_concrete.size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Parameter out of bound in ColumnMap::insert_range_from method. "
                               "[start({}) + length({}) > offsets.size({})]",
                               std::to_string(start), std::to_string(length),
                               std::to_string(src_concrete.size()));
    }

    size_t nested_offset = src_concrete.offset_at(start);
    size_t nested_length = src_concrete.offset_at(start + length) - nested_offset;

    keys_column->insert_range_from(src_concrete.get_keys(), nested_offset, nested_length);
    values_column->insert_range_from(src_concrete.get_values(), nested_offset, nested_length);

    auto& cur_offsets = get_offsets();
    const auto& src_offsets = src_concrete.get_offsets();

    if (start == 0 && cur_offsets.empty()) {
        cur_offsets.assign(src_offsets.begin(), src_offsets.begin() + length);
    } else {
        size_t old_size = cur_offsets.size();
        // -1 is ok, because PaddedPODArray pads zeros on the left.
        size_t prev_max_offset = cur_offsets.back();
        cur_offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i) {
            cur_offsets[old_size + i] = src_offsets[start + i] - nested_offset + prev_max_offset;
        }
    }
}

void ColumnMap::insert_range_from_ignore_overflow(const IColumn& src, size_t start, size_t length) {
    const ColumnMap& src_concrete = assert_cast<const ColumnMap&>(src);

    if (start + length > src_concrete.size()) {
        throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                               "Parameter out of bound in ColumnMap::insert_range_from method. "
                               "[start({}) + length({}) > offsets.size({})]",
                               std::to_string(start), std::to_string(length),
                               std::to_string(src_concrete.size()));
    }

    size_t nested_offset = src_concrete.offset_at(start);
    size_t nested_length = src_concrete.offset_at(start + length) - nested_offset;

    keys_column->insert_range_from_ignore_overflow(src_concrete.get_keys(), nested_offset,
                                                   nested_length);
    values_column->insert_range_from_ignore_overflow(src_concrete.get_values(), nested_offset,
                                                     nested_length);

    auto& cur_offsets = get_offsets();
    const auto& src_offsets = src_concrete.get_offsets();

    if (start == 0 && cur_offsets.empty()) {
        cur_offsets.assign(src_offsets.begin(), src_offsets.begin() + length);
    } else {
        size_t old_size = cur_offsets.size();
        // -1 is ok, because PaddedPODArray pads zeros on the left.
        size_t prev_max_offset = cur_offsets.back();
        cur_offsets.resize(old_size + length);

        for (size_t i = 0; i < length; ++i) {
            cur_offsets[old_size + i] = src_offsets[start + i] - nested_offset + prev_max_offset;
        }
    }
}

ColumnPtr ColumnMap::filter(const Filter& filt, ssize_t result_size_hint) const {
    auto k_arr =
            ColumnArray::create(keys_column->assume_mutable(), offsets_column->assume_mutable())
                    ->filter(filt, result_size_hint);
    auto v_arr =
            ColumnArray::create(values_column->assume_mutable(), offsets_column->assume_mutable())
                    ->filter(filt, result_size_hint);
    return ColumnMap::create(assert_cast<const ColumnArray&>(*k_arr).get_data_ptr(),
                             assert_cast<const ColumnArray&>(*v_arr).get_data_ptr(),
                             assert_cast<const ColumnArray&>(*k_arr).get_offsets_ptr());
}

size_t ColumnMap::filter(const Filter& filter) {
    MutableColumnPtr copied_off = offsets_column->clone_empty();
    copied_off->insert_range_from(*offsets_column, 0, offsets_column->size());
    ColumnArray::create(keys_column->assume_mutable(), offsets_column->assume_mutable())
            ->filter(filter);
    ColumnArray::create(values_column->assume_mutable(), copied_off->assume_mutable())
            ->filter(filter);
    return get_offsets().size();
}

MutableColumnPtr ColumnMap::permute(const Permutation& perm, size_t limit) const {
    // Make a temp column array
    auto k_arr =
            ColumnArray::create(keys_column->assume_mutable(), offsets_column->assume_mutable())
                    ->permute(perm, limit);
    auto v_arr =
            ColumnArray::create(values_column->assume_mutable(), offsets_column->assume_mutable())
                    ->permute(perm, limit);

    return ColumnMap::create(assert_cast<const ColumnArray&>(*k_arr).get_data_ptr(),
                             assert_cast<const ColumnArray&>(*v_arr).get_data_ptr(),
                             assert_cast<const ColumnArray&>(*k_arr).get_offsets_ptr());
}

void ColumnMap::shrink_padding_chars() {
    keys_column->shrink_padding_chars();
    values_column->shrink_padding_chars();
}

void ColumnMap::reserve(size_t n) {
    get_offsets().reserve(n);
    keys_column->reserve(n);
    values_column->reserve(n);
}

void ColumnMap::resize(size_t n) {
    auto last_off = get_offsets().back();
    get_offsets().resize_fill(n, last_off);
    // make new size of data column
    get_keys().resize(get_offsets().back());
    get_values().resize(get_offsets().back());
}

size_t ColumnMap::byte_size() const {
    return keys_column->byte_size() + values_column->byte_size() + offsets_column->byte_size();
    ;
}

size_t ColumnMap::allocated_bytes() const {
    return keys_column->allocated_bytes() + values_column->allocated_bytes() +
           get_offsets().allocated_bytes();
}

bool ColumnMap::has_enough_capacity(const IColumn& src) const {
    const auto& src_concrete = assert_cast<const ColumnMap&>(src);
    return keys_column->has_enough_capacity(*src_concrete.keys_column) &&
           values_column->has_enough_capacity(*src_concrete.values_column) &&
           offsets_column->has_enough_capacity(*src_concrete.offsets_column);
}

ColumnPtr ColumnMap::convert_to_full_column_if_const() const {
    return ColumnMap::create(keys_column->convert_to_full_column_if_const(),
                             values_column->convert_to_full_column_if_const(),
                             offsets_column->convert_to_full_column_if_const());
}

void ColumnMap::erase(size_t start, size_t length) {
    if (start >= size() || length == 0) {
        return;
    }
    length = std::min(length, size() - start);

    const auto& offsets_data = get_offsets();
    auto entry_start = offsets_data[start - 1];
    auto entry_end = offsets_data[start + length - 1];
    auto entry_length = entry_end - entry_start;

    keys_column->erase(entry_start, entry_length);
    values_column->erase(entry_start, entry_length);
    offsets_column->erase(start, length);

    for (auto i = start; i < size(); ++i) {
        get_offsets()[i] -= entry_length;
    }
}

template <bool positive>
struct ColumnMap::less {
    const ColumnMap& parent;
    const int nan_direction_hint;
    explicit less(const ColumnMap& parent_, int nan_direction_hint_)
            : parent(parent_), nan_direction_hint(nan_direction_hint_) {}
    bool operator()(size_t lhs, size_t rhs) const {
        size_t lhs_size = parent.size_at(lhs);
        size_t rhs_size = parent.size_at(rhs);
        size_t min_size = std::min(lhs_size, rhs_size);
        int res = 0;
        for (size_t i = 0; i < min_size; ++i) {
            if (res = parent.get_keys().compare_at(
                        parent.offset_at(lhs) + i, parent.offset_at(rhs) + i,
                        *parent.get_keys_ptr().get(), nan_direction_hint);
                res) {
                // if res != 0 , here is something different ,just return
                break;
            }
            if (res = parent.get_values().compare_at(
                        parent.offset_at(lhs) + i, parent.offset_at(rhs) + i,
                        *parent.get_values_ptr().get(), nan_direction_hint);
                res) {
                // if res != 0 , here is something different ,just return
                break;
            }
        }
        if (res == 0) {
            // then we check size of array
            res = lhs_size < rhs_size ? -1 : (lhs_size == rhs_size ? 0 : 1);
        }

        return positive ? (res < 0) : (res > 0);
    }
};

void ColumnMap::get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                                IColumn::Permutation& res) const {
    size_t s = size();
    res.resize(s);
    for (size_t i = 0; i < s; ++i) {
        res[i] = i;
    }

    if (reverse) {
        pdqsort(res.begin(), res.end(), ColumnMap::less<false>(*this, nan_direction_hint));
    } else {
        pdqsort(res.begin(), res.end(), ColumnMap::less<true>(*this, nan_direction_hint));
    }
}

void ColumnMap::sort_column(const ColumnSorter* sorter, EqualFlags& flags,
                            IColumn::Permutation& perms, EqualRange& range,
                            bool last_column) const {
    sorter->sort_column(static_cast<const ColumnMap&>(*this), flags, perms, range, last_column);
}

void ColumnMap::serialize_vec(StringRef* keys, size_t num_rows) const {
    for (size_t i = 0; i < num_rows; ++i) {
        keys[i].size += serialize_impl(const_cast<char*>(keys[i].data + keys[i].size), i);
    }
}

void ColumnMap::deserialize_vec(StringRef* keys, const size_t num_rows) {
    for (size_t i = 0; i != num_rows; ++i) {
        auto sz = deserialize_impl(keys[i].data);
        keys[i].data += sz;
        keys[i].size -= sz;
    }
}

size_t ColumnMap::get_max_row_byte_size() const {
    size_t max_size = 0;
    size_t num_rows = size();
    auto max_xz = keys_column->get_max_row_byte_size() + values_column->get_max_row_byte_size();
    for (size_t i = 0; i < num_rows; ++i) {
        max_size = std::max(max_size, size_at(i) * max_xz);
    }

    return sizeof(size_t) + max_size;
}

} // namespace doris::vectorized
