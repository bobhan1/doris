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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnString.h
// and modified by Doris

#pragma once

#include <glog/logging.h>
#include <sys/types.h>

#include <cassert>
#include <cstdint>
#include <cstring>
#include <typeinfo>
#include <vector>

#include "common/cast_set.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "common/status.h"
#include "runtime/define_primitive_type.h"
#include "util/hash_util.hpp"
#include "vec/columns/column.h"
#include "vec/common/assert_cast.h"
#include "vec/common/cow.h"
#include "vec/common/memcmp_small.h"
#include "vec/common/memcpy_small.h"
#include "vec/common/pod_array_fwd.h"
#include "vec/common/sip_hash.h"
#include "vec/common/string_ref.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
class Arena;
class ColumnSorter;

/** Column for String values.
  */
template <typename T>
class ColumnStr final : public COWHelper<IColumn, ColumnStr<T>> {
public:
    using Char = UInt8;
    using Chars = PaddedPODArray<UInt8>;

    static constexpr size_t MAX_STRINGS_OVERFLOW_SIZE = 128;

    void static check_chars_length(size_t total_length, size_t element_number, size_t rows = 0) {
        if constexpr (std::is_same_v<T, UInt32>) {
            if (UNLIKELY(total_length > MAX_STRING_SIZE)) {
                throw Exception(ErrorCode::STRING_OVERFLOW_IN_VEC_ENGINE,
                                "string column length is too large: total_length={}, "
                                "element_number={}, rows={}",
                                total_length, element_number, rows);
            }
        }
    }

private:
    // currently Offsets is uint32, if chars.size() exceeds 4G, offset will overflow.
    // limit chars.size() and check the size when inserting data into ColumnStr<T>.
    static constexpr size_t MAX_STRING_SIZE = 0xffffffff;

    friend class COWHelper<IColumn, ColumnStr<T>>;
    friend class OlapBlockDataConvertor;

    /// Maps i'th position to offset to i+1'th element. Last offset maps to the end of all chars (is the size of all chars).
    PaddedPODArray<T> offsets;

    /// Bytes of strings, placed contiguously.
    /// For convenience, every string ends with terminating zero byte. Note that strings could contain zero bytes in the middle.
    Chars chars;

    // Start position of i-th element.
    T ALWAYS_INLINE offset_at(ssize_t i) const { return offsets[i - 1]; }

    // Size of i-th element, including terminating zero.
    // assume that the length of a single element is less than 32-bit
    uint32_t ALWAYS_INLINE size_at(ssize_t i) const {
        return uint32_t(offsets[i] - offsets[i - 1]);
    }
    size_t serialize_size_at(size_t row) const override {
        auto string_size(size_at(row));
        return sizeof(string_size) + string_size;
    }

    template <bool positive>
    struct less;
    template <bool positive>
    struct lessWithCollation;

    ColumnStr() = default;

    ColumnStr(const ColumnStr<T>& src)
            : offsets(src.offsets.begin(), src.offsets.end()),
              chars(src.chars.begin(), src.chars.end()) {}

public:
    bool is_variable_length() const override { return true; }

    void sanity_check() const override;
    void sanity_check_simple() const;

    std::string get_name() const override { return "String"; }

    size_t size() const override { return offsets.size(); }

    size_t byte_size() const override { return chars.size() + offsets.size() * sizeof(offsets[0]); }

    bool has_enough_capacity(const IColumn& src) const override;

    size_t allocated_bytes() const override {
        return chars.allocated_bytes() + offsets.allocated_bytes();
    }

    MutableColumnPtr clone_resized(size_t to_size) const override;

    void shrink_padding_chars() override;

    Field operator[](size_t n) const override;

    void get(size_t n, Field& res) const override;

    StringRef get_data_at(size_t n) const override {
        DCHECK_LT(n, size());
        sanity_check_simple();
        return StringRef(&chars[offset_at(n)], size_at(n));
    }

    String get_element(size_t n) const { return get_data_at(n).to_string(); }

    void insert_value(const String& value) { insert_data(value.data(), value.size()); }

    void insert(const Field& x) override;

    void insert_many_from(const IColumn& src, size_t position, size_t length) override;

    bool is_column_string64() const override { return sizeof(T) == sizeof(uint64_t); }

    void insert_from(const IColumn& src_, size_t n) override {
        const ColumnStr<T>& src = assert_cast<const ColumnStr<T>&>(src_);
        const size_t size_to_append =
                src.offsets[n] - src.offsets[n - 1]; /// -1th index is Ok, see PaddedPODArray.

        if (!size_to_append) {
            /// shortcut for empty string
            offsets.push_back(chars.size());
        } else {
            const size_t old_size = chars.size();
            const size_t offset = src.offsets[n - 1];
            const size_t new_size = old_size + size_to_append;

            check_chars_length(new_size, offsets.size() + 1);

            chars.resize(new_size);
            memcpy_small_allow_read_write_overflow15(chars.data() + old_size, &src.chars[offset],
                                                     size_to_append);
            offsets.push_back(new_size);
        }
        sanity_check_simple();
    }

    void insert_data(const char* pos, size_t length) override {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length;

        if (length) {
            check_chars_length(new_size, offsets.size() + 1);
            chars.resize(new_size);
            memcpy(chars.data() + old_size, pos, length);
        }
        offsets.push_back(new_size);
        sanity_check_simple();
    }

    void insert_data_without_reserve(const char* pos, size_t length) {
        const size_t old_size = chars.size();
        const size_t new_size = old_size + length;

        if (length) {
            check_chars_length(new_size, offsets.size() + 1);
            chars.resize(new_size);
            memcpy(chars.data() + old_size, pos, length);
        }
        offsets.push_back_without_reserve(new_size);
        sanity_check_simple();
    }

    /// Before insert strings, the caller should calculate the total size of strings,
    /// and reserve the chars & the offsets.
    void insert_many_strings_without_reserve(const StringRef* strings, size_t num) {
        Char* data = chars.data();
        size_t offset = chars.size();
        data += offset;
        size_t length = 0;

        const char* ptr = strings[0].data;
        for (size_t i = 0; i != num; i++) {
            size_t len = strings[i].size;
            length += len;
            offset += len;
            offsets.push_back(offset);

            if (i != num - 1 && strings[i].data + len == strings[i + 1].data) {
                continue;
            }

            if (length != 0) {
                DCHECK(ptr != nullptr);
                memcpy(data, ptr, length);
                data += length;
            }

            if (LIKELY(i != num - 1)) {
                ptr = strings[i + 1].data;
                length = 0;
            }
        }
        check_chars_length(offset, offsets.size());
        chars.resize(offset);
        sanity_check_simple();
    }

    void insert_many_continuous_binary_data(const char* data, const uint32_t* offsets_,
                                            const size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }
        const auto old_size = chars.size();
        const auto begin_offset = offsets_[0];
        const size_t total_mem_size = offsets_[num] - begin_offset;
        if (LIKELY(total_mem_size > 0)) {
            check_chars_length(total_mem_size + old_size, offsets.size() + num);
            chars.resize(total_mem_size + old_size);
            memcpy(chars.data() + old_size, data + begin_offset, total_mem_size);
        }
        const auto old_rows = offsets.size();
        auto tail_offset = offsets.back();
        DCHECK(tail_offset == old_size);
        offsets.resize(old_rows + num);
        auto* offsets_ptr = &offsets[old_rows];

        for (size_t i = 0; i < num; ++i) {
            offsets_ptr[i] = tail_offset + offsets_[i + 1] - begin_offset;
        }
        DCHECK(chars.size() == offsets.back());
        sanity_check_simple();
    }

    void insert_many_strings(const StringRef* strings, size_t num) override {
        size_t new_size = 0;
        for (size_t i = 0; i < num; i++) {
            new_size += strings[i].size;
        }

        const size_t old_size = chars.size();
        check_chars_length(old_size + new_size, offsets.size() + num);
        chars.resize(old_size + new_size);

        Char* data = chars.data();
        size_t offset = old_size;
        for (size_t i = 0; i < num; i++) {
            size_t len = strings[i].size;
            if (len) {
                memcpy(data + offset, strings[i].data, len);
                offset += len;
            }
            offsets.push_back(offset);
        }
        sanity_check_simple();
    }

    template <size_t copy_length>
    void insert_many_strings_fixed_length(const StringRef* strings, size_t num) {
        size_t new_size = 0;
        for (size_t i = 0; i < num; i++) {
            new_size += strings[i].size;
        }

        const size_t old_size = chars.size();
        check_chars_length(old_size + new_size, offsets.size() + num);
        chars.resize(old_size + new_size + copy_length);

        Char* data = chars.data();
        size_t offset = old_size;
        for (size_t i = 0; i < num; i++) {
            size_t len = strings[i].size;
            if (len) {
                memcpy(data + offset, strings[i].data, copy_length);
                offset += len;
            }
            offsets.push_back(offset);
        }
        chars.resize(old_size + new_size);
        sanity_check_simple();
    }

    void insert_many_strings_overflow(const StringRef* strings, size_t num,
                                      size_t max_length) override {
        if (max_length <= 8) {
            insert_many_strings_fixed_length<8>(strings, num);
        } else if (max_length <= 16) {
            insert_many_strings_fixed_length<16>(strings, num);
        } else if (max_length <= 32) {
            insert_many_strings_fixed_length<32>(strings, num);
        } else if (max_length <= 64) {
            insert_many_strings_fixed_length<64>(strings, num);
        } else if (max_length <= 128) {
            insert_many_strings_fixed_length<128>(strings, num);
        } else {
            insert_many_strings(strings, num);
        }
        sanity_check_simple();
    }

    void insert_many_dict_data(const int32_t* data_array, size_t start_index, const StringRef* dict,
                               size_t num, uint32_t /*dict_num*/) override {
        size_t offset_size = offsets.size();
        size_t old_size = chars.size();
        size_t new_size = old_size;
        offsets.resize(offsets.size() + num);

        for (size_t i = 0; i < num; i++) {
            int32_t codeword = data_array[i + start_index];
            new_size += dict[codeword].size;
            offsets[offset_size + i] = static_cast<T>(new_size);
        }

        if (new_size > std::numeric_limits<T>::max()) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "ColumnString insert size out of range type {} [{},{}]",
                                   typeid(T).name(), std::numeric_limits<T>::min(),
                                   std::numeric_limits<T>::max());
        }
        check_chars_length(new_size, offsets.size());
        chars.resize(new_size);

        for (size_t i = start_index; i < start_index + num; i++) {
            int32_t codeword = data_array[i];
            const auto& src = dict[codeword];
            memcpy(chars.data() + old_size, src.data, src.size);
            old_size += src.size;
        }
        sanity_check_simple();
    }

    void pop_back(size_t n) override {
        size_t nested_n = offsets.back() - offset_at(offsets.size() - n);
        chars.resize(chars.size() - nested_n);
        offsets.resize_assume_reserved(offsets.size() - n);
        sanity_check_simple();
    }

    StringRef serialize_value_into_arena(size_t n, Arena& arena, char const*& begin) const override;

    const char* deserialize_and_insert_from_arena(const char* pos) override;

    void deserialize_vec(StringRef* keys, const size_t num_rows) override;

    size_t get_max_row_byte_size() const override;

    void update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                  const uint8_t* __restrict null_data) const override {
        if (null_data) {
            for (size_t i = start; i < end; ++i) {
                if (null_data[i] == 0) {
                    size_t string_size = size_at(i);
                    size_t offset = offset_at(i);
                    hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&chars[offset]),
                                                      string_size, hash);
                }
            }
        } else {
            for (size_t i = start; i < end; ++i) {
                size_t string_size = size_at(i);
                size_t offset = offset_at(i);
                hash = HashUtil::xxHash64WithSeed(reinterpret_cast<const char*>(&chars[offset]),
                                                  string_size, hash);
            }
        }
    }

    void update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                               const uint8_t* __restrict null_data) const override {
        if (null_data) {
            for (size_t i = start; i < end; ++i) {
                if (null_data[i] == 0) {
                    auto data_ref = get_data_at(i);
                    // If offset is uint32, size will not exceed, check the size when inserting data into ColumnStr<T>.
                    hash = HashUtil::zlib_crc_hash(data_ref.data,
                                                   static_cast<uint32_t>(data_ref.size), hash);
                }
            }
        } else {
            for (size_t i = start; i < end; ++i) {
                auto data_ref = get_data_at(i);
                hash = HashUtil::zlib_crc_hash(data_ref.data, static_cast<uint32_t>(data_ref.size),
                                               hash);
            }
        }
    }

    void update_hash_with_value(size_t n, SipHash& hash) const override {
        size_t string_size = size_at(n);
        size_t offset = offset_at(n);

        // TODO: Rethink we really need to update the string_size?
        hash.update(reinterpret_cast<const char*>(&string_size), sizeof(string_size));
        hash.update(reinterpret_cast<const char*>(&chars[offset]), string_size);
    }

    void update_crcs_with_value(uint32_t* __restrict hashes, PrimitiveType type, uint32_t rows,
                                uint32_t offset,
                                const uint8_t* __restrict null_data) const override;

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data) const override {
        auto s = size();
        if (null_data) {
            for (int i = 0; i < s; i++) {
                if (null_data[i] == 0) {
                    size_t string_size = size_at(i);
                    size_t offset = offset_at(i);
                    hashes[i] = HashUtil::xxHash64WithSeed(
                            reinterpret_cast<const char*>(&chars[offset]), string_size, hashes[i]);
                }
            }
        } else {
            for (int i = 0; i < s; i++) {
                size_t string_size = size_at(i);
                size_t offset = offset_at(i);
                hashes[i] = HashUtil::xxHash64WithSeed(
                        reinterpret_cast<const char*>(&chars[offset]), string_size, hashes[i]);
            }
        }
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override;

    void insert_range_from_ignore_overflow(const IColumn& src, size_t start,
                                           size_t length) override;

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override;

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;
    size_t filter(const IColumn::Filter& filter) override;

    Status filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) override;

    MutableColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;

    void sort_column(const ColumnSorter* sorter, EqualFlags& flags, IColumn::Permutation& perms,
                     EqualRange& range, bool last_column) const override;

    void insert_default() override { offsets.push_back(chars.size()); }

    void insert_many_defaults(size_t length) override {
        offsets.resize_fill(offsets.size() + length, static_cast<T>(chars.size()));
        sanity_check_simple();
    }

    int compare_at(size_t n, size_t m, const IColumn& rhs_,
                   int /*nan_direction_hint*/) const override {
        const ColumnStr<T>& rhs = assert_cast<const ColumnStr<T>&>(rhs_);
        return memcmp_small_allow_overflow15(chars.data() + offset_at(n), size_at(n),
                                             rhs.chars.data() + rhs.offset_at(m), rhs.size_at(m));
    }

    void get_permutation(bool reverse, size_t limit, int nan_direction_hint,
                         IColumn::Permutation& res) const override;

    void reserve(size_t n) override;

    void resize(size_t n) override;

    bool is_column_string() const override { return true; }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnStr<T>);
    }

    bool is_ascii() const;

    Chars& get_chars() { return chars; }
    const Chars& get_chars() const { return chars; }

    auto& get_offsets() { return offsets; }
    const auto& get_offsets() const { return offsets; }

    void clear() override {
        chars.clear();
        offsets.clear();
    }

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Method replace_column_data is not supported for ColumnString");
    }

    void compare_internal(size_t rhs_row_id, const IColumn& rhs, int nan_direction_hint,
                          int direction, std::vector<uint8_t>& cmp_res,
                          uint8_t* __restrict filter) const override;

    ColumnPtr convert_column_if_overflow() override;

    void erase(size_t start, size_t length) override;

    void serialize_vec(StringRef* keys, const size_t num_rows) const override;
    size_t serialize_impl(char* pos, size_t row) const override;
    size_t deserialize_impl(const char* pos) override;
};

using ColumnString = ColumnStr<UInt32>;
using ColumnString64 = ColumnStr<UInt64>;
} // namespace doris::vectorized
#include "common/compile_check_end.h"
