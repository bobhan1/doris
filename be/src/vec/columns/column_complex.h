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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnComplex.h
// and modified by Doris

#pragma once

#include <glog/logging.h>

#include <vector>

#include "olap/hll.h"
#include "runtime/primitive_type.h"
#include "vec/columns/column.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/columns/columns_common.h"
#include "vec/core/field.h"
#include "vec/core/types.h"

namespace doris::vectorized {

template <PrimitiveType T>
class ColumnComplexType final : public COWHelper<IColumn, ColumnComplexType<T>> {
private:
    ColumnComplexType() = default;
    ColumnComplexType(const size_t n) : data(n) {}
    friend class COWHelper<IColumn, ColumnComplexType<T>>;

public:
    using Self = ColumnComplexType;
    using value_type = typename PrimitiveTypeTraits<T>::ColumnItemType;
    using Container = std::vector<value_type>;

    size_t size() const override { return data.size(); }

    StringRef get_data_at(size_t n) const override {
        return {reinterpret_cast<const char*>(&data[n]), sizeof(data[n])};
    }

    void insert_from(const IColumn& src, size_t n) override {
        data.push_back(assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(src).get_data()[n]);
    }

    void insert_data(const char* pos, size_t /*length*/) override {
        data.push_back(*reinterpret_cast<const value_type*>(pos));
    }

    void insert_binary_data(const char* pos, size_t length) {
        insert_default();
        value_type* pvalue = &get_element(size() - 1);
        if (!length) {
            *pvalue = *reinterpret_cast<const value_type*>(pos);
            return;
        }

        if constexpr (T == TYPE_BITMAP) {
            pvalue->deserialize(pos);
        } else if constexpr (T == TYPE_HLL) {
            pvalue->deserialize(Slice(pos, length));
        } else if constexpr (T == TYPE_QUANTILE_STATE) {
            pvalue->deserialize(Slice(pos, length));
        } else {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Unexpected type in column complex");
        }
    }

    void insert_many_continuous_binary_data(const char* data, const uint32_t* offsets,
                                            const size_t num) override {
        if (UNLIKELY(num == 0)) {
            return;
        }
        // the offsets size should be num + 1
        for (size_t i = 0; i != num; ++i) {
            insert_binary_data(data + offsets[i], offsets[i + 1] - offsets[i]);
        }
    }

    void insert_many_strings(const StringRef* strings, size_t num) override {
        for (size_t i = 0; i < num; i++) {
            insert_binary_data(strings[i].data, strings[i].size);
        }
    }

    void insert_default() override { data.push_back(value_type()); }

    void insert_many_defaults(size_t length) override {
        size_t old_size = data.size();
        data.resize(old_size + length);
    }

    void clear() override { data.clear(); }

    // TODO: value_type is not a pod type, so we also need to
    // calculate the memory requested by value_type
    size_t byte_size() const override { return data.size() * sizeof(data[0]); }

    size_t allocated_bytes() const override { return byte_size(); }

    bool has_enough_capacity(const IColumn& src) const override {
        const Self& src_vec = assert_cast<const Self&>(src);
        return data.capacity() - data.size() > src_vec.size();
    }

    void insert_value(value_type value) { data.emplace_back(std::move(value)); }

    void reserve(size_t n) override { data.reserve(n); }

    void resize(size_t n) override { data.resize(n); }

    std::string get_name() const override { return type_to_string(T); }

    MutableColumnPtr clone_resized(size_t size) const override;

    void insert(const Field& x) override {
        const value_type& s = doris::vectorized::get<const value_type&>(x);
        data.push_back(s);
    }

    Field operator[](size_t n) const override {
        assert(n < size());
        return Field::create_field<T>(data[n]);
    }

    void get(size_t n, Field& res) const override {
        assert(n < size());
        res = Field::create_field<T>(data[n]);
    }

    [[noreturn]] bool get_bool(size_t n) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "get field not implemented");
    }

    [[noreturn]] Int64 get_int(size_t n) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "get field not implemented");
    }

    void insert_range_from(const IColumn& src, size_t start, size_t length) override {
        auto& col = assert_cast<const Self&>(src);
        auto& src_data = col.get_data();
        auto st = src_data.begin() + start;
        auto ed = st + length;
        data.insert(data.end(), st, ed);
    }

    void insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                             const uint32_t* indices_end) override {
        const Self& src_vec = assert_cast<const Self&>(src);
        auto new_size = indices_end - indices_begin;

        for (uint32_t i = 0; i < new_size; ++i) {
            auto offset = *(indices_begin + i);
            data.emplace_back(src_vec.get_element(offset));
        }
    }

    void insert_many_from(const IColumn& src, size_t position, size_t length) override {
        const Self& src_vec = assert_cast<const Self&>(src);
        auto val = src_vec.get_element(position);
        for (uint32_t i = 0; i < length; ++i) {
            data.emplace_back(val);
        }
    }

    void pop_back(size_t n) override { data.erase(data.end() - n, data.end()); }
    // it's impossible to use ComplexType as key , so we don't have to implement them
    [[noreturn]] StringRef serialize_value_into_arena(size_t n, Arena& arena,
                                                      char const*& begin) const override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "serialize_value_into_arena not implemented");
        __builtin_unreachable();
    }

    [[noreturn]] const char* deserialize_and_insert_from_arena(const char* pos) override {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "deserialize_and_insert_from_arena not implemented");
        __builtin_unreachable();
    }

    // maybe we do not need to impl the function
    void update_hash_with_value(size_t n, SipHash& hash) const override {
        // TODO add hash function
    }

    void update_hashes_with_value(uint64_t* __restrict hashes,
                                  const uint8_t* __restrict null_data = nullptr) const override {
        // TODO add hash function
    }

    StringRef get_raw_data() const override {
        return StringRef(reinterpret_cast<const char*>(data.data()), data.size());
    }

    bool structure_equals(const IColumn& rhs) const override {
        return typeid(rhs) == typeid(ColumnComplexType<T>);
    }

    ColumnPtr filter(const IColumn::Filter& filt, ssize_t result_size_hint) const override;

    size_t filter(const IColumn::Filter& filter) override;

    MutableColumnPtr permute(const IColumn::Permutation& perm, size_t limit) const override;

    Container& get_data() { return data; }

    const Container& get_data() const { return data; }

    const value_type& get_element(size_t n) const { return data[n]; }

    value_type& get_element(size_t n) { return data[n]; }

#ifdef BE_TEST
    int compare_at(size_t n, size_t m, const IColumn& rhs_, int nan_direction_hint) const override {
        std::string lhs = get_element(n).to_string();
        std::string rhs = assert_cast<const Self&>(rhs_).get_element(m).to_string();
        return lhs.compare(rhs);
    }
#endif

    void replace_column_data(const IColumn& rhs, size_t row, size_t self_row = 0) override {
        DCHECK(size() > self_row);
        data[self_row] = assert_cast<const Self&, TypeCheckOnRelease::DISABLE>(rhs).data[row];
    }

    void erase(size_t start, size_t length) override {
        if (start >= data.size() || length == 0) {
            return;
        }
        length = std::min(length, data.size() - start);
        size_t remain_size = data.size() - length;
        std::move(data.begin() + start + length, data.end(), data.begin() + start);
        data.resize(remain_size);
    }

    size_t serialize_size_at(size_t row) const override { return sizeof(data[row]); }

private:
    Container data;
};

template <PrimitiveType T>
MutableColumnPtr ColumnComplexType<T>::clone_resized(size_t size) const {
    auto res = this->create();

    if (size > 0) {
        auto& new_col = assert_cast<Self&>(*res);
        size_t count = std::min(size, data.size());
        new_col.insert_range_from(*this, 0, count);
        if (size > count) {
            new_col.insert_many_defaults(size - count);
        }
    }

    return res;
}

template <PrimitiveType T>
ColumnPtr ColumnComplexType<T>::filter(const IColumn::Filter& filt,
                                       ssize_t result_size_hint) const {
    size_t size = data.size();
    column_match_filter_size(size, filt.size());

    if (data.size() == 0) {
        return this->create();
    }
    auto res = this->create();
    Container& res_data = res->get_data();

    if (result_size_hint) {
        res_data.reserve(result_size_hint > 0 ? result_size_hint : size);
    }

    const UInt8* filt_pos = filt.data();
    const UInt8* filt_end = filt_pos + size;
    const value_type* data_pos = data.data();

    while (filt_pos < filt_end) {
        if (*filt_pos) {
            res_data.push_back(*data_pos);
        }

        ++filt_pos;
        ++data_pos;
    }

    return res;
}

template <PrimitiveType T>
size_t ColumnComplexType<T>::filter(const IColumn::Filter& filter) {
    size_t size = data.size();
    column_match_filter_size(size, filter.size());

    if (data.size() == 0) {
        return 0;
    }

    value_type* res_data = data.data();

    const UInt8* filter_pos = filter.data();
    const UInt8* filter_end = filter_pos + size;
    const value_type* data_pos = data.data();

    while (filter_pos < filter_end) {
        if (*filter_pos) {
            *res_data = std::move(*data_pos);
            ++res_data;
        }

        ++filter_pos;
        ++data_pos;
    }

    data.resize(res_data - data.data());

    return res_data - data.data();
}

template <PrimitiveType T>
MutableColumnPtr ColumnComplexType<T>::permute(const IColumn::Permutation& perm,
                                               size_t limit) const {
    size_t size = data.size();

    limit = limit ? std::min(size, limit) : size;

    if (perm.size() < limit) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Size of permutation is less than required.");
        __builtin_unreachable();
    }

    auto res = this->create(limit);
    typename Self::Container& res_data = res->get_data();
    for (size_t i = 0; i < limit; ++i) {
        res_data[i] = data[perm[i]];
    }

    return res;
}

using ColumnBitmap = ColumnComplexType<TYPE_BITMAP>;
using ColumnHLL = ColumnComplexType<TYPE_HLL>;
using ColumnQuantileState = ColumnComplexType<TYPE_QUANTILE_STATE>;

template <PrimitiveType T>
struct is_complex : std::false_type {};

template <>
struct is_complex<TYPE_BITMAP> : std::true_type {};
//DataTypeBitMap::FieldType = BitmapValue

template <>
struct is_complex<TYPE_HLL> : std::true_type {};
//DataTypeHLL::FieldType = HyperLogLog

template <>
struct is_complex<TYPE_QUANTILE_STATE> : std::true_type {};
//DataTypeQuantileState::FieldType = QuantileState

template <PrimitiveType T>
constexpr bool is_complex_v = is_complex<T>::value;

} // namespace doris::vectorized
