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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Columns/ColumnVariant.cpp
// and modified by Doris

#include "vec/columns/column_variant.h"

#include <assert.h>
#include <fmt/core.h>
#include <fmt/format.h>
#include <glog/logging.h>
#include <parallel_hashmap/phmap.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>

#include <algorithm>
#include <cstdlib>
#include <functional>
#include <limits>
#include <map>
#include <memory>
#include <optional>
#include <sstream>
#include <vector>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "exprs/json_functions.h"
#include "runtime/jsonb_value.h"
#include "runtime/primitive_type.h"
#include "util/simd/bits.h"
#include "vec/aggregate_functions/aggregate_function.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_vector.h"
#include "vec/common/arena.h"
#include "vec/common/assert_cast.h"
#include "vec/common/field_visitors.h"
#include "vec/common/schema_util.h"
#include "vec/common/string_buffer.hpp"
#include "vec/common/string_ref.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/field.h"
#include "vec/core/types.h"
#include "vec/data_types/convert_field_to_type.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_decimal.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_jsonb.h"
#include "vec/data_types/data_type_nothing.h"
#include "vec/data_types/data_type_nullable.h"
#include "vec/data_types/data_type_variant.h"
#include "vec/data_types/get_least_supertype.h"
#include "vec/json/path_in_data.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"
namespace {

DataTypePtr create_array_of_type(PrimitiveType type, size_t num_dimensions, bool is_nullable) {
    DataTypePtr result = type == PrimitiveType::INVALID_TYPE
                                 ? is_nullable ? make_nullable(std::make_shared<DataTypeNothing>())
                                               : std::dynamic_pointer_cast<IDataType>(
                                                         std::make_shared<DataTypeNothing>())
                                 : DataTypeFactory::instance().create_data_type(type, is_nullable);
    for (size_t i = 0; i < num_dimensions; ++i) {
        result = std::make_shared<DataTypeArray>(result);
        if (is_nullable) {
            // wrap array with nullable
            result = make_nullable(result);
        }
    }
    return result;
}

DataTypePtr get_base_type_of_array(const DataTypePtr& type) {
    /// Get raw pointers to avoid extra copying of type pointers.
    const DataTypeArray* last_array = nullptr;
    const auto* current_type = type.get();
    if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
        current_type = nullable->get_nested_type().get();
    }
    while (const auto* type_array = typeid_cast<const DataTypeArray*>(current_type)) {
        current_type = type_array->get_nested_type().get();
        last_array = type_array;
        if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
            current_type = nullable->get_nested_type().get();
        }
    }
    return last_array ? last_array->get_nested_type() : type;
}

size_t get_number_of_dimensions(const IDataType& type) {
    int num_dimensions = 0;
    const auto* current_type = &type;
    if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
        current_type = nullable->get_nested_type().get();
    }
    while (const auto* type_array = typeid_cast<const DataTypeArray*>(current_type)) {
        current_type = type_array->get_nested_type().get();
        num_dimensions += 1;
        if (const auto* nullable = typeid_cast<const DataTypeNullable*>(current_type)) {
            current_type = nullable->get_nested_type().get();
        }
    }
    return num_dimensions;
}

/// Calculates number of dimensions in array field.
/// Returns 0 for scalar fields.
class FieldVisitorToNumberOfDimensions : public StaticVisitor<size_t> {
public:
    FieldVisitorToNumberOfDimensions() = default;
    template <PrimitiveType T>
    size_t apply(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) {
        if constexpr (T == TYPE_ARRAY) {
            const size_t size = x.size();
            size_t dimensions = 0;
            for (size_t i = 0; i < size; ++i) {
                size_t element_dimensions = apply_visitor(*this, x[i]);
                dimensions = std::max(dimensions, element_dimensions);
            }
            return 1 + dimensions;
        } else {
            return 0;
        }
    }
};

// Visitor that allows to get type of scalar field
// but exclude fields contain complex field.This is a faster version
// for FieldVisitorToScalarType which does not support complex field.
class SimpleFieldVisitorToScalarType : public StaticVisitor<size_t> {
public:
    template <PrimitiveType T>
    size_t apply(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) {
        if constexpr (T == TYPE_ARRAY) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, "Array type is not supported");
        } else if constexpr (T == TYPE_BIGINT) {
            if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) {
                type = PrimitiveType::TYPE_TINYINT;
            } else if (x <= std::numeric_limits<Int16>::max() &&
                       x >= std::numeric_limits<Int16>::min()) {
                type = PrimitiveType::TYPE_SMALLINT;
            } else if (x <= std::numeric_limits<Int32>::max() &&
                       x >= std::numeric_limits<Int32>::min()) {
                type = PrimitiveType::TYPE_INT;
            } else {
                type = PrimitiveType::TYPE_BIGINT;
            }
            return 1;
        } else if constexpr (T == TYPE_NULL) {
            have_nulls = true;
            return 1;
        } else {
            type = T;
            return 1;
        }
    }
    void get_scalar_type(PrimitiveType* data_type) const { *data_type = type; }
    bool contain_nulls() const { return have_nulls; }

    bool need_convert_field() const { return false; }

private:
    PrimitiveType type = PrimitiveType::INVALID_TYPE;
    bool have_nulls = false;
};

/// Visitor that allows to get type of scalar field
/// or least common type of scalars in array.
/// More optimized version of FieldToDataType.
class FieldVisitorToScalarType : public StaticVisitor<size_t> {
public:
    template <PrimitiveType T>
    size_t apply(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) {
        if constexpr (T == TYPE_ARRAY) {
            size_t size = x.size();
            for (size_t i = 0; i < size; ++i) {
                apply_visitor(*this, x[i]);
            }
            return 0;
        } else if constexpr (T == TYPE_BIGINT) {
            field_types.insert(PrimitiveType::TYPE_BIGINT);
            if (x <= std::numeric_limits<Int8>::max() && x >= std::numeric_limits<Int8>::min()) {
                type_indexes.insert(PrimitiveType::TYPE_TINYINT);
            } else if (x <= std::numeric_limits<Int16>::max() &&
                       x >= std::numeric_limits<Int16>::min()) {
                type_indexes.insert(PrimitiveType::TYPE_SMALLINT);
            } else if (x <= std::numeric_limits<Int32>::max() &&
                       x >= std::numeric_limits<Int32>::min()) {
                type_indexes.insert(PrimitiveType::TYPE_INT);
            } else {
                type_indexes.insert(PrimitiveType::TYPE_BIGINT);
            }
            return 0;
        } else if constexpr (T == TYPE_NULL) {
            have_nulls = true;
            return 0;
        } else {
            PrimitiveTypeTraits<PrimitiveType::TYPE_ARRAY>::CppType a;
            field_types.insert(T);
            type_indexes.insert(T);
            return 0;
        }
    }
    void get_scalar_type(PrimitiveType* type) const {
        DataTypePtr data_type;
        get_least_supertype_jsonb(type_indexes, &data_type);
        *type = data_type->get_primitive_type();
    }
    bool contain_nulls() const { return have_nulls; }
    bool need_convert_field() const { return field_types.size() > 1; }

private:
    phmap::flat_hash_set<PrimitiveType> type_indexes;
    phmap::flat_hash_set<PrimitiveType> field_types;
    bool have_nulls = false;
};

/// Visitor that keeps @num_dimensions_to_keep dimensions in arrays
/// and replaces all scalars or nested arrays to @replacement at that level.
class FieldVisitorReplaceScalars : public StaticVisitor<Field> {
public:
    FieldVisitorReplaceScalars(const Field& replacement_, size_t num_dimensions_to_keep_)
            : replacement(replacement_), num_dimensions_to_keep(num_dimensions_to_keep_) {}

    template <PrimitiveType T>
    Field apply(const typename PrimitiveTypeTraits<T>::NearestFieldType& x) const {
        if constexpr (T == TYPE_ARRAY) {
            if (num_dimensions_to_keep == 0) {
                return replacement;
            }

            const size_t size = x.size();
            Array res(size);
            for (size_t i = 0; i < size; ++i) {
                res[i] = apply_visitor(
                        FieldVisitorReplaceScalars(replacement, num_dimensions_to_keep - 1), x[i]);
            }
            return Field::create_field<TYPE_ARRAY>(res);
        } else {
            return replacement;
        }
    }

private:
    const Field& replacement;
    size_t num_dimensions_to_keep;
};

} // namespace

template <typename Visitor>
void get_field_info_impl(const Field& field, FieldInfo* info) {
    Visitor to_scalar_type_visitor;
    apply_visitor(to_scalar_type_visitor, field);
    PrimitiveType type_id;
    to_scalar_type_visitor.get_scalar_type(&type_id);
    // array item's dimension may missmatch, eg. [1, 2, [1, 2, 3]]
    FieldVisitorToNumberOfDimensions v;
    *info = {
            type_id,
            to_scalar_type_visitor.contain_nulls(),
            to_scalar_type_visitor.need_convert_field(),
            apply_visitor(v, field),
    };
}

void get_field_info(const Field& field, FieldInfo* info) {
    if (field.is_complex_field()) {
        get_field_info_impl<FieldVisitorToScalarType>(field, info);
    } else {
        get_field_info_impl<SimpleFieldVisitorToScalarType>(field, info);
    }
}

ColumnVariant::Subcolumn::Subcolumn(MutableColumnPtr&& data_, DataTypePtr type, bool is_nullable_,
                                    bool is_root_)
        : least_common_type(type), is_nullable(is_nullable_), is_root(is_root_) {
    data.push_back(std::move(data_));
    data_types.push_back(type);
}

ColumnVariant::Subcolumn::Subcolumn(size_t size_, bool is_nullable_, bool is_root_)
        : least_common_type(std::make_shared<DataTypeNothing>()),
          is_nullable(is_nullable_),
          num_of_defaults_in_prefix(size_),
          is_root(is_root_) {}

size_t ColumnVariant::Subcolumn::Subcolumn::size() const {
    size_t res = num_of_defaults_in_prefix;
    for (const auto& part : data) {
        res += part->size();
    }
    return res;
}

size_t ColumnVariant::Subcolumn::Subcolumn::byteSize() const {
    size_t res = 0;
    for (const auto& part : data) {
        res += part->byte_size();
    }
    return res;
}

size_t ColumnVariant::Subcolumn::Subcolumn::allocatedBytes() const {
    size_t res = 0;
    for (const auto& part : data) {
        res += part->allocated_bytes();
    }
    return res;
}

void ColumnVariant::Subcolumn::insert(Field field) {
    FieldInfo info;
    get_field_info(field, &info);
    insert(std::move(field), std::move(info));
}

void ColumnVariant::Subcolumn::add_new_column_part(DataTypePtr type) {
    data.push_back(type->create_column());
    least_common_type = LeastCommonType {type};
    data_types.push_back(type);
}

void ColumnVariant::Subcolumn::insert(Field field, FieldInfo info) {
    auto base_type = info.scalar_type_id;
    if (base_type == PrimitiveType::INVALID_TYPE && info.num_dimensions == 0) {
        insert_default();
        return;
    }
    auto column_dim = least_common_type.get_dimensions();
    auto value_dim = info.num_dimensions;
    if (least_common_type.get_base()->get_primitive_type() == INVALID_TYPE) {
        column_dim = value_dim;
    }
    if (base_type == PrimitiveType::INVALID_TYPE) {
        value_dim = column_dim;
    }
    bool type_changed = false;
    if (value_dim != column_dim || info.num_dimensions >= 2) {
        // Deduce to JSONB
        VLOG_DEBUG << fmt::format(
                "Dimension of types mismatched between inserted value and column, "
                "expected:{}, but meet:{} for type:{}",
                column_dim, value_dim, least_common_type.get()->get_name());
        base_type = MOST_COMMON_TYPE_ID;
        value_dim = 0;
        type_changed = true;
    }
    if (data.empty()) {
        add_new_column_part(create_array_of_type(base_type, value_dim, is_nullable));
    } else if (least_common_type.get_base_type_id() != base_type &&
               base_type != PrimitiveType::INVALID_TYPE) {
        if (schema_util::is_conversion_required_between_integers(
                    base_type, least_common_type.get_base_type_id())) {
            VLOG_DEBUG << "Conversion between " << type_to_string(base_type) << " and "
                       << type_to_string(least_common_type.get_type_id());
            DataTypePtr base_data_type;
            PrimitiveType base_data_type_id;
            get_least_supertype_jsonb(
                    PrimitiveTypeSet {base_type, least_common_type.get_base_type_id()},
                    &base_data_type);
            type_changed = true;
            base_data_type_id = base_data_type->get_primitive_type();
            if (is_nullable) {
                base_data_type = make_nullable(base_data_type);
            }
            if (!least_common_type.get_base()->equals(*base_data_type)) {
                add_new_column_part(
                        create_array_of_type(base_data_type_id, value_dim, is_nullable));
            }
        }
    }

    if (type_changed || info.need_convert) {
        Field new_field;
        convert_field_to_type(field, *least_common_type.get(), &new_field);
        field = new_field;
    }

    data.back()->insert(field);
}

static DataTypePtr create_array(PrimitiveType type, size_t num_dimensions) {
    DataTypePtr result_type = type == PrimitiveType::INVALID_TYPE
                                      ? make_nullable(std::make_shared<DataTypeNothing>())
                                      : DataTypeFactory::instance().create_data_type(type, true);
    for (size_t i = 0; i < num_dimensions; ++i) {
        result_type = make_nullable(std::make_shared<DataTypeArray>(result_type));
    }
    return result_type;
}

Array create_empty_array_field(size_t num_dimensions) {
    if (num_dimensions == 0) {
        throw doris::Exception(ErrorCode::INVALID_ARGUMENT,
                               "Cannot create array field with 0 dimensions");
    }

    Array array;
    Array* current_array = &array;
    for (size_t i = 1; i < num_dimensions; ++i) {
        current_array->push_back(Field::create_field<TYPE_NULL>(Null()));
        current_array = &current_array->back().get<Array&>();
    }

    return array;
}

// Recreates column with default scalar values and keeps sizes of arrays.
static ColumnPtr recreate_column_with_default_values(const ColumnPtr& column,
                                                     PrimitiveType scalar_type,
                                                     size_t num_dimensions) {
    const auto* column_array = check_and_get_column<ColumnArray>(remove_nullable(column).get());
    if (column_array != nullptr && num_dimensions != 0) {
        return make_nullable(ColumnArray::create(
                recreate_column_with_default_values(column_array->get_data_ptr(), scalar_type,
                                                    num_dimensions - 1),
                IColumn::mutate(column_array->get_offsets_ptr())));
    }

    return create_array(scalar_type, num_dimensions)
            ->create_column()
            ->clone_resized(column->size());
}

ColumnVariant::Subcolumn ColumnVariant::Subcolumn::clone_with_default_values(
        const FieldInfo& field_info) const {
    Subcolumn new_subcolumn(*this);
    new_subcolumn.least_common_type =
            LeastCommonType {create_array(field_info.scalar_type_id, field_info.num_dimensions)};

    for (int i = 0; i < new_subcolumn.data.size(); ++i) {
        new_subcolumn.data[i] = recreate_column_with_default_values(
                new_subcolumn.data[i], field_info.scalar_type_id, field_info.num_dimensions);
        new_subcolumn.data_types[i] = create_array_of_type(field_info.scalar_type_id,
                                                           field_info.num_dimensions, is_nullable);
    }

    return new_subcolumn;
}

Field ColumnVariant::Subcolumn::get_last_field() const {
    if (data.empty()) {
        return Field();
    }

    const auto& last_part = data.back();
    assert(!last_part->empty());
    return (*last_part)[last_part->size() - 1];
}

void ColumnVariant::Subcolumn::insert_range_from(const Subcolumn& src, size_t start,
                                                 size_t length) {
    if (start + length > src.size()) {
        throw doris::Exception(
                ErrorCode::OUT_OF_BOUND,
                "Invalid range for insert_range_from: start={}, length={}, src.size={}", start,
                length, src.size());
    }
    size_t end = start + length;
    // num_rows += length;
    if (data.empty()) {
        add_new_column_part(src.get_least_common_type());
    } else if (!least_common_type.get()->equals(*src.get_least_common_type())) {
        DataTypePtr new_least_common_type;
        get_least_supertype_jsonb(DataTypes {least_common_type.get(), src.get_least_common_type()},
                                  &new_least_common_type);
        if (!new_least_common_type->equals(*least_common_type.get())) {
            add_new_column_part(std::move(new_least_common_type));
        }
    }
    if (end <= src.num_of_defaults_in_prefix) {
        data.back()->insert_many_defaults(length);
        return;
    }
    if (start < src.num_of_defaults_in_prefix) {
        data.back()->insert_many_defaults(src.num_of_defaults_in_prefix - start);
    }
    auto insert_from_part = [&](const auto& column, const auto& column_type, size_t from,
                                size_t n) {
        if (from + n > column->size()) {
            throw doris::Exception(
                    ErrorCode::OUT_OF_BOUND,
                    "Invalid range for insert_range_from: from={}, n={}, column.size={}", from, n,
                    column->size());
        }
        if (column_type->equals(*least_common_type.get())) {
            data.back()->insert_range_from(*column, from, n);
            return;
        }
        /// If we need to insert large range, there is no sense to cut part of column and cast it.
        /// Casting of all column and inserting from it can be faster.
        /// Threshold is just a guess.
        if (n * 3 >= column->size()) {
            ColumnPtr casted_column;
            Status st = schema_util::cast_column({column, column_type, ""}, least_common_type.get(),
                                                 &casted_column);
            if (!st.ok()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT, st.to_string());
            }
            data.back()->insert_range_from(*casted_column, from, n);
            return;
        }
        auto casted_column = column->cut(from, n);
        Status st = schema_util::cast_column({casted_column, column_type, ""},
                                             least_common_type.get(), &casted_column);
        if (!st.ok()) {
            throw doris::Exception(ErrorCode::INVALID_ARGUMENT, st.to_string());
        }
        data.back()->insert_range_from(*casted_column, 0, n);
    };
    size_t pos = 0;
    size_t processed_rows = src.num_of_defaults_in_prefix;
    /// Find the first part of the column that intersects the range.
    while (pos < src.data.size() && processed_rows + src.data[pos]->size() < start) {
        processed_rows += src.data[pos]->size();
        ++pos;
    }
    /// Insert from the first part of column.
    if (pos < src.data.size() && processed_rows < start) {
        size_t part_start = start - processed_rows;
        size_t part_length = std::min(src.data[pos]->size() - part_start, end - start);
        insert_from_part(src.data[pos], src.data_types[pos], part_start, part_length);
        processed_rows += src.data[pos]->size();
        ++pos;
    }
    /// Insert from the parts of column in the middle of range.
    while (pos < src.data.size() && processed_rows + src.data[pos]->size() < end) {
        insert_from_part(src.data[pos], src.data_types[pos], 0, src.data[pos]->size());
        processed_rows += src.data[pos]->size();
        ++pos;
    }
    /// Insert from the last part of column if needed.
    if (pos < src.data.size() && processed_rows < end) {
        size_t part_end = end - processed_rows;
        insert_from_part(src.data[pos], src.data_types[pos], 0, part_end);
    }
}

bool ColumnVariant::Subcolumn::is_finalized() const {
    return num_of_defaults_in_prefix == 0 && (data.empty() || (data.size() == 1));
}

template <typename Func>
MutableColumnPtr ColumnVariant::apply_for_subcolumns(Func&& func) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnVariant&>(*finalized);
        return finalized_object.apply_for_subcolumns(std::forward<Func>(func));
    }
    auto res = ColumnVariant::create(is_nullable, false);
    for (const auto& subcolumn : subcolumns) {
        auto new_subcolumn = func(subcolumn->data.get_finalized_column());
        res->add_sub_column(subcolumn->path, new_subcolumn->assume_mutable(),
                            subcolumn->data.get_least_common_type());
    }
    check_consistency();
    return res;
}

void ColumnVariant::resize(size_t n) {
    if (n == num_rows) {
        return;
    }
    if (n > num_rows) {
        insert_many_defaults(n - num_rows);
    } else {
        for (auto& subcolumn : subcolumns) {
            subcolumn->data.pop_back(num_rows - n);
        }
    }
    num_rows = n;
}

bool ColumnVariant::Subcolumn::check_if_sparse_column(size_t arg_num_rows) {
    if (arg_num_rows < config::variant_threshold_rows_to_estimate_sparse_column) {
        return false;
    }
    std::vector<double> defaults_ratio;
    for (size_t i = 0; i < data.size(); ++i) {
        defaults_ratio.push_back(data[i]->get_ratio_of_default_rows());
    }
    double default_ratio = std::accumulate(defaults_ratio.begin(), defaults_ratio.end(), 0.0) /
                           static_cast<double>(defaults_ratio.size());
    return default_ratio >= config::variant_ratio_of_defaults_as_sparse_column;
}

void ColumnVariant::Subcolumn::finalize(FinalizeMode mode) {
    if (is_finalized()) {
        return;
    }
    if (data.size() == 1 && num_of_defaults_in_prefix == 0) {
        data[0] = data[0]->convert_to_full_column_if_const();
        return;
    }
    DataTypePtr to_type = least_common_type.get();
    if (mode == FinalizeMode::WRITE_MODE && is_root) {
        // Root always JSONB type in write mode
        to_type = is_nullable ? make_nullable(std::make_shared<MostCommonType>())
                              : std::make_shared<MostCommonType>();
        least_common_type = LeastCommonType {to_type};
    }
    auto result_column = to_type->create_column();
    if (num_of_defaults_in_prefix) {
        result_column->insert_many_defaults(num_of_defaults_in_prefix);
    }
    for (size_t i = 0; i < data.size(); ++i) {
        auto& part = data[i];
        auto from_type = data_types[i];
        part = part->convert_to_full_column_if_const();
        size_t part_size = part->size();
        if (!from_type->equals(*to_type)) {
            ColumnPtr ptr;
            Status st = schema_util::cast_column({part, from_type, ""}, to_type, &ptr);
            if (!st.ok()) {
                throw doris::Exception(ErrorCode::INVALID_ARGUMENT, st.to_string());
            }
            part = ptr->convert_to_full_column_if_const();
        }
        result_column->insert_range_from(*part, 0, part_size);
    }
    data = {std::move(result_column)};
    data_types = {std::move(to_type)};
    num_of_defaults_in_prefix = 0;
}

void ColumnVariant::Subcolumn::insert_default() {
    if (data.empty()) {
        ++num_of_defaults_in_prefix;
    } else {
        data.back()->insert_default();
    }
}

void ColumnVariant::Subcolumn::insert_many_defaults(size_t length) {
    if (data.empty()) {
        num_of_defaults_in_prefix += length;
    } else {
        data.back()->insert_many_defaults(length);
    }
}

void ColumnVariant::Subcolumn::pop_back(size_t n) {
    if (n > size()) {
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Invalid number of elements to pop: {}, size: {}", n, size());
    }
    size_t num_removed = 0;
    for (auto it = data.rbegin(); it != data.rend(); ++it) {
        if (n == 0) {
            break;
        }
        auto& column = *it;
        if (n < column->size()) {
            column->pop_back(n);
            n = 0;
        } else {
            ++num_removed;
            n -= column->size();
        }
    }
    size_t sz = data.size() - num_removed;
    data.resize(sz);
    data_types.resize(sz);
    // need to update least_common_type when pop_back a column from the last
    least_common_type = sz > 0 ? LeastCommonType {data_types[sz - 1]}
                               : LeastCommonType {std::make_shared<DataTypeNothing>()};
    num_of_defaults_in_prefix -= n;
}

IColumn& ColumnVariant::Subcolumn::get_finalized_column() {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return *data[0];
}

const IColumn& ColumnVariant::Subcolumn::get_finalized_column() const {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return *data[0];
}

const ColumnPtr& ColumnVariant::Subcolumn::get_finalized_column_ptr() const {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return data[0];
}

ColumnPtr& ColumnVariant::Subcolumn::get_finalized_column_ptr() {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    return data[0];
}

void ColumnVariant::Subcolumn::remove_nullable() {
    if (!is_finalized()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn is not finalized");
    }
    data[0] = doris::vectorized::remove_nullable(data[0]);
    least_common_type.remove_nullable();
}

ColumnVariant::Subcolumn::LeastCommonType::LeastCommonType(DataTypePtr type_)
        : type(std::move(type_)),
          base_type(get_base_type_of_array(type)),
          num_dimensions(get_number_of_dimensions(*type)) {
    least_common_type_serder = type->get_serde();
    type_id = type->get_primitive_type();
    base_type_id = base_type->get_primitive_type();
}

ColumnVariant::ColumnVariant(bool is_nullable_, bool create_root_)
        : is_nullable(is_nullable_), num_rows(0) {
    if (create_root_) {
        subcolumns.create_root(Subcolumn(0, is_nullable, true /*root*/));
    }
}

ColumnVariant::ColumnVariant(bool is_nullable_, DataTypePtr type, MutableColumnPtr&& column)
        : is_nullable(is_nullable_) {
    add_sub_column({}, std::move(column), type);
}

ColumnVariant::ColumnVariant(Subcolumns&& subcolumns_, bool is_nullable_)
        : is_nullable(is_nullable_),
          subcolumns(std::move(subcolumns_)),
          num_rows(subcolumns.empty() ? 0 : (*subcolumns.begin())->data.size()) {
    check_consistency();
}

void ColumnVariant::check_consistency() const {
    if (subcolumns.empty()) {
        return;
    }
    for (const auto& leaf : subcolumns) {
        if (num_rows != leaf->data.size()) {
            throw doris::Exception(doris::ErrorCode::INTERNAL_ERROR,
                                   "unmatched column: {}, expeted rows: {}, but meet: {}",
                                   leaf->path.get_path(), num_rows, leaf->data.size());
        }
    }
}

size_t ColumnVariant::size() const {
#ifndef NDEBUG
    check_consistency();
#endif
    return num_rows;
}

MutableColumnPtr ColumnVariant::clone_resized(size_t new_size) const {
    if (new_size == 0) {
        return ColumnVariant::create(is_nullable);
    }
    // If subcolumns are empty, then res will be empty but new_size > 0
    if (subcolumns.empty()) {
        // Add an emtpy column with new_size rows
        auto res = ColumnVariant::create(true, false);
        res->set_num_rows(new_size);
        return res;
    }
    auto res = apply_for_subcolumns(
            [&](const auto& subcolumn) { return subcolumn.clone_resized(new_size); });
    return res;
}

size_t ColumnVariant::byte_size() const {
    size_t res = 0;
    for (const auto& entry : subcolumns) {
        res += entry->data.byteSize();
    }
    return res;
}

size_t ColumnVariant::allocated_bytes() const {
    size_t res = 0;
    for (const auto& entry : subcolumns) {
        res += entry->data.allocatedBytes();
    }
    return res;
}

void ColumnVariant::for_each_subcolumn(ColumnCallback callback) {
    for (auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            callback(part);
        }
    }
}

void ColumnVariant::insert_from(const IColumn& src, size_t n) {
    const auto* src_v = check_and_get_column<ColumnVariant>(src);
    // optimize when src and this column are scalar variant, since try_insert is inefficiency
    if (src_v != nullptr && src_v->is_scalar_variant() && is_scalar_variant() &&
        src_v->get_root_type()->equals(*get_root_type()) && src_v->is_finalized() &&
        is_finalized()) {
        get_root()->insert_from(*src_v->get_root(), n);
        ++num_rows;
        return;
    }
    return try_insert(src[n]);
}

void ColumnVariant::try_insert(const Field& field) {
    if (field.get_type() != PrimitiveType::TYPE_VARIANT) {
        if (field.is_null()) {
            insert_default();
            return;
        }
        auto* root = get_subcolumn({});
        // Insert to an emtpy ColumnVariant may result root null,
        // so create a root column of Variant is expected.
        if (root == nullptr) {
            bool succ = add_sub_column({}, num_rows);
            if (!succ) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Failed to add root sub column {}");
            }
            root = get_subcolumn({});
        }
        root->insert(field);
        ++num_rows;
        return;
    }
    const auto& object = field.get<const VariantMap&>();
    size_t old_size = size();
    for (const auto& [key, value] : object) {
        if (!has_subcolumn(key)) {
            bool succ = add_sub_column(key, old_size);
            if (!succ) {
                throw doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                                       "Failed to add sub column {}", key.get_path());
            }
        }
        auto* subcolumn = get_subcolumn(key);
        if (!subcolumn) {
            doris::Exception(doris::ErrorCode::INVALID_ARGUMENT,
                             fmt::format("Failed to find sub column {}", key.get_path()));
        }
        subcolumn->insert(value);
    }
    for (auto& entry : subcolumns) {
        if (old_size == entry->data.size()) {
            bool inserted = try_insert_default_from_nested(entry);
            if (!inserted) {
                entry->data.insert_default();
            }
        }
    }
    ++num_rows;
}

void ColumnVariant::insert_default() {
    for (auto& entry : subcolumns) {
        entry->data.insert_default();
    }
    ++num_rows;
}

void ColumnVariant::Subcolumn::get(size_t n, Field& res) const {
    if (least_common_type.get_base_type_id() == PrimitiveType::INVALID_TYPE) {
        res = Field();
        return;
    }

    // JSONB is a special type, it's not a scalar type, we need to handle it specially
    // 1. we try to get the JSONB Field from ColumnString which has no JSONB type info
    // 2. Array of JSONB is a special type, we get from ColumnArray of ColumnString, should convert from string Field to JSONB Field
    if (is_finalized() && least_common_type.get_base_type_id() != PrimitiveType::TYPE_JSONB) {
        // common type to get the field value
        get_finalized_column().get(n, res);
        return;
    }

    size_t ind = n;
    if (ind < num_of_defaults_in_prefix) {
        res = least_common_type.get()->get_default();
        return;
    }

    ind -= num_of_defaults_in_prefix;
    for (size_t i = 0; i < data.size(); ++i) {
        const auto& part = data[i];
        const auto& part_type = data_types[i];
        if (ind < part->size()) {
            auto non_nullable_type = vectorized::remove_nullable(part_type);
            bool is_nested_array_of_jsonb =
                    non_nullable_type->equals(*NESTED_TYPE_AS_ARRAY_OF_JSONB);

            res = non_nullable_type->get_default();
            part->get(ind, res);

            if (is_nested_array_of_jsonb) {
                convert_array_string_to_array_jsonb(res);
            } else {
                Field new_field;
                convert_field_to_type(res, *least_common_type.get(), &new_field);
                res = new_field;
            }
            return;
        }

        ind -= part->size();
    }

    throw doris::Exception(ErrorCode::OUT_OF_BOUND, "Index ({}) for getting field is out of range",
                           n);
}

Field ColumnVariant::operator[](size_t n) const {
    Field object;
    get(n, object);
    return object;
}

void ColumnVariant::get(size_t n, Field& res) const {
    if (UNLIKELY(n >= size())) {
        throw doris::Exception(ErrorCode::OUT_OF_BOUND,
                               "Index ({}) for getting field is out of range for size {}", n,
                               size());
    }
    res = Field::create_field<TYPE_VARIANT>(VariantMap());
    auto& object = res.get<VariantMap&>();

    for (const auto& entry : subcolumns) {
        Field field;
        entry->data.get(n, field);
        // Notice: we treat null as empty field, since we do not distinguish null and empty for Variant type.
        if (field.get_type() != PrimitiveType::TYPE_NULL) {
            object.try_emplace(entry->path, field);
        }
    }
    if (object.empty()) {
        res = Field();
    }
}

void ColumnVariant::add_nested_subcolumn(const PathInData& key, const FieldInfo& field_info,
                                         size_t new_size) {
    if (!key.has_nested_part()) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                               "Cannot add Nested subcolumn, because path doesn't contain Nested");
    }

    bool inserted = false;
    /// We find node that represents the same Nested type as @key.
    const auto* nested_node = subcolumns.find_best_match(key);
    // TODO a better way to handle following case:
    // {"a" : {"b" : [{"x" : 10}]}}
    // {"a" : {"b" : {"c": [{"y" : 10}]}}}
    // maybe a.b.c.y should not follow from a.b's nested data
    // If we have a nested subcolumn and it contains nested node in it's path
    if (nested_node &&
        Subcolumns::find_parent(nested_node, [](const auto& node) { return node.is_nested(); })) {
        /// Find any leaf of Nested subcolumn.
        const auto* leaf = Subcolumns::find_leaf(nested_node, [&](const auto&) { return true; });
        assert(leaf);

        /// Recreate subcolumn with default values and the same sizes of arrays.
        auto new_subcolumn = leaf->data.clone_with_default_values(field_info);

        /// It's possible that we have already inserted value from current row
        /// to this subcolumn. So, adjust size to expected.
        if (new_subcolumn.size() > new_size) {
            new_subcolumn.pop_back(new_subcolumn.size() - new_size);
        }

        assert(new_subcolumn.size() == new_size);
        inserted = subcolumns.add(key, new_subcolumn);
    } else {
        /// If node was not found just add subcolumn with empty arrays.
        inserted = subcolumns.add(key, Subcolumn(new_size, is_nullable));
    }

    if (!inserted) {
        throw doris::Exception(ErrorCode::INTERNAL_ERROR, "Subcolumn '{}' already exists",
                               key.get_path());
    }

    if (num_rows == 0) {
        num_rows = new_size;
    } else if (new_size != num_rows) {
        throw doris::Exception(
                ErrorCode::INTERNAL_ERROR,
                "Required size of subcolumn {} ({}) is inconsistent with column size ({})",
                key.get_path(), new_size, num_rows);
    }
}

void ColumnVariant::insert_range_from(const IColumn& src, size_t start, size_t length) {
#ifndef NDEBUG
    check_consistency();
#endif
    const auto& src_object = assert_cast<const ColumnVariant&>(src);
    for (const auto& entry : src_object.subcolumns) {
        if (!has_subcolumn(entry->path)) {
            if (entry->path.has_nested_part()) {
                FieldInfo field_info {
                        .scalar_type_id = entry->data.least_common_type.get_base_type_id(),
                        .have_nulls = false,
                        .need_convert = false,
                        .num_dimensions = entry->data.get_dimensions()};
                add_nested_subcolumn(entry->path, field_info, num_rows);
            } else {
                add_sub_column(entry->path, num_rows);
            }
        }
        auto* subcolumn = get_subcolumn(entry->path);
        subcolumn->insert_range_from(entry->data, start, length);
    }
    for (auto& entry : subcolumns) {
        if (!src_object.has_subcolumn(entry->path)) {
            bool inserted = try_insert_many_defaults_from_nested(entry);
            if (!inserted) {
                entry->data.insert_many_defaults(length);
            }
        }
    }
    num_rows += length;
    finalize(FinalizeMode::READ_MODE);
#ifndef NDEBUG
    check_consistency();
#endif
}

MutableColumnPtr ColumnVariant::permute(const Permutation& perm, size_t limit) const {
    if (num_rows == 0 || subcolumns.empty()) {
        if (limit == 0) {
            limit = num_rows;
        } else {
            limit = std::min(num_rows, limit);
        }

        if (perm.size() < limit) {
            throw doris::Exception(ErrorCode::INTERNAL_ERROR,
                                   "Size of permutation is less than required.");
        }
        // Add an emtpy column with limit rows
        auto res = ColumnVariant::create(true, false);
        res->set_num_rows(limit);
        return res;
    }
    return apply_for_subcolumns(
            [&](const auto& subcolumn) { return subcolumn.permute(perm, limit); });
}

void ColumnVariant::pop_back(size_t length) {
    for (auto& entry : subcolumns) {
        entry->data.pop_back(length);
    }
    num_rows -= length;
}

const ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn(const PathInData& key) const {
    const auto* node = subcolumns.find_leaf(key);
    if (node == nullptr) {
        VLOG_DEBUG << "There is no subcolumn " << key.get_path();
        return nullptr;
    }
    return &node->data;
}

const ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn_with_cache(const PathInData& key,
                                                                        size_t key_index) const {
    // Optimization by caching the order of fields (which is almost always the same)
    // and a quick check to match the next expected field, instead of searching the hash table.
    if (_prev_positions.size() > key_index && _prev_positions[key_index].second != nullptr &&
        key == _prev_positions[key_index].first) {
        return _prev_positions[key_index].second;
    }
    const auto* subcolumn = get_subcolumn(key);
    if (key_index >= _prev_positions.size()) {
        _prev_positions.resize(key_index + 1);
    }
    if (subcolumn != nullptr) {
        _prev_positions[key_index] = std::make_pair(key, subcolumn);
    }
    return subcolumn;
}

ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn(const PathInData& key, size_t key_index) {
    return const_cast<ColumnVariant::Subcolumn*>(get_subcolumn_with_cache(key, key_index));
}

const ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn(const PathInData& key,
                                                             size_t key_index) const {
    return get_subcolumn_with_cache(key, key_index);
}

ColumnVariant::Subcolumn* ColumnVariant::get_subcolumn(const PathInData& key) {
    const auto* node = subcolumns.find_leaf(key);
    if (node == nullptr) {
        VLOG_DEBUG << "There is no subcolumn " << key.get_path();
        return nullptr;
    }
    return &const_cast<Subcolumns::Node*>(node)->data;
}

bool ColumnVariant::has_subcolumn(const PathInData& key) const {
    return subcolumns.find_leaf(key) != nullptr;
}

bool ColumnVariant::add_sub_column(const PathInData& key, MutableColumnPtr&& subcolumn,
                                   DataTypePtr type) {
    size_t new_size = subcolumn->size();
    doc_structure = nullptr;
    if (key.empty() && subcolumns.empty()) {
        // create root
        subcolumns.create_root(Subcolumn(std::move(subcolumn), type, is_nullable, true));
        num_rows = new_size;
        return true;
    }
    if (key.empty() && ((!subcolumns.get_root()->is_scalar()) ||
                        subcolumns.get_root()->data.get_least_common_type()->get_primitive_type() ==
                                INVALID_TYPE)) {
        bool root_it_scalar = subcolumns.get_root()->is_scalar();
        // update root to scalar
        subcolumns.get_mutable_root()->modify_to_scalar(
                Subcolumn(std::move(subcolumn), type, is_nullable, true));
        if (!root_it_scalar) {
            subcolumns.add_leaf(subcolumns.get_root_ptr());
        }
        if (num_rows == 0) {
            num_rows = new_size;
        }
        return true;
    }
    bool inserted = subcolumns.add(key, Subcolumn(std::move(subcolumn), type, is_nullable));
    if (!inserted) {
        VLOG_DEBUG << "Duplicated sub column " << key.get_path();
        return false;
    }
    if (num_rows == 0) {
        num_rows = new_size;
    } else if (new_size != num_rows) {
        VLOG_DEBUG << "Size of subcolumn is in consistent with column";
        return false;
    }
    return true;
}

bool ColumnVariant::add_sub_column(const PathInData& key, size_t new_size) {
    if (key.empty() && subcolumns.empty()) {
        // create root
        subcolumns.create_root(Subcolumn(new_size, is_nullable, true));
        num_rows = new_size;
        return true;
    }
    if (key.empty() && (!subcolumns.get_root()->is_scalar())) {
        // update none scalar root column to scalar node
        subcolumns.get_mutable_root()->modify_to_scalar(Subcolumn(new_size, is_nullable, true));
        subcolumns.add_leaf(subcolumns.get_root_ptr());
        if (num_rows == 0) {
            num_rows = new_size;
        }
        return true;
    }
    doc_structure = nullptr;
    bool inserted = subcolumns.add(key, Subcolumn(new_size, is_nullable));
    if (!inserted) {
        VLOG_DEBUG << "Duplicated sub column " << key.get_path();
        return false;
    }
    if (num_rows == 0) {
        num_rows = new_size;
    } else if (new_size != num_rows) {
        VLOG_DEBUG << "Size of subcolumn is in consistent with column";
        return false;
    }
    return true;
}

PathsInData ColumnVariant::getKeys() const {
    PathsInData keys;
    keys.reserve(subcolumns.size());
    for (const auto& entry : subcolumns) {
        keys.emplace_back(entry->path);
    }
    return keys;
}

bool ColumnVariant::is_finalized() const {
    return std::all_of(subcolumns.begin(), subcolumns.end(),
                       [](const auto& entry) { return entry->data.is_finalized(); });
}

void ColumnVariant::Subcolumn::wrapp_array_nullable() {
    // Wrap array with nullable, treat empty array as null to elimate conflict at present
    auto& result_column = get_finalized_column_ptr();
    if (is_column<vectorized::ColumnArray>(result_column.get()) && !result_column->is_nullable()) {
        auto new_null_map = ColumnUInt8::create();
        new_null_map->reserve(result_column->size());
        auto& null_map_data = new_null_map->get_data();
        const auto* array = static_cast<const ColumnArray*>(result_column.get());
        for (size_t i = 0; i < array->size(); ++i) {
            null_map_data.push_back(array->is_default_at(i));
        }
        result_column = ColumnNullable::create(std::move(result_column), std::move(new_null_map));
        data_types[0] = make_nullable(data_types[0]);
        least_common_type = LeastCommonType {data_types[0]};
    }
}

rapidjson::Value* find_leaf_node_by_path(rapidjson::Value& json, const PathInData& path,
                                         int idx = 0) {
    if (idx >= path.get_parts().size()) {
        return &json;
    }

    std::string_view current_key = path.get_parts()[idx].key;
    if (!json.IsObject()) {
        return nullptr;
    }
    /*! RapidJSON uses 32-bit array/string indices even on 64-bit platforms,
    instead of using \c size_t. Users may override the SizeType by defining
    \ref RAPIDJSON_NO_SIZETYPEDEFINE.
    */
    rapidjson::Value name(current_key.data(), cast_set<unsigned>(current_key.size()));
    auto it = json.FindMember(name);
    if (it == json.MemberEnd()) {
        return nullptr;
    }
    rapidjson::Value& current = it->value;
    // if (idx == path.get_parts().size() - 1) {
    //     return &current;
    // }
    return find_leaf_node_by_path(current, path, idx + 1);
}

// skip empty json:
// 1. null value as empty json, todo: think a better way to disinguish empty json and null json.
// 2. nested array with only nulls, eg. [null. null],todo: think a better way to deal distinguish array null value and real null value.
// 3. empty root jsonb value(not null)
// 4. type is nothing
bool skip_empty_json(const ColumnNullable* nullable, const DataTypePtr& type,
                     PrimitiveType base_type_id, size_t row, const PathInData& path) {
    // skip nulls
    if (nullable && nullable->is_null_at(row)) {
        return true;
    }
    // check if it is empty nested json array, then skip
    if (base_type_id == PrimitiveType::TYPE_VARIANT && type->equals(*ColumnVariant::NESTED_TYPE)) {
        Field field = (*nullable)[row];
        if (field.get_type() == PrimitiveType::TYPE_ARRAY) {
            const auto& array = field.get<Array>();
            bool only_nulls_inside = true;
            for (const auto& elem : array) {
                if (elem.get_type() != PrimitiveType::TYPE_NULL) {
                    only_nulls_inside = false;
                    break;
                }
            }
            // if only nulls then skip
            return only_nulls_inside;
        }
    }
    // skip empty jsonb value
    if ((path.empty() && nullable && nullable->get_data_at(row).empty())) {
        return true;
    }
    // skip nothing type
    if (base_type_id == PrimitiveType::INVALID_TYPE) {
        return true;
    }
    return false;
}

Status find_and_set_leave_value(const IColumn* column, const PathInData& path,
                                const DataTypeSerDeSPtr& type_serde, const DataTypePtr& type,
                                PrimitiveType base_type_index, rapidjson::Value& root,
                                rapidjson::Document::AllocatorType& allocator, Arena& mem_pool,
                                size_t row) {
#ifndef NDEBUG
    // sanitize type and column
    if (column->get_name() != type->create_column()->get_name()) {
        return Status::InternalError(
                "failed to set value for path {}, expected type {}, but got {} at row {}",
                path.get_path(), type->get_name(), column->get_name(), row);
    }
#endif
    const auto* nullable = check_and_get_column<ColumnNullable>(column);
    if (skip_empty_json(nullable, type, base_type_index, row, path)) {
        return Status::OK();
    }
    // TODO could cache the result of leaf nodes with it's path info
    rapidjson::Value* target = find_leaf_node_by_path(root, path);
    if (UNLIKELY(!target)) {
        rapidjson::StringBuffer buffer;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        root.Accept(writer);
        LOG(WARNING) << "could not find path " << path.get_path()
                     << ", root: " << std::string(buffer.GetString(), buffer.GetSize());
        return Status::NotFound("Not found path {}", path.get_path());
    }
    RETURN_IF_ERROR(type_serde->write_one_cell_to_json(*column, *target, allocator, mem_pool, row));
    return Status::OK();
}

// compact null values
// {"a" : {"b" : "d" {"n" : null}, "e" : null}, "c" : 10 }
// after compact -> {"a" : {"c"} : 10}
void compact_null_values(rapidjson::Value& json, rapidjson::Document::AllocatorType& allocator) {
    if (!json.IsObject() || json.IsNull()) {
        return;
    }

    rapidjson::Value::MemberIterator it = json.MemberBegin();
    while (it != json.MemberEnd()) {
        rapidjson::Value& value = it->value;
        if (value.IsNull()) {
            it = json.EraseMember(it);
            continue;
        }
        compact_null_values(value, allocator);
        if (value.IsObject() && value.ObjectEmpty()) {
            it = json.EraseMember(it);
            continue;
        }
        ++it;
    }
}

// Construct rapidjson value from Subcolumns
void get_json_by_column_tree(rapidjson::Value& root, rapidjson::Document::AllocatorType& allocator,
                             const ColumnVariant::Subcolumns::Node* node_root) {
    if (node_root == nullptr || node_root->children.empty()) {
        root.SetNull();
        return;
    }
    root.SetObject();
    // sort to make output stable
    std::vector<StringRef> sorted_keys = node_root->get_sorted_chilren_keys();
    for (const StringRef& key : sorted_keys) {
        rapidjson::Value value(rapidjson::kObjectType);
        get_json_by_column_tree(value, allocator, node_root->get_child_node(key).get());
        root.AddMember(rapidjson::StringRef(key.data, key.size), value, allocator);
    }
}

Status ColumnVariant::serialize_one_row_to_string(size_t row, std::string* output) const {
    if (!is_finalized()) {
        const_cast<ColumnVariant*>(this)->finalize(FinalizeMode::READ_MODE);
    }
    rapidjson::StringBuffer buf;
    if (is_scalar_variant()) {
        auto type = get_root_type();
        *output = type->to_string(*get_root(), row);
        return Status::OK();
    }
    RETURN_IF_ERROR(serialize_one_row_to_json_format(row, &buf, nullptr));
    // TODO avoid copy
    *output = std::string(buf.GetString(), buf.GetSize());
    return Status::OK();
}

Status ColumnVariant::serialize_one_row_to_string(size_t row, BufferWritable& output) const {
    if (!is_finalized()) {
        const_cast<ColumnVariant*>(this)->finalize(FinalizeMode::READ_MODE);
    }
    if (is_scalar_variant()) {
        auto type = get_root_type();
        type->to_string(*get_root(), row, output);
        return Status::OK();
    }
    rapidjson::StringBuffer buf;
    RETURN_IF_ERROR(serialize_one_row_to_json_format(row, &buf, nullptr));
    output.write(buf.GetString(), buf.GetLength());
    return Status::OK();
}

Status ColumnVariant::serialize_one_row_to_json_format(size_t row, rapidjson::StringBuffer* output,
                                                       bool* is_null) const {
    CHECK(is_finalized());
    if (subcolumns.empty()) {
        if (is_null != nullptr) {
            *is_null = true;
        } else {
            rapidjson::Value root(rapidjson::kNullType);
            rapidjson::Writer<rapidjson::StringBuffer> writer(*output);
            if (!root.Accept(writer)) {
                return Status::InternalError("Failed to serialize json value");
            }
        }
        return Status::OK();
    }
    CHECK(size() > row);
    rapidjson::StringBuffer buffer;
    rapidjson::Value root(rapidjson::kNullType);
    if (doc_structure == nullptr) {
        doc_structure = std::make_shared<rapidjson::Document>();
        rapidjson::Document::AllocatorType& allocator = doc_structure->GetAllocator();
        get_json_by_column_tree(*doc_structure, allocator, subcolumns.get_root());
    }
    if (!doc_structure->IsNull()) {
        root.CopyFrom(*doc_structure, doc_structure->GetAllocator());
    }
    Arena mem_pool;

    bool serialize_root = true; // Assume all subcolumns are null by default
    for (const auto& subcolumn : subcolumns) {
        if (subcolumn->data.is_root) {
            continue; // Skip the root column
        }

        // If any non-root subcolumn is NOT null, set serialize_root to false and exit early
        if (!assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(
                     *subcolumn->data.get_finalized_column_ptr())
                     .is_null_at(row)) {
            serialize_root = false;
            break;
        }
    }
#ifndef NDEBUG
    VLOG_DEBUG << "dump structure " << JsonFunctions::print_json_value(*doc_structure);
#endif
    if (serialize_root && subcolumns.get_root()->is_scalar()) {
        // only serialize root when all other subcolumns is null
        RETURN_IF_ERROR(
                subcolumns.get_root()->data.get_least_common_type_serde()->write_one_cell_to_json(
                        subcolumns.get_root()->data.get_finalized_column(), root,
                        doc_structure->GetAllocator(), mem_pool, row));
        output->Clear();
        compact_null_values(root, doc_structure->GetAllocator());
        rapidjson::Writer<rapidjson::StringBuffer> writer(*output);
        root.Accept(writer);
        return Status::OK();
    }
    // handle subcolumns exclude root node
    for (const auto& subcolumn : subcolumns) {
        if (subcolumn->data.is_root) {
            continue;
        }
        RETURN_IF_ERROR(find_and_set_leave_value(
                subcolumn->data.get_finalized_column_ptr().get(), subcolumn->path,
                subcolumn->data.get_least_common_type_serde(),
                subcolumn->data.get_least_common_type(),
                subcolumn->data.least_common_type.get_base_type_id(), root,
                doc_structure->GetAllocator(), mem_pool, row));
    }
    compact_null_values(root, doc_structure->GetAllocator());
    if (root.IsNull() && is_null != nullptr) {
        // Fast path
        *is_null = true;
    } else {
        output->Clear();
        rapidjson::Writer<rapidjson::StringBuffer> writer(*output);
        if (!root.Accept(writer)) {
            return Status::InternalError("Failed to serialize json value");
        }
    }
    return Status::OK();
}

Status ColumnVariant::merge_sparse_to_root_column() {
    CHECK(is_finalized());
    if (sparse_columns.empty()) {
        return Status::OK();
    }
    ColumnPtr src = subcolumns.get_mutable_root()->data.get_finalized_column_ptr();
    MutableColumnPtr mresult = src->clone_empty();
    const ColumnNullable* src_null = assert_cast<const ColumnNullable*>(src.get());
    const ColumnString* src_column_ptr =
            assert_cast<const ColumnString*>(&src_null->get_nested_column());
    rapidjson::StringBuffer buffer;
    doc_structure = std::make_shared<rapidjson::Document>();
    rapidjson::Document::AllocatorType& allocator = doc_structure->GetAllocator();
    get_json_by_column_tree(*doc_structure, allocator, sparse_columns.get_root());

#ifndef NDEBUG
    VLOG_DEBUG << "dump structure " << JsonFunctions::print_json_value(*doc_structure);
#endif

    ColumnNullable* result_column_nullable =
            assert_cast<ColumnNullable*>(mresult->assume_mutable().get());
    ColumnString* result_column_ptr =
            assert_cast<ColumnString*>(&result_column_nullable->get_nested_column());
    result_column_nullable->reserve(num_rows);
    // parse each row to jsonb
    for (size_t i = 0; i < num_rows; ++i) {
        // root is not null, store original value, eg. the root is scalar type like '[1]'
        if (!src_null->empty() && !src_null->is_null_at(i)) {
            result_column_ptr->insert_data(src_column_ptr->get_data_at(i).data,
                                           src_column_ptr->get_data_at(i).size);
            result_column_nullable->get_null_map_data().push_back(0);
            continue;
        }

        // parse and encode sparse columns
        buffer.Clear();
        rapidjson::Value root(rapidjson::kNullType);
        if (!doc_structure->IsNull()) {
            root.CopyFrom(*doc_structure, doc_structure->GetAllocator());
        }
        size_t null_count = 0;
        Arena mem_pool;
        for (const auto& subcolumn : sparse_columns) {
            auto& column = subcolumn->data.get_finalized_column_ptr();
            if (assert_cast<const ColumnNullable&, TypeCheckOnRelease::DISABLE>(*column).is_null_at(
                        i)) {
                ++null_count;
                continue;
            }
            bool succ = find_and_set_leave_value(
                    column.get(), subcolumn->path, subcolumn->data.get_least_common_type_serde(),
                    subcolumn->data.get_least_common_type(),
                    subcolumn->data.least_common_type.get_base_type_id(), root,
                    doc_structure->GetAllocator(), mem_pool, i);
            if (succ && subcolumn->path.empty() && !root.IsObject()) {
                // root was modified, only handle root node
                break;
            }
        }

        // all null values, store null to sparse root
        if (null_count == sparse_columns.size()) {
            result_column_ptr->insert_default();
            result_column_nullable->get_null_map_data().push_back(1);
            continue;
        }

        // encode sparse columns into jsonb format
        compact_null_values(root, doc_structure->GetAllocator());
        // parse as jsonb value and put back to rootnode
        // TODO, we could convert to jsonb directly from rapidjson::Value for better performance, instead of parsing
        JsonBinaryValue jsonb_value;
        rapidjson::Writer<rapidjson::StringBuffer> writer(buffer);
        root.Accept(writer);
        RETURN_IF_ERROR(jsonb_value.from_json_string(buffer.GetString(), buffer.GetSize()));
        result_column_ptr->insert_data(jsonb_value.value(), jsonb_value.size());
        result_column_nullable->get_null_map_data().push_back(0);
    }
    subcolumns.get_mutable_root()->data.get_finalized_column().clear();
    // assign merged column, do insert_range_from to make a copy, instead of replace the ptr itselft
    // to make sure the root column ptr is not changed
    subcolumns.get_mutable_root()->data.get_finalized_column().insert_range_from(
            *mresult->get_ptr(), 0, num_rows);
    return Status::OK();
}

void ColumnVariant::unnest(Subcolumns::NodePtr& entry, Subcolumns& arg_subcolumns) const {
    entry->data.finalize();
    auto nested_column = entry->data.get_finalized_column_ptr()->assume_mutable();
    auto* nested_column_nullable = assert_cast<ColumnNullable*>(nested_column.get());
    auto* nested_column_array =
            assert_cast<ColumnArray*>(nested_column_nullable->get_nested_column_ptr().get());
    auto& offset = nested_column_array->get_offsets_ptr();

    auto* nested_object_nullable = assert_cast<ColumnNullable*>(
            nested_column_array->get_data_ptr()->assume_mutable().get());
    auto& nested_object_column =
            assert_cast<ColumnVariant&>(nested_object_nullable->get_nested_column());
    PathInData nested_path = entry->path;
    for (auto& nested_entry : nested_object_column.subcolumns) {
        if (nested_entry->data.least_common_type.get_base_type_id() ==
            PrimitiveType::INVALID_TYPE) {
            continue;
        }
        nested_entry->data.finalize();
        PathInDataBuilder path_builder;
        // format nested path
        path_builder.append(nested_path.get_parts(), false);
        path_builder.append(nested_entry->path.get_parts(), true);
        auto subnested_column = ColumnArray::create(
                ColumnNullable::create(nested_entry->data.get_finalized_column_ptr(),
                                       nested_object_nullable->get_null_map_column_ptr()),
                offset);
        auto nullable_subnested_column = ColumnNullable::create(
                std::move(subnested_column), nested_column_nullable->get_null_map_column_ptr());
        auto type = make_nullable(
                std::make_shared<DataTypeArray>(nested_entry->data.least_common_type.get()));
        Subcolumn subcolumn(nullable_subnested_column->assume_mutable(), type, is_nullable);
        arg_subcolumns.add(path_builder.build(), subcolumn);
    }
}

void ColumnVariant::finalize(FinalizeMode mode) {
    Subcolumns new_subcolumns;
    // finalize root first
    if (mode == FinalizeMode::WRITE_MODE || !is_null_root()) {
        new_subcolumns.create_root(subcolumns.get_root()->data);
        new_subcolumns.get_mutable_root()->data.finalize(mode);
    }
    for (auto&& entry : subcolumns) {
        const auto& least_common_type = entry->data.get_least_common_type();
        /// Do not add subcolumns, which consists only from NULLs
        if (get_base_type_of_array(least_common_type)->get_primitive_type() == INVALID_TYPE) {
            continue;
        }

        // unnest all nested columns, add them to new_subcolumns
        if (mode == FinalizeMode::WRITE_MODE &&
            least_common_type->equals(*ColumnVariant::NESTED_TYPE)) {
            unnest(entry, new_subcolumns);
            continue;
        }

        entry->data.finalize(mode);
        entry->data.wrapp_array_nullable();

        if (entry->data.is_root) {
            continue;
        }

        // Check and spilit sparse subcolumns, not support nested array at present
        if (mode == FinalizeMode::WRITE_MODE && (entry->data.check_if_sparse_column(num_rows)) &&
            !entry->path.has_nested_part()) {
            // TODO seperate ambiguous path
            sparse_columns.add(entry->path, entry->data);
            continue;
        }

        new_subcolumns.add(entry->path, entry->data);
    }
    std::swap(subcolumns, new_subcolumns);
    doc_structure = nullptr;
    _prev_positions.clear();
}

void ColumnVariant::finalize() {
    finalize(FinalizeMode::READ_MODE);
}

void ColumnVariant::ensure_root_node_type(const DataTypePtr& expected_root_type) {
    auto& root = subcolumns.get_mutable_root()->data;
    if (!root.get_least_common_type()->equals(*expected_root_type)) {
        // make sure the root type is alawys as expected
        ColumnPtr casted_column;
        THROW_IF_ERROR(
                schema_util::cast_column(ColumnWithTypeAndName {root.get_finalized_column_ptr(),
                                                                root.get_least_common_type(), ""},
                                         expected_root_type, &casted_column));
        root.data[0] = casted_column;
        root.data_types[0] = expected_root_type;
        root.least_common_type = Subcolumn::LeastCommonType {expected_root_type};
    }
}

bool ColumnVariant::empty() const {
    return subcolumns.empty() || subcolumns.begin()->get()->path.get_path() == COLUMN_NAME_DUMMY;
}

ColumnPtr get_base_column_of_array(const ColumnPtr& column) {
    if (const auto* column_array = check_and_get_column<ColumnArray>(column.get())) {
        return column_array->get_data_ptr();
    }
    return column;
}

ColumnPtr ColumnVariant::filter(const Filter& filter, ssize_t count) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnVariant&>(*finalized);
        return finalized_object.filter(filter, count);
    }
    if (num_rows == 0 || subcolumns.empty()) {
        // Add an emtpy column with filtered rows
        auto res = ColumnVariant::create(true, false);
        res->set_num_rows(count_bytes_in_filter(filter));
        return res;
    }
    auto new_column = ColumnVariant::create(true, false);
    for (auto& entry : subcolumns) {
        auto subcolumn = entry->data.get_finalized_column().filter(filter, -1);
        new_column->add_sub_column(entry->path, subcolumn->assume_mutable(),
                                   entry->data.get_least_common_type());
    }
    return new_column;
}

Status ColumnVariant::filter_by_selector(const uint16_t* sel, size_t sel_size, IColumn* col_ptr) {
    if (!is_finalized()) {
        finalize();
    }
    if (num_rows == 0 || subcolumns.empty()) {
        assert_cast<ColumnVariant*>(col_ptr)->insert_many_defaults(sel_size);
        return Status::OK();
    }
    auto* res = assert_cast<ColumnVariant*>(col_ptr);
    for (const auto& subcolumn : subcolumns) {
        auto new_subcolumn = subcolumn->data.get_least_common_type()->create_column();
        RETURN_IF_ERROR(subcolumn->data.get_finalized_column().filter_by_selector(
                sel, sel_size, new_subcolumn.get()));
        res->add_sub_column(subcolumn->path, new_subcolumn->assume_mutable(),
                            subcolumn->data.get_least_common_type());
    }
    return Status::OK();
}

size_t ColumnVariant::filter(const Filter& filter) {
    if (!is_finalized()) {
        finalize();
    }
    size_t count = filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    if (count == 0) {
        for_each_subcolumn([](auto& part) { part->clear(); });
    } else {
        for_each_subcolumn([&](auto& part) {
            if (part->size() != count) {
                if (part->is_exclusive()) {
                    const auto result_size = part->filter(filter);
                    if (result_size != count) {
                        throw Exception(ErrorCode::INTERNAL_ERROR,
                                        "result_size not euqal with filter_size, result_size={}, "
                                        "filter_size={}",
                                        result_size, count);
                    }
                    CHECK_EQ(result_size, count);
                } else {
                    part = part->filter(filter, count);
                }
            }
        });
    }
    num_rows = count;
#ifndef NDEBUG
    check_consistency();
#endif
    return count;
}

void ColumnVariant::clear_subcolumns_data() {
    for (auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            DCHECK_EQ(part->use_count(), 1);
            (*std::move(part)).clear();
        }
        entry->data.num_of_defaults_in_prefix = 0;
    }
    num_rows = 0;
}

void ColumnVariant::clear() {
    Subcolumns empty;
    std::swap(empty, subcolumns);
    num_rows = 0;
    _prev_positions.clear();
}

void ColumnVariant::create_root() {
    auto type = is_nullable ? make_nullable(std::make_shared<MostCommonType>())
                            : std::make_shared<MostCommonType>();
    add_sub_column({}, type->create_column(), type);
}

void ColumnVariant::create_root(const DataTypePtr& type, MutableColumnPtr&& column) {
    if (num_rows == 0) {
        num_rows = column->size();
    }
    add_sub_column({}, std::move(column), type);
}

DataTypePtr ColumnVariant::get_most_common_type() const {
    auto type = is_nullable ? make_nullable(std::make_shared<MostCommonType>())
                            : std::make_shared<MostCommonType>();
    return type;
}

bool ColumnVariant::is_null_root() const {
    auto* root = subcolumns.get_root();
    if (root == nullptr) {
        return true;
    }
    if (root->data.num_of_defaults_in_prefix == 0 &&
        (root->data.data.empty() ||
         root->data.get_least_common_type()->get_primitive_type() == INVALID_TYPE)) {
        return true;
    }
    return false;
}

bool ColumnVariant::is_scalar_variant() const {
    // Only root itself
    return !is_null_root() && subcolumns.get_leaves().size() == 1 &&
           subcolumns.get_root()->is_scalar();
}

const DataTypePtr ColumnVariant::NESTED_TYPE = std::make_shared<vectorized::DataTypeNullable>(
        std::make_shared<vectorized::DataTypeArray>(std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeVariant>())));

const DataTypePtr ColumnVariant::NESTED_TYPE_AS_ARRAY_OF_JSONB =
        std::make_shared<vectorized::DataTypeArray>(std::make_shared<vectorized::DataTypeNullable>(
                std::make_shared<vectorized::DataTypeJsonb>()));

DataTypePtr ColumnVariant::get_root_type() const {
    return subcolumns.get_root()->data.get_least_common_type();
}

#define SANITIZE_ROOT()                                                                            \
    if (is_null_root()) {                                                                          \
        return Status::InternalError("No root column, path {}", path.get_path());                  \
    }                                                                                              \
    if (subcolumns.get_root()->data.get_least_common_type()->get_primitive_type() != TYPE_JSONB) { \
        return Status::InternalError(                                                              \
                "Root column is not jsonb type but {}, path {}",                                   \
                subcolumns.get_root()->data.get_least_common_type()->get_name(), path.get_path()); \
    }

Status ColumnVariant::extract_root(const PathInData& path, MutableColumnPtr& dst) const {
    SANITIZE_ROOT();
    if (!path.empty()) {
        RETURN_IF_ERROR(schema_util::extract(subcolumns.get_root()->data.get_finalized_column_ptr(),
                                             path, dst));
    } else {
        if (!dst) {
            dst = subcolumns.get_root()->data.get_finalized_column_ptr()->clone_empty();
            dst->reserve(num_rows);
        }
        dst->insert_range_from(*subcolumns.get_root()->data.get_finalized_column_ptr(), 0,
                               num_rows);
    }
    return Status::OK();
}

void ColumnVariant::insert_indices_from(const IColumn& src, const uint32_t* indices_begin,
                                        const uint32_t* indices_end) {
    // optimize when src and this column are scalar variant, since try_insert is inefficiency
    const auto* src_v = check_and_get_column<ColumnVariant>(src);

    bool src_can_do_quick_insert =
            src_v != nullptr && src_v->is_scalar_variant() && src_v->is_finalized();
    // num_rows == 0 means this column is empty, we not need to check it type
    if (num_rows == 0 && src_can_do_quick_insert) {
        // add a new root column, and insert from src root column
        clear();
        add_sub_column({}, src_v->get_root()->clone_empty(), src_v->get_root_type());

        get_root()->insert_indices_from(*src_v->get_root(), indices_begin, indices_end);
        num_rows += indices_end - indices_begin;
    } else if (src_can_do_quick_insert && is_scalar_variant() &&
               src_v->get_root_type()->equals(*get_root_type())) {
        get_root()->insert_indices_from(*src_v->get_root(), indices_begin, indices_end);
        num_rows += indices_end - indices_begin;
    } else {
        for (const auto* x = indices_begin; x != indices_end; ++x) {
            try_insert(src[*x]);
        }
    }
    finalize();
}

void ColumnVariant::for_each_imutable_subcolumn(ImutableColumnCallback callback) const {
    if (!is_finalized()) {
        auto finalized = clone_finalized();
        auto& finalized_object = assert_cast<ColumnVariant&>(*finalized);
        finalized_object.for_each_imutable_subcolumn(callback);
        return;
    }
    for (const auto& entry : subcolumns) {
        for (auto& part : entry->data.data) {
            callback(*part);
        }
    }
}

bool ColumnVariant::is_exclusive() const {
    bool is_exclusive = IColumn::is_exclusive();
    for_each_imutable_subcolumn([&](const auto& subcolumn) {
        if (!subcolumn.is_exclusive()) {
            is_exclusive = false;
        }
    });
    return is_exclusive;
}

void ColumnVariant::update_hash_with_value(size_t n, SipHash& hash) const {
    for_each_imutable_subcolumn(
            [&](const auto& subcolumn) { return subcolumn.update_hash_with_value(n, hash); });
}

void ColumnVariant::update_hashes_with_value(uint64_t* __restrict hashes,
                                             const uint8_t* __restrict null_data) const {
    for_each_imutable_subcolumn([&](const auto& subcolumn) {
        return subcolumn.update_hashes_with_value(hashes, nullptr);
    });
}

void ColumnVariant::update_xxHash_with_value(size_t start, size_t end, uint64_t& hash,
                                             const uint8_t* __restrict null_data) const {
    for_each_imutable_subcolumn([&](const auto& subcolumn) {
        return subcolumn.update_xxHash_with_value(start, end, hash, nullptr);
    });
}

void ColumnVariant::update_crcs_with_value(uint32_t* __restrict hash, PrimitiveType type,
                                           uint32_t rows, uint32_t offset,
                                           const uint8_t* __restrict null_data) const {
    for_each_imutable_subcolumn([&](const auto& subcolumn) {
        return subcolumn.update_crcs_with_value(hash, type, rows, offset, nullptr);
    });
}

void ColumnVariant::update_crc_with_value(size_t start, size_t end, uint32_t& hash,
                                          const uint8_t* __restrict null_data) const {
    for_each_imutable_subcolumn([&](const auto& subcolumn) {
        return subcolumn.update_crc_with_value(start, end, hash, nullptr);
    });
}

std::string ColumnVariant::debug_string() const {
    std::stringstream res;
    res << get_name() << "(num_row = " << num_rows;
    for (auto& entry : subcolumns) {
        if (entry->data.is_finalized()) {
            res << "[column:" << entry->data.data[0]->dump_structure()
                << ",type:" << entry->data.data_types[0]->get_name()
                << ",path:" << entry->path.get_path() << "],";
        }
    }
    res << ")";
    return res.str();
}

Status ColumnVariant::sanitize() const {
#ifndef NDEBUG
    RETURN_IF_CATCH_EXCEPTION(check_consistency());
    for (const auto& subcolumn : subcolumns) {
        if (subcolumn->data.is_finalized()) {
            auto column = subcolumn->data.get_least_common_type()->create_column();
            std::string original = subcolumn->data.get_finalized_column().get_name();
            std::string expected = column->get_name();
            if (original != expected) {
                return Status::InternalError("Incompatible type between {} and {}, debug_info:",
                                             original, expected, debug_string());
            }
        }
    }

    VLOG_DEBUG << "sanitized " << debug_string();
#endif
    return Status::OK();
}

ColumnVariant::Subcolumn ColumnVariant::Subcolumn::cut(size_t start, size_t length) const {
    Subcolumn new_subcolumn(0, is_nullable);
    new_subcolumn.insert_range_from(*this, start, length);
    return new_subcolumn;
}

const ColumnVariant::Subcolumns::Node* ColumnVariant::get_leaf_of_the_same_nested(
        const Subcolumns::NodePtr& entry) const {
    const auto* leaf = subcolumns.get_leaf_of_the_same_nested(
            entry->path,
            [&](const Subcolumns::Node& node) { return node.data.size() > entry->data.size(); });
    if (leaf && leaf->data.get_least_common_typeBase()->get_primitive_type() == INVALID_TYPE) {
        return nullptr;
    }
    return leaf;
}

bool ColumnVariant::try_insert_many_defaults_from_nested(const Subcolumns::NodePtr& entry) const {
    const auto* leaf = get_leaf_of_the_same_nested(entry);
    if (!leaf) {
        return false;
    }

    size_t old_size = entry->data.size();
    FieldInfo field_info = {
            .scalar_type_id = entry->data.least_common_type.get_base_type_id(),
            .have_nulls = false,
            .need_convert = false,
            .num_dimensions = entry->data.get_dimensions(),
    };

    /// Cut the needed range from the found leaf
    /// and replace scalar values to the correct
    /// default values for given entry.
    auto new_subcolumn = leaf->data.cut(old_size, leaf->data.size() - old_size)
                                 .clone_with_default_values(field_info);

    entry->data.insert_range_from(new_subcolumn, 0, new_subcolumn.size());
    return true;
}

bool ColumnVariant::try_insert_default_from_nested(const Subcolumns::NodePtr& entry) const {
    const auto* leaf = get_leaf_of_the_same_nested(entry);
    if (!leaf) {
        return false;
    }

    auto last_field = leaf->data.get_last_field();
    if (last_field.is_null()) {
        return false;
    }

    size_t leaf_num_dimensions = leaf->data.get_dimensions();
    size_t entry_num_dimensions = entry->data.get_dimensions();

    auto default_scalar = entry_num_dimensions > leaf_num_dimensions
                                  ? Field::create_field<TYPE_ARRAY>(create_empty_array_field(
                                            entry_num_dimensions - leaf_num_dimensions))
                                  : entry->data.get_least_common_type()->get_default();

    auto default_field = apply_visitor(
            FieldVisitorReplaceScalars(default_scalar, leaf_num_dimensions), last_field);
    entry->data.insert(std::move(default_field));
    return true;
}

void ColumnVariant::Subcolumn::convert_array_string_to_array_jsonb(Field& array_field) {
    if (array_field.is_null()) {
        return;
    }
    if (array_field.get_type() != PrimitiveType::TYPE_ARRAY) {
        return;
    }
    Field converted_res = Field::create_field<TYPE_ARRAY>(Array());
    for (auto& item : array_field.get<Array&>()) {
        Field jsonb_item;
        DCHECK(item.get_type() == PrimitiveType::TYPE_STRING);
        auto& string_item = item.get<String&>();
        jsonb_item = Field::create_field<TYPE_JSONB>(
                JsonbField(string_item.c_str(), string_item.size()));
        converted_res.get<Array&>().emplace_back(std::move(jsonb_item));
    }
    array_field = std::move(converted_res);
}

} // namespace doris::vectorized
