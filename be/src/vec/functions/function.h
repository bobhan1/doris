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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Functions/IFunction.h
// and modified by Doris

#pragma once

#include <fmt/format.h>
#include <glog/logging.h>

#include <cstddef>
#include <memory>
#include <string>
#include <utility>

#include "common/exception.h"
#include "common/logging.h"
#include "common/status.h"
#include "olap/rowset/segment_v2/inverted_index_iterator.h"
#include "udf/udf.h"
#include "vec/core/block.h"
#include "vec/core/column_numbers.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/core/columns_with_type_and_name.h"
#include "vec/core/types.h"
#include "vec/data_types/data_type.h"
#include "vec/data_types/data_type_nullable.h"

namespace doris::vectorized {

struct FunctionAttr {
    bool enable_decimal256 {false};
};

#define RETURN_REAL_TYPE_FOR_DATEV2_FUNCTION(TYPE)                                             \
    bool is_nullable = false;                                                                  \
    bool is_datev2 = false;                                                                    \
    for (auto it : arguments) {                                                                \
        is_nullable = is_nullable || it.type->is_nullable();                                   \
        is_datev2 = is_datev2 || it.type->get_primitive_type() == TYPE_DATEV2 ||               \
                    it.type->get_primitive_type() == TYPE_DATETIMEV2;                          \
    }                                                                                          \
    return is_nullable || !is_datev2                                                           \
                   ? make_nullable(                                                            \
                             std::make_shared<typename PrimitiveTypeTraits<TYPE>::DataType>()) \
                   : std::make_shared<typename PrimitiveTypeTraits<TYPE>::DataType>();

#define SET_NULLMAP_IF_FALSE(EXPR) \
    if (!EXPR) [[unlikely]] {      \
        null_map[i] = true;        \
    }

class Field;
class VExpr;

// Only use dispose the variadic argument
template <typename T>
auto has_variadic_argument_types(T&& arg) -> decltype(T::get_variadic_argument_types()) {};
void has_variadic_argument_types(...);

template <typename T>
concept HasGetVariadicArgumentTypesImpl = requires(T t) {
    { t.get_variadic_argument_types_impl() } -> std::same_as<DataTypes>;
};

bool have_null_column(const Block& block, const ColumnNumbers& args);
bool have_null_column(const ColumnsWithTypeAndName& args);

/// The simplest executable object.
/// Motivation:
///  * Prepare something heavy once before main execution loop instead of doing it for each block.
///  * Provide const interface for IFunctionBase (later).
class IPreparedFunction {
public:
    virtual ~IPreparedFunction() = default;

    /// Get the main function name.
    virtual String get_name() const = 0;

    virtual Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                           uint32_t result, size_t input_rows_count, bool dry_run) const = 0;
};

using PreparedFunctionPtr = std::shared_ptr<IPreparedFunction>;

class PreparedFunctionImpl : public IPreparedFunction {
public:
    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   uint32_t result, size_t input_rows_count, bool dry_run = false) const final;

    /** If the function have non-zero number of arguments,
      *  and if all arguments are constant, that we could automatically provide default implementation:
      *  arguments are converted to ordinary columns with single value which is not const, then function is executed as usual,
      *  and then the result is converted to constant column.
      */
    virtual bool use_default_implementation_for_constants() const { return true; }

    /** If use_default_implementation_for_nulls() is true, after execute the function,
      * whether need to replace the nested data of null data to the default value.
      * E.g. for binary arithmetic exprs, need return true to avoid false overflow.
      */
    virtual bool need_replace_null_data_to_default() const { return false; }

protected:
    virtual Status execute_impl_dry_run(FunctionContext* context, Block& block,
                                        const ColumnNumbers& arguments, uint32_t result,
                                        size_t input_rows_count) const {
        return execute_impl(context, block, arguments, result, input_rows_count);
    }

    virtual Status execute_impl(FunctionContext* context, Block& block,
                                const ColumnNumbers& arguments, uint32_t result,
                                size_t input_rows_count) const = 0;

    /** Default implementation in presence of Nullable arguments or NULL constants as arguments is the following:
      *  if some of arguments are NULL constants then return NULL constant,
      *  if some of arguments are Nullable, then execute function as usual for block,
      *   where Nullable columns are substituted with nested columns (they have arbitrary values in rows corresponding to NULL value)
      *   and wrap result in Nullable column where NULLs are in all rows where any of arguments are NULL.
      */
    virtual bool use_default_implementation_for_nulls() const { return true; }

    virtual bool skip_return_type_check() const { return false; }

    /** Some arguments could remain constant during this implementation.
      * Every argument required const must write here and no checks elsewhere.
      */
    virtual ColumnNumbers get_arguments_that_are_always_constant() const { return {}; }

private:
    Status default_implementation_for_nulls(FunctionContext* context, Block& block,
                                            const ColumnNumbers& args, uint32_t result,
                                            size_t input_rows_count, bool dry_run,
                                            bool* executed) const;
    Status default_implementation_for_constant_arguments(FunctionContext* context, Block& block,
                                                         const ColumnNumbers& args, uint32_t result,
                                                         size_t input_rows_count, bool dry_run,
                                                         bool* executed) const;
    Status execute_without_low_cardinality_columns(FunctionContext* context, Block& block,
                                                   const ColumnNumbers& arguments, uint32_t result,
                                                   size_t input_rows_count, bool dry_run) const;
    Status _execute_skipped_constant_deal(FunctionContext* context, Block& block,
                                          const ColumnNumbers& args, uint32_t result,
                                          size_t input_rows_count, bool dry_run) const;
};

/// Function with known arguments and return type.
class IFunctionBase {
public:
    virtual ~IFunctionBase() = default;

    /// Get the main function name.
    virtual String get_name() const = 0;

    virtual const DataTypes& get_argument_types() const = 0;
    virtual const DataTypePtr& get_return_type() const = 0;

    /// Do preparations and return executable.
    /// sample_block should contain data types of arguments and values of constants, if relevant.
    virtual PreparedFunctionPtr prepare(FunctionContext* context, const Block& sample_block,
                                        const ColumnNumbers& arguments, uint32_t result) const = 0;

    /// Override this when function need to store state in the `FunctionContext`, or do some
    /// preparation work according to information from `FunctionContext`.
    virtual Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    Status execute(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                   uint32_t result, size_t input_rows_count, bool dry_run = false) const {
        try {
            return prepare(context, block, arguments, result)
                    ->execute(context, block, arguments, result, input_rows_count, dry_run);
        } catch (const Exception& e) {
            return e.to_status();
        }
    }

    virtual Status evaluate_inverted_index(
            const ColumnsWithTypeAndName& arguments,
            const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
            std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
            segment_v2::InvertedIndexResultBitmap& bitmap_result) const {
        return Status::OK();
    }

    /// Do cleaning work when function is finished, i.e., release state variables in the
    /// `FunctionContext` which are registered in `prepare` phase.
    virtual Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) {
        return Status::OK();
    }

    virtual bool is_use_default_implementation_for_constants() const = 0;

    virtual bool is_udf_function() const { return false; }

    virtual bool can_push_down_to_index() const { return false; }
};

using FunctionBasePtr = std::shared_ptr<IFunctionBase>;

/// Creates IFunctionBase from argument types list.
class IFunctionBuilder {
public:
    virtual ~IFunctionBuilder() = default;

    /// Get the main function name.
    virtual String get_name() const = 0;

    /// Override and return true if function could take different number of arguments.
    virtual bool is_variadic() const = 0;

    /// For non-variadic functions, return number of arguments; otherwise return zero (that should be ignored).
    virtual size_t get_number_of_arguments() const = 0;

    /// Throw if number of arguments is incorrect. Default implementation will check only in non-variadic case.
    virtual void check_number_of_arguments(size_t number_of_arguments) const = 0;

    /// Check arguments and return IFunctionBase.
    virtual FunctionBasePtr build(const ColumnsWithTypeAndName& arguments,
                                  const DataTypePtr& return_type) const = 0;

    /// For higher-order functions (functions, that have lambda expression as at least one argument).
    /// You pass data types with empty DataTypeFunction for lambda arguments.
    /// This function will replace it with DataTypeFunction containing actual types.
    virtual DataTypes get_variadic_argument_types() const = 0;

    /// Returns indexes of arguments, that must be ColumnConst
    virtual ColumnNumbers get_arguments_that_are_always_constant() const = 0;
};

using FunctionBuilderPtr = std::shared_ptr<IFunctionBuilder>;

inline std::string get_types_string(const ColumnsWithTypeAndName& arguments) {
    std::string types;
    for (const auto& argument : arguments) {
        if (!types.empty()) {
            types += ", ";
        }
        types += argument.type->get_name();
    }
    return types;
}

/// used in function_factory. when we register a function, save a builder. to get a function, to get a builder.
/// will use DefaultFunctionBuilder as the default builder in function's registration if we didn't explicitly specify.
class FunctionBuilderImpl : public IFunctionBuilder {
public:
    FunctionBasePtr build(const ColumnsWithTypeAndName& arguments,
                          const DataTypePtr& return_type) const final {
        if (skip_return_type_check()) {
            return build_impl(arguments, return_type);
        }
        const DataTypePtr& func_return_type = get_return_type(arguments);
        if (func_return_type == nullptr) {
            throw doris::Exception(
                    ErrorCode::INTERNAL_ERROR,
                    "function return type check failed, function_name={}, "
                    "expect_return_type={}, real_return_type is nullptr, input_arguments={}",
                    get_name(), return_type->get_name(), get_types_string(arguments));
        }

        // check return types equal.
        if (!(return_type->equals(*func_return_type) ||
              // For null constant argument, `get_return_type` would return
              // Nullable<DataTypeNothing> when `use_default_implementation_for_nulls` is true.
              (return_type->is_nullable() && func_return_type->is_nullable() &&
               ((DataTypeNullable*)func_return_type.get())
                               ->get_nested_type()
                               ->get_primitive_type() == INVALID_TYPE) ||
              is_date_or_datetime_or_decimal(return_type, func_return_type) ||
              is_array_nested_type_date_or_datetime_or_decimal(return_type, func_return_type))) {
            LOG_WARNING(
                    "function return type check failed, function_name={}, "
                    "expect_return_type={}, real_return_type={}, input_arguments={}",
                    get_name(), return_type->get_name(), func_return_type->get_name(),
                    get_types_string(arguments));
            return nullptr;
        }
        return build_impl(arguments, return_type);
    }

    bool is_variadic() const override { return false; }

    // Default implementation. Will check only in non-variadic case.
    void check_number_of_arguments(size_t number_of_arguments) const override;
    // the return type should be same with what FE plans.
    // it returns: `get_return_type_impl` if `use_default_implementation_for_nulls` = false
    //  `get_return_type_impl` warpped in NULL if `use_default_implementation_for_nulls` = true and input has NULL
    DataTypePtr get_return_type(const ColumnsWithTypeAndName& arguments) const;

    DataTypes get_variadic_argument_types() const override {
        return get_variadic_argument_types_impl();
    }

    ColumnNumbers get_arguments_that_are_always_constant() const override { return {}; }

protected:
    // Get the result type by argument type. If the function does not apply to these arguments, throw an exception.
    // the get_return_type_impl and its overrides should only return the nested type if `use_default_implementation_for_nulls` is true.
    // whether to wrap in nullable type will be automatically decided.
    virtual DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) {
            data_types[i] = arguments[i].type;
        }
        return get_return_type_impl(data_types);
    }

    virtual DataTypePtr get_return_type_impl(const DataTypes& /*arguments*/) const {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "get_return_type is not implemented for {}", get_name());
        return nullptr;
    }

    /** If use_default_implementation_for_nulls() is true, than change arguments for get_return_type() and build_impl():
      *  if some of arguments are Nullable(Nothing) then don't call get_return_type(), call build_impl() with return_type = Nullable(Nothing),
      *  if some of arguments are Nullable, then:
      *   - Nullable types are substituted with nested types for get_return_type() function
      *   - WRAP get_return_type() RESULT IN NULLABLE type and pass to build_impl
      *
      * Otherwise build returns build_impl(arguments, get_return_type(arguments));
      */
    virtual bool use_default_implementation_for_nulls() const { return true; }

    virtual bool skip_return_type_check() const { return false; }

    virtual bool need_replace_null_data_to_default() const { return false; }

    /// return a real function object to execute. called in build(...).
    virtual FunctionBasePtr build_impl(const ColumnsWithTypeAndName& arguments,
                                       const DataTypePtr& return_type) const = 0;

    virtual DataTypes get_variadic_argument_types_impl() const { return {}; }

private:
    bool is_date_or_datetime_or_decimal(const DataTypePtr& return_type,
                                        const DataTypePtr& func_return_type) const;
    bool is_array_nested_type_date_or_datetime_or_decimal(
            const DataTypePtr& return_type, const DataTypePtr& func_return_type) const;
};

/// Previous function interface.
class IFunction : public std::enable_shared_from_this<IFunction>,
                  public FunctionBuilderImpl,
                  public IFunctionBase,
                  public PreparedFunctionImpl {
public:
    String get_name() const override = 0;

    /// Notice: We should not change the column in the block, because the column may be shared by multiple expressions or exec nodes.
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const override = 0;

    /// Override this functions to change default implementation behavior. See details in IMyFunction.
    bool use_default_implementation_for_nulls() const override { return true; }

    bool skip_return_type_check() const override { return false; }

    bool need_replace_null_data_to_default() const override { return false; }

    /// all constancy check should use this function to do automatically
    ColumnNumbers get_arguments_that_are_always_constant() const override { return {}; }

    bool is_use_default_implementation_for_constants() const override {
        return use_default_implementation_for_constants();
    }

    using PreparedFunctionImpl::execute;
    using PreparedFunctionImpl::execute_impl_dry_run;
    using FunctionBuilderImpl::get_return_type_impl;
    using FunctionBuilderImpl::get_variadic_argument_types_impl;
    using FunctionBuilderImpl::get_return_type;

    [[noreturn]] PreparedFunctionPtr prepare(FunctionContext* context,
                                             const Block& /*sample_block*/,
                                             const ColumnNumbers& /*arguments*/,
                                             uint32_t /*result*/) const final {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "prepare is not implemented for IFunction {}", get_name());
        __builtin_unreachable();
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return Status::OK();
    }

    [[noreturn]] const DataTypes& get_argument_types() const final {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "get_argument_types is not implemented for IFunction {}",
                               get_name());
        __builtin_unreachable();
    }

    [[noreturn]] const DataTypePtr& get_return_type() const final {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "get_return_type is not implemented for IFunction {}", get_name());
        __builtin_unreachable();
    }

protected:
    FunctionBasePtr build_impl(const ColumnsWithTypeAndName& /*arguments*/,
                               const DataTypePtr& /*return_type*/) const final {
        throw doris::Exception(ErrorCode::NOT_IMPLEMENTED_ERROR,
                               "build_impl is not implemented for IFunction {}", get_name());
        __builtin_unreachable();
        return {};
    }
};

/// Wrappers over IFunction. If we (default)use DefaultFunction as wrapper, all function execution will go through this.

class DefaultExecutable final : public PreparedFunctionImpl {
public:
    explicit DefaultExecutable(std::shared_ptr<IFunction> function_)
            : function(std::move(function_)) {}

    String get_name() const override { return function->get_name(); }

protected:
    Status execute_impl(FunctionContext* context, Block& block, const ColumnNumbers& arguments,
                        uint32_t result, size_t input_rows_count) const final {
        return function->execute_impl(context, block, arguments, result, input_rows_count);
    }

    Status evaluate_inverted_index(
            const ColumnsWithTypeAndName& arguments,
            const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
            std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
            segment_v2::InvertedIndexResultBitmap& bitmap_result) const {
        return function->evaluate_inverted_index(arguments, data_type_with_names, iterators,
                                                 num_rows, bitmap_result);
    }

    Status execute_impl_dry_run(FunctionContext* context, Block& block,
                                const ColumnNumbers& arguments, uint32_t result,
                                size_t input_rows_count) const final {
        return function->execute_impl_dry_run(context, block, arguments, result, input_rows_count);
    }
    bool use_default_implementation_for_nulls() const final {
        return function->use_default_implementation_for_nulls();
    }

    bool skip_return_type_check() const final { return function->skip_return_type_check(); }
    bool need_replace_null_data_to_default() const final {
        return function->need_replace_null_data_to_default();
    }
    bool use_default_implementation_for_constants() const final {
        return function->use_default_implementation_for_constants();
    }
    ColumnNumbers get_arguments_that_are_always_constant() const final {
        return function->get_arguments_that_are_always_constant();
    }

private:
    std::shared_ptr<IFunction> function;
};

/*
 * when we register a function which didn't specify its base(i.e. inherited from IFunction), actually we use this as a wrapper.
 * it saves real implementation as `function`. 
*/
class DefaultFunction final : public IFunctionBase {
public:
    DefaultFunction(std::shared_ptr<IFunction> function_, DataTypes arguments_,
                    DataTypePtr return_type_)
            : function(std::move(function_)),
              arguments(std::move(arguments_)),
              return_type(std::move(return_type_)) {}

    String get_name() const override { return function->get_name(); }

    const DataTypes& get_argument_types() const override { return arguments; }
    const DataTypePtr& get_return_type() const override { return return_type; }

    // return a default wrapper for IFunction.
    PreparedFunctionPtr prepare(FunctionContext* context, const Block& /*sample_block*/,
                                const ColumnNumbers& /*arguments*/,
                                uint32_t /*result*/) const override {
        return std::make_shared<DefaultExecutable>(function);
    }

    Status open(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return function->open(context, scope);
    }

    Status close(FunctionContext* context, FunctionContext::FunctionStateScope scope) override {
        return function->close(context, scope);
    }

    Status evaluate_inverted_index(
            const ColumnsWithTypeAndName& args,
            const std::vector<vectorized::IndexFieldNameAndTypePair>& data_type_with_names,
            std::vector<segment_v2::IndexIterator*> iterators, uint32_t num_rows,
            segment_v2::InvertedIndexResultBitmap& bitmap_result) const override {
        return function->evaluate_inverted_index(args, data_type_with_names, iterators, num_rows,
                                                 bitmap_result);
    }

    bool is_use_default_implementation_for_constants() const override {
        return function->is_use_default_implementation_for_constants();
    }

    bool can_push_down_to_index() const override { return function->can_push_down_to_index(); }

private:
    std::shared_ptr<IFunction> function;
    DataTypes arguments;
    DataTypePtr return_type;
};

class DefaultFunctionBuilder : public FunctionBuilderImpl {
public:
    explicit DefaultFunctionBuilder(std::shared_ptr<IFunction> function_)
            : function(std::move(function_)) {}

    void check_number_of_arguments(size_t number_of_arguments) const override {
        return function->check_number_of_arguments(number_of_arguments);
    }

    String get_name() const override { return function->get_name(); }
    bool is_variadic() const override { return function->is_variadic(); }
    size_t get_number_of_arguments() const override { return function->get_number_of_arguments(); }

    ColumnNumbers get_arguments_that_are_always_constant() const override {
        return function->get_arguments_that_are_always_constant();
    }

protected:
    DataTypePtr get_return_type_impl(const DataTypes& arguments) const override {
        return function->get_return_type_impl(arguments);
    }
    DataTypePtr get_return_type_impl(const ColumnsWithTypeAndName& arguments) const override {
        return function->get_return_type_impl(arguments);
    }

    bool use_default_implementation_for_nulls() const override {
        return function->use_default_implementation_for_nulls();
    }

    bool skip_return_type_check() const override { return function->skip_return_type_check(); }

    bool need_replace_null_data_to_default() const override {
        return function->need_replace_null_data_to_default();
    }

    FunctionBasePtr build_impl(const ColumnsWithTypeAndName& arguments,
                               const DataTypePtr& return_type) const override {
        DataTypes data_types(arguments.size());
        for (size_t i = 0; i < arguments.size(); ++i) {
            data_types[i] = arguments[i].type;
        }
        return std::make_shared<DefaultFunction>(function, data_types, return_type);
    }

    DataTypes get_variadic_argument_types_impl() const override {
        return function->get_variadic_argument_types_impl();
    }

private:
    std::shared_ptr<IFunction> function;
};

using FunctionPtr = std::shared_ptr<IFunction>;

/** Return ColumnNullable of src, with null map as OR-ed null maps of args columns in blocks.
  * Or ColumnConst(ColumnNullable) if the result is always NULL or if the result is constant and always not NULL.
  */
ColumnPtr wrap_in_nullable(const ColumnPtr& src, const Block& block, const ColumnNumbers& args,
                           uint32_t result, size_t input_rows_count);

} // namespace doris::vectorized
