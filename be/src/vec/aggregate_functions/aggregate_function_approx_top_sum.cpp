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

#include "vec/aggregate_functions/aggregate_function_approx_top_sum.h"

#include "common/exception.h"
#include "vec/aggregate_functions/aggregate_function_simple_factory.h"
#include "vec/aggregate_functions/helpers.h"
#include "vec/data_types/data_type.h"

namespace doris::vectorized {
#include "common/compile_check_begin.h"

template <size_t N>
AggregateFunctionPtr create_aggregate_function_multi_top_sum_impl(
        const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    if (N == argument_types.size() - 3) {
        return creator_with_integer_type_with_index<N>::template create<
                AggregateFunctionApproxTopSumSimple>(argument_types, result_is_nullable, attr,
                                                     attr.column_names);
    } else {
        return create_aggregate_function_multi_top_sum_impl<N - 1>(argument_types,
                                                                   result_is_nullable, attr);
    }
}

template <>
AggregateFunctionPtr create_aggregate_function_multi_top_sum_impl<0>(
        const DataTypes& argument_types, const bool result_is_nullable,
        const AggregateFunctionAttr& attr) {
    return creator_with_integer_type::create<AggregateFunctionApproxTopSumSimple>(
            argument_types, result_is_nullable, attr, attr.column_names);
}

AggregateFunctionPtr create_aggregate_function_approx_top_sum(const std::string& name,
                                                              const DataTypes& argument_types,
                                                              const bool result_is_nullable,
                                                              const AggregateFunctionAttr& attr) {
    if (argument_types.size() < 3) {
        return nullptr;
    }

    constexpr size_t max_param_value = 10;
    if (argument_types.size() > max_param_value) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "Argument types size exceeds the supported limit.");
    }

    return create_aggregate_function_multi_top_sum_impl<max_param_value>(argument_types,
                                                                         result_is_nullable, attr);
}

void register_aggregate_function_approx_top_sum(AggregateFunctionSimpleFactory& factory) {
    factory.register_function_both("approx_top_sum", create_aggregate_function_approx_top_sum);
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized