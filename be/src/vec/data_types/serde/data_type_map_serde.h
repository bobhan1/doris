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

#pragma once

#include <glog/logging.h>
#include <stdint.h>

#include <ostream>

#include "common/status.h"
#include "data_type_serde.h"

namespace doris {
class PValues;

namespace vectorized {
class IColumn;
class Arena;
#include "common/compile_check_begin.h"

class DataTypeMapSerDe : public DataTypeSerDe {
public:
    DataTypeMapSerDe(const DataTypeSerDeSPtr& _key_serde, const DataTypeSerDeSPtr& _value_serde,
                     int nesting_level = 1)
            : DataTypeSerDe(nesting_level), key_serde(_key_serde), value_serde(_value_serde) {}

    std::string get_name() const override {
        return "Map(" + key_serde->get_name() + ", " + value_serde->get_name() + ")";
    }

    Status serialize_one_cell_to_json(const IColumn& column, int64_t row_num, BufferWritable& bw,
                                      FormatOptions& options) const override;
    Status serialize_column_to_json(const IColumn& column, int64_t start_idx, int64_t end_idx,
                                    BufferWritable& bw, FormatOptions& options) const override;
    Status deserialize_one_cell_from_json(IColumn& column, Slice& slice,
                                          const FormatOptions& options) const override;

    Status deserialize_column_from_json_vector(IColumn& column, std::vector<Slice>& slices,
                                               uint64_t* num_deserialized,
                                               const FormatOptions& options) const override;

    Status deserialize_one_cell_from_hive_text(
            IColumn& column, Slice& slice, const FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;

    Status deserialize_column_from_hive_text_vector(
            IColumn& column, std::vector<Slice>& slices, uint64_t* num_deserialized,
            const FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;

    Status serialize_one_cell_to_hive_text(
            const IColumn& column, int64_t row_num, BufferWritable& bw, FormatOptions& options,
            int hive_text_complex_type_delimiter_level = 1) const override;

    Status write_column_to_pb(const IColumn& column, PValues& result, int64_t start,
                              int64_t end) const override;
    Status read_column_from_pb(IColumn& column, const PValues& arg) const override;
    void write_one_cell_to_jsonb(const IColumn& column, JsonbWriter& result, Arena& mem_pool,
                                 int32_t col_id, int64_t row_num) const override;

    void read_one_cell_from_jsonb(IColumn& column, const JsonbValue* arg) const override;
    Status write_column_to_arrow(const IColumn& column, const NullMap* null_map,
                                 arrow::ArrayBuilder* array_builder, int64_t start, int64_t end,
                                 const cctz::time_zone& ctz) const override;
    Status read_column_from_arrow(IColumn& column, const arrow::Array* arrow_array, int64_t start,
                                  int64_t end, const cctz::time_zone& ctz) const override;

    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<true>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;
    Status write_column_to_mysql(const IColumn& column, MysqlRowBuffer<false>& row_buffer,
                                 int64_t row_idx, bool col_const,
                                 const FormatOptions& options) const override;

    Status write_column_to_orc(const std::string& timezone, const IColumn& column,
                               const NullMap* null_map, orc::ColumnVectorBatch* orc_col_batch,
                               int64_t start, int64_t end, vectorized::Arena& arena) const override;

    void set_return_object_as_string(bool value) override {
        DataTypeSerDe::set_return_object_as_string(value);
        key_serde->set_return_object_as_string(value);
        value_serde->set_return_object_as_string(value);
    }

    virtual DataTypeSerDeSPtrs get_nested_serdes() const override {
        return {key_serde, value_serde};
    }

private:
    template <bool is_binary_format>
    Status _write_column_to_mysql(const IColumn& column, MysqlRowBuffer<is_binary_format>& result,
                                  int64_t row_idx, bool col_const,
                                  const FormatOptions& options) const;

    DataTypeSerDeSPtr key_serde;
    DataTypeSerDeSPtr value_serde;
};
#include "common/compile_check_end.h"
} // namespace vectorized
} // namespace doris
