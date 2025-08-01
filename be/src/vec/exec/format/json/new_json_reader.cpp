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

#include "vec/exec/format/json/new_json_reader.h"

#include <fmt/format.h>
#include <gen_cpp/Metrics_types.h>
#include <gen_cpp/PlanNodes_types.h>
#include <gen_cpp/Types_types.h>
#include <glog/logging.h>
#include <rapidjson/error/en.h>
#include <rapidjson/reader.h>
#include <rapidjson/stringbuffer.h>
#include <rapidjson/writer.h>
#include <simdjson/simdjson.h> // IWYU pragma: keep

#include <algorithm>
#include <cinttypes>
#include <cstdio>
#include <cstring>
#include <map>
#include <memory>
#include <string_view>
#include <utility>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/status.h"
#include "exprs/json_functions.h"
#include "io/file_factory.h"
#include "io/fs/buffered_reader.h"
#include "io/fs/file_reader.h"
#include "io/fs/stream_load_pipe.h"
#include "io/fs/tracing_file_reader.h"
#include "runtime/define_primitive_type.h"
#include "runtime/descriptors.h"
#include "runtime/runtime_state.h"
#include "runtime/types.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_array.h"
#include "vec/columns/column_map.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_string.h"
#include "vec/columns/column_struct.h"
#include "vec/common/assert_cast.h"
#include "vec/core/column_with_type_and_name.h"
#include "vec/data_types/data_type_array.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_map.h"
#include "vec/data_types/data_type_number.h" // IWYU pragma: keep
#include "vec/data_types/data_type_struct.h"
#include "vec/exec/format/file_reader/new_plain_text_line_reader.h"
#include "vec/exec/scan/scanner.h"

namespace doris::io {
struct IOContext;
enum class FileCachePolicy : uint8_t;
} // namespace doris::io

namespace doris::vectorized {
#include "common/compile_check_begin.h"
using namespace ErrorCode;

NewJsonReader::NewJsonReader(RuntimeState* state, RuntimeProfile* profile, ScannerCounter* counter,
                             const TFileScanRangeParams& params, const TFileRangeDesc& range,
                             const std::vector<SlotDescriptor*>& file_slot_descs, bool* scanner_eof,
                             io::IOContext* io_ctx)
        : _vhandle_json_callback(nullptr),
          _state(state),
          _profile(profile),
          _counter(counter),
          _params(params),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _file_reader(nullptr),
          _line_reader(nullptr),
          _reader_eof(false),
          _decompressor(nullptr),
          _skip_first_line(false),
          _next_row(0),
          _total_rows(0),
          _value_allocator(_value_buffer, sizeof(_value_buffer)),
          _parse_allocator(_parse_buffer, sizeof(_parse_buffer)),
          _origin_json_doc(&_value_allocator, sizeof(_parse_buffer), &_parse_allocator),
          _scanner_eof(scanner_eof),
          _current_offset(0),
          _io_ctx(io_ctx) {
    _read_timer = ADD_TIMER(_profile, "ReadTime");
    if (_range.__isset.compress_type) {
        // for compatibility
        _file_compress_type = _range.compress_type;
    } else {
        _file_compress_type = _params.compress_type;
    }
    _init_system_properties();
    _init_file_description();
}

NewJsonReader::NewJsonReader(RuntimeProfile* profile, const TFileScanRangeParams& params,
                             const TFileRangeDesc& range,
                             const std::vector<SlotDescriptor*>& file_slot_descs,
                             io::IOContext* io_ctx)
        : _vhandle_json_callback(nullptr),
          _state(nullptr),
          _profile(profile),
          _params(params),
          _range(range),
          _file_slot_descs(file_slot_descs),
          _line_reader(nullptr),
          _reader_eof(false),
          _decompressor(nullptr),
          _skip_first_line(false),
          _next_row(0),
          _total_rows(0),
          _value_allocator(_value_buffer, sizeof(_value_buffer)),
          _parse_allocator(_parse_buffer, sizeof(_parse_buffer)),
          _origin_json_doc(&_value_allocator, sizeof(_parse_buffer), &_parse_allocator),
          _io_ctx(io_ctx) {
    if (_range.__isset.compress_type) {
        // for compatibility
        _file_compress_type = _range.compress_type;
    } else {
        _file_compress_type = _params.compress_type;
    }
    _init_system_properties();
    _init_file_description();
}

void NewJsonReader::_init_system_properties() {
    if (_range.__isset.file_type) {
        // for compatibility
        _system_properties.system_type = _range.file_type;
    } else {
        _system_properties.system_type = _params.file_type;
    }
    _system_properties.properties = _params.properties;
    _system_properties.hdfs_params = _params.hdfs_params;
    if (_params.__isset.broker_addresses) {
        _system_properties.broker_addresses.assign(_params.broker_addresses.begin(),
                                                   _params.broker_addresses.end());
    }
}

void NewJsonReader::_init_file_description() {
    _file_description.path = _range.path;
    _file_description.file_size = _range.__isset.file_size ? _range.file_size : -1;

    if (_range.__isset.fs_name) {
        _file_description.fs_name = _range.fs_name;
    }
}

Status NewJsonReader::init_reader(
        const std::unordered_map<std::string, VExprContextSPtr>& col_default_value_ctx,
        bool is_load) {
    _is_load = is_load;

    // generate _col_default_value_map
    RETURN_IF_ERROR(_get_column_default_value(_file_slot_descs, col_default_value_ctx));

    //use serde insert data to column.
    for (auto* slot_desc : _file_slot_descs) {
        _serdes.emplace_back(slot_desc->get_data_type_ptr()->get_serde());
    }

    // create decompressor.
    // _decompressor may be nullptr if this is not a compressed file
    RETURN_IF_ERROR(Decompressor::create_decompressor(_file_compress_type, &_decompressor));

    RETURN_IF_ERROR(_simdjson_init_reader());
    return Status::OK();
}

Status NewJsonReader::get_next_block(Block* block, size_t* read_rows, bool* eof) {
    if (_reader_eof) {
        *eof = true;
        return Status::OK();
    }

    const int batch_size = std::max(_state->batch_size(), (int)_MIN_BATCH_SIZE);

    while (block->rows() < batch_size && !_reader_eof) {
        if (UNLIKELY(_read_json_by_line && _skip_first_line)) {
            size_t size = 0;
            const uint8_t* line_ptr = nullptr;
            RETURN_IF_ERROR(_line_reader->read_line(&line_ptr, &size, &_reader_eof, _io_ctx));
            _skip_first_line = false;
            continue;
        }

        bool is_empty_row = false;

        RETURN_IF_ERROR(
                _read_json_column(_state, *block, _file_slot_descs, &is_empty_row, &_reader_eof));
        if (is_empty_row) {
            // Read empty row, just continue
            continue;
        }
        ++(*read_rows);
    }

    return Status::OK();
}

Status NewJsonReader::get_columns(std::unordered_map<std::string, DataTypePtr>* name_to_type,
                                  std::unordered_set<std::string>* missing_cols) {
    for (const auto& slot : _file_slot_descs) {
        name_to_type->emplace(slot->col_name(), slot->type());
    }
    return Status::OK();
}

// init decompressor, file reader and line reader for parsing schema
Status NewJsonReader::init_schema_reader() {
    RETURN_IF_ERROR(_get_range_params());
    // create decompressor.
    // _decompressor may be nullptr if this is not a compressed file
    RETURN_IF_ERROR(Decompressor::create_decompressor(_file_compress_type, &_decompressor));
    RETURN_IF_ERROR(_open_file_reader(true));
    if (_read_json_by_line) {
        RETURN_IF_ERROR(_open_line_reader());
    }
    // generate _parsed_jsonpaths and _parsed_json_root
    RETURN_IF_ERROR(_parse_jsonpath_and_json_root());
    return Status::OK();
}

Status NewJsonReader::get_parsed_schema(std::vector<std::string>* col_names,
                                        std::vector<DataTypePtr>* col_types) {
    bool eof = false;
    const uint8_t* json_str = nullptr;
    std::unique_ptr<uint8_t[]> json_str_ptr;
    size_t size = 0;
    if (_line_reader != nullptr) {
        RETURN_IF_ERROR(_line_reader->read_line(&json_str, &size, &eof, _io_ctx));
    } else {
        size_t read_size = 0;
        RETURN_IF_ERROR(_read_one_message(&json_str_ptr, &read_size));
        json_str = json_str_ptr.get();
        size = read_size;
        if (read_size == 0) {
            eof = true;
        }
    }

    if (size == 0 || eof) {
        return Status::EndOfFile("Empty file.");
    }

    // clear memory here.
    _value_allocator.Clear();
    _parse_allocator.Clear();
    bool has_parse_error = false;

    // parse jsondata to JsonDoc
    // As the issue: https://github.com/Tencent/rapidjson/issues/1458
    // Now, rapidjson only support uint64_t, So lagreint load cause bug. We use kParseNumbersAsStringsFlag.
    if (_num_as_string) {
        has_parse_error =
                _origin_json_doc.Parse<rapidjson::kParseNumbersAsStringsFlag>((char*)json_str, size)
                        .HasParseError();
    } else {
        has_parse_error = _origin_json_doc.Parse((char*)json_str, size).HasParseError();
    }

    if (has_parse_error) {
        return Status::DataQualityError(
                "Parse json data for JsonDoc failed. code: {}, error info: {}",
                _origin_json_doc.GetParseError(),
                rapidjson::GetParseError_En(_origin_json_doc.GetParseError()));
    }

    // set json root
    if (!_parsed_json_root.empty()) {
        _json_doc = JsonFunctions::get_json_object_from_parsed_json(
                _parsed_json_root, &_origin_json_doc, _origin_json_doc.GetAllocator());
        if (_json_doc == nullptr) {
            return Status::DataQualityError("JSON Root not found.");
        }
    } else {
        _json_doc = &_origin_json_doc;
    }

    if (_json_doc->IsArray() && !_strip_outer_array) {
        return Status::DataQualityError(
                "JSON data is array-object, `strip_outer_array` must be TRUE.");
    }
    if (!_json_doc->IsArray() && _strip_outer_array) {
        return Status::DataQualityError(
                "JSON data is not an array-object, `strip_outer_array` must be FALSE.");
    }

    rapidjson::Value* objectValue = nullptr;
    if (_json_doc->IsArray()) {
        if (_json_doc->Size() == 0) {
            // may be passing an empty json, such as "[]"
            return Status::InternalError<false>("Empty first json line");
        }
        objectValue = &(*_json_doc)[0];
    } else {
        objectValue = _json_doc;
    }

    if (!objectValue->IsObject()) {
        return Status::DataQualityError("JSON data is not an object. but: {}",
                                        objectValue->GetType());
    }

    // use jsonpaths to col_names
    if (!_parsed_jsonpaths.empty()) {
        for (auto& _parsed_jsonpath : _parsed_jsonpaths) {
            size_t len = _parsed_jsonpath.size();
            if (len == 0) {
                return Status::InvalidArgument("It's invalid jsonpaths.");
            }
            std::string key = _parsed_jsonpath[len - 1].key;
            col_names->emplace_back(key);
            col_types->emplace_back(
                    DataTypeFactory::instance().create_data_type(PrimitiveType::TYPE_STRING, true));
        }
        return Status::OK();
    }

    for (int i = 0; i < objectValue->MemberCount(); ++i) {
        auto it = objectValue->MemberBegin() + i;
        col_names->emplace_back(it->name.GetString());
        col_types->emplace_back(make_nullable(std::make_shared<vectorized::DataTypeString>()));
    }
    return Status::OK();
}

Status NewJsonReader::_get_range_params() {
    if (!_params.__isset.file_attributes) {
        return Status::InternalError<false>("BE cat get file_attributes");
    }

    // get line_delimiter
    if (_params.file_attributes.__isset.text_params &&
        _params.file_attributes.text_params.__isset.line_delimiter) {
        _line_delimiter = _params.file_attributes.text_params.line_delimiter;
        _line_delimiter_length = _line_delimiter.size();
    }

    if (_params.file_attributes.__isset.jsonpaths) {
        _jsonpaths = _params.file_attributes.jsonpaths;
    }
    if (_params.file_attributes.__isset.json_root) {
        _json_root = _params.file_attributes.json_root;
    }
    if (_params.file_attributes.__isset.read_json_by_line) {
        _read_json_by_line = _params.file_attributes.read_json_by_line;
    }
    if (_params.file_attributes.__isset.strip_outer_array) {
        _strip_outer_array = _params.file_attributes.strip_outer_array;
    }
    if (_params.file_attributes.__isset.num_as_string) {
        _num_as_string = _params.file_attributes.num_as_string;
    }
    if (_params.file_attributes.__isset.fuzzy_parse) {
        _fuzzy_parse = _params.file_attributes.fuzzy_parse;
    }
    if (_range.table_format_params.table_format_type == "hive") {
        _is_hive_table = true;
    }
    if (_params.file_attributes.__isset.openx_json_ignore_malformed) {
        _openx_json_ignore_malformed = _params.file_attributes.openx_json_ignore_malformed;
    }
    return Status::OK();
}

static Status ignore_malformed_json_append_null(Block& block) {
    for (auto& column : block.get_columns()) {
        if (!column->is_nullable()) [[unlikely]] {
            return Status::DataQualityError("malformed json, but the column `{}` is not nullable.",
                                            column->get_name());
        }
        static_cast<ColumnNullable*>(column->assume_mutable().get())->insert_default();
    }
    return Status::OK();
}

Status NewJsonReader::_open_file_reader(bool need_schema) {
    int64_t start_offset = _range.start_offset;
    if (start_offset != 0) {
        start_offset -= 1;
    }

    _current_offset = start_offset;

    if (_params.file_type == TFileType::FILE_STREAM) {
        // Due to http_stream needs to pre read a portion of the data to parse column information, so it is set to true here
        RETURN_IF_ERROR(FileFactory::create_pipe_reader(_range.load_id, &_file_reader, _state,
                                                        need_schema));
    } else {
        _file_description.mtime = _range.__isset.modification_time ? _range.modification_time : 0;
        io::FileReaderOptions reader_options =
                FileFactory::get_reader_options(_state, _file_description);
        auto file_reader = DORIS_TRY(io::DelegateReader::create_file_reader(
                _profile, _system_properties, _file_description, reader_options,
                io::DelegateReader::AccessMode::SEQUENTIAL, _io_ctx,
                io::PrefetchRange(_range.start_offset, _range.size)));
        _file_reader = _io_ctx ? std::make_shared<io::TracingFileReader>(std::move(file_reader),
                                                                         _io_ctx->file_reader_stats)
                               : file_reader;
    }
    return Status::OK();
}

Status NewJsonReader::_open_line_reader() {
    int64_t size = _range.size;
    if (_range.start_offset != 0) {
        // When we fetch range doesn't start from 0, size will += 1.
        size += 1;
        _skip_first_line = true;
    } else {
        _skip_first_line = false;
    }
    _line_reader = NewPlainTextLineReader::create_unique(
            _profile, _file_reader, _decompressor.get(),
            std::make_shared<PlainTextLineReaderCtx>(_line_delimiter, _line_delimiter_length,
                                                     false),
            size, _current_offset);
    return Status::OK();
}

Status NewJsonReader::_parse_jsonpath_and_json_root() {
    // parse jsonpaths
    if (!_jsonpaths.empty()) {
        rapidjson::Document jsonpaths_doc;
        if (!jsonpaths_doc.Parse(_jsonpaths.c_str(), _jsonpaths.length()).HasParseError()) {
            if (!jsonpaths_doc.IsArray()) {
                return Status::InvalidJsonPath("Invalid json path: {}", _jsonpaths);
            }
            for (int i = 0; i < jsonpaths_doc.Size(); i++) {
                const rapidjson::Value& path = jsonpaths_doc[i];
                if (!path.IsString()) {
                    return Status::InvalidJsonPath("Invalid json path: {}", _jsonpaths);
                }
                std::vector<JsonPath> parsed_paths;
                JsonFunctions::parse_json_paths(path.GetString(), &parsed_paths);
                _parsed_jsonpaths.push_back(std::move(parsed_paths));
            }

        } else {
            return Status::InvalidJsonPath("Invalid json path: {}", _jsonpaths);
        }
    }

    // parse jsonroot
    if (!_json_root.empty()) {
        JsonFunctions::parse_json_paths(_json_root, &_parsed_json_root);
    }
    return Status::OK();
}

Status NewJsonReader::_read_json_column(RuntimeState* state, Block& block,
                                        const std::vector<SlotDescriptor*>& slot_descs,
                                        bool* is_empty_row, bool* eof) {
    return (this->*_vhandle_json_callback)(state, block, slot_descs, is_empty_row, eof);
}

Status NewJsonReader::_read_one_message(std::unique_ptr<uint8_t[]>* file_buf, size_t* read_size) {
    switch (_params.file_type) {
    case TFileType::FILE_LOCAL:
        [[fallthrough]];
    case TFileType::FILE_HDFS:
        [[fallthrough]];
    case TFileType::FILE_S3: {
        size_t file_size = _file_reader->size();
        file_buf->reset(new uint8_t[file_size]);
        Slice result(file_buf->get(), file_size);
        RETURN_IF_ERROR(_file_reader->read_at(_current_offset, result, read_size, _io_ctx));
        _current_offset += *read_size;
        break;
    }
    case TFileType::FILE_STREAM: {
        RETURN_IF_ERROR(_read_one_message_from_pipe(file_buf, read_size));
        break;
    }
    default: {
        return Status::NotSupported<false>("no supported file reader type: {}", _params.file_type);
    }
    }
    return Status::OK();
}

Status NewJsonReader::_read_one_message_from_pipe(std::unique_ptr<uint8_t[]>* file_buf,
                                                  size_t* read_size) {
    auto* stream_load_pipe = dynamic_cast<io::StreamLoadPipe*>(_file_reader.get());

    // first read: read from the pipe once.
    RETURN_IF_ERROR(stream_load_pipe->read_one_message(file_buf, read_size));

    // When the file is not chunked, the entire file has already been read.
    if (!stream_load_pipe->is_chunked_transfer()) {
        return Status::OK();
    }

    std::vector<uint8_t> buf;
    uint64_t cur_size = 0;

    // second read: continuously read data from the pipe until all data is read.
    std::unique_ptr<uint8_t[]> read_buf;
    size_t read_buf_size = 0;
    while (true) {
        RETURN_IF_ERROR(stream_load_pipe->read_one_message(&read_buf, &read_buf_size));
        if (read_buf_size == 0) {
            break;
        } else {
            buf.insert(buf.end(), read_buf.get(), read_buf.get() + read_buf_size);
            cur_size += read_buf_size;
            read_buf_size = 0;
            read_buf.reset();
        }
    }

    // No data is available during the second read.
    if (cur_size == 0) {
        return Status::OK();
    }

    std::unique_ptr<uint8_t[]> total_buf = std::make_unique<uint8_t[]>(cur_size + *read_size);

    // copy the data during the first read
    memcpy(total_buf.get(), file_buf->get(), *read_size);

    // copy the data during the second read
    memcpy(total_buf.get() + *read_size, buf.data(), cur_size);
    *file_buf = std::move(total_buf);
    *read_size += cur_size;
    return Status::OK();
}

// ---------SIMDJSON----------
// simdjson, replace none simdjson function if it is ready
Status NewJsonReader::_simdjson_init_reader() {
    RETURN_IF_ERROR(_get_range_params());

    RETURN_IF_ERROR(_open_file_reader(false));
    if (_read_json_by_line) {
        RETURN_IF_ERROR(_open_line_reader());
    }

    // generate _parsed_jsonpaths and _parsed_json_root
    RETURN_IF_ERROR(_parse_jsonpath_and_json_root());

    //improve performance
    if (_parsed_jsonpaths.empty()) { // input is a simple json-string
        _vhandle_json_callback = &NewJsonReader::_simdjson_handle_simple_json;
    } else { // input is a complex json-string and a json-path
        if (_strip_outer_array) {
            _vhandle_json_callback = &NewJsonReader::_simdjson_handle_flat_array_complex_json;
        } else {
            _vhandle_json_callback = &NewJsonReader::_simdjson_handle_nested_complex_json;
        }
    }
    _ondemand_json_parser = std::make_unique<simdjson::ondemand::parser>();
    for (int i = 0; i < _file_slot_descs.size(); ++i) {
        _slot_desc_index[StringRef {_file_slot_descs[i]->col_name()}] = i;
        if (_file_slot_descs[i]->is_skip_bitmap_col()) {
            skip_bitmap_col_idx = i;
        }
    }
    _simdjson_ondemand_padding_buffer.resize(_padded_size);
    _simdjson_ondemand_unscape_padding_buffer.resize(_padded_size);
    return Status::OK();
}

Status NewJsonReader::_handle_simdjson_error(simdjson::simdjson_error& error, Block& block,
                                             size_t num_rows, bool* eof) {
    fmt::memory_buffer error_msg;
    fmt::format_to(error_msg, "Parse json data failed. code: {}, error info: {}", error.error(),
                   error.what());
    _counter->num_rows_filtered++;
    // Before continuing to process other rows, we need to first clean the fail parsed row.
    for (int i = 0; i < block.columns(); ++i) {
        auto column = block.get_by_position(i).column->assume_mutable();
        if (column->size() > num_rows) {
            column->pop_back(column->size() - num_rows);
        }
    }

    RETURN_IF_ERROR(_state->append_error_msg_to_file(
            [&]() -> std::string {
                return std::string(_simdjson_ondemand_padding_buffer.data(), _original_doc_size);
            },
            [&]() -> std::string { return fmt::to_string(error_msg); }));
    return Status::OK();
}

Status NewJsonReader::_simdjson_handle_simple_json(RuntimeState* /*state*/, Block& block,
                                                   const std::vector<SlotDescriptor*>& slot_descs,
                                                   bool* is_empty_row, bool* eof) {
    // simple json
    size_t size = 0;
    simdjson::error_code error;
    size_t num_rows = block.rows();
    try {
        // step1: get and parse buf to get json doc
        RETURN_IF_ERROR(_simdjson_parse_json(&size, is_empty_row, eof, &error));
        if (size == 0 || *eof) {
            *is_empty_row = true;
            return Status::OK();
        }

        // step2: get json value by json doc
        Status st = _get_json_value(&size, eof, &error, is_empty_row);
        if (st.is<DATA_QUALITY_ERROR>()) {
            if (_is_load) {
                return Status::OK();
            } else if (_openx_json_ignore_malformed) {
                RETURN_IF_ERROR(ignore_malformed_json_append_null(block));
                return Status::OK();
            }
        }

        RETURN_IF_ERROR(st);
        if (*is_empty_row || *eof) {
            return Status::OK();
        }

        // step 3: write columns by json value
        RETURN_IF_ERROR(
                _simdjson_handle_simple_json_write_columns(block, slot_descs, is_empty_row, eof));
    } catch (simdjson::simdjson_error& e) {
        RETURN_IF_ERROR(_handle_simdjson_error(e, block, num_rows, eof));
        if (*_scanner_eof) {
            // When _scanner_eof is true and valid is false, it means that we have encountered
            // unqualified data and decided to stop the scan.
            *is_empty_row = true;
            return Status::OK();
        }
    }

    return Status::OK();
}

Status NewJsonReader::_simdjson_handle_simple_json_write_columns(
        Block& block, const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
        bool* eof) {
    simdjson::ondemand::object objectValue;
    size_t num_rows = block.rows();
    bool valid = false;
    try {
        if (_json_value.type() == simdjson::ondemand::json_type::array) {
            _array = _json_value.get_array();
            if (_array.count_elements() == 0) {
                // may be passing an empty json, such as "[]"
                RETURN_IF_ERROR(_append_error_msg(nullptr, "Empty json line", "", nullptr));
                if (*_scanner_eof) {
                    *is_empty_row = true;
                    return Status::OK();
                }
                return Status::OK();
            }

            _array_iter = _array.begin();
            while (true) {
                objectValue = *_array_iter;
                RETURN_IF_ERROR(
                        _simdjson_set_column_value(&objectValue, block, slot_descs, &valid));
                if (!valid) {
                    if (*_scanner_eof) {
                        // When _scanner_eof is true and valid is false, it means that we have encountered
                        // unqualified data and decided to stop the scan.
                        *is_empty_row = true;
                        return Status::OK();
                    }
                }
                ++_array_iter;
                if (_array_iter == _array.end()) {
                    // Hint to read next json doc
                    break;
                }
            }
        } else {
            objectValue = _json_value;
            RETURN_IF_ERROR(_simdjson_set_column_value(&objectValue, block, slot_descs, &valid));
            if (!valid) {
                if (*_scanner_eof) {
                    *is_empty_row = true;
                    return Status::OK();
                }
            }
            *is_empty_row = false;
        }
    } catch (simdjson::simdjson_error& e) {
        RETURN_IF_ERROR(_handle_simdjson_error(e, block, num_rows, eof));
        if (!valid) {
            if (*_scanner_eof) {
                *is_empty_row = true;
                return Status::OK();
            }
        }
    }
    return Status::OK();
}

Status NewJsonReader::_simdjson_handle_flat_array_complex_json(
        RuntimeState* /*state*/, Block& block, const std::vector<SlotDescriptor*>& slot_descs,
        bool* is_empty_row, bool* eof) {
    // array complex json
    size_t size = 0;
    simdjson::error_code error;
    size_t num_rows = block.rows();
    try {
        // step1: get and parse buf to get json doc
        RETURN_IF_ERROR(_simdjson_parse_json(&size, is_empty_row, eof, &error));
        if (size == 0 || *eof) {
            *is_empty_row = true;
            return Status::OK();
        }

        // step2: get json value by json doc
        Status st = _get_json_value(&size, eof, &error, is_empty_row);
        if (st.is<DATA_QUALITY_ERROR>()) {
            return Status::OK();
        }
        RETURN_IF_ERROR(st);
        if (*is_empty_row) {
            return Status::OK();
        }

        // step 3: write columns by json value
        RETURN_IF_ERROR(_simdjson_handle_flat_array_complex_json_write_columns(block, slot_descs,
                                                                               is_empty_row, eof));
    } catch (simdjson::simdjson_error& e) {
        RETURN_IF_ERROR(_handle_simdjson_error(e, block, num_rows, eof));
        if (*_scanner_eof) {
            // When _scanner_eof is true and valid is false, it means that we have encountered
            // unqualified data and decided to stop the scan.
            *is_empty_row = true;
            return Status::OK();
        }
    }

    return Status::OK();
}

Status NewJsonReader::_simdjson_handle_flat_array_complex_json_write_columns(
        Block& block, const std::vector<SlotDescriptor*>& slot_descs, bool* is_empty_row,
        bool* eof) {
// Advance one row in array list, if it is the endpoint, stop advance and break the loop
#define ADVANCE_ROW()                  \
    ++_array_iter;                     \
    if (_array_iter == _array.end()) { \
        break;                         \
    }

    simdjson::ondemand::object cur;
    size_t num_rows = block.rows();
    try {
        bool valid = true;
        _array = _json_value.get_array();
        _array_iter = _array.begin();

        while (true) {
            cur = (*_array_iter).get_object();
            // extract root
            if (!_parsed_from_json_root && !_parsed_json_root.empty()) {
                simdjson::ondemand::value val;
                Status st = JsonFunctions::extract_from_object(cur, _parsed_json_root, &val);
                if (UNLIKELY(!st.ok())) {
                    if (st.is<NOT_FOUND>()) {
                        RETURN_IF_ERROR(_append_error_msg(nullptr, st.to_string(), "", nullptr));
                        ADVANCE_ROW();
                        continue;
                    }
                    return st;
                }
                if (val.type() != simdjson::ondemand::json_type::object) {
                    RETURN_IF_ERROR(_append_error_msg(nullptr, "Not object item", "", nullptr));
                    ADVANCE_ROW();
                    continue;
                }
                cur = val.get_object();
            }
            RETURN_IF_ERROR(_simdjson_write_columns_by_jsonpath(&cur, slot_descs, block, &valid));
            ADVANCE_ROW();
            if (!valid) {
                continue; // process next line
            }
            *is_empty_row = false;
        }
    } catch (simdjson::simdjson_error& e) {
        RETURN_IF_ERROR(_handle_simdjson_error(e, block, num_rows, eof));
        if (*_scanner_eof) {
            // When _scanner_eof is true and valid is false, it means that we have encountered
            // unqualified data and decided to stop the scan.
            *is_empty_row = true;
            return Status::OK();
        }
    }

    return Status::OK();
}

Status NewJsonReader::_simdjson_handle_nested_complex_json(
        RuntimeState* /*state*/, Block& block, const std::vector<SlotDescriptor*>& slot_descs,
        bool* is_empty_row, bool* eof) {
    // nested complex json
    while (true) {
        size_t num_rows = block.rows();
        simdjson::ondemand::object cur;
        size_t size = 0;
        simdjson::error_code error;
        try {
            RETURN_IF_ERROR(_simdjson_parse_json(&size, is_empty_row, eof, &error));
            if (size == 0 || *eof) {
                *is_empty_row = true;
                return Status::OK();
            }
            Status st = _get_json_value(&size, eof, &error, is_empty_row);
            if (st.is<DATA_QUALITY_ERROR>()) {
                continue; // continue to read next
            }
            RETURN_IF_ERROR(st);
            if (*is_empty_row) {
                return Status::OK();
            }
            *is_empty_row = false;
            bool valid = true;
            if (_json_value.type() != simdjson::ondemand::json_type::object) {
                RETURN_IF_ERROR(_append_error_msg(nullptr, "Not object item", "", nullptr));
                continue;
            }
            cur = _json_value.get_object();
            st = _simdjson_write_columns_by_jsonpath(&cur, slot_descs, block, &valid);
            if (!st.ok()) {
                RETURN_IF_ERROR(_append_error_msg(nullptr, st.to_string(), "", nullptr));
                // Before continuing to process other rows, we need to first clean the fail parsed row.
                for (int i = 0; i < block.columns(); ++i) {
                    auto column = block.get_by_position(i).column->assume_mutable();
                    if (column->size() > num_rows) {
                        column->pop_back(column->size() - num_rows);
                    }
                }
                continue;
            }
            if (!valid) {
                // there is only one line in this case, so if it return false, just set is_empty_row true
                // so that the caller will continue reading next line.
                *is_empty_row = true;
            }
            break; // read a valid row
        } catch (simdjson::simdjson_error& e) {
            RETURN_IF_ERROR(_handle_simdjson_error(e, block, num_rows, eof));
            if (*_scanner_eof) {
                // When _scanner_eof is true and valid is false, it means that we have encountered
                // unqualified data and decided to stop the scan.
                *is_empty_row = true;
                return Status::OK();
            }
            continue;
        }
    }
    return Status::OK();
}

size_t NewJsonReader::_column_index(const StringRef& name, size_t key_index) {
    /// Optimization by caching the order of fields (which is almost always the same)
    /// and a quick check to match the next expected field, instead of searching the hash table.
    if (_prev_positions.size() > key_index && name == _prev_positions[key_index]->first) {
        return _prev_positions[key_index]->second;
    }
    auto it = _slot_desc_index.find(name);
    if (it != _slot_desc_index.end()) {
        if (key_index < _prev_positions.size()) {
            _prev_positions[key_index] = it;
        }
        return it->second;
    }
    return size_t(-1);
}

Status NewJsonReader::_simdjson_set_column_value(simdjson::ondemand::object* value, Block& block,
                                                 const std::vector<SlotDescriptor*>& slot_descs,
                                                 bool* valid) {
    // set
    _seen_columns.assign(block.columns(), false);
    size_t cur_row_count = block.rows();
    bool has_valid_value = false;
    // iterate through object, simdjson::ondemond will parsing on the fly
    size_t key_index = 0;
    for (auto field : *value) {
        std::string_view key = field.unescaped_key();
        StringRef name_ref(key.data(), key.size());
        std::string key_string;
        if (_is_hive_table) {
            key_string = name_ref.to_string();
            std::transform(key_string.begin(), key_string.end(), key_string.begin(), ::tolower);
            name_ref = StringRef(key_string);
        }
        const size_t column_index = _column_index(name_ref, key_index++);
        if (UNLIKELY(ssize_t(column_index) < 0)) {
            // This key is not exist in slot desc, just ignore
            continue;
        }
        if (column_index == skip_bitmap_col_idx) {
            continue;
        }
        if (_seen_columns[column_index]) {
            if (_is_hive_table) {
                //Since value can only be traversed once,
                // we can only insert the original value first, then delete it, and then reinsert the new value
                block.get_by_position(column_index).column->assume_mutable()->pop_back(1);
            } else {
                continue;
            }
        }
        simdjson::ondemand::value val = field.value();
        auto* column_ptr = block.get_by_position(column_index).column->assume_mutable().get();
        RETURN_IF_ERROR(_simdjson_write_data_to_column(
                val, slot_descs[column_index]->type(), column_ptr,
                slot_descs[column_index]->col_name(), _serdes[column_index], valid));
        if (!(*valid)) {
            return Status::OK();
        }
        _seen_columns[column_index] = true;
        has_valid_value = true;
    }

    if (!has_valid_value && _is_load) {
        std::string col_names;
        for (auto* slot_desc : slot_descs) {
            col_names.append(slot_desc->col_name() + ", ");
        }
        RETURN_IF_ERROR(_append_error_msg(value,
                                          "There is no column matching jsonpaths in the json file, "
                                          "columns:[{}], please check columns "
                                          "and jsonpaths:" +
                                                  _jsonpaths,
                                          col_names, valid));
        return Status::OK();
    }

    if (_should_process_skip_bitmap_col()) {
        _append_empty_skip_bitmap_value(block, cur_row_count);
    }

    // fill missing slot
    int nullcount = 0;
    for (size_t i = 0; i < slot_descs.size(); ++i) {
        if (_seen_columns[i]) {
            continue;
        }
        if (i == skip_bitmap_col_idx) {
            continue;
        }

        auto* slot_desc = slot_descs[i];
        auto* column_ptr = block.get_by_position(i).column->assume_mutable().get();

        // Quick path to insert default value, instead of using default values in the value map.
        if (!_should_process_skip_bitmap_col() &&
            (_col_default_value_map.empty() ||
             _col_default_value_map.find(slot_desc->col_name()) == _col_default_value_map.end())) {
            column_ptr->insert_default();
            continue;
        }
        if (!slot_desc->is_materialized()) {
            continue;
        }
        if (column_ptr->size() < cur_row_count + 1) {
            DCHECK(column_ptr->size() == cur_row_count);
            if (_should_process_skip_bitmap_col()) {
                // not found, skip this column in flexible partial update
                if (slot_desc->is_key() && !slot_desc->is_auto_increment()) {
                    RETURN_IF_ERROR(
                            _append_error_msg(value,
                                              "The key columns can not be ommited in flexible "
                                              "partial update, missing key column: {}",
                                              slot_desc->col_name(), valid));
                    // remove this line in block
                    for (size_t index = 0; index < block.columns(); ++index) {
                        auto column = block.get_by_position(index).column->assume_mutable();
                        if (column->size() != cur_row_count) {
                            DCHECK(column->size() == cur_row_count + 1);
                            column->pop_back(1);
                            DCHECK(column->size() == cur_row_count);
                        }
                    }
                    return Status::OK();
                }
                _set_skip_bitmap_mark(slot_desc, column_ptr, block, cur_row_count, valid);
                column_ptr->insert_default();
            } else {
                RETURN_IF_ERROR(_fill_missing_column(slot_desc, _serdes[i], column_ptr, valid));
                if (!(*valid)) {
                    return Status::OK();
                }
            }
            ++nullcount;
        }
        DCHECK(column_ptr->size() == cur_row_count + 1);
    }

    // There is at least one valid value here
    DCHECK(nullcount < block.columns());
    *valid = true;
    return Status::OK();
}

Status NewJsonReader::_simdjson_write_data_to_column(simdjson::ondemand::value& value,
                                                     const DataTypePtr& type_desc,
                                                     vectorized::IColumn* column_ptr,
                                                     const std::string& column_name,
                                                     DataTypeSerDeSPtr serde, bool* valid) {
    ColumnNullable* nullable_column = nullptr;
    vectorized::IColumn* data_column_ptr = column_ptr;
    DataTypeSerDeSPtr data_serde = serde;

    if (column_ptr->is_nullable()) {
        nullable_column = reinterpret_cast<ColumnNullable*>(column_ptr);

        data_column_ptr = nullable_column->get_nested_column().get_ptr().get();
        data_serde = serde->get_nested_serdes()[0];

        // kNullType will put 1 into the Null map, so there is no need to push 0 for kNullType.
        if (value.type() == simdjson::ondemand::json_type::null) {
            nullable_column->insert_default();
            *valid = true;
            return Status::OK();
        }
    } else if (value.type() == simdjson::ondemand::json_type::null) [[unlikely]] {
        if (_is_load) {
            RETURN_IF_ERROR(_append_error_msg(
                    nullptr, "Json value is null, but the column `{}` is not nullable.",
                    column_name, valid));
            return Status::OK();
        } else {
            return Status::DataQualityError(
                    "Json value is null, but the column `{}` is not nullable.", column_name);
        }
    }

    auto primitive_type = type_desc->get_primitive_type();
    if (_is_load || !is_complex_type(primitive_type)) {
        if (value.type() == simdjson::ondemand::json_type::string) {
            std::string_view value_string = value.get_string();
            Slice slice {value_string.data(), value_string.size()};
            RETURN_IF_ERROR(data_serde->deserialize_one_cell_from_json(*data_column_ptr, slice,
                                                                       _serde_options));

        } else {
            // Maybe we can `switch (value->GetType()) case: kNumberType`.
            // Note that `if (value->IsInt())`, but column is FloatColumn.
            std::string_view json_str = simdjson::to_json_string(value);
            Slice slice {json_str.data(), json_str.size()};
            RETURN_IF_ERROR(data_serde->deserialize_one_cell_from_json(*data_column_ptr, slice,
                                                                       _serde_options));
        }
    } else if (primitive_type == TYPE_STRUCT) {
        if (value.type() != simdjson::ondemand::json_type::object) [[unlikely]] {
            return Status::DataQualityError(
                    "Json value isn't object, but the column `{}` is struct.", column_name);
        }

        const auto* type_struct =
                assert_cast<const DataTypeStruct*>(remove_nullable(type_desc).get());
        auto sub_col_size = type_struct->get_elements().size();
        simdjson::ondemand::object struct_value = value.get_object();
        auto sub_serdes = data_serde->get_nested_serdes();
        auto* struct_column_ptr = assert_cast<ColumnStruct*>(data_column_ptr);

        std::map<std::string, size_t> sub_col_name_to_idx;
        for (size_t sub_col_idx = 0; sub_col_idx < sub_col_size; sub_col_idx++) {
            sub_col_name_to_idx.emplace(type_struct->get_element_name(sub_col_idx), sub_col_idx);
        }
        std::vector<bool> has_value(sub_col_size, false);
        for (simdjson::ondemand::field sub : struct_value) {
            std::string_view sub_key_view = sub.unescaped_key();
            std::string sub_key(sub_key_view.data(), sub_key_view.length());
            std::transform(sub_key.begin(), sub_key.end(), sub_key.begin(), ::tolower);

            if (sub_col_name_to_idx.find(sub_key) == sub_col_name_to_idx.end()) [[unlikely]] {
                continue;
            }
            size_t sub_column_idx = sub_col_name_to_idx[sub_key];
            auto sub_column_ptr = struct_column_ptr->get_column(sub_column_idx).get_ptr();

            if (has_value[sub_column_idx]) [[unlikely]] {
                // Since struct_value can only be traversed once, we can only insert
                // the original value first, then delete it, and then reinsert the new value.
                sub_column_ptr->pop_back(1);
            }
            has_value[sub_column_idx] = true;

            const auto& sub_col_type = type_struct->get_element(sub_column_idx);
            RETURN_IF_ERROR(_simdjson_write_data_to_column(
                    sub.value(), sub_col_type, sub_column_ptr.get(), column_name + "." + sub_key,
                    sub_serdes[sub_column_idx], valid));
        }

        //fill missing subcolumn
        for (size_t sub_col_idx = 0; sub_col_idx < sub_col_size; sub_col_idx++) {
            if (has_value[sub_col_idx]) {
                continue;
            }

            auto sub_column_ptr = struct_column_ptr->get_column(sub_col_idx).get_ptr();
            if (sub_column_ptr->is_nullable()) {
                sub_column_ptr->insert_default();
                continue;
            } else [[unlikely]] {
                return Status::DataQualityError(
                        "Json file structColumn miss field {} and this column isn't nullable.",
                        column_name + "." + type_struct->get_element_name(sub_col_idx));
            }
        }
    } else if (primitive_type == TYPE_MAP) {
        if (value.type() != simdjson::ondemand::json_type::object) [[unlikely]] {
            return Status::DataQualityError("Json value isn't object, but the column `{}` is map.",
                                            column_name);
        }
        simdjson::ondemand::object object_value = value.get_object();

        auto sub_serdes = data_serde->get_nested_serdes();
        auto* map_column_ptr = assert_cast<ColumnMap*>(data_column_ptr);

        size_t field_count = 0;
        for (simdjson::ondemand::field member_value : object_value) {
            auto f = [](std::string_view key_view, const DataTypePtr& type_desc,
                        vectorized::IColumn* column_ptr, DataTypeSerDeSPtr serde,
                        vectorized::DataTypeSerDe::FormatOptions serde_options, bool* valid) {
                auto* data_column_ptr = column_ptr;
                auto data_serde = serde;
                if (column_ptr->is_nullable()) {
                    auto* nullable_column = static_cast<ColumnNullable*>(column_ptr);

                    nullable_column->get_null_map_data().push_back(0);
                    data_column_ptr = nullable_column->get_nested_column().get_ptr().get();
                    data_serde = serde->get_nested_serdes()[0];
                }
                Slice slice(key_view.data(), key_view.length());

                RETURN_IF_ERROR(data_serde->deserialize_one_cell_from_json(*data_column_ptr, slice,
                                                                           serde_options));
                return Status::OK();
            };

            RETURN_IF_ERROR(f(member_value.unescaped_key(),
                              assert_cast<const DataTypeMap*>(remove_nullable(type_desc).get())
                                      ->get_key_type(),
                              map_column_ptr->get_keys_ptr()->assume_mutable()->get_ptr().get(),
                              sub_serdes[0], _serde_options, valid));

            simdjson::ondemand::value field_value = member_value.value();
            RETURN_IF_ERROR(_simdjson_write_data_to_column(
                    field_value,
                    assert_cast<const DataTypeMap*>(remove_nullable(type_desc).get())
                            ->get_value_type(),
                    map_column_ptr->get_values_ptr()->assume_mutable()->get_ptr().get(),
                    column_name + ".value", sub_serdes[1], valid));
            field_count++;
        }

        auto& offsets = map_column_ptr->get_offsets();
        offsets.emplace_back(offsets.back() + field_count);

    } else if (primitive_type == TYPE_ARRAY) {
        if (value.type() != simdjson::ondemand::json_type::array) [[unlikely]] {
            return Status::DataQualityError("Json value isn't array, but the column `{}` is array.",
                                            column_name);
        }

        simdjson::ondemand::array array_value = value.get_array();

        auto sub_serdes = data_serde->get_nested_serdes();
        auto* array_column_ptr = assert_cast<ColumnArray*>(data_column_ptr);

        int field_count = 0;
        for (simdjson::ondemand::value sub_value : array_value) {
            RETURN_IF_ERROR(_simdjson_write_data_to_column(
                    sub_value,
                    assert_cast<const DataTypeArray*>(remove_nullable(type_desc).get())
                            ->get_nested_type(),
                    array_column_ptr->get_data().get_ptr().get(), column_name + ".element",
                    sub_serdes[0], valid));
            field_count++;
        }
        auto& offsets = array_column_ptr->get_offsets();
        offsets.emplace_back(offsets.back() + field_count);

    } else {
        return Status::InternalError("Not support load to complex column.");
    }
    //We need to finally set the nullmap of column_nullable to keep the size consistent with data_column
    if (nullable_column && value.type() != simdjson::ondemand::json_type::null) {
        nullable_column->get_null_map_data().push_back(0);
    }
    *valid = true;
    return Status::OK();
}

Status NewJsonReader::_append_error_msg(simdjson::ondemand::object* obj, std::string error_msg,
                                        std::string col_name, bool* valid) {
    std::string err_msg;
    if (!col_name.empty()) {
        fmt::memory_buffer error_buf;
        fmt::format_to(error_buf, error_msg, col_name, _jsonpaths);
        err_msg = fmt::to_string(error_buf);
    } else {
        err_msg = error_msg;
    }

    _counter->num_rows_filtered++;
    if (valid != nullptr) {
        // current row is invalid
        *valid = false;
    }

    RETURN_IF_ERROR(_state->append_error_msg_to_file(
            [&]() -> std::string {
                if (!obj) {
                    return "";
                }
                std::string_view str_view;
                (void)!obj->raw_json().get(str_view);
                return std::string(str_view.data(), str_view.size());
            },
            [&]() -> std::string { return err_msg; }));
    return Status::OK();
}

Status NewJsonReader::_simdjson_parse_json(size_t* size, bool* is_empty_row, bool* eof,
                                           simdjson::error_code* error) {
    SCOPED_TIMER(_read_timer);
    // step1: read buf from pipe.
    if (_line_reader != nullptr) {
        RETURN_IF_ERROR(_line_reader->read_line(&_json_str, size, eof, _io_ctx));
    } else {
        size_t length = 0;
        RETURN_IF_ERROR(_read_one_message(&_json_str_ptr, &length));
        _json_str = _json_str_ptr.get();
        *size = length;
        if (length == 0) {
            *eof = true;
        }
    }
    if (*eof) {
        return Status::OK();
    }

    // step2: init json parser iterate.
    if (*size + simdjson::SIMDJSON_PADDING > _padded_size) {
        // For efficiency reasons, simdjson requires a string with a few bytes (simdjson::SIMDJSON_PADDING) at the end.
        // Hence, a re-allocation is needed if the space is not enough.
        _simdjson_ondemand_padding_buffer.resize(*size + simdjson::SIMDJSON_PADDING);
        _simdjson_ondemand_unscape_padding_buffer.resize(*size + simdjson::SIMDJSON_PADDING);
        _padded_size = *size + simdjson::SIMDJSON_PADDING;
    }
    // trim BOM since simdjson does not handle UTF-8 Unicode (with BOM)
    if (*size >= 3 && static_cast<char>(_json_str[0]) == '\xEF' &&
        static_cast<char>(_json_str[1]) == '\xBB' && static_cast<char>(_json_str[2]) == '\xBF') {
        // skip the first three BOM bytes
        _json_str += 3;
        *size -= 3;
    }
    memcpy(&_simdjson_ondemand_padding_buffer.front(), _json_str, *size);
    _original_doc_size = *size;
    *error = _ondemand_json_parser
                     ->iterate(std::string_view(_simdjson_ondemand_padding_buffer.data(), *size),
                               _padded_size)
                     .get(_original_json_doc);
    return Status::OK();
}

Status NewJsonReader::_judge_empty_row(size_t size, bool eof, bool* is_empty_row) {
    if (size == 0 || eof) {
        *is_empty_row = true;
        return Status::OK();
    }

    if (!_parsed_jsonpaths.empty() && _strip_outer_array) {
        _total_rows = _json_value.count_elements().value();
        _next_row = 0;

        if (_total_rows == 0) {
            // meet an empty json array.
            *is_empty_row = true;
        }
    }
    return Status::OK();
}

Status NewJsonReader::_get_json_value(size_t* size, bool* eof, simdjson::error_code* error,
                                      bool* is_empty_row) {
    SCOPED_TIMER(_read_timer);
    auto return_quality_error = [&](fmt::memory_buffer& error_msg,
                                    const std::string& doc_info) -> Status {
        _counter->num_rows_filtered++;
        RETURN_IF_ERROR(_state->append_error_msg_to_file(
                [&]() -> std::string { return doc_info; },
                [&]() -> std::string { return fmt::to_string(error_msg); }));
        if (*_scanner_eof) {
            // Case A: if _scanner_eof is set to true in "append_error_msg_to_file", which means
            // we meet enough invalid rows and the scanner should be stopped.
            // So we set eof to true and return OK, the caller will stop the process as we meet the end of file.
            *eof = true;
            return Status::OK();
        }
        return Status::DataQualityError(fmt::to_string(error_msg));
    };
    if (*error != simdjson::error_code::SUCCESS) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Parse json data for JsonDoc failed. code: {}, error info: {}",
                       *error, simdjson::error_message(*error));
        return return_quality_error(error_msg, std::string((char*)_json_str, *size));
    }
    auto type_res = _original_json_doc.type();
    if (type_res.error() != simdjson::error_code::SUCCESS) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Parse json data for JsonDoc failed. code: {}, error info: {}",
                       type_res.error(), simdjson::error_message(type_res.error()));
        return return_quality_error(error_msg, std::string((char*)_json_str, *size));
    }
    simdjson::ondemand::json_type type = type_res.value();
    if (type != simdjson::ondemand::json_type::object &&
        type != simdjson::ondemand::json_type::array) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "Not an json object or json array");
        return return_quality_error(error_msg, std::string((char*)_json_str, *size));
    }
    if (!_parsed_json_root.empty() && type == simdjson::ondemand::json_type::object) {
        try {
            // set json root
            // if it is an array at top level, then we should iterate the entire array in
            // ::_simdjson_handle_flat_array_complex_json
            simdjson::ondemand::object object = _original_json_doc;
            Status st = JsonFunctions::extract_from_object(object, _parsed_json_root, &_json_value);
            if (!st.ok()) {
                fmt::memory_buffer error_msg;
                fmt::format_to(error_msg, "{}", st.to_string());
                return return_quality_error(error_msg, std::string((char*)_json_str, *size));
            }
            _parsed_from_json_root = true;
        } catch (simdjson::simdjson_error& e) {
            fmt::memory_buffer error_msg;
            fmt::format_to(error_msg, "Encounter error while extract_from_object, error: {}",
                           e.what());
            return return_quality_error(error_msg, std::string((char*)_json_str, *size));
        }
    } else {
        _json_value = _original_json_doc;
    }

    if (_json_value.type() == simdjson::ondemand::json_type::array && !_strip_outer_array) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "{}",
                       "JSON data is array-object, `strip_outer_array` must be TRUE.");
        return return_quality_error(error_msg, std::string((char*)_json_str, *size));
    }

    if (_json_value.type() != simdjson::ondemand::json_type::array && _strip_outer_array) {
        fmt::memory_buffer error_msg;
        fmt::format_to(error_msg, "{}",
                       "JSON data is not an array-object, `strip_outer_array` must be FALSE.");
        return return_quality_error(error_msg, std::string((char*)_json_str, *size));
    }
    RETURN_IF_ERROR(_judge_empty_row(*size, *eof, is_empty_row));
    return Status::OK();
}

Status NewJsonReader::_simdjson_write_columns_by_jsonpath(
        simdjson::ondemand::object* value, const std::vector<SlotDescriptor*>& slot_descs,
        Block& block, bool* valid) {
    // write by jsonpath
    bool has_valid_value = false;
    for (size_t i = 0; i < slot_descs.size(); i++) {
        auto* slot_desc = slot_descs[i];
        if (!slot_desc->is_materialized()) {
            continue;
        }
        auto* column_ptr = block.get_by_position(i).column->assume_mutable().get();
        simdjson::ondemand::value json_value;
        Status st;
        if (i < _parsed_jsonpaths.size()) {
            st = JsonFunctions::extract_from_object(*value, _parsed_jsonpaths[i], &json_value);
            if (!st.ok() && !st.is<NOT_FOUND>()) {
                return st;
            }
        }
        if (i < _parsed_jsonpaths.size() && JsonFunctions::is_root_path(_parsed_jsonpaths[i])) {
            // Indicate that the jsonpath is "$.", read the full root json object, insert the original doc directly
            ColumnNullable* nullable_column = nullptr;
            IColumn* target_column_ptr = nullptr;
            if (slot_desc->is_nullable()) {
                nullable_column = assert_cast<ColumnNullable*>(column_ptr);
                target_column_ptr = &nullable_column->get_nested_column();
                nullable_column->get_null_map_data().push_back(0);
            }
            auto* column_string = assert_cast<ColumnString*>(target_column_ptr);
            column_string->insert_data(_simdjson_ondemand_padding_buffer.data(),
                                       _original_doc_size);
            has_valid_value = true;
        } else if (i >= _parsed_jsonpaths.size() || st.is<NOT_FOUND>()) {
            // not match in jsondata, filling with default value
            RETURN_IF_ERROR(_fill_missing_column(slot_desc, _serdes[i], column_ptr, valid));
            if (!(*valid)) {
                return Status::OK();
            }
        } else {
            RETURN_IF_ERROR(_simdjson_write_data_to_column(json_value, slot_desc->type(),
                                                           column_ptr, slot_desc->col_name(),
                                                           _serdes[i], valid));
            if (!(*valid)) {
                return Status::OK();
            }
            has_valid_value = true;
        }
    }
    if (!has_valid_value) {
        // there is no valid value in json line but has filled with default value before
        // so remove this line in block
        std::string col_names;
        for (int i = 0; i < block.columns(); ++i) {
            auto column = block.get_by_position(i).column->assume_mutable();
            column->pop_back(1);
        }
        for (auto* slot_desc : slot_descs) {
            col_names.append(slot_desc->col_name() + ", ");
        }
        RETURN_IF_ERROR(_append_error_msg(value,
                                          "There is no column matching jsonpaths in the json file, "
                                          "columns:[{}], please check columns "
                                          "and jsonpaths:" +
                                                  _jsonpaths,
                                          col_names, valid));
        return Status::OK();
    }
    *valid = true;
    return Status::OK();
}

Status NewJsonReader::_get_column_default_value(
        const std::vector<SlotDescriptor*>& slot_descs,
        const std::unordered_map<std::string, VExprContextSPtr>& col_default_value_ctx) {
    for (auto* slot_desc : slot_descs) {
        auto it = col_default_value_ctx.find(slot_desc->col_name());
        if (it != col_default_value_ctx.end() && it->second != nullptr) {
            const auto& ctx = it->second;
            // NULL_LITERAL means no valid value of current column
            if (ctx->root()->node_type() == TExprNodeType::type::NULL_LITERAL) {
                continue;
            }
            // empty block to save default value of slot_desc->col_name()
            Block block;
            // If block is empty, some functions will produce no result. So we insert a column with
            // single value here.
            block.insert({ColumnUInt8::create(1), std::make_shared<DataTypeUInt8>(), ""});
            int result = -1;
            RETURN_IF_ERROR(ctx->execute(&block, &result));
            DCHECK(result != -1);
            auto column = block.get_by_position(result).column;
            DCHECK(column->size() == 1);
            _col_default_value_map.emplace(slot_desc->col_name(),
                                           column->get_data_at(0).to_string());
        }
    }
    return Status::OK();
}

Status NewJsonReader::_fill_missing_column(SlotDescriptor* slot_desc, DataTypeSerDeSPtr serde,
                                           IColumn* column_ptr, bool* valid) {
    auto col_value = _col_default_value_map.find(slot_desc->col_name());
    if (col_value == _col_default_value_map.end()) {
        if (slot_desc->is_nullable()) {
            auto* nullable_column = static_cast<ColumnNullable*>(column_ptr);
            nullable_column->insert_default();
        } else {
            if (_is_load) {
                RETURN_IF_ERROR(_append_error_msg(
                        nullptr, "The column `{}` is not nullable, but it's not found in jsondata.",
                        slot_desc->col_name(), valid));
            } else {
                return Status::DataQualityError(
                        "The column `{}` is not nullable, but it's not found in jsondata.",
                        slot_desc->col_name());
            }
        }
    } else {
        const std::string& v_str = col_value->second;
        Slice column_default_value {v_str};
        RETURN_IF_ERROR(serde->deserialize_one_cell_from_json(*column_ptr, column_default_value,
                                                              _serde_options));
    }
    *valid = true;
    return Status::OK();
}

void NewJsonReader::_append_empty_skip_bitmap_value(Block& block, size_t cur_row_count) {
    auto* skip_bitmap_nullable_col_ptr = assert_cast<ColumnNullable*>(
            block.get_by_position(skip_bitmap_col_idx).column->assume_mutable().get());
    auto* skip_bitmap_col_ptr =
            assert_cast<ColumnBitmap*>(skip_bitmap_nullable_col_ptr->get_nested_column_ptr().get());
    DCHECK(skip_bitmap_nullable_col_ptr->size() == cur_row_count);
    // should append an empty bitmap for every row wheather this line misses columns
    skip_bitmap_nullable_col_ptr->get_null_map_data().push_back(0);
    skip_bitmap_col_ptr->insert_default();
    DCHECK(skip_bitmap_col_ptr->size() == cur_row_count + 1);
}

void NewJsonReader::_set_skip_bitmap_mark(SlotDescriptor* slot_desc, IColumn* column_ptr,
                                          Block& block, size_t cur_row_count, bool* valid) {
    // we record the missing column's column unique id in skip bitmap
    // to indicate which columns need to do the alignment process
    auto* skip_bitmap_nullable_col_ptr = assert_cast<ColumnNullable*>(
            block.get_by_position(skip_bitmap_col_idx).column->assume_mutable().get());
    auto* skip_bitmap_col_ptr =
            assert_cast<ColumnBitmap*>(skip_bitmap_nullable_col_ptr->get_nested_column_ptr().get());
    DCHECK(skip_bitmap_col_ptr->size() == cur_row_count + 1);
    auto& skip_bitmap = skip_bitmap_col_ptr->get_data().back();
    skip_bitmap.add(slot_desc->col_unique_id());
}

void NewJsonReader::_collect_profile_before_close() {
    if (_line_reader != nullptr) {
        _line_reader->collect_profile_before_close();
    }
    if (_file_reader != nullptr) {
        _file_reader->collect_profile_before_close();
    }
}

#include "common/compile_check_end.h"
} // namespace doris::vectorized
