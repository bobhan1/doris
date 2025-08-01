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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Core/Block.cpp
// and modified by Doris

#include "vec/core/block.h"

#include <fmt/format.h>
#include <gen_cpp/data.pb.h>
#include <glog/logging.h>
#include <snappy.h>
#include <streamvbyte.h>
#include <sys/types.h>

#include <algorithm>
#include <cassert>
#include <iomanip>
#include <iterator>
#include <limits>
#include <ranges>

#include "agent/be_exec_version_manager.h"
#include "common/compiler_util.h" // IWYU pragma: keep
#include "common/config.h"
#include "common/logging.h"
#include "common/status.h"
#include "runtime/descriptors.h"
#include "runtime/thread_context.h"
#include "util/block_compression.h"
#include "util/faststring.h"
#include "util/runtime_profile.h"
#include "util/simd/bits.h"
#include "util/slice.h"
#include "vec/columns/column.h"
#include "vec/columns/column_const.h"
#include "vec/columns/column_nothing.h"
#include "vec/columns/column_nullable.h"
#include "vec/columns/column_vector.h"
#include "vec/common/assert_cast.h"
#include "vec/data_types/data_type_factory.hpp"
#include "vec/data_types/data_type_nullable.h"

class SipHash;

namespace doris::segment_v2 {
enum CompressionTypePB : int;
} // namespace doris::segment_v2
#include "common/compile_check_begin.h"
namespace doris::vectorized {
template <typename T>
void clear_blocks(moodycamel::ConcurrentQueue<T>& blocks,
                  RuntimeProfile::Counter* memory_used_counter = nullptr) {
    T block;
    while (blocks.try_dequeue(block)) {
        if (memory_used_counter) {
            if constexpr (std::is_same_v<T, Block>) {
                memory_used_counter->update(-block.allocated_bytes());
            } else {
                memory_used_counter->update(-block->allocated_bytes());
            }
        }
    }
}

template void clear_blocks<Block>(moodycamel::ConcurrentQueue<Block>&,
                                  RuntimeProfile::Counter* memory_used_counter);
template void clear_blocks<BlockUPtr>(moodycamel::ConcurrentQueue<BlockUPtr>&,
                                      RuntimeProfile::Counter* memory_used_counter);

Block::Block(std::initializer_list<ColumnWithTypeAndName> il) : data {il} {
    initialize_index_by_name();
}

Block::Block(ColumnsWithTypeAndName data_) : data {std::move(data_)} {
    initialize_index_by_name();
}

Block::Block(const std::vector<SlotDescriptor*>& slots, size_t block_size,
             bool ignore_trivial_slot) {
    for (auto* const slot_desc : slots) {
        if (ignore_trivial_slot && !slot_desc->is_materialized()) {
            continue;
        }
        auto column_ptr = slot_desc->get_empty_mutable_column();
        column_ptr->reserve(block_size);
        insert(ColumnWithTypeAndName(std::move(column_ptr), slot_desc->get_data_type_ptr(),
                                     slot_desc->col_name()));
    }
}

Block::Block(const std::vector<SlotDescriptor>& slots, size_t block_size,
             bool ignore_trivial_slot) {
    std::vector<SlotDescriptor*> slot_ptrs(slots.size());
    for (size_t i = 0; i < slots.size(); ++i) {
        slot_ptrs[i] = const_cast<SlotDescriptor*>(&slots[i]);
    }
    *this = Block(slot_ptrs, block_size, ignore_trivial_slot);
}

Status Block::deserialize(const PBlock& pblock) {
    swap(Block());
    int be_exec_version = pblock.has_be_exec_version() ? pblock.be_exec_version() : 0;
    RETURN_IF_ERROR(BeExecVersionManager::check_be_exec_version(be_exec_version));

    const char* buf = nullptr;
    std::string compression_scratch;
    if (pblock.compressed()) {
        // Decompress
        SCOPED_RAW_TIMER(&_decompress_time_ns);
        const char* compressed_data = pblock.column_values().c_str();
        size_t compressed_size = pblock.column_values().size();
        size_t uncompressed_size = 0;
        if (pblock.has_compression_type() && pblock.has_uncompressed_size()) {
            BlockCompressionCodec* codec;
            RETURN_IF_ERROR(get_block_compression_codec(pblock.compression_type(), &codec));
            uncompressed_size = pblock.uncompressed_size();
            // Should also use allocator to allocate memory here.
            compression_scratch.resize(uncompressed_size);
            Slice decompressed_slice(compression_scratch);
            RETURN_IF_ERROR(codec->decompress(Slice(compressed_data, compressed_size),
                                              &decompressed_slice));
            DCHECK(uncompressed_size == decompressed_slice.size);
        } else {
            bool success = snappy::GetUncompressedLength(compressed_data, compressed_size,
                                                         &uncompressed_size);
            DCHECK(success) << "snappy::GetUncompressedLength failed";
            compression_scratch.resize(uncompressed_size);
            success = snappy::RawUncompress(compressed_data, compressed_size,
                                            compression_scratch.data());
            DCHECK(success) << "snappy::RawUncompress failed";
        }
        _decompressed_bytes = uncompressed_size;
        buf = compression_scratch.data();
    } else {
        buf = pblock.column_values().data();
    }

    for (const auto& pcol_meta : pblock.column_metas()) {
        DataTypePtr type = DataTypeFactory::instance().create_data_type(pcol_meta);
        MutableColumnPtr data_column = type->create_column();
        // Here will try to allocate large memory, should return error if failed.
        RETURN_IF_CATCH_EXCEPTION(
                buf = type->deserialize(buf, &data_column, pblock.be_exec_version()));
        data.emplace_back(data_column->get_ptr(), type, pcol_meta.name());
    }
    initialize_index_by_name();

    return Status::OK();
}

void Block::reserve(size_t count) {
    index_by_name.reserve(count);
    data.reserve(count);
}

void Block::initialize_index_by_name() {
    for (size_t i = 0, size = data.size(); i < size; ++i) {
        index_by_name[data[i].name] = i;
    }
}

void Block::insert(size_t position, const ColumnWithTypeAndName& elem) {
    if (position > data.size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "invalid input position, position={}, data.size={}, names={}", position,
                        data.size(), dump_names());
    }

    for (auto& name_pos : index_by_name) {
        if (name_pos.second >= position) {
            ++name_pos.second;
        }
    }

    index_by_name.emplace(elem.name, position);
    data.emplace(data.begin() + position, elem);
}

void Block::insert(size_t position, ColumnWithTypeAndName&& elem) {
    if (position > data.size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "invalid input position, position={}, data.size={}, names={}", position,
                        data.size(), dump_names());
    }

    for (auto& name_pos : index_by_name) {
        if (name_pos.second >= position) {
            ++name_pos.second;
        }
    }

    index_by_name.emplace(elem.name, position);
    data.emplace(data.begin() + position, std::move(elem));
}

void Block::clear_names() {
    index_by_name.clear();
    for (auto& entry : data) {
        entry.name.clear();
    }
}

void Block::insert(const ColumnWithTypeAndName& elem) {
    index_by_name.emplace(elem.name, data.size());
    data.emplace_back(elem);
}

void Block::insert(ColumnWithTypeAndName&& elem) {
    index_by_name.emplace(elem.name, data.size());
    data.emplace_back(std::move(elem));
}

void Block::erase(const std::set<size_t>& positions) {
    for (unsigned long position : std::ranges::reverse_view(positions)) {
        erase(position);
    }
}

void Block::erase_tail(size_t start) {
    DCHECK(start <= data.size()) << fmt::format(
            "Position out of bound in Block::erase(), max position = {}", data.size());
    data.erase(data.begin() + start, data.end());
    for (auto it = index_by_name.begin(); it != index_by_name.end();) {
        if (it->second >= start) {
            index_by_name.erase(it++);
        } else {
            ++it;
        }
    }
    if (start < row_same_bit.size()) {
        row_same_bit.erase(row_same_bit.begin() + start, row_same_bit.end());
    }
}

void Block::erase(size_t position) {
    DCHECK(!data.empty()) << "Block is empty";
    DCHECK_LT(position, data.size()) << fmt::format(
            "Position out of bound in Block::erase(), max position = {}", data.size() - 1);

    erase_impl(position);
}

void Block::erase_impl(size_t position) {
    data.erase(data.begin() + position);

    for (auto it = index_by_name.begin(); it != index_by_name.end();) {
        if (it->second == position) {
            index_by_name.erase(it++);
        } else {
            if (it->second > position) {
                --it->second;
            }
            ++it;
        }
    }
    if (position < row_same_bit.size()) {
        row_same_bit.erase(row_same_bit.begin() + position);
    }
}

void Block::erase(const String& name) {
    auto index_it = index_by_name.find(name);
    if (index_it == index_by_name.end()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "No such name in Block, name={}, block_names={}",
                        name, dump_names());
    }

    erase_impl(index_it->second);
}

ColumnWithTypeAndName& Block::safe_get_by_position(size_t position) {
    if (position >= data.size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "invalid input position, position={}, data.size={}, names={}", position,
                        data.size(), dump_names());
    }
    return data[position];
}

const ColumnWithTypeAndName& Block::safe_get_by_position(size_t position) const {
    if (position >= data.size()) {
        throw Exception(ErrorCode::INTERNAL_ERROR,
                        "invalid input position, position={}, data.size={}, names={}", position,
                        data.size(), dump_names());
    }
    return data[position];
}

ColumnWithTypeAndName& Block::get_by_name(const std::string& name) {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "No such name in Block, name={}, block_names={}",
                        name, dump_names());
    }

    return data[it->second];
}

const ColumnWithTypeAndName& Block::get_by_name(const std::string& name) const {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "No such name in Block, name={}, block_names={}",
                        name, dump_names());
    }

    return data[it->second];
}

ColumnWithTypeAndName* Block::try_get_by_name(const std::string& name) {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        return nullptr;
    }
    return &data[it->second];
}

const ColumnWithTypeAndName* Block::try_get_by_name(const std::string& name) const {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        return nullptr;
    }
    return &data[it->second];
}

bool Block::has(const std::string& name) const {
    return index_by_name.end() != index_by_name.find(name);
}

size_t Block::get_position_by_name(const std::string& name) const {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "No such name in Block, name={}, block_names={}",
                        name, dump_names());
    }

    return it->second;
}

void Block::check_number_of_rows(bool allow_null_columns) const {
    ssize_t rows = -1;
    for (const auto& elem : data) {
        if (!elem.column && allow_null_columns) {
            continue;
        }

        if (!elem.column) {
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Column {} in block is nullptr, in method check_number_of_rows.",
                            elem.name);
        }

        ssize_t size = elem.column->size();

        if (rows == -1) {
            rows = size;
        } else if (rows != size) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "Sizes of columns doesn't match, block={}",
                            dump_structure());
        }
    }
}

Status Block::check_type_and_column() const {
#ifndef NDEBUG
    for (const auto& elem : data) {
        if (!elem.column) {
            continue;
        }
        if (!elem.type) {
            continue;
        }

        // ColumnNothing is a special column type, it is used to represent a column that
        // is not materialized, so we don't need to check it.
        if (check_and_get_column<ColumnNothing>(elem.column.get())) {
            continue;
        }

        const auto& type = elem.type;
        const auto& column = elem.column;

        auto st = type->check_column(*column);
        if (!st.ok()) {
            return Status::InternalError(
                    "Column {} in block is not compatible with its column type :{}, data type :{}, "
                    "error: {}",
                    elem.name, column->get_name(), type->get_name(), st.msg());
        }
    }
#endif
    return Status::OK();
}

size_t Block::rows() const {
    for (const auto& elem : data) {
        if (elem.column) {
            return elem.column->size();
        }
    }

    return 0;
}

void Block::set_num_rows(size_t length) {
    if (rows() > length) {
        for (auto& elem : data) {
            if (elem.column) {
                elem.column = elem.column->shrink(length);
            }
        }
        if (length < row_same_bit.size()) {
            row_same_bit.resize(length);
        }
    }
}

void Block::skip_num_rows(int64_t& length) {
    auto origin_rows = rows();
    if (origin_rows <= length) {
        clear();
        length -= origin_rows;
    } else {
        for (auto& elem : data) {
            if (elem.column) {
                elem.column = elem.column->cut(length, origin_rows - length);
            }
        }
        if (length < row_same_bit.size()) {
            row_same_bit.assign(row_same_bit.begin() + length, row_same_bit.end());
        }
    }
}

size_t Block::bytes() const {
    size_t res = 0;
    for (const auto& elem : data) {
        if (!elem.column) {
            std::stringstream ss;
            for (const auto& e : data) {
                ss << e.name + " ";
            }
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Column {} in block is nullptr, in method bytes. All Columns are {}",
                            elem.name, ss.str());
        }
        res += elem.column->byte_size();
    }

    return res;
}

std::string Block::columns_bytes() const {
    std::stringstream res;
    res << "column bytes: [";
    for (const auto& elem : data) {
        if (!elem.column) {
            std::stringstream ss;
            for (const auto& e : data) {
                ss << e.name + " ";
            }
            throw Exception(ErrorCode::INTERNAL_ERROR,
                            "Column {} in block is nullptr, in method bytes. All Columns are {}",
                            elem.name, ss.str());
        }
        res << ", " << elem.column->byte_size();
    }
    res << "]";
    return res.str();
}

size_t Block::allocated_bytes() const {
    size_t res = 0;
    for (const auto& elem : data) {
        if (!elem.column) {
            // Sometimes if expr failed, then there will be a nullptr
            // column left in the block.
            continue;
        }
        res += elem.column->allocated_bytes();
    }

    return res;
}

std::string Block::dump_names() const {
    std::string out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) {
            out += ", ";
        }
        out += it->name;
    }
    return out;
}

std::string Block::dump_types() const {
    std::string out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) {
            out += ", ";
        }
        out += it->type->get_name();
    }
    return out;
}

std::string Block::dump_data(size_t begin, size_t row_limit, bool allow_null_mismatch) const {
    std::vector<std::string> headers;
    std::vector<int> headers_size;
    for (const auto& it : data) {
        std::string s = fmt::format("{}({})", it.name, it.type->get_name());
        headers_size.push_back(s.size() > 15 ? (int)s.size() : 15);
        headers.emplace_back(s);
    }

    std::stringstream out;
    // header upper line
    auto line = [&]() {
        for (size_t i = 0; i < columns(); ++i) {
            out << std::setfill('-') << std::setw(1) << "+" << std::setw(headers_size[i]) << "-";
        }
        out << std::setw(1) << "+" << std::endl;
    };
    line();
    // header text
    for (size_t i = 0; i < columns(); ++i) {
        out << std::setfill(' ') << std::setw(1) << "|" << std::left << std::setw(headers_size[i])
            << headers[i];
    }
    out << std::setw(1) << "|" << std::endl;
    // header bottom line
    line();
    if (rows() == 0) {
        return out.str();
    }
    // content
    for (size_t row_num = begin; row_num < rows() && row_num < row_limit + begin; ++row_num) {
        for (size_t i = 0; i < columns(); ++i) {
            if (!data[i].column || data[i].column->empty()) {
                out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                    << std::right;
                continue;
            }
            std::string s;
            if (data[i].column) { // column may be const
                // for code inside `default_implementation_for_nulls`, there's could have: type = null, col != null
                if (data[i].type->is_nullable() && !data[i].column->is_concrete_nullable()) {
                    assert(allow_null_mismatch);
                    s = assert_cast<const DataTypeNullable*>(data[i].type.get())
                                ->get_nested_type()
                                ->to_string(*data[i].column, row_num);
                } else {
                    s = data[i].to_string(row_num);
                }
            }
            if (s.length() > headers_size[i]) {
                s = s.substr(0, headers_size[i] - 3) + "...";
            }
            out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                << std::right << s;
        }
        out << std::setw(1) << "|" << std::endl;
    }
    // bottom line
    line();
    if (row_limit < rows()) {
        out << rows() << " rows in block, only show first " << row_limit << " rows." << std::endl;
    }
    return out.str();
}

std::string Block::dump_one_line(size_t row, int column_end) const {
    assert(column_end <= columns());
    fmt::memory_buffer line;
    for (int i = 0; i < column_end; ++i) {
        if (LIKELY(i != 0)) {
            // TODO: need more effective function of to string. now the impl is slow
            fmt::format_to(line, " {}", data[i].to_string(row));
        } else {
            fmt::format_to(line, "{}", data[i].to_string(row));
        }
    }
    return fmt::to_string(line);
}

std::string Block::dump_structure() const {
    std::string out;
    for (auto it = data.begin(); it != data.end(); ++it) {
        if (it != data.begin()) {
            out += ", \n";
        }
        out += it->dump_structure();
    }
    return out;
}

Block Block::clone_empty() const {
    Block res;
    for (const auto& elem : data) {
        res.insert(elem.clone_empty());
    }
    return res;
}

MutableColumns Block::clone_empty_columns() const {
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        columns[i] = data[i].column ? data[i].column->clone_empty() : data[i].type->create_column();
    }
    return columns;
}

Columns Block::get_columns() const {
    size_t num_columns = data.size();
    Columns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        columns[i] = data[i].column->convert_to_full_column_if_const();
    }
    return columns;
}

Columns Block::get_columns_and_convert() {
    size_t num_columns = data.size();
    Columns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        data[i].column = data[i].column->convert_to_full_column_if_const();
        columns[i] = data[i].column;
    }
    return columns;
}

MutableColumns Block::mutate_columns() {
    size_t num_columns = data.size();
    MutableColumns columns(num_columns);
    for (size_t i = 0; i < num_columns; ++i) {
        DCHECK(data[i].type);
        columns[i] = data[i].column ? (*std::move(data[i].column)).mutate()
                                    : data[i].type->create_column();
    }
    return columns;
}

void Block::set_columns(MutableColumns&& columns) {
    DCHECK_GE(columns.size(), data.size())
            << fmt::format("Invalid size of columns, columns size: {}, data size: {}",
                           columns.size(), data.size());
    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) {
        data[i].column = std::move(columns[i]);
    }
}

Block Block::clone_with_columns(MutableColumns&& columns) const {
    Block res;

    size_t num_columns = data.size();
    for (size_t i = 0; i < num_columns; ++i) {
        res.insert({std::move(columns[i]), data[i].type, data[i].name});
    }

    return res;
}

Block Block::clone_without_columns(const std::vector<int>* column_offset) const {
    Block res;

    if (column_offset != nullptr) {
        size_t num_columns = column_offset->size();
        for (size_t i = 0; i < num_columns; ++i) {
            res.insert({nullptr, data[(*column_offset)[i]].type, data[(*column_offset)[i]].name});
        }
    } else {
        size_t num_columns = data.size();
        for (size_t i = 0; i < num_columns; ++i) {
            res.insert({nullptr, data[i].type, data[i].name});
        }
    }
    return res;
}

const ColumnsWithTypeAndName& Block::get_columns_with_type_and_name() const {
    return data;
}

std::vector<std::string> Block::get_names() const {
    std::vector<std::string> res;
    res.reserve(columns());

    for (const auto& elem : data) {
        res.push_back(elem.name);
    }

    return res;
}

DataTypes Block::get_data_types() const {
    DataTypes res;
    res.reserve(columns());

    for (const auto& elem : data) {
        res.push_back(elem.type);
    }

    return res;
}

void Block::clear() {
    data.clear();
    index_by_name.clear();
    row_same_bit.clear();
}

void Block::clear_column_data(int64_t column_size) noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    // data.size() greater than column_size, means here have some
    // function exec result in block, need erase it here
    if (column_size != -1 and data.size() > column_size) {
        for (int64_t i = data.size() - 1; i >= column_size; --i) {
            erase(i);
        }
    }
    for (auto& d : data) {
        if (d.column) {
            // Temporarily disable reference count check because a column might be referenced multiple times within a block.
            // Queries like this: `select c, c from t1;`
            (*std::move(d.column)).assume_mutable()->clear();
        }
    }
    row_same_bit.clear();
}

void Block::erase_tmp_columns() noexcept {
    auto all_column_names = get_names();
    for (auto& name : all_column_names) {
        if (name.rfind(BeConsts::BLOCK_TEMP_COLUMN_PREFIX, 0) == 0) {
            erase(name);
        }
    }
}

void Block::clear_column_mem_not_keep(const std::vector<bool>& column_keep_flags,
                                      bool need_keep_first) {
    if (data.size() >= column_keep_flags.size()) {
        auto origin_rows = rows();
        for (size_t i = 0; i < column_keep_flags.size(); ++i) {
            if (!column_keep_flags[i]) {
                data[i].column = data[i].column->clone_empty();
            }
        }

        if (need_keep_first && !column_keep_flags[0]) {
            auto first_column = data[0].column->clone_empty();
            first_column->resize(origin_rows);
            data[0].column = std::move(first_column);
        }
    }
}

void Block::swap(Block& other) noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    data.swap(other.data);
    index_by_name.swap(other.index_by_name);
    row_same_bit.swap(other.row_same_bit);
}

void Block::swap(Block&& other) noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    data = std::move(other.data);
    index_by_name = std::move(other.index_by_name);
    row_same_bit = std::move(other.row_same_bit);
}

void Block::shuffle_columns(const std::vector<int>& result_column_ids) {
    Container tmp_data;
    tmp_data.reserve(result_column_ids.size());
    for (const int result_column_id : result_column_ids) {
        tmp_data.push_back(data[result_column_id]);
    }
    swap(Block {tmp_data});
}

void Block::update_hash(SipHash& hash) const {
    for (size_t row_no = 0, num_rows = rows(); row_no < num_rows; ++row_no) {
        for (const auto& col : data) {
            col.column->update_hash_with_value(row_no, hash);
        }
    }
}

void Block::filter_block_internal(Block* block, const std::vector<uint32_t>& columns_to_filter,
                                  const IColumn::Filter& filter) {
    size_t count = filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    for (const auto& col : columns_to_filter) {
        auto& column = block->get_by_position(col).column;
        if (column->size() == count) {
            continue;
        }
        if (count == 0) {
            block->get_by_position(col).column->assume_mutable()->clear();
            continue;
        }
        if (column->is_exclusive()) {
            const auto result_size = column->assume_mutable()->filter(filter);
            if (result_size != count) [[unlikely]] {
                throw Exception(ErrorCode::INTERNAL_ERROR,
                                "result_size not equal with filter_size, result_size={}, "
                                "filter_size={}",
                                result_size, count);
            }
        } else {
            column = column->filter(filter, count);
        }
    }
}

void Block::filter_block_internal(Block* block, const IColumn::Filter& filter,
                                  uint32_t column_to_keep) {
    std::vector<uint32_t> columns_to_filter;
    columns_to_filter.resize(column_to_keep);
    for (uint32_t i = 0; i < column_to_keep; ++i) {
        columns_to_filter[i] = i;
    }
    filter_block_internal(block, columns_to_filter, filter);
}

void Block::filter_block_internal(Block* block, const IColumn::Filter& filter) {
    const size_t count =
            filter.size() - simd::count_zero_num((int8_t*)filter.data(), filter.size());
    for (int i = 0; i < block->columns(); ++i) {
        auto& column = block->get_by_position(i).column;
        if (column->is_exclusive()) {
            column->assume_mutable()->filter(filter);
        } else {
            column = column->filter(filter, count);
        }
    }
}

Status Block::append_to_block_by_selector(MutableBlock* dst,
                                          const IColumn::Selector& selector) const {
    RETURN_IF_CATCH_EXCEPTION({
        DCHECK_EQ(data.size(), dst->mutable_columns().size());
        for (size_t i = 0; i < data.size(); i++) {
            // FIXME: this is a quickfix. we assume that only partition functions make there some
            if (!is_column_const(*data[i].column)) {
                data[i].column->append_data_by_selector(dst->mutable_columns()[i], selector);
            }
        }
    });
    return Status::OK();
}

Status Block::filter_block(Block* block, const std::vector<uint32_t>& columns_to_filter,
                           size_t filter_column_id, size_t column_to_keep) {
    const auto& filter_column = block->get_by_position(filter_column_id).column;
    if (const auto* nullable_column = check_and_get_column<ColumnNullable>(*filter_column)) {
        const auto& nested_column = nullable_column->get_nested_column_ptr();

        MutableColumnPtr mutable_holder =
                nested_column->use_count() == 1
                        ? nested_column->assume_mutable()
                        : nested_column->clone_resized(nested_column->size());

        auto* concrete_column = assert_cast<ColumnUInt8*>(mutable_holder.get());
        const auto* __restrict null_map = nullable_column->get_null_map_data().data();
        IColumn::Filter& filter = concrete_column->get_data();
        auto* __restrict filter_data = filter.data();

        const size_t size = filter.size();
        for (size_t i = 0; i < size; ++i) {
            filter_data[i] &= !null_map[i];
        }
        RETURN_IF_CATCH_EXCEPTION(filter_block_internal(block, columns_to_filter, filter));
    } else if (const auto* const_column = check_and_get_column<ColumnConst>(*filter_column)) {
        bool ret = const_column->get_bool(0);
        if (!ret) {
            for (const auto& col : columns_to_filter) {
                std::move(*block->get_by_position(col).column).assume_mutable()->clear();
            }
        }
    } else {
        const IColumn::Filter& filter =
                assert_cast<const doris::vectorized::ColumnUInt8&>(*filter_column).get_data();
        RETURN_IF_CATCH_EXCEPTION(filter_block_internal(block, columns_to_filter, filter));
    }

    erase_useless_column(block, column_to_keep);
    return Status::OK();
}

Status Block::filter_block(Block* block, size_t filter_column_id, size_t column_to_keep) {
    std::vector<uint32_t> columns_to_filter;
    columns_to_filter.resize(column_to_keep);
    for (uint32_t i = 0; i < column_to_keep; ++i) {
        columns_to_filter[i] = i;
    }
    return filter_block(block, columns_to_filter, filter_column_id, column_to_keep);
}

Status Block::serialize(int be_exec_version, PBlock* pblock,
                        /*std::string* compressed_buffer,*/ size_t* uncompressed_bytes,
                        size_t* compressed_bytes, segment_v2::CompressionTypePB compression_type,
                        bool allow_transfer_large_data) const {
    RETURN_IF_ERROR(BeExecVersionManager::check_be_exec_version(be_exec_version));
    pblock->set_be_exec_version(be_exec_version);

    // calc uncompressed size for allocation
    size_t content_uncompressed_size = 0;
    for (const auto& c : *this) {
        PColumnMeta* pcm = pblock->add_column_metas();
        c.to_pb_column_meta(pcm);
        DCHECK(pcm->type() != PGenericType::UNKNOWN) << " forget to set pb type";
        // get serialized size
        content_uncompressed_size +=
                c.type->get_uncompressed_serialized_bytes(*(c.column), pblock->be_exec_version());
    }

    // serialize data values
    // when data type is HLL, content_uncompressed_size maybe larger than real size.
    std::string column_values;
    try {
        // TODO: After support c++23, we should use resize_and_overwrite to replace resize
        column_values.resize(content_uncompressed_size);
    } catch (...) {
        std::string msg = fmt::format("Try to alloc {} bytes for pblock column values failed.",
                                      content_uncompressed_size);
        LOG(WARNING) << msg;
        return Status::BufferAllocFailed(msg);
    }
    char* buf = column_values.data();

    for (const auto& c : *this) {
        buf = c.type->serialize(*(c.column), buf, pblock->be_exec_version());
    }
    *uncompressed_bytes = content_uncompressed_size;
    const size_t serialize_bytes = buf - column_values.data() + STREAMVBYTE_PADDING;
    *compressed_bytes = serialize_bytes;
    column_values.resize(serialize_bytes);

    // compress
    if (compression_type != segment_v2::NO_COMPRESSION && content_uncompressed_size > 0) {
        SCOPED_RAW_TIMER(&_compress_time_ns);
        pblock->set_compression_type(compression_type);
        pblock->set_uncompressed_size(serialize_bytes);

        BlockCompressionCodec* codec;
        RETURN_IF_ERROR(get_block_compression_codec(compression_type, &codec));

        faststring buf_compressed;
        RETURN_IF_ERROR_OR_CATCH_EXCEPTION(
                codec->compress(Slice(column_values.data(), serialize_bytes), &buf_compressed));
        size_t compressed_size = buf_compressed.size();
        if (LIKELY(compressed_size < serialize_bytes)) {
            // TODO: rethink the logic here may copy again ?
            pblock->set_column_values(buf_compressed.data(), buf_compressed.size());
            pblock->set_compressed(true);
            *compressed_bytes = compressed_size;
        } else {
            pblock->set_column_values(std::move(column_values));
        }

        VLOG_ROW << "uncompressed size: " << content_uncompressed_size
                 << ", compressed size: " << compressed_size;
    } else {
        pblock->set_column_values(std::move(column_values));
    }
    if (!allow_transfer_large_data && *compressed_bytes >= std::numeric_limits<int32_t>::max()) {
        return Status::InternalError("The block is large than 2GB({}), can not send by Protobuf.",
                                     *compressed_bytes);
    }
    return Status::OK();
}

MutableBlock::MutableBlock(const std::vector<TupleDescriptor*>& tuple_descs, int reserve_size,
                           bool ignore_trivial_slot) {
    for (auto* const tuple_desc : tuple_descs) {
        for (auto* const slot_desc : tuple_desc->slots()) {
            if (ignore_trivial_slot && !slot_desc->is_materialized()) {
                continue;
            }
            _data_types.emplace_back(slot_desc->get_data_type_ptr());
            _columns.emplace_back(_data_types.back()->create_column());
            if (reserve_size != 0) {
                _columns.back()->reserve(reserve_size);
            }
            _names.push_back(slot_desc->col_name());
        }
    }
    initialize_index_by_name();
}

size_t MutableBlock::rows() const {
    for (const auto& column : _columns) {
        if (column) {
            return column->size();
        }
    }

    return 0;
}

void MutableBlock::swap(MutableBlock& another) noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    _columns.swap(another._columns);
    _data_types.swap(another._data_types);
    _names.swap(another._names);
    index_by_name.swap(another.index_by_name);
}

void MutableBlock::add_row(const Block* block, int row) {
    const auto& block_data = block->get_columns_with_type_and_name();
    for (size_t i = 0; i < _columns.size(); ++i) {
        _columns[i]->insert_from(*block_data[i].column.get(), row);
    }
}

Status MutableBlock::add_rows(const Block* block, const uint32_t* row_begin,
                              const uint32_t* row_end, const std::vector<int>* column_offset) {
    RETURN_IF_CATCH_EXCEPTION({
        DCHECK_LE(columns(), block->columns());
        if (column_offset != nullptr) {
            DCHECK_EQ(columns(), column_offset->size());
        }
        const auto& block_data = block->get_columns_with_type_and_name();
        for (size_t i = 0; i < _columns.size(); ++i) {
            const auto& src_col = column_offset ? block_data[(*column_offset)[i]] : block_data[i];
            DCHECK_EQ(_data_types[i]->get_name(), src_col.type->get_name());
            auto& dst = _columns[i];
            const auto& src = *src_col.column.get();
            DCHECK_GE(src.size(), row_end - row_begin);
            dst->insert_indices_from(src, row_begin, row_end);
        }
    });
    return Status::OK();
}

Status MutableBlock::add_rows(const Block* block, size_t row_begin, size_t length) {
    RETURN_IF_CATCH_EXCEPTION({
        DCHECK_LE(columns(), block->columns());
        const auto& block_data = block->get_columns_with_type_and_name();
        for (size_t i = 0; i < _columns.size(); ++i) {
            DCHECK_EQ(_data_types[i]->get_name(), block_data[i].type->get_name());
            auto& dst = _columns[i];
            const auto& src = *block_data[i].column.get();
            dst->insert_range_from(src, row_begin, length);
        }
    });
    return Status::OK();
}

Status MutableBlock::add_rows(const Block* block, const std::vector<int64_t>& rows) {
    RETURN_IF_CATCH_EXCEPTION({
        DCHECK_LE(columns(), block->columns());
        const auto& block_data = block->get_columns_with_type_and_name();
        const size_t length = std::ranges::distance(rows);
        for (size_t i = 0; i < _columns.size(); ++i) {
            DCHECK_EQ(_data_types[i]->get_name(), block_data[i].type->get_name());
            auto& dst = _columns[i];
            const auto& src = *block_data[i].column.get();
            dst->reserve(dst->size() + length);
            for (auto row : rows) {
                // we can introduce a new function like `insert_assume_reserved` for IColumn.
                dst->insert_from(src, row);
            }
        }
    });
    return Status::OK();
}

void MutableBlock::erase(const String& name) {
    auto index_it = index_by_name.find(name);
    if (index_it == index_by_name.end()) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "No such name in Block, name={}, block_names={}",
                        name, dump_names());
    }

    auto position = index_it->second;

    _columns.erase(_columns.begin() + position);
    _data_types.erase(_data_types.begin() + position);
    _names.erase(_names.begin() + position);

    for (auto it = index_by_name.begin(); it != index_by_name.end();) {
        if (it->second == position) {
            index_by_name.erase(it++);
        } else {
            if (it->second > position) {
                --it->second;
            }
            ++it;
        }
    }
    // if (position < row_same_bit.size()) {
    //     row_same_bit.erase(row_same_bit.begin() + position);
    // }
}

Block MutableBlock::to_block(int start_column) {
    return to_block(start_column, (int)_columns.size());
}

Block MutableBlock::to_block(int start_column, int end_column) {
    ColumnsWithTypeAndName columns_with_schema;
    columns_with_schema.reserve(end_column - start_column);
    for (size_t i = start_column; i < end_column; ++i) {
        columns_with_schema.emplace_back(std::move(_columns[i]), _data_types[i], _names[i]);
    }
    return {columns_with_schema};
}

std::string MutableBlock::dump_data(size_t row_limit) const {
    std::vector<std::string> headers;
    std::vector<int> headers_size;
    for (size_t i = 0; i < columns(); ++i) {
        std::string s = _data_types[i]->get_name();
        headers_size.push_back(s.size() > 15 ? (int)s.size() : 15);
        headers.emplace_back(s);
    }

    std::stringstream out;
    // header upper line
    auto line = [&]() {
        for (size_t i = 0; i < columns(); ++i) {
            out << std::setfill('-') << std::setw(1) << "+" << std::setw(headers_size[i]) << "-";
        }
        out << std::setw(1) << "+" << std::endl;
    };
    line();
    // header text
    for (size_t i = 0; i < columns(); ++i) {
        out << std::setfill(' ') << std::setw(1) << "|" << std::left << std::setw(headers_size[i])
            << headers[i];
    }
    out << std::setw(1) << "|" << std::endl;
    // header bottom line
    line();
    if (rows() == 0) {
        return out.str();
    }
    // content
    for (size_t row_num = 0; row_num < rows() && row_num < row_limit; ++row_num) {
        for (size_t i = 0; i < columns(); ++i) {
            if (_columns[i].get()->empty()) {
                out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                    << std::right;
                continue;
            }
            std::string s = _data_types[i]->to_string(*_columns[i].get(), row_num);
            if (s.length() > headers_size[i]) {
                s = s.substr(0, headers_size[i] - 3) + "...";
            }
            out << std::setfill(' ') << std::setw(1) << "|" << std::setw(headers_size[i])
                << std::right << s;
        }
        out << std::setw(1) << "|" << std::endl;
    }
    // bottom line
    line();
    if (row_limit < rows()) {
        out << rows() << " rows in block, only show first " << row_limit << " rows." << std::endl;
    }
    return out.str();
}

std::unique_ptr<Block> Block::create_same_struct_block(size_t size, bool is_reserve) const {
    auto temp_block = Block::create_unique();
    for (const auto& d : data) {
        auto column = d.type->create_column();
        if (is_reserve) {
            column->reserve(size);
        } else {
            column->insert_many_defaults(size);
        }
        temp_block->insert({std::move(column), d.type, d.name});
    }
    return temp_block;
}

void Block::shrink_char_type_column_suffix_zero(const std::vector<size_t>& char_type_idx) {
    for (auto idx : char_type_idx) {
        if (idx < data.size()) {
            auto& col_and_name = this->get_by_position(idx);
            col_and_name.column->assume_mutable()->shrink_padding_chars();
        }
    }
}

size_t MutableBlock::allocated_bytes() const {
    size_t res = 0;
    for (const auto& col : _columns) {
        if (col) {
            res += col->allocated_bytes();
        }
    }

    return res;
}

void MutableBlock::clear_column_data() noexcept {
    SCOPED_SKIP_MEMORY_CHECK();
    for (auto& col : _columns) {
        if (col) {
            col->clear();
        }
    }
}

void MutableBlock::initialize_index_by_name() {
    for (size_t i = 0, size = _names.size(); i < size; ++i) {
        index_by_name[_names[i]] = i;
    }
}

bool MutableBlock::has(const std::string& name) const {
    return index_by_name.end() != index_by_name.find(name);
}

size_t MutableBlock::get_position_by_name(const std::string& name) const {
    auto it = index_by_name.find(name);
    if (index_by_name.end() == it) {
        throw Exception(ErrorCode::INTERNAL_ERROR, "No such name in Block, name={}, block_names={}",
                        name, dump_names());
    }

    return it->second;
}

std::string MutableBlock::dump_names() const {
    std::string out;
    for (auto it = _names.begin(); it != _names.end(); ++it) {
        if (it != _names.begin()) {
            out += ", ";
        }
        out += *it;
    }
    return out;
}
#include "common/compile_check_end.h"
} // namespace doris::vectorized
