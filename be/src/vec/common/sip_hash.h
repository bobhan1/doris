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
// https://github.com/ClickHouse/ClickHouse/blob/master/src/Common/SipHash.h
// and modified by Doris

#pragma once

/** SipHash is a fast cryptographic hash function for short strings.
  * Taken from here: https://www.131002.net/siphash/
  *
  * This is SipHash 2-4 variant.
  *
  * Two changes are made:
  * - returns also 128 bits, not only 64;
  * - done streaming (can be calculated in parts).
  *
  * On short strings (URL, search phrases) more than 3 times faster than MD5 from OpenSSL.
  * (~ 700 MB/sec, 15 million strings per second)
  */

#include <string>
#include <type_traits>

#include "common/compiler_util.h" // IWYU pragma: keep
#include "vec/common/unaligned.h"
#include "vec/core/types.h"

namespace doris {

#define ROTL(x, b) static_cast<vectorized::UInt64>(((x) << (b)) | ((x) >> (64 - (b))))

#define SIPROUND           \
    do {                   \
        v0 += v1;          \
        v1 = ROTL(v1, 13); \
        v1 ^= v0;          \
        v0 = ROTL(v0, 32); \
        v2 += v3;          \
        v3 = ROTL(v3, 16); \
        v3 ^= v2;          \
        v0 += v3;          \
        v3 = ROTL(v3, 21); \
        v3 ^= v0;          \
        v2 += v1;          \
        v1 = ROTL(v1, 17); \
        v1 ^= v2;          \
        v2 = ROTL(v2, 32); \
    } while (0)

class SipHash {
private:
    /// State.
    vectorized::UInt64 v0;
    vectorized::UInt64 v1;
    vectorized::UInt64 v2;
    vectorized::UInt64 v3;

    /// How many bytes have been processed.
    vectorized::UInt64 cnt;

    /// The current 8 bytes of input data.
    union {
        vectorized::UInt64 current_word;
        vectorized::UInt8 current_bytes[8];
    };

    ALWAYS_INLINE void finalize() {
        /// In the last free byte, we write the remainder of the division by 256.
        current_bytes[7] = cnt;

        v3 ^= current_word;
        SIPROUND;
        SIPROUND;
        v0 ^= current_word;

        v2 ^= 0xff;
        SIPROUND;
        SIPROUND;
        SIPROUND;
        SIPROUND;
    }

public:
    /// Arguments - seed.
    SipHash(vectorized::UInt64 k0 = 0, vectorized::UInt64 k1 = 0) {
        /// Initialize the state with some random bytes and seed.
        v0 = 0x736f6d6570736575ULL ^ k0;
        v1 = 0x646f72616e646f6dULL ^ k1;
        v2 = 0x6c7967656e657261ULL ^ k0;
        v3 = 0x7465646279746573ULL ^ k1;

        cnt = 0;
        current_word = 0;
    }

    void update(const char* data, vectorized::UInt64 size) {
        const char* end = data + size;

        /// We'll finish to process the remainder of the previous update, if any.
        if (cnt & 7) {
            while (cnt & 7 && data < end) {
                current_bytes[cnt & 7] = *data;
                ++data;
                ++cnt;
            }

            /// If we still do not have enough bytes to an 8-byte word.
            if (cnt & 7) return;

            v3 ^= current_word;
            SIPROUND;
            SIPROUND;
            v0 ^= current_word;
        }

        cnt += end - data;

        while (data + 8 <= end) {
            current_word = unaligned_load<vectorized::UInt64>(data);

            v3 ^= current_word;
            SIPROUND;
            SIPROUND;
            v0 ^= current_word;

            data += 8;
        }

        /// Pad the remainder, which is missing up to an 8-byte word.
        current_word = 0;
        switch (end - data) {
        case 7:
            current_bytes[6] = data[6];
            [[fallthrough]];
        case 6:
            current_bytes[5] = data[5];
            [[fallthrough]];
        case 5:
            current_bytes[4] = data[4];
            [[fallthrough]];
        case 4:
            current_bytes[3] = data[3];
            [[fallthrough]];
        case 3:
            current_bytes[2] = data[2];
            [[fallthrough]];
        case 2:
            current_bytes[1] = data[1];
            [[fallthrough]];
        case 1:
            current_bytes[0] = data[0];
            [[fallthrough]];
        case 0:
            break;
        }
    }

    template <typename T>
    void update(const T& x) {
        if constexpr (std::is_same_v<T, std::string>) {
            throw Exception(ErrorCode::INTERNAL_ERROR, "String should not use SipHash!");
        }
        update(reinterpret_cast<const char*>(&x), sizeof(x));
    }

    /// Get the result in some form. This can only be done once!

    void get128(char* out) {
        finalize();
        reinterpret_cast<vectorized::UInt64*>(out)[0] = v0 ^ v1;
        reinterpret_cast<vectorized::UInt64*>(out)[1] = v2 ^ v3;
    }

    /// template for avoiding 'unsigned long long' vs 'unsigned long' problem on old poco in macos
    template <typename T>
    ALWAYS_INLINE void get128(T& lo, T& hi) {
        static_assert(sizeof(T) == 8);
        finalize();
        lo = v0 ^ v1;
        hi = v2 ^ v3;
    }

    vectorized::UInt64 get64() {
        finalize();
        return v0 ^ v1 ^ v2 ^ v3;
    }

    template <typename T>
    ALWAYS_INLINE void get128(T& dst) {
        static_assert(sizeof(T) == 16);
        get128(reinterpret_cast<char*>(&dst));
    }
};

#undef ROTL
#undef SIPROUND

#include <cstddef>

inline void sip_hash128(const char* data, const size_t size, char* out) {
    SipHash hash;
    hash.update(data, size);
    hash.get128(out);
}

} // namespace doris