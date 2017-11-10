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

#ifndef PARQUET_UTIL_COMPARISON_H
#define PARQUET_UTIL_COMPARISON_H

#include <algorithm>
#include <type_traits>

#include "parquet/exception.h"
#include "parquet/schema.h"
#include "parquet/types.h"

namespace parquet {

class PARQUET_EXPORT Comparator {
 public:
  virtual ~Comparator() = default;
  static std::shared_ptr<Comparator> Make(const ColumnDescriptor* descr);
};

// The default comparison is SIGNED
template <typename DType, bool is_signed = true>
class PARQUET_EXPORT CompareDefault : public Comparator {
 public:
  using T = typename DType::c_type;
  bool operator()(const T& a, const T& b) const { return a < b; }
};

template <typename DType>
class PARQUET_EXPORT CompareDefault<DType, false> : public Comparator {
 public:
  using T = typename DType::c_type;
  using UnsignedType = typename std::make_unsigned<T>::type;
  bool operator()(const T& a, const T& b) const {
    return static_cast<UnsignedType>(a) < static_cast<UnsignedType>(b);
  }
};

template <>
class PARQUET_EXPORT CompareDefault<ByteArrayType, true> : public Comparator {
 public:
  bool operator()(const ByteArray<const int8_t*>& a,
                  const ByteArray<const int8_t*>& b) const {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
  }
};

template <>
class PARQUET_EXPORT CompareDefault<ByteArrayType, false> : public Comparator {
 public:
  bool operator()(const ByteArray<const uint8_t*>& a,
                  const ByteArray<const uint8_t*>& b) const {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
  }
};

template <>
class PARQUET_EXPORT CompareDefault<Int96Type> : public Comparator {
 public:
  bool operator()(const Int96& a, const Int96& b) const {
    // Only the MSB bit is by Signed comparison
    // For little-endian, this is the last bit of Int96 type
    const auto amsb = static_cast<int32_t>(a.value[2]);
    const auto bmsb = static_cast<int32_t>(b.value[2]);

    return (amsb != bmsb && amsb < bmsb) ||
           (a.value[1] != b.value[1] && a.value[1] < b.value[1]) ||
           (a.value[0] < b.value[0]);
  }
};

template <>
class PARQUET_EXPORT CompareDefault<Int96Type, false> : public Comparator {
 public:
  bool operator()(const Int96& a, const Int96& b) const {
    if (a.value[2] != b.value[2]) {
      return (a.value[2] < b.value[2]);
    } else if (a.value[1] != b.value[1]) {
      return (a.value[1] < b.value[1]);
    }
    return (a.value[0] < b.value[0]);
  }
};

template <>
class PARQUET_EXPORT CompareDefault<FLBAType, true> : public Comparator {
 public:
  template <typename I,
            typename = typename std::enable_if<
                std::is_same<typename std::iterator_traits<I>::value_type,
                             const int8_t*>::value,
                I>::type>
  bool operator()(const FixedLenByteArray<I>& a, const FixedLenByteArray<I>& b) const {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
  }
};

template <>
class PARQUET_EXPORT CompareDefault<FLBAType, false> : public Comparator {
 public:
  template <typename I,
            typename = typename std::enable_if<
                std::is_same<typename std::iterator_traits<I>::value_type,
                             const uint8_t*>::value,
                I>::type>
  bool operator()(const FixedLenByteArray<I>& a, const FixedLenByteArray<I>& b) const {
    return std::lexicographical_compare(a.begin(), a.end(), b.begin(), b.end());
  }
};

typedef CompareDefault<BooleanType, true> CompareDefaultBoolean;
typedef CompareDefault<Int32Type, true> CompareDefaultInt32;
typedef CompareDefault<Int32Type, false> CompareUnsignedInt32;
typedef CompareDefault<Int64Type, true> CompareDefaultInt64;
typedef CompareDefault<Int64Type, false> CompareUnsignedInt64;
typedef CompareDefault<Int96Type, true> CompareDefaultInt96;
typedef CompareDefault<Int96Type, false> CompareUnsignedInt96;
typedef CompareDefault<FloatType> CompareDefaultFloat;
typedef CompareDefault<DoubleType> CompareDefaultDouble;
typedef CompareDefault<ByteArrayType, false> CompareDefaultByteArray;
typedef CompareDefault<FLBAType, false> CompareDefaultFLBA;

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wattributes"
#endif

PARQUET_EXTERN_TEMPLATE CompareDefault<BooleanType, true>;
PARQUET_EXTERN_TEMPLATE CompareDefault<Int32Type, true>;
PARQUET_EXTERN_TEMPLATE CompareDefault<Int32Type, false>;
PARQUET_EXTERN_TEMPLATE CompareDefault<Int64Type, true>;
PARQUET_EXTERN_TEMPLATE CompareDefault<Int64Type, false>;
PARQUET_EXTERN_TEMPLATE CompareDefault<Int96Type, true>;
PARQUET_EXTERN_TEMPLATE CompareDefault<Int96Type, false>;
PARQUET_EXTERN_TEMPLATE CompareDefault<FloatType>;
PARQUET_EXTERN_TEMPLATE CompareDefault<DoubleType>;
PARQUET_EXTERN_TEMPLATE CompareDefault<ByteArrayType, false>;
PARQUET_EXTERN_TEMPLATE CompareDefault<FLBAType, false>;

#if defined(__GNUC__) && !defined(__clang__)
#pragma GCC diagnostic pop
#endif
}  // namespace parquet

#endif  // PARQUET_UTIL_COMPARISON_H
