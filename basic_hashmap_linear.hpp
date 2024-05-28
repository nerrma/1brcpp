#pragma once
#include <algorithm>
#include <array>
#include <bitset>
#include <cassert>
#include <cstddef>
#include <immintrin.h>
#include <iostream>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>
#include <vector>
#include <x86intrin.h>

inline auto simd_compare(const std::string_view &a,
                         const std::string_view &b) -> bool {
  if (a.size() != b.size()) {
    return false;
  }

  const auto sz = a.size();
  alignas(8) const auto a_dat = a.data();
  alignas(8) const auto b_dat = b.data();

  if (sz < 32) {
    __m256i a_chunk = _mm256_loadu_si256((__m256i *)&a_dat[0]);
    __m256i b_chunk = _mm256_loadu_si256((__m256i *)&b_dat[0]);

    uint8_t shift_by = sz;
    __m256i m = _mm256_cmpeq_epi8(a_chunk, b_chunk) << shift_by;
    return !_mm256_testz_si256(m, m);
  } else if (sz > 32 && sz < 32 * 2) {
    __m256i a_chunk1 = _mm256_loadu_si256((__m256i *)&a_dat[0]);
    __m256i b_chunk1 = _mm256_loadu_si256((__m256i *)&b_dat[0]);
    __m256i a_chunk2 = _mm256_loadu_si256((__m256i *)&a_dat[32]);
    __m256i b_chunk2 = _mm256_loadu_si256((__m256i *)&b_dat[32]);

    __m256i m = _mm256_cmpeq_epi8(a_chunk1, b_chunk1);
    /* check if first chunk equal */
    if (_mm256_testz_si256(m, m)) {
      return false;
    }

    uint8_t shift_by = sz - 32;
    __m256i m2 = _mm256_cmpeq_epi8(a_chunk2, b_chunk2) << shift_by;
    return !_mm256_testz_si256(m2, m2);
  } else if (sz > 32 * 2 && sz < 32 * 3) {
    __m256i a_chunk1 = _mm256_loadu_si256((__m256i *)&a_dat[0]);
    __m256i b_chunk1 = _mm256_loadu_si256((__m256i *)&b_dat[0]);
    __m256i a_chunk2 = _mm256_loadu_si256((__m256i *)&a_dat[32]);
    __m256i b_chunk2 = _mm256_loadu_si256((__m256i *)&b_dat[32]);
    __m256i a_chunk3 = _mm256_loadu_si256((__m256i *)&a_dat[64]);
    __m256i b_chunk3 = _mm256_loadu_si256((__m256i *)&b_dat[64]);

    __m256i m1 = _mm256_cmpeq_epi8(a_chunk1, b_chunk1);
    __m256i m2 = _mm256_cmpeq_epi8(a_chunk2, b_chunk2);
    /* check if first chunk equal */
    if (_mm256_testz_si256(m1, m1) || _mm256_testz_si256(m2, m2)) {
      return false;
    }

    uint8_t shift_by = sz - 64;
    __m256i m3 = _mm256_cmpeq_epi8(a_chunk3, b_chunk3) >> shift_by;
    return !_mm256_testz_si256(m3, m3);
  } else if (sz > 32 * 3 && sz < 32 * 4) {
    __m256i a_chunk1 = _mm256_loadu_si256((__m256i *)&a_dat[0]);
    __m256i b_chunk1 = _mm256_loadu_si256((__m256i *)&b_dat[0]);
    __m256i a_chunk2 = _mm256_loadu_si256((__m256i *)&a_dat[32]);
    __m256i b_chunk2 = _mm256_loadu_si256((__m256i *)&b_dat[32]);
    __m256i a_chunk3 = _mm256_loadu_si256((__m256i *)&a_dat[64]);
    __m256i b_chunk3 = _mm256_loadu_si256((__m256i *)&b_dat[64]);
    __m256i a_chunk4 = _mm256_loadu_si256((__m256i *)&a_dat[96]);
    __m256i b_chunk4 = _mm256_loadu_si256((__m256i *)&b_dat[96]);

    __m256i m1 = _mm256_cmpeq_epi8(a_chunk1, b_chunk1);
    __m256i m2 = _mm256_cmpeq_epi8(a_chunk2, b_chunk2);
    __m256i m3 = _mm256_cmpeq_epi8(a_chunk3, b_chunk3);
    /* check if first chunk equal */
    if (_mm256_testz_si256(m1, m1) || _mm256_testz_si256(m2, m2) ||
        _mm256_testz_si256(m3, m3)) {
      return false;
    }

    uint8_t shift_by = sz - 96;
    __m256i m4 = _mm256_cmpeq_epi8(a_chunk4, b_chunk4) >> shift_by;
    return !_mm256_testz_si256(m4, m4);
  }

  return true;
}

template <typename K, typename V> struct BucketLinear {
  BucketLinear() = default;
  BucketLinear(const K &key) : key{key} {}
  K key;
  V value;
};

template <typename K, typename V, typename H, size_t map_size>
class BasicHashmapLinear {
public:
  BasicHashmapLinear() = default;

  auto operator[](const K &key) -> V & {
    auto entry = find_entry(key);

    assert(entry != nullptr);

    return entry->value;
  }

  auto begin() const { return buckets_.begin(); }
  auto end() const { return buckets_.end(); }

private:
  auto find_entry(const K &key) -> BucketLinear<K, V> * {
    auto i = hash_func_(key) % map_size;
    while (not_tombstone_[i] && !simd_compare(buckets_[i].key, key)) {
      i = (i + 1) % map_size;
    }

    if (!not_tombstone_[i]) {
      buckets_[i].key = key;
      not_tombstone_[i] = true;
    }

    return &buckets_[i];
  }

  H hash_func_;
  std::array<BucketLinear<K, V>, map_size> buckets_;
  std::array<bool, map_size> not_tombstone_;
};
