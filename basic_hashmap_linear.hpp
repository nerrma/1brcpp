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
  const auto sz = a.size();
  alignas(8) const auto a_dat = a.data();
  alignas(8) const auto b_dat = b.data();
  bool match = false;
  for (int i = 0; i < sz && !match; i += 32) {
    __m256i a_chunk = _mm256_loadu_si256((__m256i *)&a_dat[i]);
    __m256i b_chunk = _mm256_loadu_si256((__m256i *)&b_dat[i]);
    uint8_t shift_by = 32 * (i + 1) > sz ? (sz - i) : 32;

    __m256i m = _mm256_cmpeq_epi8(a_chunk, b_chunk) << shift_by;
    match |= _mm256_testz_si256(a_chunk, b_chunk);
  }

  return !match;
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
    if (!buckets_[i].key.empty()) {
      buckets_[i].key = key;
      return &buckets_[i];
    }

    const auto sz = key.size();
    while (!buckets_[i].key.empty() && buckets_[i].key.size() != sz &&
           buckets_[i].key != key) {
      i = (i + 1) % map_size;
    }

    buckets_[i].key = key;

    return &buckets_[i];
  }

  H hash_func_;
  std::array<BucketLinear<K, V>, map_size> buckets_;
};
