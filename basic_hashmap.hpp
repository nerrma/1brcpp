#pragma once
#include <array>
#include <cassert>
#include <cstddef>
#include <iterator>
#include <memory>
#include <optional>
#include <utility>
#include <vector>

template <typename K, typename V> struct Bucket {
  Bucket() = delete;
  Bucket(const K &key) : key{key} {}
  K key;
  V value;
};

// TODO: add concept for callable T
template <typename K, typename V, typename H, size_t map_size>
class BasicHashmap {
public:
  BasicHashmap() = default;

  auto operator[](const K &key) -> V & {
    auto entry = find_entry(key);
    if (entry == nullptr) {
      entry = add_entry(key);
    }

    assert(entry != nullptr);

    return entry->value;
  }

  struct Iterator {
    using iterator_category = std::forward_iterator_tag;
    using difference_type = std::ptrdiff_t;
    using value_type = std::pair<K, V>;
    using pointer = std::pair<K, V> *;
    using reference = std::pair<K, V>;

    Iterator() = delete;
    Iterator(const std::array<std::vector<Bucket<K, V>>, map_size> buckets,
             size_t idx, size_t bucket_idx)
        : buckets(buckets), idx(idx), bucket_idx(bucket_idx) {}

    Iterator(size_t idx, size_t bucket_idx)
        : idx(idx), bucket_idx(bucket_idx) {}

    reference operator*() const {
      assert(idx < map_size);
      assert(bucket_idx < buckets[idx].size());
      assert(!buckets[idx].empty());
      return std::make_pair(buckets[idx][bucket_idx].key,
                            buckets[idx][bucket_idx].value);
    }

    pointer operator->() const {
      return std::make_pair(*buckets[idx][bucket_idx].key,
                            *buckets[idx][bucket_idx].value);
    }

    Iterator &operator++() {
      if (bucket_idx < buckets[idx].size() - 1) {
        bucket_idx++;
      } else {
        idx++;
        while (buckets[idx].empty() && idx < map_size) {
          idx++;
        }
        bucket_idx = 0;
      }

      return *this;
    }

    Iterator &operator++(int) {
      Iterator tmp = *this;
      ++(*this);
      return tmp;
    }

    friend bool operator==(const Iterator &a, const Iterator &b) {
      return a.idx == b.idx && a.bucket_idx == b.bucket_idx;
    }

    friend bool operator!=(const Iterator &a, const Iterator &b) {
      return a.idx != b.idx || a.bucket_idx != b.bucket_idx;
    }

  private:
    const std::array<std::vector<Bucket<K, V>>, map_size> buckets;
    size_t idx;
    size_t bucket_idx;
  };

  Iterator begin() const { return Iterator(std::move(buckets_), 0, 0); }
  Iterator end() const { return Iterator(map_size, 0); }

private:
  auto find_entry(const K &key) -> Bucket<K, V> * {
    auto cur_bucket = std::nullopt;
    std::vector<Bucket<K, V>> &entries = buckets_[hash_func_(key) % map_size];
    for (size_t i = 0; i < entries.size(); i++) {
      if (entries[i].key == key) {
        return &entries[i];
      }
    }

    return nullptr;
  }

  auto add_entry(const K &key) -> Bucket<K, V> * {
    // no checking, precondition is that we do not have a clash
    std::vector<Bucket<K, V>> &entries = buckets_[hash_func_(key) % map_size];
    entries.emplace_back(key);
    return &entries[entries.size() - 1];
  }

  H hash_func_;
  std::array<std::vector<Bucket<K, V>>, map_size> buckets_;
};
