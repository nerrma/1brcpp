#pragma GCC target("avx2")
#pragma GCC optimise("O3")

#include "basic_hashmap.hpp"
#include <algorithm>
#include <assert.h>
#include <bits/types/FILE.h>
#include <bitset>
#include <cctype>
#include <climits>
#include <cstddef>
#include <cstdio>
#include <exception>
#include <fstream>
#include <functional>
#include <ios>
#include <iostream>
#include <map>
#include <ostream>
#include <sstream>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
#include <x86intrin.h>

const char DELIMITER = ';';
const size_t NUM_THREADS = 12;

/* Store relevant city data. */
struct CityEntry {
  int city_min = INT_MAX;
  int city_max = INT_MIN;
  long long sum = 0;
  int count = 0;

  CityEntry() = default;

  inline auto update(int new_temp) -> void {
    city_min = std::min(city_min, new_temp);
    city_max = std::max(city_max, new_temp);
    sum += new_temp;
    count++;
  }

  inline auto update_entry(CityEntry const &o) -> void {
    city_min = std::min(city_min, o.city_min);
    city_max = std::max(city_max, o.city_max);
    sum += o.sum;
    count += o.count;
  }
};

auto operator<<(std::ostream &os, CityEntry const &city) -> std::ostream & {
  return os << (double)city.city_min / 10 << "/"
            << ((double)city.sum / (double)city.count) / 10 << "/"
            << (double)city.city_max / 10;
}

struct HashStrView {
  // hash returns a simple (but fast) hash for the first n bytes of data
  auto operator()(const std::string_view &s) const -> size_t {
    return s[0] * 1 + s[1] * 2 + s[s.size() - 2] * 100 + s[s.size() - 1] * 200;
  }
};

inline auto round_to_aligned_32(size_t read_to) -> size_t {
  return read_to - (read_to % 32);
}

typedef BasicHashmap<std::string_view, CityEntry, HashStrView, 2048>
    inter_map_tp;

/* Process a chunk and populate the given map */
auto process_chunk(inter_map_tp &city_map, std::string_view const &buf,
                   size_t start_idx) -> void {
  const auto n = buf.size();

  alignas(8) const auto dat = buf.data();
  __m256i c_mask = _mm256_set1_epi8(';');
  __m256i n_mask = _mm256_set1_epi8('\n');
  uint32_t colon_mask = 0;
  uint32_t newline_mask = 0;
  size_t i = 0;
  size_t prev_newline = 0;
  size_t prev_delim = 0;
  bool long_stream = false;
  for (; i < n; i += 32) {
    // std::cout << " \t\t\t\t i " << i << std::endl;
    __m256i y = _mm256_load_si256((__m256i *)&dat[i]);
    __m256i c = _mm256_cmpeq_epi8(c_mask, y);
    __m256i n = _mm256_cmpeq_epi8(n_mask, y);
    colon_mask = _mm256_movemask_epi8(c);
    newline_mask = _mm256_movemask_epi8(n);
    do {
      auto line_end = __builtin_ctz(newline_mask);
      auto colon_end = __builtin_ctz(colon_mask);

      size_t start = prev_newline ? prev_newline + 1 : 0;
      auto newline_pos = std::max(start, i) + line_end;
      auto delim_pos =
          (!long_stream && prev_delim) ? prev_delim : i + colon_end;

      if (newline_mask == 0) {
        prev_delim = delim_pos;
        break;
      }

      std::string_view name = buf.substr(start, delim_pos - start);
      std::string_view rem =
          buf.substr(delim_pos + 1, newline_pos - delim_pos - 1);

      long_stream = false;

      int temp = 0;
      bool is_neg = rem[0] == '-';
      if (rem[is_neg + 1] == '.') {
        temp = (rem[is_neg] - '0') * 10 + (rem[is_neg + 2] - '0');
      } else {
        temp = (rem[is_neg] - '0') * 100 + (rem[is_neg + 1] - '0') * 10 +
               (rem[is_neg + 3] - '0');
      }

      temp *= is_neg ? -1 : 1;

      newline_mask = newline_mask >> (line_end + 1);
      colon_mask = colon_mask >> (line_end + 1);

      if (start >= start_idx) {
        city_map[name].update(temp);
      }

      prev_newline = newline_pos;
      prev_delim = (colon_mask && line_end < 31)
                       ? (newline_pos + 1 + __builtin_ctz(colon_mask))
                       : 0;

      if (line_end >= 31) {
        break;
      }
    } while (newline_mask && prev_newline < buf.size());

    long_stream = prev_delim == 0;
    // std::cout << "prev_delim " << prev_delim << " long_stream " <<
    // long_stream
    //           << std::endl;
  }
}

auto main(int argc, char *argv[]) -> int {
  if (argc < 2) {
    std::cout << "Usage: ./1brc <filename>" << std::endl;
    return -1;
  }

  FILE *file_ptr = fopen(argv[1], "r");
  if (!file_ptr) {
    std::cout << "Unable to open file!" << std::endl;
    return -1;
  }

  // get file size
  struct stat f_stat;
  stat(argv[1], &f_stat);

  auto chunk_size = f_stat.st_size / NUM_THREADS;

  char *buf = reinterpret_cast<char *>(
      mmap(NULL, f_stat.st_size, PROT_READ, MAP_PRIVATE, file_ptr->_fileno, 0));
  assert(buf != (char *)-1);

  std::vector<std::jthread> pool;
  std::vector<inter_map_tp> maps(NUM_THREADS);
  pool.reserve(NUM_THREADS);

  // bind variables to arguments (as a capture)
  size_t read_to = 0;
  size_t cur_chunk_end = 0;
  for (int i = 0; i < NUM_THREADS; i++) {
    cur_chunk_end += chunk_size;
    cur_chunk_end = cur_chunk_end < (size_t)f_stat.st_size
                        ? cur_chunk_end
                        : (size_t)f_stat.st_size;

    while (cur_chunk_end < f_stat.st_size && buf[cur_chunk_end] != '\n' &&
           buf[cur_chunk_end] != EOF) {
      cur_chunk_end++;
    }

    pool.push_back(std::jthread(
        process_chunk, std::ref(maps[i]),
        std::string_view(buf + round_to_aligned_32(read_to),
                         cur_chunk_end - round_to_aligned_32(read_to)),
        read_to - round_to_aligned_32(read_to)));
    read_to = cur_chunk_end + 1;
  }

  pool.clear();

  // merge all maps
  // std::map<std::string_view, CityEntry> city_map;
  // std::unordered_map<std::string_view, CityEntry> city_map;
  inter_map_tp city_map;
  for (auto &inter : maps) {
    for (auto const &[k, v] : inter) {
      city_map[k].update_entry(v);
    }
  }

  std::vector<std::pair<std::string_view, CityEntry>> city_vec(city_map.begin(),
                                                               city_map.end());
  std::sort(city_vec.begin(), city_vec.end(),
            [](const auto &p1, const auto &p2) -> bool {
              return p1.first < p2.first;
            });

  std::cout.precision(3);
  std::cout << "{";
  const int n = city_vec.size();
  for (int i = 0; auto const &[k, v] : city_vec) {
    std::cout << k << "=" << v;
    if (i < n - 1) {
      std::cout << ", ";
    }
    i++;
  }
  std::cout << "}\n";

  close(file_ptr->_fileno);
  munmap(buf, f_stat.st_size);

  return 0;
}
