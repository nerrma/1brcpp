#include "basic_hashmap.hpp"
#include <algorithm>
#include <assert.h>
#include <bits/types/FILE.h>
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
#include <stdexcept>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>

const char DELIMITER = ';';
const size_t PAGE_SIZE = 0x1000;
const size_t NUM_THREADS = 16;

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
    unsigned int h = 0;

    for (int i = 0; i < s.size(); i++) {
      h = (h * 31) + s[i];
    }

    return h;
  }
};

// typedef std::unordered_map<std::string, CityEntry, HashStr> inter_map_tp;
typedef BasicHashmap<std::string_view, CityEntry, HashStrView, 2048>
    inter_map_tp;

constexpr inline int quick_pow10(int n) {
  constexpr int pow10[10] = {1,      10,      100,      1000,      10000,
                             100000, 1000000, 10000000, 100000000, 1000000000};

  return pow10[n];
}

/**
 * Parse entry into {location, temp} pair.
 */
auto parse_entry(std::string_view const &inp)
    -> std::pair<std::string_view, int> {
  const int n = inp.size();

  auto delim_pos = inp.find(DELIMITER);
  [[unlikely]] if (delim_pos == std::string::npos || delim_pos == 0 ||
                   inp.empty()) {
    throw std::runtime_error(delim_pos == std::string::npos ? "n" : "0");
  }

  std::string_view name = inp.substr(0, delim_pos);
  auto rem = inp.substr(delim_pos + 1, n);

  int temp = 0;
  bool is_neg = rem[0] == '-';
  int mod = (is_neg ? -1 : 1);
  auto rem_sz = rem.size();
  bool before_dec = true;
  for (int i = is_neg; i < rem_sz; i++) {
    char c = rem[i];

    bool is_dec = c == '.';
    before_dec = is_dec ? false : before_dec;

    temp += (c - '0') * quick_pow10(rem_sz - i - 1 - before_dec) * !is_dec;
  }

  temp *= mod;

  return {name, temp};
}

auto custom_getline(const char *buf, std::string_view &line, size_t num_read,
                    size_t end) -> size_t {
  bool start_newline = buf[num_read] == '\n';
  auto i = start_newline ? num_read + 1 : num_read;
  while (buf[i] != '\n' && i < end) {
    i++;
  }

  line = std::string_view(buf + num_read + start_newline,
                          i - num_read - start_newline);

  return i - num_read;
}

/* Process a chunk and populate the given map */
auto process_chunk(inter_map_tp &city_map, char *buf, size_t size) -> void {
  std::string_view line;
  size_t tot_read = 0;
  while (auto num_read = custom_getline(buf, line, tot_read, size)) {
    auto [city_name, temp] = parse_entry(line);
    city_map[city_name].update(temp);
    tot_read += num_read + 1;
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

  std::map<std::string_view, CityEntry> city_map;
  std::vector<std::jthread> pool;
  std::vector<inter_map_tp> maps(NUM_THREADS);
  pool.reserve(NUM_THREADS);

  // bind variables to arguments (as a capture)
  size_t read_to = 0;
  size_t cur_chunk_end = 0;
  for (int i = 0; i < NUM_THREADS; i++) {
    cur_chunk_end += chunk_size;
    cur_chunk_end = std::min(cur_chunk_end, (size_t)f_stat.st_size);

    while (cur_chunk_end < f_stat.st_size && buf[cur_chunk_end] != '\n' &&
           buf[cur_chunk_end] != EOF) {
      cur_chunk_end++;
    }

    pool.push_back(std::jthread(process_chunk, std::ref(maps[i]), buf + read_to,
                                cur_chunk_end - read_to));
    read_to = cur_chunk_end;
  }

  pool.clear();

  // merge all maps
  for (auto &inter : maps) {
    for (auto const &[k, v] : inter) {
      city_map[k].update_entry(v);
    }
  }

  std::cout.precision(3);
  std::cout << "{";
  const int n = city_map.size();
  for (int i = 0; auto const &[k, v] : city_map) {
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
