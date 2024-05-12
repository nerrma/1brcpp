#include <algorithm>
#include <assert.h>
#include <bits/types/FILE.h>
#include <cstddef>
#include <cstdio>
#include <exception>
#include <fstream>
#include <functional>
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
const size_t NORMALISE = 100;
const size_t NUM_THREADS = 16;

/* Store relevant city data. */
struct CityEntry {
  double city_min;
  double city_max;
  double sum;
  size_t count;

  CityEntry() = default;

  inline auto update(double new_temp) -> void {
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
  return os << city.city_min << "/" << ((double)city.sum / (double)city.count)
            << "/" << city.city_max;
}

struct HashStr {

  // hash returns a simple (but fast) hash for the first n bytes of data
  auto operator()(const std::string &s) const -> size_t {
    unsigned int h = 0;

    for (int i = 0; i < s.size(); i++) {
      h = (h * 31) + s[i];
    }

    return h;
  }
};

typedef std::unordered_map<std::string, CityEntry, HashStr> inter_map_tp;

constexpr inline int quick_pow10(int n) {
  constexpr int pow10[10] = {1,      10,      100,      1000,      10000,
                             100000, 1000000, 10000000, 100000000, 1000000000};

  return pow10[n];
}

/**
 * Parse entry into {location, temp} pair.
 */
auto parse_entry(std::string const &inp) -> std::pair<std::string, double> {
  const int n = inp.size();

  auto delim_pos = inp.find(DELIMITER);
  [[unlikely]] if (delim_pos == std::string::npos || delim_pos == 0 ||
                   inp.empty()) {
    throw std::runtime_error(delim_pos == std::string::npos ? "n" : "0");
  }

  std::string name = inp.substr(0, delim_pos);
  double temp = 0;
  auto rem = inp.substr(delim_pos + 1, n);
  auto dec_pos = rem.find('.');
  bool pre_dec = true;
  int mod = (rem[0] == '-' ? -1 : 1);
  bool is_neg = rem[0] == '-';
  for (int i = rem.size() - 1; i >= is_neg; i--) {
    char c = rem[i];

    if (pre_dec) {
      if (c == '.') {
        pre_dec = false;
        continue;
      }
      temp += (double)(int)(c - '0') / (double)quick_pow10(i - dec_pos);
    } else {
      temp += (int)(c - '0') * quick_pow10(dec_pos - i - 1);
    }
  }

  temp *= mod;

  return {name, temp};
}

/* Process a chunk and populate the given map */
auto process_chunk(inter_map_tp &city_map, char *buf, size_t size) -> void {
  std::istringstream m_file;
  m_file.rdbuf()->pubsetbuf(buf, size);
  std::string line;
  while (std::getline(m_file, line)) {
    [[unlikely]] if (line.empty() || line.size() < 3)
      continue;
    auto [city_name, temp] = parse_entry(line);
    city_map[city_name].update(temp);
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
  std::cout << "got chunk sizes of " << chunk_size << " for file size "
            << f_stat.st_size << std::endl;

  char *buf = reinterpret_cast<char *>(
      mmap(NULL, f_stat.st_size, PROT_READ, MAP_PRIVATE, file_ptr->_fileno, 0));
  assert(buf != (char *)-1);

  std::map<std::string, CityEntry> city_map;
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
    std::cout << " read to " << read_to << " chunk end " << cur_chunk_end
              << " size " << f_stat.st_size << std::endl;
  }

  pool.clear();

  // merge all maps
  for (auto &inter : maps) {
    for (auto const &[k, v] : inter) {
      city_map[k].update_entry(v);
    }
  }

  std::cout << "{";
  const int n = city_map.size();
  for (int i = 0; auto const &[k, v] : city_map) {
    std::cout << k << v;
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
