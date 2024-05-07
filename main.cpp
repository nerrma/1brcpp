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
#include <mutex>
#include <ostream>
#include <sstream>
#include <stdexcept>
#include <streambuf>
#include <string>
#include <string_view>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <tuple>
#include <unistd.h>
#include <unordered_map>
#include <vector>

const char DELIMITER = ';';
const size_t PAGE_SIZE = 0x1000;
const size_t NUM_THREADS = 20;
const size_t CHUNK_PADDING = 20;

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
  return os << city.city_min << "/" << city.sum / (double)city.count << "/"
            << city.city_max;
}

inline auto round_to_nearest_page(size_t num) -> size_t {
  return num + PAGE_SIZE - 1 - (num + PAGE_SIZE - 1) % PAGE_SIZE;
}

typedef std::unordered_map<std::string, CityEntry> inter_map_tp;

constexpr int quick_pow10(int n) {
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
  if (delim_pos == std::string::npos || delim_pos == 0 || inp.empty()) {
    throw std::runtime_error(delim_pos == std::string::npos ? "n" : "0");
  }

  std::string name = inp.substr(0, delim_pos);
  double temp = 0;
  bool pre_dec = true;
  bool is_negative = false;
  auto rem = inp.substr(delim_pos + 1, n);
  int dec_pos = rem.find('.');
  for (int i = rem.size(); i >= 0; i--) {
    char c = rem[i];
    if (c == '.') {
      pre_dec = false;
      continue;
    }

    if (c == '-') {
      is_negative = true;
      continue;
    }

    if (pre_dec) {
      temp += (double)(c - '0') / quick_pow10(i - dec_pos);
    } else {
      temp += (int)(c - '0') * quick_pow10(dec_pos - i);
    }
  }

  if (is_negative)
    temp = -temp;

  return {name, temp};
}

/* Process a chunk and populate the given map */
auto process_chunk(inter_map_tp &city_map, size_t chunk_size, int fd,
                   size_t file_size, size_t start) -> void {

  chunk_size = std::min(chunk_size, file_size - start);

  void *buf =
      mmap(NULL, chunk_size + CHUNK_PADDING, PROT_READ, MAP_PRIVATE, fd, start);
  assert(buf != (void *)-1);

  std::istringstream m_file;
  m_file.rdbuf()->pubsetbuf(reinterpret_cast<char *>(buf),
                            chunk_size + CHUNK_PADDING);
  std::string line;
  bool needs_overrun = false;
  while (std::getline(m_file, line) &&
         (m_file.tellg() <= chunk_size || needs_overrun)) {
    try {
      auto [city_name, temp] = parse_entry(line);
      city_map[city_name].update(temp);
      needs_overrun = false;
    } catch (std::runtime_error const &e) {
      if (e.what()[0] == 'n') {
        needs_overrun = true;
      }
    }
  }

  munmap(buf, chunk_size);
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

  auto chunk_size = round_to_nearest_page(f_stat.st_size / NUM_THREADS);
  std::cout << "got chunk sizes of " << chunk_size << " for file size "
            << f_stat.st_size << std::endl;

  std::map<std::string, CityEntry> city_map;
  std::vector<std::jthread> pool;
  std::vector<inter_map_tp> maps(NUM_THREADS);
  pool.reserve(NUM_THREADS);

  // bind variables to arguments (as a capture)
  auto process_chunk_bind =
      std::bind(process_chunk, std::placeholders::_1, chunk_size,
                file_ptr->_fileno, f_stat.st_size, std::placeholders::_2);
  for (int i = 0; i < NUM_THREADS; i++) {
    pool.push_back(
        std::jthread(process_chunk_bind, std::ref(maps[i]), i * chunk_size));
  }

  pool.clear();

  // merge all maps
  for (auto &inter : maps) {
    for (auto const &[k, v] : inter) {
      city_map[k].update_entry(v);
    }
  }

  std::cout << "{";
  for (auto const &[k, v] : city_map) {
    std::cout << k << v << ", ";
  }
  std::cout << "}\n";

  close(file_ptr->_fileno);

  return 0;
}
