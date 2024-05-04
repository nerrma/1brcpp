#include <algorithm>
#include <fstream>
#include <iostream>
#include <ostream>
#include <string>
#include <tuple>
#include <map>

const char DELIMITER = ';';

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

};

auto operator<<(std::ostream& os, CityEntry const& city) -> std::ostream&  {
    return os << city.city_min << "/" << city.sum/(double)city.count << "/" << city.city_max;
}

/**
 * Parse entry into {location, temp} pair.
 */
auto parse_entry(std::string const &inp) -> std::pair<std::string, double> {
  const int n = inp.size();

  std::string name = inp.substr(0, inp.find(DELIMITER));
  double min_temp = std::stod(inp.substr(inp.find(DELIMITER) + 1, n));

  return {name, min_temp};
}

auto main(int argc, char *argv[]) -> int {

  if (argc < 2) {
    std::cout << "Usage: ./1brc <filename>" << std::endl;
    return -1;
  }

  std::ifstream m_file(argv[1]);
  if (!m_file.is_open()) {
    std::cout << "Unable to open file!" << std::endl;
    return -1;
  }

  std::map<std::string, CityEntry> city_map;

  std::string line;
  while (std::getline(m_file, line)) {
      auto [city_name, temp] = parse_entry(line);

      city_map[city_name].update(temp);
  }

  std::cout << "{";
  for(auto const& [k, v] : city_map) {
      std::cout << k << v << ", ";
  }
  std::cout << "}";

  return 0;
}
