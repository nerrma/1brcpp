##
# 1 Billion Row Challenge in C++
#
# @file
# @version 0.1

.PHONY: compile

compile: main.cpp
	g++ --std=gnu++20 -march=native -march=native -mtune=native -flto -Wall \
	-Wextra -Wpedantic -g -lpthread -m64 -O3 \
	-Wformat=2 -Wconversion -Wundef -Winline -Wimplicit-fallthrough \
	main.cpp -o target/1brc

debug: main.cpp
	g++ --std=gnu++20 -march=native -g3 main.cpp -o target/1brc

run: compile
	./target/1brc ./data/measurements.txt

clean:
	rm ./target/1brc


# end
