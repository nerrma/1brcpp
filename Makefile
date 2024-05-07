##
# 1 Billion Row Challenge in C++
#
# @file
# @version 0.1

all: compile

compile: main.cpp
	g++ --std=gnu++20 -O3 main.cpp -o target/1brc

run: compile
	./target/1brc ./data/measurements.txt

clean:
	rm ./target/1brc


# end
