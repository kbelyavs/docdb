CXX=g++
CXXFLAGS=-std=c++11 -Wall
INC=-I./include

engine_obj := $(patsubst %.c,%.o,$(wildcard engine/*.cpp))
objects := $(patsubst %.c,%.o,$(wildcard *.cpp)) $(engine_obj)

docdb: clean $(objects)
	$(CXX) $(INC) -o docdb $(objects) $(CXXFLAGS)

test: docdb lint
	./docdb

#perf: docdb
#	time ./docdb

lint:
	cpplint --filter=-build/include_subdir,-build/c++11 --recursive *

clean:
	rm -f docdb
