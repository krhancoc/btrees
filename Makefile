CXXFLAGS="--std=c++17"

objects = main.o btree.o buf.o

main: $(objects)
	c++ $(objects) -o main
.PHONY: main

$(objects): %.o: %.cc

clean:
	rm $(objects)
	rm main
.PHONY: clean

