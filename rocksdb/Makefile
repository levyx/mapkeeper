include ../Makefile.config

EXECUTABLE = mapkeeper_rocksdb

all :
	g++ -std=gnu++11 -Wall -o $(EXECUTABLE) *cpp -I $(THRIFT_DIR)/include/thrift -I $(THRIFT_DIR)/include \
	-L/usr/local/lib -I/usr/local/include\
	-lz -lbz2 -lpthread -lboost_thread -lboost_filesystem -lboost_program_options -lboost_system \
	-lthrift -lrocksdb -lleveldb -I ../thrift/gen-cpp \
	-L $(THRIFT_DIR)/lib \
	-L ../thrift/gen-cpp -lmapkeeper \
	-Wl,-rpath,\$$ORIGIN/../thrift/gen-cpp \
	-Wl,-rpath,$(THRIFT_DIR)/lib

run:
	./$(EXECUTABLE) --sync

clean :
	- rm -rf $(THRIFT_SRC) $(EXECUTABLE) *.o 

wipe:
	- rm -rf data/*
