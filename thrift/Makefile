include ../Makefile.config

all:
	$(THRIFT_DIR)/bin/thrift --gen cpp mapkeeper.thrift
	make -C gen-cpp

clean:
	make -C gen-cpp clean
	cd gen-java && mvn clean
