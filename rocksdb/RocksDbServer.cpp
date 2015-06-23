/*
 * Copyright 2015 Levyx, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
/**
 * This is a implementation of the mapkeeper interface that uses 
 * rocksdb (rocksdb.org and https://github.com/facebook/rocksdb).
 *
 * The code here is mostly based on the LevelDB implementation of
 * mapkeeper.
 *
 * https://github.com/m1ch1/mapkeeper/tree/master/leveldb
 *
 */
#include <iostream>
#include <cstdio>
#include "MapKeeper.h"

#include <leveldb/db.h>
#include <leveldb/cache.h>

#include <rocksdb/options.h>
#include <rocksdb/db.h>
#include <rocksdb/env.h>
#include <rocksdb/table.h>
#include <rocksdb/cache.h>
#include <rocksdb/filter_policy.h>
#include <rocksdb/utilities/leveldb_options.h>

#include <boost/program_options.hpp>
#include <boost/ptr_container/ptr_map.hpp>
#include <boost/thread/shared_mutex.hpp>
#include <boost/filesystem.hpp>
#include <sys/types.h>
#include <dirent.h>
#include <errno.h>
#include <arpa/inet.h>

#include <protocol/TBinaryProtocol.h>
#include <server/TThreadedServer.h>
#include <transport/TServerSocket.h>
#include <transport/TBufferTransports.h>


using namespace ::apache::thrift;
using namespace ::apache::thrift::protocol;
using namespace ::apache::thrift::transport;
using namespace ::apache::thrift::server;
using namespace ::apache::thrift::concurrency;

//using boost::shared_ptr;
using namespace boost::filesystem;
namespace po = boost::program_options;

using namespace mapkeeper;
using namespace rocksdb;

int syncmode;
int blindinsert;
int blindupdate;
class RocksDbServer: virtual public MapKeeperIf {
public:
    RocksDbServer(const std::string& directoryName,
                  uint32_t writeBufferSizeMb, uint32_t blockCacheSizeMb) : 
        directoryName_(directoryName),
        writeBufferSizeMb_(writeBufferSizeMb),
	blockCacheSizeMb_(blockCacheSizeMb) {
		// open all the existing databases
		rocksdb::DB *db;
		rocksdb::Options options;
		BlockBasedTableOptions bb_opt;
		rocksdb::Env* env = rocksdb::Env::Default();

		env->SetBackgroundThreads(1, Env::Priority::HIGH);
		env->SetBackgroundThreads(2, Env::Priority::LOW);

		options.create_if_missing = false;
		options.error_if_exists = false;

		std::shared_ptr<Cache> cache_ = rocksdb::NewLRUCache(blockCacheSizeMb_ * 1024 * 1024);
		std::shared_ptr<const FilterPolicy> filter_policy_(NewBloomFilterPolicy(10, true));
		bb_opt.block_cache = cache_;
		bb_opt.filter_policy = filter_policy_;
		bb_opt.block_size = 65536;
		options.table_factory.reset(NewBlockBasedTableFactory(bb_opt));

		options.write_buffer_size = writeBufferSizeMb_ * 1024 * 1024; // 1 GB default
		options.compression = kNoCompression;
		options.IncreaseParallelism();
		options.OptimizeLevelStyleCompaction();
		options.env = env;
		options.disableDataSync = true;

		options.compaction_style = kCompactionStyleLevel;
		options.max_write_buffer_number = 4;
		options.target_file_size_base = 67108864; // 64MB
		options.max_background_compactions = 8;
		options.level0_file_num_compaction_trigger = 4;
		options.level0_slowdown_writes_trigger = 17;
		options.level0_stop_writes_trigger = 24;
		options.num_levels = 4;
		options.max_bytes_for_level_base = options.level0_file_num_compaction_trigger * options.write_buffer_size;
		options.max_bytes_for_level_multiplier = 8;


		boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;

		directory_iterator end_itr;
		for (directory_iterator itr(directoryName); itr != end_itr;itr++) {
			if (is_directory(itr->status())) {
				std::string mapName = itr->path().filename().string();
				rocksdb::Status status = rocksdb::DB::Open(options, itr->path().string(), &db);
				assert(status.ok());
				maps_.insert(mapName, db);
			}
		}
	}

    ResponseCode::type ping() {
        return ResponseCode::Success;
    }

    ResponseCode::type addMap(const std::string& mapName) {
        rocksdb::DB* db;
        rocksdb::Options options;
	rocksdb::Env* env = rocksdb::Env::Default();
	BlockBasedTableOptions bb_opt;

	env->SetBackgroundThreads(1, Env::Priority::HIGH);
	env->SetBackgroundThreads(2, Env::Priority::LOW);

        options.create_if_missing = true;
        options.error_if_exists = false;

	std::shared_ptr<Cache> cache_ = rocksdb::NewLRUCache(blockCacheSizeMb_ * 1024 * 1024);
	std::shared_ptr<const FilterPolicy> filter_policy_(NewBloomFilterPolicy(10, true));
	bb_opt.block_cache = cache_;
	bb_opt.filter_policy = filter_policy_;
	bb_opt.block_size = 65536;
	options.table_factory.reset(NewBlockBasedTableFactory(bb_opt));

        options.write_buffer_size = writeBufferSizeMb_ * 1024 * 1024; // 1 GB default
	options.max_write_buffer_number = 4;
	options.compression = kNoCompression;
	options.IncreaseParallelism();
	options.OptimizeLevelStyleCompaction();
	options.env = env;
	options.disableDataSync = true;

	options.compaction_style = kCompactionStyleLevel;
	options.max_background_compactions = 8;
	options.target_file_size_base = 67108864; // 64MB
	options.level0_file_num_compaction_trigger = 4;
	options.level0_slowdown_writes_trigger = 17;
	options.level0_stop_writes_trigger = 24;
	options.num_levels = 4;
	options.max_bytes_for_level_base = options.level0_file_num_compaction_trigger * options.write_buffer_size;
	options.max_bytes_for_level_multiplier = 8;

	rocksdb::Status status = rocksdb::DB::Open(options, directoryName_ + "/" + mapName, &db);
	if (!status.ok()) {
		// TODO check return code
		fprintf(stderr, "Open status (%s)\n", status.ToString().c_str());
		return ResponseCode::MapExists;
	}
	std::string mapName_ = mapName;
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        maps_.insert(mapName_, db);
        return ResponseCode::Success;
    }

    ResponseCode::type dropMap(const std::string& mapName) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr;
        boost::unique_lock< boost::shared_mutex> writeLock(mutex_);;
        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        maps_.erase(itr);
        DestroyDB(directoryName_ + "/" + mapName, rocksdb::Options());
        return ResponseCode::Success;
    }

    void listMaps(StringListResponse& _return) {
        boost::unique_lock< boost::shared_mutex > writeLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr;
        for (itr = maps_.begin(); itr != maps_.end(); itr++) {
            _return.values.push_back(itr->first);
        }
        _return.responseCode = ResponseCode::Success;
    }

    void scan(RecordListResponse& _return, const std::string& mapName,
              const ScanOrder::type order,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::Success;
            return;
        }
        if (order == ScanOrder::Ascending) {
            scanAscending(_return, itr->second, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
        } else {
            scanDescending(_return, itr->second, startKey, startKeyIncluded, endKey, endKeyIncluded, maxRecords, maxBytes);
        }
    }

    void scanAscending(RecordListResponse& _return, rocksdb::DB* db, 
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        _return.responseCode = ResponseCode::ScanEnded;
        int numBytes = 0;
        rocksdb::Iterator* itr = db->NewIterator(rocksdb::ReadOptions());
        _return.responseCode = ResponseCode::ScanEnded;
        for (itr->Seek(startKey); itr->Valid(); itr->Next()) {
            Record record;
            record.key = itr->key().ToString();
            record.value = itr->value().ToString();
            if (!startKeyIncluded && startKey == record.key) {
                continue;
            }
            if (!endKey.empty()) {
                if (endKeyIncluded && endKey < record.key) {
                  break;
                }
                if (!endKeyIncluded && endKey <= record.key) {
                  break;
                }
            }
            numBytes += record.key.size() + record.value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = ResponseCode::Success;
                break;
            }
        }
        assert(itr->status().ok());
        delete itr;
    }

    void scanDescending(RecordListResponse& _return, rocksdb::DB* db,
              const std::string& startKey, const bool startKeyIncluded, 
              const std::string& endKey, const bool endKeyIncluded,
              const int32_t maxRecords, const int32_t maxBytes) {
        int numBytes = 0;
        rocksdb::Iterator* itr = db->NewIterator(rocksdb::ReadOptions());
        _return.responseCode = ResponseCode::ScanEnded;
        if (endKey.empty()) {
            itr->SeekToLast();
        } else {
            itr->Seek(endKey);
        }
        for (; itr->Valid(); itr->Prev()) {
            Record record;
            record.key = itr->key().ToString();
            record.value = itr->value().ToString();
            if (!endKeyIncluded && endKey == record.key) {
                continue;
            }
            if (startKeyIncluded && startKey > record.key) {
                break;
            }
            if (!startKeyIncluded && startKey >= record.key) {
                break;
            }
            numBytes += record.key.size() + record.value.size();
            _return.records.push_back(record);
            if (_return.records.size() >= (uint32_t)maxRecords || numBytes >= maxBytes) {
                _return.responseCode = ResponseCode::Success;
                break;
            }
        }
        assert(itr->status().ok());
        delete itr;
    }

    void get(BinaryResponse& _return, const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            _return.responseCode = ResponseCode::MapNotFound;
            return;
        }
        rocksdb::Status status = itr->second->Get(rocksdb::ReadOptions(), key, &(_return.value));
        if (status.IsNotFound()) {
            _return.responseCode = ResponseCode::RecordNotFound;
            return;
        } else if (!status.ok()) {
            _return.responseCode = ResponseCode::Error;
            return;
        }
        _return.responseCode = ResponseCode::Success;
    }

    ResponseCode::type put(const std::string& mapName, const std::string& key, const std::string& value) {
        std::string mapName_ = mapName;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr;
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;

        itr = maps_.find(mapName_);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }

        rocksdb::WriteOptions options;
        options.sync = syncmode ? true : false;
	options.disableWAL = true;
        rocksdb::Status status = itr->second->Put(options, key, value);

        if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type insert(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
	if(!blindinsert) {
	  std::string recordValue;
	  rocksdb::Status status = itr->second->Get(rocksdb::ReadOptions(), key, &recordValue);
	  if (status.ok()) {
            return ResponseCode::RecordExists;
	  } else if (!status.IsNotFound()) {
            return ResponseCode::Error;
	  }
	}
        rocksdb::WriteOptions options;
        options.sync = syncmode ? true : false;
	options.disableWAL = true;
	rocksdb::Status status = itr->second->Put(options, key, value);
        if (!status.ok()) {
            printf("insert not ok! %s\n", status.ToString().c_str());
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type update(const std::string& mapName, const std::string& key, const std::string& value) {
        // TODO Get and Put should be within a same transaction
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        std::string recordValue;
	if(!blindupdate) {
	  rocksdb::Status status = itr->second->Get(rocksdb::ReadOptions(), key, &recordValue);
	  if (status.IsNotFound()) {
            return ResponseCode::RecordNotFound;
	  } else if (!status.ok()) {
            return ResponseCode::Error;
	  }
	}
        rocksdb::WriteOptions options;
        options.sync = syncmode ? true : false;
	options.disableWAL = true;
	rocksdb::Status status = itr->second->Put(options, key, value);
        if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

    ResponseCode::type remove(const std::string& mapName, const std::string& key) {
        boost::shared_lock< boost::shared_mutex> readLock(mutex_);;
        boost::ptr_map<std::string, rocksdb::DB>::iterator itr = maps_.find(mapName);
        if (itr == maps_.end()) {
            return ResponseCode::MapNotFound;
        }
        rocksdb::WriteOptions options;
        options.sync = true;
        rocksdb::Status status = itr->second->Delete(options, key);
        if (status.IsNotFound()) {
            return ResponseCode::RecordNotFound;
        } else if (!status.ok()) {
            return ResponseCode::Error;
        }
        return ResponseCode::Success;
    }

private:
    std::string directoryName_; // directory to store db files.
    uint32_t writeBufferSizeMb_; 
    uint32_t blockCacheSizeMb_; 
    rocksdb::Cache* cache_;
    boost::ptr_map<std::string, rocksdb::DB> maps_;
    boost::shared_mutex mutex_; // protect map_
};

int main(int argc, char **argv) {
    int port;
    int writeBufferSizeMb;
    int blockCacheSizeMb;
    std::string dir;
    po::variables_map vm;
    po::options_description config("");
    config.add_options()
        ("help,h", "produce help message")
        ("sync,s", "synchronous writes")
        ("blindinsert,i", "skip record existence check for inserts")
        ("blindupdate,u",  "skip record existence check for updates")
        ("port,p", po::value<int>(&port)->default_value(9090), "port to listen to")
        ("datadir,d", po::value<std::string>(&dir)->default_value("data"), "data directory")
        ("write-buffer-mb,w", po::value<int>(&writeBufferSizeMb)->default_value(1024), "LevelDB write buffer size in MB")
        ("block-cache-mb,b", po::value<int>(&blockCacheSizeMb)->default_value(1024), "LevelDB block cache size in MB")
        ;
    po::options_description cmdline_options;
    cmdline_options.add(config);
    store(po::command_line_parser(argc, argv).options(cmdline_options).run(), vm);
    notify(vm);
    if (vm.count("help")) {
        std::cout << config << std::endl; 
        exit(0);
    }
    syncmode = vm.count("sync");
    blindinsert = vm.count("blindinsert");
    blindupdate = vm.count("blindupdate");
    boost::shared_ptr<RocksDbServer> handler(new RocksDbServer(dir, writeBufferSizeMb, blockCacheSizeMb));
    boost::shared_ptr<TProcessor> processor(new MapKeeperProcessor(handler));
    boost::shared_ptr<TServerTransport> serverTransport(new TServerSocket(port));
    boost::shared_ptr<TTransportFactory> transportFactory(new TFramedTransportFactory());
    boost::shared_ptr<TProtocolFactory> protocolFactory(new TBinaryProtocolFactory());
    TThreadedServer server (processor, serverTransport, transportFactory, protocolFactory);
    server.serve();

    return 0;
}
