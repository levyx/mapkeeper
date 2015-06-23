## Dependencies

### Boost
  
    wget http://superb-sea2.dl.sourceforge.net/project/boost/boost/1.48.0/boost_1_48_0.tar.gz
    tar xfvz boost_1_48_0.tar.gz
    cd boost_1_48_0
    ./bootstrap.sh --prefix=/usr/local
    sudo ./b2 install 

### RockDB

    https://github.com/facebook/rocksdb.git
    cd rocksdb
    make static_lib
    sudo make install

## Running RocksDB MapKeeper Server

After installing all the dependencies, run:

    make

to compile. To start the server, execute the command:

    ./mapkeeper_rocksdb

You can see the complete list of available options by passing `-h` to `mapkeeper_leveldb` 
command.
  
## Configuration Parameters

There are many tunable parameters that affects RocksDB performance.. 

### `--sync | -s`

Synchronous write is turned off by default. Use this option If you need the write
operations to be durable, 

### `--blindinsert | -i`

By default, MapKeeper server tries to read the record before writing the new record
to RocksDB, and insert fails with `ResponseCode::RecordExists` if the record already
exists. Use this option if you don't care whether the record already exists in the
database.
  
### `--blindupdate | -u`
    
Similar to inserts, MapKeeper server reads the record before applying update by default.
Use this option to bypass the record existence check.

### `--write-buffer-mb | -w`

Write buffer size in megabytes. In general, larger buffer means better performance
and longer recovery time during startup. Default to 1024MB. 

### `--block-cache-mb | -b`

Block Cache size in megabytes (default to 1024MB). Again, bigger the better.

## Related Pages

* [Official RocksDB Documentation](www.rocksdb.org)
* [RocksDB Tuning Tips](https://github.com/facebook/rocksdb/wiki/RocksDB-Tuning-Guide)
