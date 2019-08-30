part of isyumi_deltist;

// ただのプロキシ
// いるのか？
class RocksDB {
  void createDB(String path, List<String> columnFamilyNames) {
    rocksdb_api.createDB(path, columnFamilyNames);
  }

  List<int> open(String path, List<String> columnFamilyNames) {
    return rocksdb_api.open(path, columnFamilyNames);
  }

  void close(int db, List<int> columnFamilyHandlers) {
    rocksdb_api.close(db, columnFamilyHandlers);
  }

  void put(int db, int columnFamilyHandler, Uint8List key, Uint8List value) {
    rocksdb_api.put(db, columnFamilyHandler, key, value);
  }

  void delete(int db, int columnFamilyHandler, Uint8List key) {
    rocksdb_api.delete(db, columnFamilyHandler, key);
  }

  Uint8List get(int db, int columnFamilyHandle, Uint8List key) {
    return rocksdb_api.get(db, columnFamilyHandle, key);
  }

  int createWriteBatch() {
    return rocksdb_api.createWriteBatch();
  }

  void writeBatch_Put(
      int handle, int columnFamilyHandle, Uint8List key, Uint8List value) {
    rocksdb_api.writeBatch_Put(handle, columnFamilyHandle, key, value);
  }

  void writeBatch_Delete(int columnFamilyHandle, int handle, Uint8List key) {
    rocksdb_api.writeBatch_Delete(columnFamilyHandle, handle, key);
  }

  void write(int db, int writeBatch) {
    rocksdb_api.write(db, writeBatch);
  }

  int seek_Start(int db, int handler, Uint8List prefix) {
    return rocksdb_api.seek_Start(db, handler, prefix);
  }

  Uint8List seek_Key(int seek) {
    return rocksdb_api.seek_Key(seek);
  }

  Uint8List seek_Value(int seek) {
    return rocksdb_api.seek_Value(seek);
  }

  bool seek_Next(int seek) {
    return rocksdb_api.seek_Next(seek);
  }

  bool seek_HasNext(int seek) {
    return rocksdb_api.seek_HasNext(seek);
  }

  void seek_End(int seek) {
    return rocksdb_api.seek_End(seek);
  }

  Map<Uint8List, Uint8List> seekAll(int db, int handler, Uint8List prefix) {
    var results = <Uint8List, Uint8List>{};
    var iter = seek_Start(db, handler, prefix);
    for (; seek_HasNext(iter); seek_Next(iter)) {
      var key = seek_Key(iter);
      var value = seek_Value(iter);
      results[key] = value;
    }
    seek_End(iter);
    return results;
  }
}
