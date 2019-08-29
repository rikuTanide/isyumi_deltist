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
}
