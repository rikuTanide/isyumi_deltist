part of isyumi_deltist;

class StringStore {
  Uint8List autoIncrementIdKey = intToU8List(1);

  Uint8List idToString(
      RocksDB rocksdb, StoreRocksDBPointer storeRocksDBPointer, Uint8List id) {
    return rocksdb.get(
        storeRocksDBPointer.dbPointer, storeRocksDBPointer.idToStrPointer, id);
  }

  Uint8List stringToId(
      RocksDB rocksdb, StoreRocksDBPointer storeRocksDBPointer, Uint8List str) {
    var id = rocksdb.get(
        storeRocksDBPointer.dbPointer, storeRocksDBPointer.strToIdPointer, str);
    if (id == null) {
      var newAutoIncrementIDUintList =
          createNewAutoIncrementId(rocksdb, storeRocksDBPointer);
      rocksdb.put(storeRocksDBPointer.dbPointer,
          storeRocksDBPointer.strToIdPointer, str, newAutoIncrementIDUintList);
      rocksdb.put(storeRocksDBPointer.dbPointer,
          storeRocksDBPointer.idToStrPointer, newAutoIncrementIDUintList, str);
      return newAutoIncrementIDUintList;
    }
    return id;
  }

  Uint8List createNewAutoIncrementId(
      RocksDB rocksdb, StoreRocksDBPointer storeRocksDBPointer) {
    var autoIncrementIdUintList = rocksdb.get(storeRocksDBPointer.dbPointer,
        storeRocksDBPointer.metaTablePointer, autoIncrementIdKey);
    if (autoIncrementIdUintList == null) {
      var newAutoIncrementIdUintList = intToU8List(0);
      rocksdb.put(
          storeRocksDBPointer.dbPointer,
          storeRocksDBPointer.metaTablePointer,
          autoIncrementIdKey,
          newAutoIncrementIdUintList);
      return newAutoIncrementIdUintList;
    }
    var autoIncrementId = u8ListToInt(autoIncrementIdUintList);
    var newAutoIncrementId = autoIncrementId + 1;
    var newAutoIncrementIdUintList = intToU8List(newAutoIncrementId);
    rocksdb.put(
        storeRocksDBPointer.dbPointer,
        storeRocksDBPointer.metaTablePointer,
        autoIncrementIdKey,
        newAutoIncrementIdUintList);
    return newAutoIncrementIdUintList;
  }
}

class TextStore {
  Uint8List textToHash(RocksDB rocksdb, StoreRocksDBPointer storeRocksDBPointer,
      Uint8List text) {
    var bytes = Uint8List.fromList(sha256.convert(text).bytes);
    if (rocksdb.get(storeRocksDBPointer.dbPointer,
            storeRocksDBPointer.hashToTextPointer, bytes) ==
        null) {
      rocksdb.put(storeRocksDBPointer.dbPointer,
          storeRocksDBPointer.hashToTextPointer, bytes, text);
    }
    return bytes;
  }

  Uint8List hashToText(RocksDB rocksdb, StoreRocksDBPointer storeRocksDBPointer,
      Uint8List hash) {
    return rocksdb.get(storeRocksDBPointer.dbPointer,
        storeRocksDBPointer.hashToTextPointer, hash);
  }
}
