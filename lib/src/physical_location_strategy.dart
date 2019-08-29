part of isyumi_deltist;

// テーブルとデータをディスクにどのように配置するかの戦略
//  Tableごとに１DB
//  全てのDBに
//  INDEX_1/INDEX_2...
//  table
//
//
//
//  Text型とString型は、複数のテーブルで使いまわされると考えられるので全体で一つのTextStoreで管理する
//
//  とりあえずprimary Keyを保存するところまで

class PhysicalLocationStrategy {
  void createTables(RocksDB rocksdb, String dbPath, List<Table> tables) {
    var storePath = path.join(dbPath, "store");
    Directory(storePath).createSync();
    rocksdb.createDB(storePath, ["strToId", "idToStr", "hashToText", "meta"]);

    var tablesPath = path.join(dbPath, "tables");
    Directory(tablesPath).createSync();
    tables.forEach((t) {
      createTable(rocksdb, dbPath, t);
    });
  }

  void createTable(RocksDB rocksdb, String dbPath, Table table) {
    var tablePath = path.join(dbPath, "tables", table.tableName);
    rocksdb.createDB(tablePath, ["table"]);
  }

  OpenTablesResults openTables(
      RocksDB rocksdb, String dbPath, List<Table> tables) {
    var storePath = path.join(dbPath, "store");
    Directory(storePath).createSync();
    var storePointers =
        rocksdb.open(storePath, ["strToId", "idToStr", "hashToText", "meta"]);

    var openTablesResults = OpenTablesResults();

    openTablesResults.storeRocksDBPointer = StoreRocksDBPointer()
      ..dbPointer = storePointers[0]
      ..defaultColumnFamilyPointer = storePointers[1]
      ..strToIdPointer = storePointers[2]
      ..idToStrPointer = storePointers[3]
      ..hashToTextPointer = storePointers[4]
      ..metaTablePointer = storePointers[5];

    tables.forEach((t) {
      var tableRocksDBPointer = open(rocksdb, dbPath, t);
      openTablesResults.tableRocksDBPointers[t] = tableRocksDBPointer;
    });
    return openTablesResults;
  }

  TableRocksDBPointer open(RocksDB rocksdb, String dbPath, Table table) {
    var tablePath = path.join(dbPath, "tables", table.tableName);
    var handlers = rocksdb.open(tablePath, ["table"]);
    return TableRocksDBPointer()
      ..dbPointer = handlers[0]
      ..defaultColumnFamilyPointer = handlers[1]
      ..tablePointer = handlers[2];
  }

  void close(
      RocksDB rocksdb,
      Map<TableOrView, TableRocksDBPointer> tableRocksDBPointers,
      StoreRocksDBPointer storeRocksDBPointer) {
    tableRocksDBPointers.forEach((_, handler) {
      rocksdb.close(handler.dbPointer,
          [handler.defaultColumnFamilyPointer, handler.tablePointer]);
    });

    rocksdb.close(storeRocksDBPointer.dbPointer, [
      storeRocksDBPointer.defaultColumnFamilyPointer,
      storeRocksDBPointer.strToIdPointer,
      storeRocksDBPointer.idToStrPointer,
      storeRocksDBPointer.hashToTextPointer,
      storeRocksDBPointer.metaTablePointer,
    ]);
  }

  void put(
      RocksDB rocksdb,
      Map<TableOrView, TableRocksDBPointer> tableRocksDBPointers,
      Table table,
      Uint8List key,
      Uint8List value) {
    var handler = tableRocksDBPointers[table];
    rocksdb.put(handler.dbPointer, handler.tablePointer, key, value);
  }

  Uint8List get(
      RocksDB rocksdb,
      Map<TableOrView, TableRocksDBPointer> tableRocksDBPointers,
      TableOrView table,
      Uint8List key) {
    var pointers = tableRocksDBPointers[table];
    return rocksdb.get(pointers.dbPointer, pointers.tablePointer, key);
  }
}

class OpenTablesResults {
  StoreRocksDBPointer storeRocksDBPointer;
  Map<Table, TableRocksDBPointer> tableRocksDBPointers = {};
}

class TableRocksDBPointer {
  int dbPointer;
  int defaultColumnFamilyPointer;
  int tablePointer;
}

class StoreRocksDBPointer {
  int dbPointer;
  int defaultColumnFamilyPointer;
  int strToIdPointer;
  int idToStrPointer;
  int hashToTextPointer;
  int metaTablePointer;
}
