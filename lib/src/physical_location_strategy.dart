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
  void createTables(RocksDB rocksdb, String dbPath, List<Table> tables,
      List<View> views, List<Index> indexes) {
    var storePath = path.join(dbPath, "store");
    Directory(storePath).createSync();
    rocksdb.createDB(storePath, ["strToId", "idToStr", "hashToText", "meta"]);

    var tablesPath = path.join(dbPath, "tables");
    Directory(tablesPath).createSync();
    tables.forEach((t) {
      var matchIndexes = indexes.where((i) => i.table == t).toList();
      createTable(rocksdb, dbPath, t, matchIndexes);
    });

    var viewsPath = path.join(dbPath, "views");
    Directory(viewsPath).createSync();
    views.forEach((v) {
      var matchIndexes = indexes.where((i) => i.table == v).toList();
      createView(rocksdb, dbPath, v, matchIndexes);
    });
  }

  void createTable(
      RocksDB rocksdb, String dbPath, Table table, List<Index> indexes) {
    var tablePath = path.join(dbPath, "tables", table.tableName);
    var names = indexes.map((i) => i.name);
    rocksdb.createDB(tablePath, ["table", ...names]);
  }

  void createView(
      RocksDB rocksdb, String dbPath, View view, List<Index> indexes) {
    var tablePath = path.join(dbPath, "views", view.tableName);
    var names = indexes.map((i) => i.name);
    rocksdb.createDB(tablePath, ["view", ...names]);
  }

  OpenTablesResults openTables(RocksDB rocksdb, String dbPath,
      List<Table> tables, List<View> views, List<Index> indexes) {
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
      var matchIndexes = indexes.where((i) => i.table == t).toList();
      var tableRocksDBPointer = openTable(rocksdb, dbPath, t, matchIndexes);
      openTablesResults.tableRocksDBPointers[t] = tableRocksDBPointer;
    });

    views.forEach((v) {
      var matchIndexes = indexes.where((i) => i.table == v).toList();
      var viewRocksDBPointer = openView(rocksdb, dbPath, v, matchIndexes);
      openTablesResults.viewRocksDBPointers[v] = viewRocksDBPointer;
    });

    return openTablesResults;
  }

  TableRocksDBPointer openTable(
      RocksDB rocksdb, String dbPath, Table table, List<Index> indexes) {
    var tablePath = path.join(dbPath, "tables", table.tableName);
    var names = indexes.map((i) => i.name);
    var handlers = rocksdb.open(tablePath, ["table", ...names]);

    var indexPointers = <int, int>{};
    for (var i = 0; i < indexes.length; i++) {
      var index = indexes[i];
      indexPointers[index.indexID] = handlers[i + 3];
    }

    return TableRocksDBPointer()
      ..dbPointer = handlers[0]
      ..defaultColumnFamilyPointer = handlers[1]
      ..tablePointer = handlers[2]
      ..indexPointers = indexPointers;
  }

  ViewRocksDBPointer openView(
      RocksDB rocksdb, String dbPath, View view, List<Index> indexes) {
    var tablePath = path.join(dbPath, "views", view.tableName);
    var names = indexes.map((i) => i.name);
    var handlers = rocksdb.open(tablePath, ["view", ...names]);

    var indexPointers = <int, int>{};
    for (var i = 0; i < indexes.length; i++) {
      var index = indexes[i];
      indexPointers[index.indexID] = handlers[i + 3];
    }

    return ViewRocksDBPointer()
      ..dbPointer = handlers[0]
      ..defaultColumnFamilyPointer = handlers[1]
      ..viewPointer = handlers[2]
      ..indexPointers = indexPointers;
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
      Map<View, ViewRocksDBPointer> viewRocksDBPointers,
      TableOrView table,
      Uint8List key,
      Uint8List value) {
    if (table is Table) {
      var handler = tableRocksDBPointers[table];
      rocksdb.put(handler.dbPointer, handler.tablePointer, key, value);
    } else if (table is View) {
      var handler = viewRocksDBPointers[table];
      rocksdb.put(handler.dbPointer, handler.viewPointer, key, value);
    }
  }

  Uint8List get(
      RocksDB rocksdb,
      Map<TableOrView, TableRocksDBPointer> tableRocksDBPointers,
      Map<View, ViewRocksDBPointer> viewRocksDBPointers,
      TableOrView table,
      Uint8List key) {
    if (table is Table) {
      var db = tableRocksDBPointers[table].dbPointer;
      var columnHandle = tableRocksDBPointers[table].tablePointer;
      return rocksdb.get(db, columnHandle, key);
    } else if (table is View) {
      var db = viewRocksDBPointers[table].dbPointer;
      var columnHandle = viewRocksDBPointers[table].viewPointer;
      return rocksdb.get(db, columnHandle, key);
    }
    throw "tableでもviewでもないもの";
  }

  Uint8List getByIndex(
      RocksDB rocksdb,
      Map<TableOrView, TableRocksDBPointer> tableRocksDBPointers,
      Map<View, ViewRocksDBPointer> viewRocksDBPointers,
      TableOrView table,
      Index index,
      Uint8List key) {
    if (table is Table) {
      var db = tableRocksDBPointers[table].dbPointer;
      var columnHandle =
          tableRocksDBPointers[table].indexPointers[index.indexID];
      var reference = rocksdb.get(db, columnHandle, key);
      return get(
          rocksdb, tableRocksDBPointers, viewRocksDBPointers, table, reference);
    } else if (table is View) {
      var db = viewRocksDBPointers[table].dbPointer;
      var columnHandle =
          viewRocksDBPointers[table].indexPointers[index.indexID];
      var reference = rocksdb.get(db, columnHandle, key);
      return get(
          rocksdb, tableRocksDBPointers, viewRocksDBPointers, table, reference);
    }
    throw "tableでもviewでもないもの";
  }

  void putIndex(
      RocksDB rocksdb,
      Map<Table, TableRocksDBPointer> tableRocksDBPointers,
      Map<View, ViewRocksDBPointer> viewRocksDBPointers,
      List<IndexKeyValue> writeDatas) {
    var batch = WriteBatchEachTable()
      ..tableRocksDBPointers = tableRocksDBPointers
      ..viewRocksDBPointers = viewRocksDBPointers
      ..rocksdb = rocksdb;

    for (var keyValue in writeDatas) {
      batch.writeIndex(
          keyValue.index, keyValue.primaryKeys, keyValue.reference);
    }
    batch.writeAll();
  }

  Map<Uint8List, Uint8List> seek(
      RocksDB rocksdb,
      Map<TableOrView, TableRocksDBPointer> tableRocksDBPointers,
      Map<View, ViewRocksDBPointer> viewRocksDBPointers,
      TableOrView table,
      Uint8List prefix) {
    if (table is Table) {
      var db = tableRocksDBPointers[table].dbPointer;
      var columnHandle = tableRocksDBPointers[table].tablePointer;
      return rocksdb.seekAll(db, columnHandle, prefix);
    } else if (table is View) {
      var db = viewRocksDBPointers[table].dbPointer;
      var columnHandle = viewRocksDBPointers[table].viewPointer;
      return rocksdb.seekAll(db, columnHandle, prefix);
    }
    throw "tableでもviewでもない";
  }

  Map<Uint8List, Uint8List> seekByIndex(
      RocksDB rocksdb,
      Map<TableOrView, TableRocksDBPointer> tableRocksDBPointers,
      Map<View, ViewRocksDBPointer> viewRocksDBPointers,
      TableOrView table,
      Index index,
      Uint8List prefix) {
    int getIndexPointer() {
      if (table is Table) {
        return tableRocksDBPointers[table].indexPointers[index.indexID];
      } else if (table is View) {
        return viewRocksDBPointers[table].indexPointers[index.indexID];
      }
      throw "tableでもviewでもない";
    }

    int getDbPointer() {
      if (table is Table) {
        return tableRocksDBPointers[table].dbPointer;
      } else if (table is View) {
        return viewRocksDBPointers[table].dbPointer;
      }
      throw "tableでもviewでもない";
    }

    var db = getDbPointer();
    var indexHandle = getIndexPointer();

    return rocksdb.seekAll(db, indexHandle, prefix).map((k, v) => MapEntry(
        v, get(rocksdb, tableRocksDBPointers, viewRocksDBPointers, table, v)));
  }

  void delete(
      RocksDB rocksDB,
      Map<Table, TableRocksDBPointer> tableRocksDBPointers,
      Map<View, ViewRocksDBPointer> viewRocksDBPointers,
      TableOrView table,
      Uint8List key) {
    var pointer = DBPointers()
      ..tableRocksDBPointers = tableRocksDBPointers
      ..viewRocksDBPointers = viewRocksDBPointers;

    var db = pointer.getDbPointer(table);
    var columnHandle = pointer.getTablePointer(table);
    rocksDB.delete(db, columnHandle, key);
  }

  void deleteIndex(RocksDB rocksDB, DBPointers dbPointers, TableOrView table,
      List<IndexKeyValue> indexKeyValues) {
    var db = dbPointers.getDbPointer(table);
    for (var index in indexKeyValues) {
      rocksDB.delete(db, dbPointers.getIndexPointer(table, index.index),
          index.primaryKeys);
    }
  }
}

class DBPointers {
  Map<Table, TableRocksDBPointer> tableRocksDBPointers;
  Map<View, ViewRocksDBPointer> viewRocksDBPointers;

  int getDbPointer(TableOrView table) {
    if (table is Table) {
      return tableRocksDBPointers[table].dbPointer;
    } else if (table is View) {
      return viewRocksDBPointers[table].dbPointer;
    }
    throw "tableでもviewでもない";
  }

  int getTablePointer(TableOrView table) {
    if (table is Table) {
      return tableRocksDBPointers[table].tablePointer;
    } else if (table is View) {
      return viewRocksDBPointers[table].viewPointer;
    }
    throw "tableでもviewでもない";
  }

  int getIndexPointer(TableOrView table, Index index) {
    if (table is Table) {
      return tableRocksDBPointers[table].indexPointers[index.indexID];
    } else if (table is View) {
      return viewRocksDBPointers[table].indexPointers[index.indexID];
    }
    throw "tableでもviewでもない";
  }
}

// 複数のテーブルにWriteBatchする時にテーブルごとに分割してくれる君
class WriteBatchEachTable {
  Map<Index, int> _tableIndexBatches = {};
  Map<Index, int> _viewIndexBatches = {};
  Map<Table, TableRocksDBPointer> tableRocksDBPointers;
  Map<View, ViewRocksDBPointer> viewRocksDBPointers;

  RocksDB rocksdb;

  void writeIndex(Index index, Uint8List key, Uint8List value) {
    var table = index.table;
    if (table is Table) {
      _tableIndexBatches.putIfAbsent(index, () => rocksdb.createWriteBatch());
      var indexHandle =
          tableRocksDBPointers[table].indexPointers[index.indexID];
      rocksdb.writeBatch_Put(
          _tableIndexBatches[index], indexHandle, key, value);
    } else if (table is View) {
      _viewIndexBatches.putIfAbsent(index, () => rocksdb.createWriteBatch());
      var indexHandle = viewRocksDBPointers[table].indexPointers[index.indexID];
      rocksdb.writeBatch_Put(_viewIndexBatches[index], indexHandle, key, value);
    } else {
      throw "viewでもtableでもない";
    }
  }

  void writeAll() {
    for (var index in _tableIndexBatches.keys) {
      var batch = _tableIndexBatches[index];
      var dbPointer = tableRocksDBPointers[index.table].dbPointer;
      rocksdb.write(dbPointer, batch);
    }
    for (var index in _viewIndexBatches.keys) {
      var batch = _viewIndexBatches[index];
      var dbPointer = viewRocksDBPointers[index.table].dbPointer;
      rocksdb.write(dbPointer, batch);
    }
  }
}

class OpenTablesResults {
  StoreRocksDBPointer storeRocksDBPointer;
  Map<Table, TableRocksDBPointer> tableRocksDBPointers = {};
  Map<View, ViewRocksDBPointer> viewRocksDBPointers = {};
}

class TableRocksDBPointer {
  int dbPointer;
  int defaultColumnFamilyPointer;
  int tablePointer;
  Map<int, int> indexPointers;
}

class ViewRocksDBPointer {
  int dbPointer;
  int defaultColumnFamilyPointer;
  int viewPointer;
  Map<int, int> indexPointers;
}

class StoreRocksDBPointer {
  int dbPointer;
  int defaultColumnFamilyPointer;
  int strToIdPointer;
  int idToStrPointer;
  int hashToTextPointer;
  int metaTablePointer;
}
