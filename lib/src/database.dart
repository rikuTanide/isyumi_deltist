part of isyumi_deltist;

class GlobalState {
  List<TableOrView> tableViews = []; // 全部のテーブルとビュー
  List<Table> tables = []; // あとでRawTableクラスを作る
  List<View> views = [];
  Map<Table, TableRocksDBPointer> tableRocksDBPointers = {};
  Map<View, ViewRocksDBPointer> viewRocksDBPointers = {};
  StoreRocksDBPointer storeRocksDBPointer = StoreRocksDBPointer();

  // テーブル更新の戦略
  List<RawDataUpdateStrategy> rawDataUpdateStrategies = [];
  List<BytesReadStrategy> bytesReadStrategies = [];
  List<MaterializeStrategy> materializeStrategies;
  List<Index> indexes;
}

class Middleware {
  StringStore stringStore = StringStore();
  TextStore textStore = TextStore();
  RocksDB rocksDB = RocksDB();
  PhysicalLocationStrategy physicalLocationStrategy =
      PhysicalLocationStrategy();
}

// ここ以外でGlobalStateを書き換えない
class Actions {
  void setTables(
      GlobalState globalState, List<Table> tables, List<View> views) {
    var tableViews = <TableOrView>[]..addAll(tables)..addAll(views);

    _checkDuplicatedTableName(tableViews);
    _checkPrimaryKeys(tables);
    _checkViewColumnParent(views);

    var indexStrategies = createIndexStrategies(views);

    globalState
      ..tables = tables
      ..views = views
      ..tableViews = tableViews
      ..rawDataUpdateStrategies = createRawDataUpdateStrategies(tables)
      ..bytesReadStrategies = createBytesReadStrategies(tableViews)
      ..indexes = indexStrategies.indexes
      ..materializeStrategies = indexStrategies.materializeStrategies;
  }

  // DBの初期化
  void create(String dbPath, GlobalState globalState, Middleware middleware) {
    middleware.physicalLocationStrategy.createTables(middleware.rocksDB, dbPath,
        globalState.tables, globalState.views, globalState.indexes);
  }

  // DBを開きハンドラを登録する
  void open(String dbPath, GlobalState globalState, Middleware middleware) {
    var results = middleware.physicalLocationStrategy.openTables(
        middleware.rocksDB,
        dbPath,
        globalState.tables,
        globalState.views,
        globalState.indexes);

    globalState
      ..storeRocksDBPointer = results.storeRocksDBPointer
      ..tableRocksDBPointers = results.tableRocksDBPointers
      ..viewRocksDBPointers = results.viewRocksDBPointers;
  }

  ChangeDataList write(
      GlobalState globalState, Middleware middleware, WritableRow wr) {
    var table = wr._table;
    _checkDatabaseTable(globalState, table);
    _checkFullyColumnValues(wr);
    var storeTuple = StoreTuple(
        middleware.rocksDB,
        globalState.storeRocksDBPointer,
        middleware.stringStore,
        middleware.textStore);
    var keyValue =
        tableWrite(wr, globalState.rawDataUpdateStrategies, storeTuple);
    middleware.physicalLocationStrategy.put(
        middleware.rocksDB,
        globalState.tableRocksDBPointers,
        globalState.viewRocksDBPointers,
        table,
        keyValue.primaryKeys,
        keyValue.otherColumns);
    var indexKeyValues =
        createIndexWriteOperations(wr._table, keyValue, globalState.indexes);

    middleware.physicalLocationStrategy.putIndex(
        middleware.rocksDB,
        globalState.tableRocksDBPointers,
        globalState.viewRocksDBPointers,
        indexKeyValues);

    var changeDataList = ChangeDataList()
      .._globalState = globalState
      .._middleware = middleware;

    writeRecursive(
      wr._table,
      keyValue.primaryKeys,
      keyValue.otherColumns,
      globalState.materializeStrategies,
      middleware.rocksDB,
      middleware.physicalLocationStrategy,
      globalState.tableRocksDBPointers,
      globalState.viewRocksDBPointers,
      globalState.indexes,
      changeDataList,
    );
    return changeDataList;
  }

  ReadableRow read(
      GlobalState globalState, Middleware middleware, ReadKey readKey) {
    var table = readKey._table;

    _checkDatabaseTableOrView(globalState, table);
    _checkFullyPrimaryKeys(readKey._table, readKey._sets);

    var storeTuple = StoreTuple(
        middleware.rocksDB,
        globalState.storeRocksDBPointer,
        middleware.stringStore,
        middleware.textStore);

    var key = createReadKey(readKey._table, readKey._sets,
        globalState.bytesReadStrategies, storeTuple);
    var valueBytes = middleware.physicalLocationStrategy.get(
        middleware.rocksDB,
        globalState.tableRocksDBPointers,
        globalState.viewRocksDBPointers,
        table,
        key);
    var value = readTable(
        readKey, valueBytes, globalState.bytesReadStrategies, storeTuple);

    return ReadableRow()
      .._sets = value
      .._table = table;
  }

  void close(GlobalState globalState, Middleware middleware) {
    middleware.physicalLocationStrategy.close(middleware.rocksDB,
        globalState.tableRocksDBPointers, globalState.storeRocksDBPointer);
  }

  void writeRecursive(
      TableOrView table,
      Uint8List primaryKeys,
      Uint8List otherColumns,
      List<MaterializeStrategy> strategies,
      RocksDB rocksDB,
      PhysicalLocationStrategy physicalLocationStrategy,
      Map<Table, TableRocksDBPointer> tableRocksDBPointers,
      Map<View, ViewRocksDBPointer> viewRocksDBPointers,
      List<Index> indexes,
      ChangeDataList changeDataList) {
    var writeData = changePropagation(
      table,
      primaryKeys,
      otherColumns,
      strategies,
      rocksDB,
      physicalLocationStrategy,
      tableRocksDBPointers,
      viewRocksDBPointers,
    );

    // refsに追記
    for (var w in writeData) {
      changeDataList._add(w.view, w.primaryKeys, w.otherColumns, w.putOrDelete);
    }

    for (var w in writeData) {
      physicalLocationStrategy.put(rocksDB, tableRocksDBPointers,
          viewRocksDBPointers, w.view, w.primaryKeys, w.otherColumns);
      var keyValue = KeyValue()
        ..primaryKeys = w.primaryKeys
        ..otherColumns = w.otherColumns;
      var indexKeyValues =
          createIndexWriteOperations(w.view, keyValue, indexes);
      physicalLocationStrategy.putIndex(
          rocksDB, tableRocksDBPointers, viewRocksDBPointers, indexKeyValues);

      writeRecursive(
          w.view,
          w.primaryKeys,
          w.otherColumns,
          strategies,
          rocksDB,
          physicalLocationStrategy,
          tableRocksDBPointers,
          viewRocksDBPointers,
          indexes,
          changeDataList);
    }
  }

  List<ReadableRow> fullScan(
      GlobalState globalState, Middleware middleware, TableOrView table) {
    var keyValues = middleware.physicalLocationStrategy.seek(
        middleware.rocksDB,
        globalState.tableRocksDBPointers,
        globalState.viewRocksDBPointers,
        table,
        Uint8List(0));

    var storeTuple = StoreTuple(
        middleware.rocksDB,
        globalState.storeRocksDBPointer,
        middleware.stringStore,
        middleware.textStore);

    List<ReadableRow> rows = [];

    for (var keyBytes in keyValues.keys) {
      var valueBytes = keyValues[keyBytes];
      var value = readTableRecord(table, keyBytes, valueBytes,
          globalState.bytesReadStrategies, storeTuple);
      var row = ReadableRow()
        .._sets = value
        .._table = table;
      rows.add(row);
    }
    return rows;
  }

  ChangeDataList delete(
      GlobalState globalState, Middleware middleware, DeleteRow deleteRow) {
    _checkFullyPrimaryKeys(deleteRow._table, deleteRow._sets);

    var storeTuple = StoreTuple(
        middleware.rocksDB,
        globalState.storeRocksDBPointer,
        middleware.stringStore,
        middleware.textStore);

    var key = createReadKey(deleteRow._table, deleteRow._sets,
        globalState.bytesReadStrategies, storeTuple);
    var valueBytes = middleware.physicalLocationStrategy.get(
        middleware.rocksDB,
        globalState.tableRocksDBPointers,
        globalState.viewRocksDBPointers,
        deleteRow._table,
        key);

    if (valueBytes == null) {
      return ChangeDataList();
    }

    middleware.physicalLocationStrategy.delete(
        middleware.rocksDB,
        globalState.tableRocksDBPointers,
        globalState.viewRocksDBPointers,
        deleteRow._table,
        key);

    var deleteKeyValue = KeyValue()
      ..primaryKeys = key
      ..otherColumns = valueBytes;

    var indexKeyValues = createIndexWriteOperations(
        deleteRow._table, deleteKeyValue, globalState.indexes);

    var pointers = DBPointers()
      ..viewRocksDBPointers = globalState.viewRocksDBPointers
      ..tableRocksDBPointers = globalState.tableRocksDBPointers;

    middleware.physicalLocationStrategy.deleteIndex(
        middleware.rocksDB, pointers, deleteRow._table, indexKeyValues);

    var changeDataList = ChangeDataList()
      .._globalState = globalState
      .._middleware = middleware;

    deleteRecursive(
      deleteRow._table,
      key,
      valueBytes,
      globalState.materializeStrategies,
      middleware.rocksDB,
      middleware.physicalLocationStrategy,
      globalState.tableRocksDBPointers,
      globalState.viewRocksDBPointers,
      globalState.indexes,
      changeDataList,
    );
    return changeDataList;
  }

  void deleteRecursive(
      TableOrView table,
      Uint8List primaryKeys,
      Uint8List otherColumns,
      List<MaterializeStrategy> strategies,
      RocksDB rocksDB,
      PhysicalLocationStrategy physicalLocationStrategy,
      Map<Table, TableRocksDBPointer> tableRocksDBPointers,
      Map<View, ViewRocksDBPointer> viewRocksDBPointers,
      List<Index> indexes,
      ChangeDataList changeDataList) {
    var writeData = deletePropagation(
      table,
      primaryKeys,
      otherColumns,
      strategies,
      rocksDB,
      physicalLocationStrategy,
      tableRocksDBPointers,
      viewRocksDBPointers,
    );

    // refsに追記
    for (var w in writeData) {
      changeDataList._add(w.view, w.primaryKeys, w.otherColumns, w.putOrDelete);
    }

    for (var w in writeData) {
      if (w.putOrDelete == PutOrDelete.put) {
        physicalLocationStrategy.put(rocksDB, tableRocksDBPointers,
            viewRocksDBPointers, w.view, w.primaryKeys, w.otherColumns);
      } else if (w.putOrDelete == PutOrDelete.delete) {
        physicalLocationStrategy.delete(rocksDB, tableRocksDBPointers,
            viewRocksDBPointers, w.view, w.primaryKeys);
      } else {
        throw "tableでもviewでもない";
      }

      var deleteKeyValue = KeyValue()
        ..primaryKeys = w.primaryKeys
        ..otherColumns = w.otherColumns;

      var indexKeyValues =
          createIndexWriteOperations(w.view, deleteKeyValue, indexes);

      var pointers = DBPointers()
        ..viewRocksDBPointers = viewRocksDBPointers
        ..tableRocksDBPointers = tableRocksDBPointers;

      physicalLocationStrategy.deleteIndex(
          rocksDB, pointers, w.view, indexKeyValues);

//      deleteRecursive(
//          w.view,
//          w.primaryKeys,
//          w.otherColumns,
//          strategies,
//          rocksDB,
//          physicalLocationStrategy,
//          tableRocksDBPointers,
//          viewRocksDBPointers,
//          indexes,
//          changeDataList);
    }
  }
}

void _checkViewColumnParent(List<View> views) {
  if (!_hasViewColumnParentsMatch(views)) {
    throw ViewColumnParentException();
  }
}

class ViewColumnParentException {
  String toString() {
    "ViewColumnのviewの指定が間違っています。ViewColumnの第二引数には自分自身を渡します。";
  }
}

bool _hasViewColumnParentsMatch(List<View> views) {
  for (var view in views) {
    for (var column in view.columns) {
      if (column.view != view) {
        return false;
      }
    }
  }
  return true;
}

class Database {
  GlobalState _globalState = GlobalState();
  Middleware _middleware = Middleware();
  Actions _actions = Actions();

  Database(List<Table> tables, List<View> views) {
    _actions.setTables(_globalState, tables, views);
  }

  void create(String dbPath) {
    _actions.create(dbPath, _globalState, _middleware);
  }

  void open(String dbPath) {
    _actions.open(dbPath, _globalState, _middleware);
  }

  ChangeDataList write(WritableRow wr) {
    return _actions.write(_globalState, _middleware, wr);
  }

  ReadableRow read(ReadKey readKey) {
    return _actions.read(_globalState, _middleware, readKey);
  }

  void close() {
    _actions.close(_globalState, _middleware);
  }

  List<ReadableRow> fullScan(TableOrView table) {
    return _actions.fullScan(_globalState, _middleware, table);
  }

  ChangeDataList delete(DeleteRow deleteRow) {
    return _actions.delete(_globalState, _middleware, deleteRow);
  }
}

void _checkPrimaryKeys(List<Table> tables) {
  for (var table in tables) {
    if (table.primaryKeys.isEmpty) {
      throw EmptyPrimaryKeysException(table.tableName);
    }
    for (var primaryKey in table.primaryKeys) {
      if (!table.columns.contains(primaryKey)) {
        throw ColumnIsNotPartOfTableException(table.tableName, primaryKey.name);
      }
    }
    for (var primaryKey in table.primaryKeys) {
      if (primaryKey is TextColumn) {
        throw PrimaryKeyTypeException(table.tableName, primaryKey.name);
      }
    }
  }
}

void _checkDatabaseTable(GlobalState globalState, Table table) {
  if (!globalState.tables.contains(table)) {
    throw UnknownTableException(table.tableName);
  }
}

void _checkDatabaseTableOrView(GlobalState globalState, TableOrView table) {
  if (!globalState.tableViews.contains(table)) {
    throw UnknownTableException(table.tableName);
  }
}

class UnknownTableException implements Exception {
  final String tableName;

  UnknownTableException(this.tableName);

  String toString() {
    return "$tableName はこのデータベースに登録されていません";
  }
}

void _checkFullyColumnValues(WritableRow wr) {
  var table = wr._table;
  var sets = wr._sets;

  // tableにあってsetsにないモノを探す
  for (var column in table.columns) {
    if (!sets.containsKey(column)) {
      throw IncompleteWriteRequestException(column.name);
    }
  }
}

void _checkFullyPrimaryKeys(TableOrView table, Map<Column, dynamic> sets) {
  // tableにあってsetsにないモノを探す
  for (var column in table.primaryKeys) {
    if (!sets.containsKey(column)) {
      throw IncompleteReadRequestException(column.name);
    }
  }
}

class IncompleteWriteRequestException implements Exception {
  final String columnName;

  IncompleteWriteRequestException(this.columnName);

  String toString() {
    return "$columnName に値がセットされていません";
  }
}

class IncompleteReadRequestException implements Exception {
  final String columnName;

  IncompleteReadRequestException(this.columnName);

  String toString() {
    return "$columnName に値がセットされていません";
  }
}

void _checkDuplicatedTableName(List<TableOrView> tables) {
  var names = Set<String>();
  for (var table in tables) {
    if (names.contains(table.tableName)) {
      throw DuplicatedTableNameException(table.tableName);
    }
    names.add(table.tableName);
  }
}

class DuplicatedTableNameException implements Exception {
  final String tableName;

  DuplicatedTableNameException(this.tableName);

  String toString() {
    return "$tableName というテーブルが複数あります";
  }
}

class EmptyPrimaryKeysException implements Exception {
  final String tableName;

  EmptyPrimaryKeysException(this.tableName);

  String toString() {
    return "$tableName にプライマリーキーがありません";
  }
}

class PrimaryKeyTypeException implements Exception {
  final String tableName;
  final String columnName;

  PrimaryKeyTypeException(this.tableName, this.columnName);

  String toString() {
    return "$tableName.$columnName はテキスト型なのでプライマリーキーになれません";
  }
}

class ChangeDataList {
  Map<View, List<Reference>> map = {};
  GlobalState _globalState;
  Middleware _middleware;

  void _add(
      View view, Uint8List key, Uint8List value, PutOrDelete putOrDelete) {
    map.putIfAbsent(view, () => []);
    var ref = Reference()
      .._middleware = _middleware
      .._globalState = _globalState
      .._view = view
      .._key = key
      .._value = value
      ..putOrDelete;
    map[view].add(ref);
  }
}

class Reference {
  GlobalState _globalState;
  Middleware _middleware;
  PutOrDelete putOrDelete;

  View _view;

  Uint8List _key, _value;

  ReadableRow row() {
    var storeTuple = StoreTuple(
        _middleware.rocksDB,
        _globalState.storeRocksDBPointer,
        _middleware.stringStore,
        _middleware.textStore);

    var value = readTableRecord(
        _view, _key, _value, _globalState.bytesReadStrategies, storeTuple);
    return ReadableRow()
      .._sets = value
      .._table = _view;
  }

  String toString() {
    var row = this.row();
    var keys = _view.primaryKeys
        .map<dynamic>((k) => "${k.name}=${row._sets[k]}")
        .join(",");
    var values = getOtherColumns(_view)
        .map<dynamic>((k) => "${k.name}=${row._sets[k]}")
        .join(",");
    var pod = putOrDelete == PutOrDelete.delete ? "delete" : "put";
    return "$pod ${_view.tableName} ${keys} ${values}";
  }
}
