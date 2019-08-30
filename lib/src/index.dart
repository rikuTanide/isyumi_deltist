part of isyumi_deltist;

class IndexKeyValue {
  Index index;
  Uint8List primaryKeys;
  Uint8List reference;
}

List<IndexKeyValue> createIndexWriteOperations(
    TableOrView table, KeyValue writeData, List<Index> indexes) {
  var matchIndexes = indexes.where((i) => i.table == table);
  return matchIndexes.map((i) => createIndexRow(i, writeData)).toList();
}

IndexKeyValue createIndexRow(Index index, KeyValue writeData) {
  var key = Uint8List(index.columnsLength);

  for (var copy in index.primaryKeyToKey) {
    key[copy.to] = writeData.primaryKeys[copy.from];
  }
  for (var copy in index.otherColumnsToKey) {
    key[copy.to] = writeData.otherColumns[copy.from];
  }
  return IndexKeyValue()
    ..index = index
    ..primaryKeys = key
    ..reference = writeData.primaryKeys;
}

Iterable<ViewKeyValue> changePropagation(
    TableOrView table,
    Uint8List key,
    Uint8List value,
    List<MaterializeStrategy> strategies,
    RocksDB rocksDB,
    PhysicalLocationStrategy physicalLocationStrategy,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers) {
  return strategies
      .where((strategy) => strategy.parent == table)
      .map((strategy) {
    if (strategy is UnionViewMaterializeStrategyFromPrimaryTable) {
      return createPrimaryUnionViewRecord(strategy, key, value);
    } else if (strategy is UnionViewMaterializeStrategyFromSecondaryTable) {
      return createSecondaryUnionViewRecord(strategy, physicalLocationStrategy,
          key, value, rocksDB, tableRocksDBPointers, viewRocksDBPointers);
    } else if (strategy is InnerJoinMaterializeStrategy) {
      return createInnerJoinViewRecord(strategy, physicalLocationStrategy, key,
          value, rocksDB, tableRocksDBPointers, viewRocksDBPointers);
    } else if (strategy is SelectViewMaterializeStrategy) {
      return createSelectViewRecord(strategy, key, value);
    } else {
      throw "知らないView";
    }
  }).expand((prev) => prev);
}

Iterable<ViewKeyValue> createSelectViewRecord(
    SelectViewMaterializeStrategy strategy, Uint8List key, Uint8List value) {
  var primaryKey = Uint8List(strategy.primaryKeyLength);
  var otherColumns = Uint8List(strategy.otherColumnsLength);

  for (var copy in strategy.parentPrimaryKeyToPrimaryKey) {
    primaryKey[copy.to] = key[copy.from];
  }
  for (var copy in strategy.parentOtherColumnToPrimaryKey) {
    primaryKey[copy.to] = value[copy.from];
  }
  for (var copy in strategy.parentPrimaryKeyToOtherColumns) {
    otherColumns[copy.to] = key[copy.from];
  }
  for (var copy in strategy.parentOtherColumnToOtherColumns) {
    otherColumns[copy.to] = value[copy.from];
  }

  return [
    ViewKeyValue()
      ..putOrDelete = PutOrDelete.put
      ..view = strategy.view
      ..primaryKeys = primaryKey
      ..otherColumns = otherColumns
  ];
}

Iterable<ViewKeyValue> createInnerJoinViewRecord(
    InnerJoinMaterializeStrategy strategy,
    PhysicalLocationStrategy physicalLocationStrategy,
    Uint8List key,
    Uint8List value,
    RocksDB rocksDB,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers) {
  // まず相手方をgetかseekする
  // GetならあったらJoinしてPut
  // なかったらスルー

  // seekならJoinの一覧

  ViewKeyValue mapping(
      Uint8List otherParentPrimaryKeys, Uint8List otherParentOtherColumns) {
    var primaryKey = Uint8List(strategy.primaryKeyLength);
    var otherColumns = Uint8List(strategy.otherColumnsLength);

    for (var copy in strategy.parentPrimaryKeyToPrimaryKey) {
      primaryKey[copy.to] = key[copy.from];
    }
    for (var copy in strategy.parentOtherColumnToPrimaryKey) {
      primaryKey[copy.to] = value[copy.from];
    }
    for (var copy in strategy.otherParentPrimaryKeyToPrimaryKey) {
      primaryKey[copy.to] = otherParentPrimaryKeys[copy.from];
    }
    for (var copy in strategy.otherParentOtherColumnToPrimaryKey) {
      primaryKey[copy.to] = otherParentOtherColumns[copy.from];
    }

    for (var copy in strategy.parentPrimaryKeyToOtherColumn) {
      otherColumns[copy.to] = key[copy.from];
    }
    for (var copy in strategy.parentOtherColumnToOtherColumn) {
      otherColumns[copy.to] = value[copy.from];
    }
    for (var copy in strategy.otherParentPrimaryKeyToOtherColumn) {
      otherColumns[copy.to] = otherParentPrimaryKeys[copy.from];
    }
    for (var copy in strategy.otherParentOtherColumnToOtherColumn) {
      otherColumns[copy.to] = otherParentOtherColumns[copy.from];
    }

    return ViewKeyValue()
      ..putOrDelete = PutOrDelete.put
      ..primaryKeys = primaryKey
      ..otherColumns = otherColumns;
  }

  var otherParentPrimaryKey = Uint8List(strategy.otherParentPrimaryKeyLength);

  for (var copy in strategy.parentPrimaryKeyToOtherParentPrimaryKey) {
    otherParentPrimaryKey[copy.to] = key[copy.from];
  }
  for (var copy in strategy.parentOtherColumnsToOtherParentPrimaryKey) {
    otherParentPrimaryKey[copy.to] = value[copy.from];
  }

  if (strategy.getOrSeek == GetOrSeek.get) {
    var otherParentOtherColumns = physicalLocationStrategy.get(
      rocksDB,
      tableRocksDBPointers,
      viewRocksDBPointers,
      strategy.otherParent,
      otherParentPrimaryKey,
    );
    if (otherParentOtherColumns == null) {
      return [];
    }
    return [mapping(otherParentPrimaryKey, otherParentOtherColumns)];
  }

  var otherData = physicalLocationStrategy.seek(
    rocksDB,
    tableRocksDBPointers,
    viewRocksDBPointers,
    strategy.otherParent,
    otherParentPrimaryKey,
  );
  return otherData.keys.map((key) => mapping(key, otherData[key]));
}

class ViewKeyValue {
  View view;
  Uint8List primaryKeys;
  Uint8List otherColumns;
  PutOrDelete putOrDelete;

  String toString() {
    return "${view.tableName}, $primaryKeys $otherColumns";
  }
}

enum PutOrDelete {
  put,
  delete,
}

Iterable<ViewKeyValue> createSecondaryUnionViewRecord(
    UnionViewMaterializeStrategyFromSecondaryTable strategy,
    PhysicalLocationStrategy physicalLocationStrategy,
    Uint8List key,
    Uint8List value,
    RocksDB rocksDB,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers) {
  // まず偉い方の親にそのKeyの値があるか確認する

  var primaryTablePrimaryKey = Uint8List(strategy.primaryTablePrimaryKeyLength);
  for (var copy in strategy.secondaryTablePrimaryKeyToPrimaryTablePrimaryKey) {
    primaryTablePrimaryKey[copy.to] = key[copy.from];
  }
  for (var copy
      in strategy.secondaryTableOtherColumnsToPrimaryTablePrimaryKey) {
    primaryTablePrimaryKey[copy.to] = value[copy.from];
  }

  var primaryTableOtherColumns = physicalLocationStrategy.get(
    rocksDB,
    tableRocksDBPointers,
    viewRocksDBPointers,
    strategy.primaryTable,
    primaryTablePrimaryKey,
  );

  if (primaryTableOtherColumns == null) {
    var primaryKeys = Uint8List(strategy.primaryKeyLength);
    var otherColumns = Uint8List(strategy.otherColumnsLength);

    // Secondary Parentからデータを作る
    for (var copy in strategy.secondaryTablePrimaryKeyToPrimaryKey) {
      primaryKeys[copy.to] = key[copy.from];
    }
    for (var copy in strategy.secondaryTableOtherColumnToPrimaryKey) {
      primaryKeys[copy.to] = value[copy.from];
    }
    for (var copy in strategy.secondaryTablePrimaryKeyToOtherColumn) {
      otherColumns[copy.to] = key[copy.from];
    }
    for (var copy in strategy.secondaryTableOtherColumnToOtherColumn) {
      otherColumns[copy.to] = value[copy.from];
    }
    return [
      ViewKeyValue()
        ..putOrDelete = PutOrDelete.put
        ..view = strategy.view
        ..primaryKeys = primaryKeys
        ..otherColumns = otherColumns
    ];
  }

  var primaryKeys = Uint8List(strategy.primaryKeyLength);
  var otherColumns = Uint8List(strategy.otherColumnsLength);

  // Secondary Parentからデータを作る
  for (var copy in strategy.primaryTablePrimaryKeyToPrimaryKey) {
    primaryKeys[copy.to] = primaryTablePrimaryKey[copy.from];
  }
  for (var copy in strategy.primaryTableOtherColumnsToPrimaryKey) {
    primaryKeys[copy.to] = primaryTableOtherColumns[copy.from];
  }
  for (var copy in strategy.primaryTablePrimaryKeyToOtherColumn) {
    otherColumns[copy.to] = primaryTablePrimaryKey[copy.from];
  }
  for (var copy in strategy.primaryTableOtherColumnsToOtherColumn) {
    otherColumns[copy.to] = primaryTableOtherColumns[copy.from];
  }
  return [
    ViewKeyValue()
      ..putOrDelete = PutOrDelete.put
      ..view = strategy.view
      ..primaryKeys = primaryKeys
      ..otherColumns = otherColumns
  ];
}

Iterable<ViewKeyValue> createPrimaryUnionViewRecord(
    UnionViewMaterializeStrategyFromPrimaryTable strategy,
    Uint8List key,
    Uint8List value) {
  var primaryKey = Uint8List(strategy.primaryKeyLength);
  var otherColumns = Uint8List(strategy.otherColumnsLength);

  for (var copy in strategy.parentPrimaryKeyToPrimaryKey) {
    primaryKey[copy.to] = key[copy.from];
  }
  for (var copy in strategy.parentOtherColumnToPrimaryKey) {
    primaryKey[copy.to] = value[copy.from];
  }
  for (var copy in strategy.parentPrimaryKeyToOtherColumns) {
    otherColumns[copy.to] = key[copy.from];
  }
  for (var copy in strategy.parentOtherColumnToOtherColumns) {
    otherColumns[copy.to] = value[copy.from];
  }

  return [
    ViewKeyValue()
      ..putOrDelete = PutOrDelete.put
      ..view = strategy.view
      ..primaryKeys = primaryKey
      ..otherColumns = otherColumns
  ];
}
