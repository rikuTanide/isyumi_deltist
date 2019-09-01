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

Uint8List createSelectViewPrimaryKey(
    SelectViewMaterializeStrategy strategy, Uint8List key, Uint8List value) {
  var primaryKey = Uint8List(strategy.primaryKeyLength);
  for (var copy in strategy.parentPrimaryKeyToPrimaryKey) {
    primaryKey[copy.to] = key[copy.from];
  }
  for (var copy in strategy.parentOtherColumnToPrimaryKey) {
    primaryKey[copy.to] = value[copy.from];
  }
  return primaryKey;
}

Iterable<ViewKeyValue> createSelectViewRecord(
    SelectViewMaterializeStrategy strategy, Uint8List key, Uint8List value) {
  var otherColumns = Uint8List(strategy.otherColumnsLength);
  var primaryKey = createSelectViewPrimaryKey(strategy, key, value);

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
  var otherParentIndex = strategy.otherParentIndex;

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
      ..view = strategy.view
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
    return createInnerJoinRecordByGetOtherColumn(
        otherParentIndex,
        physicalLocationStrategy,
        rocksDB,
        tableRocksDBPointers,
        viewRocksDBPointers,
        strategy,
        otherParentPrimaryKey,
        mapping);
  }

  return createInnerJoinRecordBySeekOtherColumns(
      otherParentIndex,
      physicalLocationStrategy,
      rocksDB,
      tableRocksDBPointers,
      viewRocksDBPointers,
      strategy,
      otherParentPrimaryKey,
      mapping);
}

Iterable<ViewKeyValue> createInnerJoinRecordBySeekOtherColumns(
    UsePrimaryKeyOrIndex otherParentIndex,
    PhysicalLocationStrategy physicalLocationStrategy,
    RocksDB rocksDB,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers,
    InnerJoinMaterializeStrategy strategy,
    Uint8List otherParentPrimaryKey,
    ViewKeyValue mapping(
        Uint8List otherParentPrimaryKeys, Uint8List otherParentOtherColumns)) {
  Map<Uint8List, Uint8List> seekOrSeekByIndex() {
    if (otherParentIndex is UsePrimaryKey) {
      return physicalLocationStrategy.seek(
        rocksDB,
        tableRocksDBPointers,
        viewRocksDBPointers,
        strategy.otherParent,
        otherParentPrimaryKey,
      );
    } else if (otherParentIndex is UseIndex) {
      return physicalLocationStrategy.seekByIndex(
        rocksDB,
        tableRocksDBPointers,
        viewRocksDBPointers,
        strategy.otherParent,
        otherParentIndex.index,
        otherParentPrimaryKey,
      );
    }
  }

  var otherData = seekOrSeekByIndex();
  return otherData.keys.map((key) => mapping(key, otherData[key]));
}

List<ViewKeyValue> createInnerJoinRecordByGetOtherColumn(
    UsePrimaryKeyOrIndex otherParentIndex,
    PhysicalLocationStrategy physicalLocationStrategy,
    RocksDB rocksDB,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers,
    InnerJoinMaterializeStrategy strategy,
    Uint8List otherParentPrimaryKey,
    ViewKeyValue mapping(
        Uint8List otherParentPrimaryKeys, Uint8List otherParentOtherColumns)) {
  Uint8List getOrGetByIndex() {
    if (otherParentIndex is UsePrimaryKey) {
      return physicalLocationStrategy.get(
        rocksDB,
        tableRocksDBPointers,
        viewRocksDBPointers,
        strategy.otherParent,
        otherParentPrimaryKey,
      );
    } else if (otherParentIndex is UseIndex) {
      return physicalLocationStrategy.getByIndex(
        rocksDB,
        tableRocksDBPointers,
        viewRocksDBPointers,
        strategy.otherParent,
        otherParentIndex.index,
        otherParentPrimaryKey,
      );
    }
    throw "indexでもprimary keyでもない";
  }

  var otherParentOtherColumns = getOrGetByIndex();
  if (otherParentOtherColumns == null) {
    return [];
  }
  return [mapping(otherParentPrimaryKey, otherParentOtherColumns)];
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

Uint8List createPrimaryParentPrimaryKeyFromSecondaryTable(
  UnionViewMaterializeStrategyFromSecondaryTable strategy,
  Uint8List key,
  Uint8List value,
) {
  var primaryTablePrimaryKey = Uint8List(strategy.primaryTablePrimaryKeyLength);

  for (var copy in strategy.secondaryTablePrimaryKeyToPrimaryTablePrimaryKey) {
    primaryTablePrimaryKey[copy.to] = key[copy.from];
  }
  for (var copy
      in strategy.secondaryTableOtherColumnsToPrimaryTablePrimaryKey) {
    primaryTablePrimaryKey[copy.to] = value[copy.from];
  }
  return primaryTablePrimaryKey;
}

Uint8List createSecondaryUnionViewPrimaryKey(
    UnionViewMaterializeStrategyFromSecondaryTable strategy,
    Uint8List key,
    Uint8List value) {
  var primaryKeys = Uint8List(strategy.primaryKeyLength);
  for (var copy in strategy.secondaryTablePrimaryKeyToPrimaryKey) {
    primaryKeys[copy.to] = key[copy.from];
  }
  for (var copy in strategy.secondaryTableOtherColumnToPrimaryKey) {
    primaryKeys[copy.to] = value[copy.from];
  }
  return primaryKeys;
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

  var primaryTablePrimaryKey = createPrimaryParentPrimaryKeyFromSecondaryTable(
    strategy,
    key,
    value,
  );

  var primaryTableOtherColumns = physicalLocationStrategy.get(
    rocksDB,
    tableRocksDBPointers,
    viewRocksDBPointers,
    strategy.primaryTable,
    primaryTablePrimaryKey,
  );

  if (primaryTableOtherColumns == null) {
    var primaryKeys = createSecondaryUnionViewPrimaryKey(strategy, key, value);
    var otherColumns = Uint8List(strategy.otherColumnsLength);

    // Secondary Parentからデータを作る
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
  // Primaryがデータを持っていたら何もしなくていい
  // もともとPrimaryを元にデータを作る機能があったが
  // 8/31にけした。

  return [];
}

Uint8List createPrimaryUnionViewPrimaryKey(
    UnionViewMaterializeStrategyFromPrimaryTable strategy,
    Uint8List key,
    Uint8List value) {
  var primaryKey = Uint8List(strategy.primaryKeyLength);
  for (var copy in strategy.parentPrimaryKeyToPrimaryKey) {
    primaryKey[copy.to] = key[copy.from];
  }
  for (var copy in strategy.parentOtherColumnToPrimaryKey) {
    primaryKey[copy.to] = value[copy.from];
  }
  return primaryKey;
}

Iterable<ViewKeyValue> createPrimaryUnionViewRecord(
    UnionViewMaterializeStrategyFromPrimaryTable strategy,
    Uint8List key,
    Uint8List value) {
  var primaryKey = createPrimaryUnionViewPrimaryKey(strategy, key, value);
  var otherColumns = Uint8List(strategy.otherColumnsLength);

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

Iterable<ViewKeyValue> deletePropagation(
    TableOrView table,
    Uint8List key,
    Uint8List value,
    List<MaterializeStrategy> strategies,
    RocksDB rocksDB,
    PhysicalLocationStrategy physicalLocationStrategy,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers) {
  return strategies.where((s) => s.parent == table).map((strategy) {
    if (strategy is SelectViewMaterializeStrategy) {
      return deleteSelectViewMaterializeRecord(
          strategy,
          physicalLocationStrategy,
          key,
          value,
          rocksDB,
          tableRocksDBPointers,
          viewRocksDBPointers);
    } else if (strategy is UnionViewMaterializeStrategyFromPrimaryTable) {
      return deletePrimaryUnionViewMaterialize(
          strategy,
          physicalLocationStrategy,
          key,
          value,
          rocksDB,
          tableRocksDBPointers,
          viewRocksDBPointers);
    } else if (strategy is UnionViewMaterializeStrategyFromSecondaryTable) {
      return deleteSecondaryUnionViewMaterialize(
          strategy,
          physicalLocationStrategy,
          key,
          value,
          rocksDB,
          tableRocksDBPointers,
          viewRocksDBPointers);
    } else if (strategy is InnerJoinMaterializeStrategy) {
      return deleteInnerJoinViewMaterialize(strategy, physicalLocationStrategy,
          key, value, rocksDB, tableRocksDBPointers, viewRocksDBPointers);
    } else {
      throw "知らないView";
    }
  }).expand((e) => e);
}

Iterable<ViewKeyValue> deleteSelectViewMaterializeRecord(
    SelectViewMaterializeStrategy strategy,
    PhysicalLocationStrategy physicalLocationStrategy,
    Uint8List key,
    Uint8List value,
    RocksDB rocksDB,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers) {
  var primaryKey = createSelectViewPrimaryKey(strategy, key, value);

  var otherColumns = physicalLocationStrategy.get(rocksDB, tableRocksDBPointers,
      viewRocksDBPointers, strategy.view, primaryKey);

  if (otherColumns == null) {
    return [];
  }

  return [
    ViewKeyValue()
      ..view = strategy.view
      ..putOrDelete = PutOrDelete.delete
      ..primaryKeys = primaryKey
      ..otherColumns = otherColumns
  ];
}

// こっちは自分がメインだから自分がなくなったらViewを消していい
Iterable<ViewKeyValue> deletePrimaryUnionViewMaterialize(
    UnionViewMaterializeStrategyFromPrimaryTable strategy,
    PhysicalLocationStrategy physicalLocationStrategy,
    Uint8List key,
    Uint8List value,
    RocksDB rocksDB,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers) {
  var primaryKey = createPrimaryUnionViewPrimaryKey(strategy, key, value);

  var otherColumns = physicalLocationStrategy.get(rocksDB, tableRocksDBPointers,
      viewRocksDBPointers, strategy.view, primaryKey);

  if (otherColumns == null) {
    return [];
  }

  return [
    ViewKeyValue()
      ..view = strategy.view
      ..putOrDelete = PutOrDelete.delete
      ..primaryKeys = primaryKey
      ..otherColumns = otherColumns
  ];
}

// こっちは自分がメインだから自分がなくなったらViewを消していい
Iterable<ViewKeyValue> deleteSecondaryUnionViewMaterialize(
    UnionViewMaterializeStrategyFromSecondaryTable strategy,
    PhysicalLocationStrategy physicalLocationStrategy,
    Uint8List key,
    Uint8List value,
    RocksDB rocksDB,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers) {
  var primaryParentPrimaryKey =
      createPrimaryParentPrimaryKeyFromSecondaryTable(strategy, key, value);

  var primaryParentOtherColumns = physicalLocationStrategy.get(
      rocksDB,
      tableRocksDBPointers,
      viewRocksDBPointers,
      strategy.view,
      primaryParentPrimaryKey);

  if (primaryParentOtherColumns == null) {
    var primaryKey = createSecondaryUnionViewPrimaryKey(
      strategy,
      key,
      value,
    );
    var otherColumns = physicalLocationStrategy.get(rocksDB,
        tableRocksDBPointers, viewRocksDBPointers, strategy.view, primaryKey);
    return [
      ViewKeyValue()
        ..view = strategy.view
        ..putOrDelete = PutOrDelete.delete
        ..primaryKeys = primaryKey
        ..otherColumns = otherColumns
    ];
  }
  return [];
}

Iterable<ViewKeyValue> deleteInnerJoinViewMaterialize(
    InnerJoinMaterializeStrategy strategy,
    PhysicalLocationStrategy physicalLocationStrategy,
    Uint8List key,
    Uint8List value,
    RocksDB rocksDB,
    Map<Table, TableRocksDBPointer> tableRocksDBPointers,
    Map<View, ViewRocksDBPointer> viewRocksDBPointers) {
  var primaryKey = Uint8List(strategy.ownIndexLength);
  for (var copy in strategy.parentPrimaryKeyToOwnIndex) {
    primaryKey[copy.to] = key[copy.from];
  }

  for (var copy in strategy.parentOtherColumnsToOwnIndex) {
    primaryKey[copy.to] = value[copy.from];
  }

  Uint8List getOrIndexGet(Uint8List primaryKey) {
    var ownIndex = strategy.ownIndex;
    if (ownIndex is UsePrimaryKey) {
      return physicalLocationStrategy.get(rocksDB, tableRocksDBPointers,
          viewRocksDBPointers, strategy.view, primaryKey);
    } else if (ownIndex is UseIndex) {
      return physicalLocationStrategy.getByIndex(rocksDB, tableRocksDBPointers,
          viewRocksDBPointers, strategy.view, ownIndex.index, primaryKey);
    } else {
      throw "indexでもprimary keyでもない";
    }
  }

  Map<Uint8List, Uint8List> seekOrIndexSeek(Uint8List prefix) {
    var ownIndex = strategy.ownIndex;
    if (ownIndex is UsePrimaryKey) {
      return physicalLocationStrategy.seek(rocksDB, tableRocksDBPointers,
          viewRocksDBPointers, strategy.view, prefix);
    } else if (ownIndex is UseIndex) {
      return physicalLocationStrategy.seekByIndex(rocksDB, tableRocksDBPointers,
          viewRocksDBPointers, strategy.view, ownIndex.index, prefix);
    } else {
      throw "indexでもprimary keyでもない";
    }
  }

  if (strategy.parentIndexGetOrSeek == GetOrSeek.get) {
    var otherColumns = getOrIndexGet(primaryKey);
    // これ、新しいDartの演算子で綺麗にかけそう
    if (otherColumns == null) {
      return [];
    } else {
      return [
        ViewKeyValue()
          ..view = strategy.view
          ..primaryKeys = primaryKey
          ..otherColumns = otherColumns
          ..putOrDelete = PutOrDelete.delete
      ];
    }
  } else {
    return seekOrIndexSeek(primaryKey).entries.map((e) => ViewKeyValue()
      ..view = strategy.view
      ..primaryKeys = e.key
      ..otherColumns = e.value
      ..putOrDelete = PutOrDelete.delete);
  }
}
