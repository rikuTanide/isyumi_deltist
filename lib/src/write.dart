part of isyumi_deltist;

class WritableRow {
  final Table _table;
  final Map<Column, dynamic> _sets = <Column, dynamic>{};

  WritableRow(this._table);

  void set<E extends T, T>(Column<T> column, E value) {
    _checkColumnParent(_table, column);
    _sets[column] = value;
  }
}

void _checkColumnParent(TableOrView table, Column column) {
  if (!_isTableColumn(table, column)) {
    throw ColumnIsNotPartOfTableException(table.tableName, column.name);
  }
}

class ColumnIsNotPartOfTableException implements Exception {
  final String tableName, columnName;

  ColumnIsNotPartOfTableException(this.tableName, this.columnName);

  String toString() {
    return "$columnName は $tableName のカラムではありません";
  }
}

KeyValue tableWrite(
    WritableRow wr,
    List<RawDataUpdateStrategy> rawDataUpdateStrategies,
    StoreTuple storeTuple) {
  for (var strategy in rawDataUpdateStrategies) {
    if (strategy.table != wr._table) {
      continue;
    }

    var byteValues = <Column, Uint8List>{};
    for (var column in wr._sets.keys) {
      dynamic value = wr._sets[column];
      var byteValue = dartValueToU8int(storeTuple, column, value);
      byteValues[column] = byteValue;
    }

    var primaryKey = Uint8List(strategy.primaryKeysLength);
    var otherColumns = Uint8List(strategy.otherColumnsLength);

    for (var setter in strategy.primaryKeysSetStrategy) {
      var byteValue = byteValues[setter.column];
      primaryKey[setter.to] = byteValue[setter.from];
    }

    for (var setter in strategy.otherColumnsSetStrategy) {
      var byteValue = byteValues[setter.column];
      otherColumns[setter.to] = byteValue[setter.from];
    }

    return KeyValue()
      ..primaryKeys = primaryKey
      ..otherColumns = otherColumns;
  }
  throw "テーブル書き込み戦略がなかった";
}

class KeyValue {
  Uint8List primaryKeys;
  Uint8List otherColumns;
}
