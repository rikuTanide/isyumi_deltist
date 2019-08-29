part of isyumi_deltist;

class ReadableRow {
  TableOrView _table;
  Map<Column, dynamic> _sets = <Column, dynamic>{};

  T get<T, E extends T>(Column<T> column) {
    _checkColumnParent(_table, column);
    return _sets[column] as T;
  }
}

class ReadKey {
  TableOrView _table;
  final Map<Column, dynamic> _sets = <Column, dynamic>{};

  ReadKey(this._table);

  void set<T, E extends T>(Column<T> column, E value) {
    _checkColumnParent(_table, column);
    _sets[column] = value;
  }
}

Uint8List createReadKey(ReadKey readKey,
    List<BytesReadStrategy> bytesReadStrategies, StoreTuple storeTuple) {
  for (var strategy in bytesReadStrategies) {
    if (strategy.table != readKey._table) {
      continue;
    }
    var byteValues = <Column, Uint8List>{};
    for (var column in readKey._sets.keys) {
      dynamic value = readKey._sets[column];
      var byteValue = dartValueToU8int(storeTuple, column, value);
      byteValues[column] = byteValue;
    }

    var primaryKey = Uint8List(strategy.primaryKeyLength);

    for (var setter in strategy.createPrimaryKeysStrategy) {
      var byteValue = byteValues[setter.column];
      primaryKey[setter.to] = byteValue[setter.from];
    }
    return primaryKey;
  }

  throw "読み込み戦略がないためキーが作れない";
}

Map<Column, dynamic> readTable(ReadKey readKey, Uint8List record,
    List<BytesReadStrategy> bytesReadStrategies, StoreTuple storeTuple) {
  for (var strategy in bytesReadStrategies) {
    if (strategy.table != readKey._table) {
      continue;
    }
    var byteValues = <Column, dynamic>{};
    for (var column in strategy.createColumnsFromOtherColumns) {
      var byteValue = Uint8List(column.length);
      for (var copy in column.copies) {
        byteValue[copy.to] = record[copy.from];
      }

      dynamic value = u8IntToDartValue(storeTuple, column.column, byteValue);

      byteValues[column.column] = value;
    }

    for (var column in readKey._sets.keys) {
      byteValues[column] = readKey._sets[column];
    }

    return byteValues;
  }

  throw "読み込み戦略がないため読み込めない";
}
