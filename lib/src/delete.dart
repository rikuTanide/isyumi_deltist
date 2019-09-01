part of isyumi_deltist;

class DeleteRow {
  final Table _table;
  final Map<Column, dynamic> _sets = <Column, dynamic>{};

  DeleteRow(this._table);

  void set<E extends T, T>(Column<T> column, E value) {
    _checkColumnParent(_table, column);
    _sets[column] = value;
  }
}

Uint8List tableDelete(
    DeleteRow dr,
    List<RawDataUpdateStrategy> rawDataUpdateStrategies,
    StoreTuple storeTuple) {
  for (var strategy in rawDataUpdateStrategies) {
    if (strategy.table != dr._table) {
      continue;
    }

    var byteValues = <Column, Uint8List>{};
    for (var column in dr._sets.keys) {
      dynamic value = dr._sets[column];
      var byteValue = dartValueToU8int(storeTuple, column, value);
      byteValues[column] = byteValue;
    }

    var primaryKey = Uint8List(strategy.primaryKeysLength);

    for (var setter in strategy.primaryKeysSetStrategy) {
      var byteValue = byteValues[setter.column];
      primaryKey[setter.to] = byteValue[setter.from];
    }
    return primaryKey;
  }
  throw "テーブル書き込み戦略がなかった";
}
