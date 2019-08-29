part of isyumi_deltist;

Uint8List dartValueToU8int(StoreTuple storeTuple, Column key, dynamic value) {
  if (key is UintColumn) {
    return intToU8List(value);
  } else if (key is StringColumn) {
    var bytes = stringToU8List(storeTuple, key.code, value);
    return bytes;
  } else if (key is TextColumn) {
    return textToU8List(storeTuple, value);
  } else if (key is BoolColumn) {
    return boolToU8List(value);
  } else if (key is DateTimeColumn) {
    return dateTimeToU8List(value);
  }

  throw "知らない型のカラム";
}

dynamic u8IntToDartValue(
  StoreTuple storeTuple,
  Column key,
  Uint8List value,
) {
  if (key is UintColumn) {
    return u8ListToInt(value);
  } else if (key is BoolColumn) {
    return u8ListToBool(value);
  } else if (key is DateTimeColumn) {
    return u8ListToDateTime(value);
  } else if (key is StringColumn) {
    return u8ListToString(storeTuple, key.code, value);
  } else if (key is TextColumn) {
    return u8ListToText(storeTuple, value);
  }

  throw "知らない型のカラム";
}

List<Column> getSortedPrimaryKeys(TableOrView table) {
  return getSortedColumns(table.primaryKeys);
}

Set<Column> getOtherColumns(TableOrView table) {
  return table.columns.difference(table.primaryKeys);
}

List<Column> getSortedOtherColumns(TableOrView table) {
  return getSortedColumns(getOtherColumns(table));
}

List<Column> getSortedColumns(Iterable<Column> columns) {
  return columns.toList(growable: false)
    ..sort((c1, c2) => c1.name.compareTo(c2.name));
}

Uint8List stringToU8List(StoreTuple storeTuple, Code code, Object value) {
  if (value is String) {
    if (code == Code.Utf8) {
      return storeTuple.strStore.stringToId(storeTuple.rocksdb,
          storeTuple.storeRocksDBPointer, Utf8Encoder().convert(value));
    } else if (code == Code.Ascii) {
      return storeTuple.strStore.stringToId(storeTuple.rocksdb,
          storeTuple.storeRocksDBPointer, AsciiEncoder().convert(value));
    }
    throw "知らないエンコード";
  }
  throw "Stringじゃないものが来た";
}

Uint8List intToU8List(Object value) {
  if (value is int) {
    var list = Uint8List(8);
    for (var i = 0; i < 8; i++) {
      var cell = value >> (8 * i);
      var fill = cell & 255;
      list[7 - i] = fill;
    }
    return list;
  }
  throw "intじゃないのが来た";
}

Uint8List textToU8List(StoreTuple storeTuple, Object value) {
  Uint8List getDelegateId(Uint8List codes) {
    return storeTuple.textStore
        .textToHash(storeTuple.rocksdb, storeTuple.storeRocksDBPointer, codes);
  }

  if (value is String) {
    return getDelegateId(Utf8Encoder().convert(value));
  }
  throw "Stringじゃないのが来た";
}

Uint8List boolToU8List(Object value) {
  if (value is bool) {
    return Uint8List.fromList([value ? 0 : 0xff]);
  }
  throw "boolじゃないのが来た";
}

Uint8List dateTimeToU8List(Object value) {
  if (value is DateTime) {
    return intToU8List(value.millisecondsSinceEpoch);
  }
  throw "DateTimeじゃないのが来た";
}

int u8ListToInt(Uint8List list) {
  var results = 0;
  for (var i = 0; i < 8; i++) {
    results = results << 8;
    results += list[i];
  }
  return results;
}

String u8ListToString(StoreTuple storeTuple, Code code, Uint8List id) {
  var bytes = storeTuple.strStore
      .idToString(storeTuple.rocksdb, storeTuple.storeRocksDBPointer, id);
  if (code == Code.Ascii) {
    return AsciiDecoder().convert(bytes);
  } else if (code == Code.Utf8) {
    return Utf8Decoder().convert(bytes);
  }
  throw "知らないエンコード";
}

String u8ListToText(StoreTuple storeTuple, Uint8List hash) {
  return Utf8Decoder().convert(storeTuple.textStore
      .hashToText(storeTuple.rocksdb, storeTuple.storeRocksDBPointer, hash));
}

bool u8ListToBool(Uint8List value) {
  if (value[0] == 0) {
    return true;
  } else if (value[0] == 0xff) {
    return false;
  }
  throw "0でも255でもないものが来た";
}

DateTime u8ListToDateTime(Uint8List value) {
  int time = u8ListToInt(value);
  return DateTime.fromMillisecondsSinceEpoch(time);
}

// 使い捨てにすること
class StoreTuple {
  StoreTuple(
      this.rocksdb, this.storeRocksDBPointer, this.strStore, this.textStore);

  final RocksDB rocksdb;
  final StringStore strStore;
  final TextStore textStore;
  final StoreRocksDBPointer storeRocksDBPointer;
}
