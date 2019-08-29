library isyumi_deltist;

import 'dart:collection';

import 'package:quiver/core.dart';
import 'package:rocksdb_interop/rocksdb_interop.dart' as rocksdb_api;
import 'dart:io';
import 'package:path/path.dart' as path;
import 'dart:typed_data';
import 'dart:convert';
import 'package:crypto/crypto.dart';
import 'package:quiver/collection.dart';

part 'src/view_columns.dart';

part 'src/view.dart';

part 'src/write.dart';

part 'src/read.dart';

part 'src/database.dart';

part 'src/utils.dart';

part 'src/rocks_db_wrapper.dart';

part 'src/table_expressions.dart';

part 'src/blob_store.dart';

part 'src/physical_location_strategy.dart';

part 'src/view_update_strategy.dart';

part 'src/create_index_strategy.dart';

enum Code {
  Utf8,
  Ascii,
}

// WritableRowの引数を限定するため（ViewのColumnに書き込めるようなコードをかけないように）

class StringColumn extends Column<String> {
  final String name;
  final Code code;

  @override
  final byteLength = 8;

  StringColumn(this.name, this.code);
}

class UintColumn extends Column<int> {
  final String name;
  final int bitLength;

  @override
  final byteLength = 8;

  // その他8bitとかは後で作る
  UintColumn.bit64(this.name) : this.bitLength = 64;
}

class TextColumn extends Column<String> {
  final String name;

  @override
  final byteLength = 32;

  TextColumn(this.name);
}

class BoolColumn extends Column<bool> {
  final String name;

  @override
  final byteLength = 1;

  BoolColumn(this.name);
}

class DateTimeColumn extends Column<DateTime> {
  final String name;

  @override
  final byteLength = 8;

  DateTimeColumn(this.name);
}

abstract class Column<T> {
  String get name;

  int get byteLength;

  String toString() => name;
}

abstract class Table implements TableOrView {
  final String tableName;

  final String singular;

  Table(this.tableName, this.singular);

  Set<Column> get columns;

  Set<Column> get primaryKeys;
}

abstract class TableOrView {
  String get tableName;

  Set<Column> get primaryKeys;

  Set<Column> get columns;
}
