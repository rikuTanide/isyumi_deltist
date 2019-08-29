import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'package:test/test.dart';
import 'dart:io';

void main() {
  test("プライマリーキーが空ならエラー", () {
    expect(() => Database([TableA()], []),
        Throws(TypeMatcher<EmptyPrimaryKeysException>()));
  });
  test("プライマリーキーがcolumnsになければエラー", () {
    expect(() => Database([TableB()], []),
        Throws(TypeMatcher<ColumnIsNotPartOfTableException>()));
  });
  test("プライマリーキーがtext型ならエラー", () {
    expect(() => Database([TableC()], []),
        Throws(TypeMatcher<PrimaryKeyTypeException>()));
  });
}

class TableA extends Table {
  TableA() : super("users", "user");

  @override
  Set<Column> get columns => {};

  @override
  Set<Column> get primaryKeys => {};
}

class TableB extends Table {
  final UintColumn id1 = UintColumn.bit64("id1");
  final UintColumn id2 = UintColumn.bit64("id2");
  final StringColumn name = StringColumn("name", Code.Utf8);

  TableB() : super("users", "user");

  @override
  Set<Column> get columns => {id1, name};

  @override
  Set<Column> get primaryKeys => {id1, id2};
}

class TableC extends Table {
  final TextColumn id1 = TextColumn("id1");
  final StringColumn name = StringColumn("name", Code.Utf8);

  TableC() : super("users", "user");

  @override
  Set<Column> get columns => {id1, name};

  @override
  Set<Column> get primaryKeys => {id1};
}
