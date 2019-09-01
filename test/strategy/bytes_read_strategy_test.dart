import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'package:test/test.dart';
import 'dart:io';

void main() {
  test("生データ取得戦略が正しく立案できるか", () {
    var tableA = TableA();
    var strategy = createBytesReadStrategy(tableA);

    expect(strategy.table, equals(tableA));
    expect(strategy.readOtherColumnsStrategy.length, equals(3));
    expect(strategy.readOtherColumnsStrategy[0].length, equals(8));
    expect(
        strategy.readOtherColumnsStrategy[0].copies[0],
        equals(CopyU8int()
          ..to = 0
          ..from = 0));
    expect(
        strategy.readOtherColumnsStrategy[1].copies[0],
        equals(CopyU8int()
          ..to = 0
          ..from = 8));
  });
}

class TableA extends Table {
  final UintColumn aID = UintColumn.bit64("aID");

  final TextColumn aText = TextColumn("aText");
  final DateTimeColumn aDate = DateTimeColumn("aDate");
  final StringColumn aString = StringColumn("aString", Code.Ascii);
  final BoolColumn aBool = BoolColumn("aBool");

  TableA() : super("as", "f");

  @override
  Set<Column> get columns => {aID, aText, aDate, aString, aBool};

  @override
  Set<Column> get primaryKeys => {aID, aBool};
}
