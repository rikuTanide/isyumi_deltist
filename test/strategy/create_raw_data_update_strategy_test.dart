import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'package:test/test.dart';
import 'dart:io';

void main() {
  test("生データ更新戦略が正しく立案できるか", () {
    var tableA = TableA();
    var strategy = createRawDataUpdateStrategy(tableA);

    expect(strategy.table, equals(tableA));
    expect(strategy.primaryKeysLength, equals(9));
    expect(
        strategy.primaryKeysSetStrategy,
        equals([
          SetU8int()
            ..column = tableA.aBool
            ..to = 0
            ..from = 0,
          SetU8int()
            ..column = tableA.aID
            ..to = 1
            ..from = 0,
          SetU8int()
            ..column = tableA.aID
            ..to = 2
            ..from = 1,
          SetU8int()
            ..column = tableA.aID
            ..to = 3
            ..from = 2,
          SetU8int()
            ..column = tableA.aID
            ..to = 4
            ..from = 3,
          SetU8int()
            ..column = tableA.aID
            ..to = 5
            ..from = 4,
          SetU8int()
            ..column = tableA.aID
            ..to = 6
            ..from = 5,
          SetU8int()
            ..column = tableA.aID
            ..to = 7
            ..from = 6,
          SetU8int()
            ..column = tableA.aID
            ..to = 8
            ..from = 7,
        ]));
    expect(strategy.otherColumnsLength, equals(32 + 8 + 8));


    expect(
        strategy.otherColumnsSetStrategy[0],
        equals(SetU8int()
          ..column = tableA.aDate
          ..to = 0
          ..from = 0));
    expect(
        strategy.otherColumnsSetStrategy[1],
        equals(SetU8int()
          ..column = tableA.aDate
          ..to = 1
          ..from = 1));
    expect(
        strategy.otherColumnsSetStrategy[8],
        equals(SetU8int()
          ..column = tableA.aString
          ..to = 8
          ..from = 0));

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
