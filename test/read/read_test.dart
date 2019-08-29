import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'package:test/test.dart';
import 'dart:io';

void main() {
  var tableD = TableD();
  var tableB = TableB();
  var viewC = ViewC(tableD);
  var viewA = ViewA(tableB, viewC);
  var tableE = TableE();
  var db = Database([tableB, tableD], [viewA, viewC]);

  test("DBの持っているテーブルじゃなければエラー", () {
    var readKey = ReadKey(tableE);
    expect(
        () => db.read(readKey), Throws(TypeMatcher<UnknownTableException>()));
  });
  test("Primary Keyが埋まっていなければエラー", () {
    var readKey = ReadKey(viewA);
    expect(() => db.read(readKey),
        Throws(TypeMatcher<IncompleteReadRequestException>()));
  });

  test("Set時にそのテーブルのカラムじゃなければエラー", () {
    var readKey = ReadKey(viewA);
    expect(() => readKey.set(tableB.bID, 2),
        Throws(TypeMatcher<ColumnIsNotPartOfTableException>()));
  });

  test("書き込んだデータが読み込める", () {
    var dbPath = './test_db/read_test';
    var dbDir = Directory(dbPath);
    if (dbDir.existsSync()) {
      dbDir.deleteSync(recursive: true);
    }
    dbDir.createSync();

    var tableF = TableF();
    var db = Database([tableF], [])
      ..create(dbPath)
      ..open(dbPath);
    var wr = WritableRow(tableF);
    wr.set(tableF.fID, 2);
    wr.set(tableF.fString, "some string");
    wr.set(tableF.fText, "なにかのテキスト");
    wr.set(tableF.fBool, false);
    wr.set(tableF.fDate, DateTime(2019, 1, 2));
    db.write(wr);
    db.close();

    var db2 = Database([tableF], [])..open(dbPath);
    var readKey = ReadKey(tableF)..set(tableF.fID, 2);
    var readableRow = db2.read(readKey);
    var fID = readableRow.get(tableF.fID);
    var fString = readableRow.get(tableF.fString);
    var fText = readableRow.get(tableF.fText);
    var fBool = readableRow.get(tableF.fBool);
    var fDate = readableRow.get(tableF.fDate);
    db2.close();

    expect(fID, equals(2));
    expect(fString, equals("some string"));
    expect(fText, equals("なにかのテキスト"));
    expect(fBool, equals(false));
    expect(fDate, equals(DateTime(2019, 1, 2)));
  });

  test("Get時にそのテーブルのカラムじゃなければエラー", () {
    var dbPath = './test_db/read_test';
    var dbDir = Directory(dbPath);
    if (dbDir.existsSync()) {
      dbDir.deleteSync(recursive: true);
    }
    dbDir.createSync();

    var db = Database([tableB, tableD], [viewA, viewC])
      ..create(dbPath)
      ..open(dbPath);
    var wr = WritableRow(tableB);
    wr.set(tableB.bID, 2);
    wr.set(tableB.bDate, DateTime(2019, 9, 1));
    db.write(wr);
    db.close();

    var db2 = Database([tableB, tableD], [viewA, viewC])..open(dbPath);
    var readKey = ReadKey(tableB)..set(tableB.bID, 2);
    var readableRow = db2.read(readKey);
    var date = readableRow.get(tableB.bDate);
    db2.close();

    expect(date, equals(DateTime(2019, 9, 1)));

    expect(() => readableRow.get(viewA.aID),
        Throws(TypeMatcher<ColumnIsNotPartOfTableException>()));
  });
}

class ViewA extends InnerJoin {
  ViewColumn<int> get aID => JoinColumn("aID", this, b.bID, c.cID);

  @override
  Set<ViewColumn> get columns => {aID};

  @override
  JoinOn get on => OnEqual(this, b.bID, c.cID);

  @override
  Set<ViewColumn> get primaryKeys => {aID};

  final TableB b;
  final ViewC c;

  ViewA(this.b, this.c) : super("as", "a", b, c);
}

class TableB extends Table {
  final UintColumn bID = UintColumn.bit64("bID");

  final DateTimeColumn bDate = DateTimeColumn("bDate");

  TableB() : super("bs", "b");

  @override
  Set<Column> get columns => {bID, bDate};

  @override
  Set<Column> get primaryKeys => {bID};
}

class ViewC extends SelectView {
  ViewColumn<int> get cID => SelectColumn("cID", this, d.dID);

  @override
  Set<ViewColumn> get columns => {cID};

  @override
  Set<ViewColumn> get primaryKeys => {cID};

  final TableD d;

  ViewC(this.d) : super("cs", "c", d);
}

class TableD extends Table {
  final UintColumn dID = UintColumn.bit64("dID");

  TableD() : super("ds", "d");

  @override
  Set<Column> get columns => {dID};

  @override
  Set<Column> get primaryKeys => {dID};
}

class TableE extends Table {
  final UintColumn eID = UintColumn.bit64("eID");

  TableE() : super("es", "e");

  @override
  Set<Column> get columns => {eID};

  @override
  Set<Column> get primaryKeys => {eID};
}

class TableF extends Table {
  final UintColumn fID = UintColumn.bit64("eID");

  final TextColumn fText = TextColumn("fText");
  final DateTimeColumn fDate = DateTimeColumn("fDate");
  final StringColumn fString = StringColumn("fString", Code.Ascii);
  final BoolColumn fBool = BoolColumn("fString");

  TableF() : super("fs", "f");

  @override
  Set<Column> get columns => {fID, fText, fDate, fString, fBool};

  @override
  Set<Column> get primaryKeys => {fID};
}
