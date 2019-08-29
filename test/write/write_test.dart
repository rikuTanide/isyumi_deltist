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

  test("テーブルとは違うカラムが来たらエラー", () {
    var wr = WritableRow(tableD);

    expect(() => wr.set(tableB.bID, 1),
        Throws(TypeMatcher<ColumnIsNotPartOfTableException>()));
  });

  test("全ての列が埋まっていなかったらエラー", () {
    var wr = WritableRow(tableD);

    expect(() => db.write(wr),
        Throws(TypeMatcher<IncompleteWriteRequestException>()));
  });

  test("DBの知らないテーブルに書き込もうとするとエラー", () {
    var wr = WritableRow(tableE);

    expect(() => db.write(wr), Throws(TypeMatcher<UnknownTableException>()));
  });

  test("書き込める", () {
    var dbPath = './test_db/write_test';

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

  TableB() : super("bs", "b");

  @override
  Set<Column> get columns => {bID};

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
