import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'package:test/test.dart';

void main() {
  var tableD = TableD();
  var tableB = TableB();
  var viewC = ViewC(tableD);
  var viewA = ViewA(tableB, viewC);

  test("テーブル名が重複していたらエラーにする", () {
    expect(() => Database([tableB, tableD], [viewA, viewC]),
        Throws(TypeMatcher<DuplicatedTableNameException>()));
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

  // ここに名前かぶりがある
  TableD() : super("as", "a");

  @override
  Set<Column> get columns => {dID};

  @override
  Set<Column> get primaryKeys => {dID};
}
