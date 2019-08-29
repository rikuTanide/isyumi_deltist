import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'package:test/test.dart';
import 'dart:io';

void main() {
  test("ViewColumnのViewが一致していなかったらエラー", () {
    var tableD = TableD();
    var tableB = TableB();
    var viewC = ViewC(tableD);
    var viewA = ViewA(tableB, viewC);
    var viewH = ViewH(tableB, viewC);

    expect(() => Database([tableB, tableD], [viewC, viewA, viewH]),
        Throws(TypeMatcher<ViewColumnParentException>()));
  });
}

class ViewA extends InnerJoin {
  ViewColumn<int> get aID => SelectColumn("aID", c, b.bID);

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

class ViewG extends Union<TableB, TableB> {
  ViewG(TableB t1, TableB t2)
      : super("gs", "g", t1, t2, WhenDuplicateStrategy.DontWorry);

  UnionColumn<int> get key =>
      UnionColumn<int>("key", this, leftTable.bID, this.rightTable.bID);

  @override
  Set<ViewColumn> get columns => {key};

  @override
  Set<ViewColumn> get primaryKeys => {key};
}

class ViewH extends InnerJoin {
  ViewColumn<int> get hID => JoinColumn("hID", this, b.bID, c.cID);

  ViewColumn<int> get attr => c.cID;

  @override
  Set<ViewColumn> get columns => {hID};

  @override
  JoinOn get on => OnEqual(this, b.bID, c.cID);

  @override
  Set<ViewColumn> get primaryKeys => {hID};

  final TableB b;
  final ViewC c;

  ViewH(this.b, this.c) : super("hs", "h", b, c);
}
