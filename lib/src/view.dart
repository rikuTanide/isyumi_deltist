part of isyumi_deltist;

abstract class View implements TableOrView {
  final String tableName;

  final String singular;

  View(this.tableName, this.singular);

  Set<ViewColumn> get columns;

  Set<ViewColumn> get primaryKeys;

  List<TableOrView> get references;
}

abstract class SelectView<T extends TableOrView> extends View {
  final T parent;

  @override
  List<TableOrView> get references => [parent];

  SelectView(String tableName, String singular, this.parent)
      : super(tableName, singular);
}

enum WhenDuplicateStrategy {
  DontWorry,
  OverwriteLeftWithRight, // 左を右で上書き
  OverwriteRightWithLeft, // 右を左で上書き
}

abstract class Union<T extends TableOrView, S extends TableOrView>
    extends View {
  T leftTable;

  S rightTable;

  @override
  List<TableOrView> get references => [leftTable, rightTable];

  final WhenDuplicateStrategy whenDuplicate;

  Union(String tableName, String singular, this.leftTable, this.rightTable,
      this.whenDuplicate)
      : super(tableName, singular);
}

class JoinOnAnd extends JoinOn {
  final JoinOn left;
  final JoinOn right;

  JoinOnAnd(this.left, this.right);
}

class JoinOnOr extends JoinOn {
  final JoinOn left;
  final JoinOn right;

  JoinOnOr(this.left, this.right);
}

class JoinOn {
  JoinOn operator &(JoinOn right) => JoinOnAnd(this, right);

  JoinOn operator |(JoinOn right) => JoinOnOr(this, right);
}

class OnEqual extends JoinOn {
  final InnerJoin view;
  final Column left;
  final Column right;

  OnEqual(this.view, this.left, this.right) {
    if (!view.leftTable.columns.contains(left)) {
      throw "OnEqualのLeftが${view.leftTable.tableName}のカラムじゃない";
    }
    if (!view.rightTable.columns.contains(right)) {
      throw "OnEqualのRightが${view.leftTable.tableName}のカラムじゃない";
    }
  }
}

abstract class InnerJoin<T extends TableOrView, S extends TableOrView>
    extends View {
  final T leftTable;

  final S rightTable;

  @override
  List<TableOrView> get references => [leftTable, rightTable];

  InnerJoin(String tableName, String singular, this.leftTable, this.rightTable)
      : super(tableName, singular);

  JoinOn get on;
}
