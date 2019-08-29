part of isyumi_deltist;

abstract class ViewColumn<T> extends Column<T> {
  final String name;
  final View view;

  ViewColumn(this.name, this.view);

  String toString() => view.tableName + "/" + name;

}

class UnionColumn<T> extends ViewColumn<T> {
  final Column<T> left;
  final Column<T> right;

  @override
  int get byteLength => left.byteLength;

  UnionColumn(String name, View view, this.left, this.right)
      : super(name, view);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is UnionColumn &&
          runtimeType == other.runtimeType &&
          name == other.name &&
          view == other.view &&
          left == other.left &&
          right == other.right;

  @override
  int get hashCode =>
      name.hashCode ^ view.hashCode ^ left.hashCode ^ right.hashCode;
}

class SelectColumn<T> extends ViewColumn<T> {
  final Column<T> from;

  @override
  int get byteLength => from.byteLength;

  SelectColumn(String name, View view, this.from) : super(name, view);

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SelectColumn &&
          runtimeType == other.runtimeType &&
          name == other.name &&
          view == other.view &&
          from == other.from;

  @override
  int get hashCode => name.hashCode ^ view.hashCode ^ from.hashCode;
}

class JoinColumn<T> extends ViewColumn<T> {
  final Column<T> left;
  final Column<T> right;

  @override
  int get byteLength => left.byteLength;

  JoinColumn(String name, InnerJoin view, this.left, this.right) : super(name, view){
    if (!view.leftTable.columns.contains(left)) {
      throw "JoinColumnのLeftが${view.leftTable.tableName}のカラムじゃない";
    }
    if (!view.rightTable.columns.contains(right)) {
      throw "JoinColumnのRightが${view.leftTable.tableName}のカラムじゃない";
    }
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is JoinColumn &&
          runtimeType == other.runtimeType &&
          name == other.name &&
          view == other.view &&
          left == other.left &&
          right == other.right;

  @override
  int get hashCode =>
      name.hashCode ^ view.hashCode ^ left.hashCode ^ right.hashCode;
}
