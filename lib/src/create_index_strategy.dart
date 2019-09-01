part of isyumi_deltist;

enum GetOrSeek {
  get,
  seek,
}

enum UseIndexReason { left, right, ownFromLeft, ownFromRight, parent }

class CreateIndexRequest {
  TableOrView table;
  List<Column> columns;
}

abstract class UsePrimaryKeyOrIndex {}

class UsePrimaryKey implements UsePrimaryKeyOrIndex {
  String toString() {
    return "primary key";
  }
}

class UseIndex implements UsePrimaryKeyOrIndex {
  Index index;

  String toString() {
    return index.columns.toString();
  }
}

class Index {
  TableOrView table;
  int columnsLength;
  List<Column> columns;
  int indexID;
  String name;

  // valueはKeyをコピーするだけ
  List<CopyU8int> primaryKeyToKey;
  List<CopyU8int> otherColumnsToKey;

  String toString() {
    var primaryKeyToKeyDescription = primaryKeyToKey
        .map((c) =>
            "$name.key[${leftPad(c.to)}] = ${table.tableName}.key[${leftPad(c.from)}]")
        .join("\n");
    var otherColumnsToKeyDescription = otherColumnsToKey
        .map((c) =>
            "$name.key[${leftPad(c.to)}] = ${table.tableName}.value[${leftPad(c.from)}]")
        .join("\n");

    return """
${table.tableName}#$indexID($name)
${columns}
$primaryKeyToKeyDescription

$otherColumnsToKeyDescription
""";
  }
}

class ViewIndexMapping {
  TableOrView table; // 検索先
  TableOrView consumer;

  UseIndexReason reason;
  GetOrSeek getOrSeek;

  Map<Column, Column> parentToOwn;
  Map<Column, Column> ownToParent;

  UsePrimaryKeyOrIndex determinedIndex;
  List<Column> requestIndex;
}

class UseIndexRequest {
  TableOrView table;

  // 順番は関係ない。後ろにPrimaryKeyをつめない
  Set<Column> columns;

  TableOrView consumer;

  UseIndexReason reason;

  Map<Column, Column> parentToOwn;
  Map<Column, Column> ownToParent;

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is UseIndexRequest &&
          runtimeType == other.runtimeType &&
          table == other.table &&
          columns == other.columns;

  @override
  int get hashCode => table.hashCode ^ columns.hashCode;

  String toString() {
    return "${consumer.tableName}->${table.tableName}: ${columns.toString()}";
  }
}

class IndexTree extends DelegatingMap<Column, IndexTree> {
  @override
  Map<Column, IndexTree> delegate = {};

  void set(Set<Column> columns) {
    if (columns.isEmpty) {
      return;
    }

    var first =
        columns.reduce((c1, c2) => c1.name.compareTo(c2.name) == -1 ? c1 : c2);

    var others = columns.toSet()..remove(first);

    delegate.putIfAbsent(first, () => IndexTree());

    delegate[first].set(others);
  }

  String toString() {
    return delegate.toString();
  }

  List<List<Column>> serialize() {
    var results = <List<Column>>[];
    for (var column in delegate.keys) {
      if (delegate[column].isEmpty) {
        results.add([column]);
      } else {
        var children = delegate[column].serialize();
        for (var child in children) {
          var parallel = [column]..addAll(child);
          results.add(parallel);
        }
      }
    }
    return results;
  }
}

class IndexStrategies {
  List<MaterializeStrategy> materializeStrategies;
  List<Index> indexes;
}

IndexStrategies createIndexStrategies(List<View> views) {
  var builders = views.map((v) => ViewStrategyBuilder(v));
  var useIndexRequests = getUseIndexRequest(builders);
  var tableIndexTree = createTableTree(useIndexRequests);
  var createIndexRequests = filterEnoughPrimaryKey(tableIndexTree.serialize());
  var indexes = createIndexes(createIndexRequests).toList();
  var viewIndexMappings =
      useIndexRequests.map((r) => findCreatedIndexes(r, indexes));
  var strategies = builders
      .map((b) => b.createMaterializeStrategy(viewIndexMappings))
      .toList();
  var results = <MaterializeStrategy>[];
  for (var strs in strategies) {
    for (var str in strs) {
      results.add(str);
    }
  }
  return IndexStrategies()
    ..indexes = indexes
    ..materializeStrategies = results;
}

TableIndexTree createTableTree(Iterable<UseIndexRequest> useIndexRequests) {
  var tableIndexTree = TableIndexTree();
  for (var index in useIndexRequests) {
    tableIndexTree.set(index.table, index.columns);
  }
  return tableIndexTree;
}

class TableIndexTree extends DelegatingMap<TableOrView, IndexTree> {
  @override
  Map<TableOrView, IndexTree> delegate = {};

  void set(TableOrView table, Set<Column> columns) {
    delegate.putIfAbsent(table, () => IndexTree());
    delegate[table].set(columns);
  }

  String toString() {
    return delegate.toString();
  }

  List<CreateIndexRequest> serialize() {
    var list = <CreateIndexRequest>[];
    for (var table in delegate.keys) {
      for (var index in delegate[table].serialize()) {
        var req = CreateIndexRequest()
          ..table = table
          ..columns = index;
        list.add(req);
      }
    }
    return list;
  }
}

List<UseIndexRequest> getUseIndexRequest(
    Iterable<ViewStrategyBuilder> builders) {
  var indexes = <UseIndexRequest>[];
  for (var b in builders) {
    var index = b.getUseIndex();
    indexes.addAll(index);
  }
  return indexes;
}

Iterable<Index> createIndexes(Iterable<CreateIndexRequest> requests) {
  var l = requests.toList();
  return requests.map((r) => createIndex(r, l.indexOf(r)));
}

Index createIndex(CreateIndexRequest req, int indexID) {
  var additionalPrimaryKeys =
      getSortedColumns(req.table.primaryKeys.difference(req.columns.toSet()));
  var columns = [...req.columns, ...additionalPrimaryKeys];
  return Index()
    ..table = req.table
    ..columns = columns //+ otherColumns()
    ..name = "index:" + req.columns.map((c) => c.name).join("-")
    ..indexID = indexID
    ..columnsLength = columnsByteLength(columns)
    ..primaryKeyToKey = mapColumnsToIndex(columns, req.table.primaryKeys)
    ..otherColumnsToKey =
        mapColumnsToIndex(columns, getOtherColumns(req.table));
}

List<CopyU8int> mapColumnsToIndex(
    List<Column> indexColumns, Set<Column> originalColumns) {
  var results = List<CopyU8int>();
  var count = 0;
  for (var column in indexColumns) {
    if (!originalColumns.contains(column)) {
      count += column.byteLength;
      continue;
    }
    for (var i = 0; i < column.byteLength; i++) {
      var copy = CopyU8int()
        ..from = countColumnOffset(originalColumns, column) + i
        ..to = count;
      results.add(copy);
      count++;
    }
  }
  return results;
}

Iterable<CreateIndexRequest> filterEnoughPrimaryKey(
    List<CreateIndexRequest> createIndexRequests) {
  return createIndexRequests.where((c) => !isEnoughPrimaryKey(c));
}

bool isEnoughPrimaryKey(CreateIndexRequest c) {
  if (c.table.primaryKeys.length < c.columns.length) {
    return false;
  }

  var keys = getLeadPrimaryKeys(c.table, c.columns.length);

  for (var i = 0; i < keys.length; i++) {
    if (keys[i] != c.columns[i]) {
      return false;
    }
  }
  return true;
}

List<Column> getLeadPrimaryKeys(TableOrView table, int length) {
  return getSortedPrimaryKeys(table).take(length).toList();
}

ViewIndexMapping findCreatedIndexes(
    UseIndexRequest req, Iterable<Index> indexes) {
  // PrimaryKeyと先頭が一致するか
  // 　一致したら全てのPrimaryKeyを持っているか？ GetOrElse
  // 一致するIndexはなんだ？
  // そのIndexと長さが一致するか？
  // エラー

  if (canUsePrimaryKey(req)) {
    return ViewIndexMapping()
      ..reason = req.reason
      ..determinedIndex = UsePrimaryKey()
      ..getOrSeek = req.columns.length == req.table.primaryKeys.length
          ? GetOrSeek.get
          : GetOrSeek.seek
      ..requestIndex = getSortedColumns(req.columns)
      ..parentToOwn = req.parentToOwn
      ..ownToParent = req.ownToParent
      ..table = req.table
      ..consumer = req.consumer;
  }

  var index = indexes.firstWhere((i) => findCreatedIndex(i, req));

  return ViewIndexMapping()
    ..reason = req.reason
    ..determinedIndex = (UseIndex()..index = index)
    ..getOrSeek = req.columns.length == index.columns.length
        ? GetOrSeek.get
        : GetOrSeek.seek
    ..requestIndex = getSortedColumns(req.columns)
    ..parentToOwn = req.parentToOwn
    ..ownToParent = req.ownToParent
    ..table = req.table
    ..consumer = req.consumer;
}

bool canUsePrimaryKey(UseIndexRequest req) {
  var sortedPrimaryKeys = getSortedPrimaryKeys(req.table);
  var columns = getSortedColumns(req.columns);
  for (var i = 0; i < req.columns.length; i++) {
    if (columns[i] != sortedPrimaryKeys[i]) {
      return false;
    }
  }
  return true;
}

bool findCreatedIndex(Index index, UseIndexRequest req) {
  var columns = getSortedColumns(req.columns);

  for (var i = 0; i < req.columns.length; i++) {
    if (columns[i] != index.columns[i]) {
      return false;
    }
  }
  return true;
}

// is 文を一回で済ますため
abstract class ViewStrategyBuilder {
  factory ViewStrategyBuilder(View t) {
    if (t is SelectView) {
      return SelectViewStrategyBuilder(t);
    } else if (t is Union) {
      return UnionViewStrategyBuilder(t);
    } else if (t is InnerJoin) {
      return InnerJoinStrategyBuilder(t);
    }
    throw "知らないタイプのビュー";
  }

  /// primary keyも含める
  /// どのカラムをどのカラムにマッピングするかという情報は必要だから
  List<UseIndexRequest> getUseIndex();

  List<MaterializeStrategy> createMaterializeStrategy(
      Iterable<ViewIndexMapping> viewIndexMappings);
}

abstract class MaterializeStrategy {
  TableOrView parent;
}

abstract class UseIndexStrategy {}

//　右の親の分と左の親の分がある
class InnerJoinMaterializeStrategy implements MaterializeStrategy {
  TableOrView parent;
  View view;

  TableOrView otherParent;
  UsePrimaryKeyOrIndex otherParentIndex;

  GetOrSeek getOrSeek;

  int otherParentPrimaryKeyLength;
  List<CopyU8int> parentPrimaryKeyToOtherParentPrimaryKey;
  List<CopyU8int> parentOtherColumnsToOtherParentPrimaryKey;

  int primaryKeyLength;
  List<CopyU8int> parentPrimaryKeyToPrimaryKey;
  List<CopyU8int> parentOtherColumnToPrimaryKey;
  List<CopyU8int> otherParentPrimaryKeyToPrimaryKey;
  List<CopyU8int> otherParentOtherColumnToPrimaryKey;

  int otherColumnsLength;
  List<CopyU8int> parentPrimaryKeyToOtherColumn;
  List<CopyU8int> parentOtherColumnToOtherColumn;
  List<CopyU8int> otherParentPrimaryKeyToOtherColumn;
  List<CopyU8int> otherParentOtherColumnToOtherColumn;

  /// 自分を消したり更新したりする時に使うIndex
  UsePrimaryKeyOrIndex ownIndex;
  int ownIndexLength; // Seekの場合があるからIndexの長さとは限らない
  List<CopyU8int> parentPrimaryKeyToOwnIndex;
  List<CopyU8int> parentOtherColumnsToOwnIndex;

  // ここはparentじゃなくてown？
  GetOrSeek parentIndexGetOrSeek;

  String toString() {
    var parentPrimaryKeyToOtherParentPrimaryKeyDescription =
        parentPrimaryKeyToOtherParentPrimaryKey
            .map((c) =>
                "${otherParent.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
            .join("\n");
    var parentOtherColumnsToOtherParentPrimaryKeyDescription =
        parentOtherColumnsToOtherParentPrimaryKey
            .map((c) =>
                "${otherParent.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
            .join("\n");

    var parentPrimaryKeyToPrimaryKeyDescription = parentPrimaryKeyToPrimaryKey
        .map((c) =>
            "${view.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
        .join("\n");
    var parentOtherColumnToPrimaryKeyDescription = parentOtherColumnToPrimaryKey
        .map((c) =>
            "${view.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.value[${leftPad(c.from)}]")
        .join("\n");
    var otherParentPrimaryKeyToPrimaryKeyDescription =
        otherParentPrimaryKeyToPrimaryKey
            .map((c) =>
                "${view.tableName}.key[${leftPad(c.to)}] = ${otherParent.tableName}.key[${leftPad(c.from)}]")
            .join("\n");
    var otherParentOtherColumnToPrimaryKeyDescription =
        otherParentOtherColumnToPrimaryKey
            .map((c) =>
                "${view.tableName}.key[${leftPad(c.to)}] = ${otherParent.tableName}.value[${leftPad(c.from)}]")
            .join("\n");

    var parentPrimaryKeyToOtherColumnDescription = parentPrimaryKeyToOtherColumn
        .map((c) =>
            "${view.tableName}.value[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
        .join("\n");
    var parentOtherColumnToOtherColumnDescription = parentOtherColumnToOtherColumn
        .map((c) =>
            "${view.tableName}.value[${leftPad(c.to)}] = ${parent.tableName}.value[${leftPad(c.from)}]")
        .join("\n");
    var otherParentPrimaryKeyToOtherColumnDescription =
        otherParentPrimaryKeyToOtherColumn
            .map((c) =>
                "${view.tableName}.value[${leftPad(c.to)}] = ${otherParent.tableName}.key[${leftPad(c.from)}]")
            .join("\n");
    var otherParentOtherColumnToOtherColumnDescription =
        otherParentOtherColumnToOtherColumn
            .map((c) =>
                "${view.tableName}.value[${leftPad(c.to)}] = ${otherParent.tableName}.value[${leftPad(c.from)}]")
            .join("\n");

    var parentPrimaryKeyToOwnIndexDescription = parentPrimaryKeyToOwnIndex
        .map((c) =>
            "${view.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
        .join("\n");
    var parentOtherColumnsToOwnIndexDescription = parentOtherColumnsToOwnIndex
        .map((c) =>
            "${view.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.value[${leftPad(c.from)}]")
        .join("\n");

    return """
${parent.tableName} <- マージ ${otherParent.tableName}
  -> ${view.tableName}
 
相手のテーブルに問い合わせる時
$otherParentIndex ${getOrSeek.toString()}
Keyの長さ: $otherParentPrimaryKeyLength
$parentPrimaryKeyToOtherParentPrimaryKeyDescription
$parentOtherColumnsToOtherParentPrimaryKeyDescription

${view.tableName}
Keyの長さ: $primaryKeyLength
Valueの長さ: $otherColumnsLength
$parentPrimaryKeyToPrimaryKeyDescription
$parentOtherColumnToPrimaryKeyDescription
$otherParentPrimaryKeyToPrimaryKeyDescription
$otherParentOtherColumnToPrimaryKeyDescription
$parentPrimaryKeyToOtherColumnDescription
$parentOtherColumnToOtherColumnDescription
$otherParentPrimaryKeyToOtherColumnDescription
$otherParentOtherColumnToOtherColumnDescription

自分を消す時
$ownIndex ${parentIndexGetOrSeek.toString()}
Keyの長さ: $ownIndexLength
$parentPrimaryKeyToOwnIndexDescription
$parentOtherColumnsToOwnIndexDescription
""";
  }
}

class SelectViewMaterializeStrategy implements MaterializeStrategy {
  TableOrView parent;
  View view;

  int primaryKeyLength;
  int otherColumnsLength;
  List<CopyU8int> parentPrimaryKeyToPrimaryKey;
  List<CopyU8int> parentOtherColumnToPrimaryKey;
  List<CopyU8int> parentPrimaryKeyToOtherColumns;
  List<CopyU8int> parentOtherColumnToOtherColumns;

// 変更されたのがOtherColumnsだけならTableを変更する

  String toString() {
    var parentPrimaryKeyToPrimaryKeyDescription = parentPrimaryKeyToPrimaryKey
        .map((c) =>
            "key[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
        .join("\n");

    var parentOtherColumnToPrimaryKeyDescription = parentOtherColumnToPrimaryKey
        .map((c) =>
            "key[${leftPad(c.to)}] = ${parent.tableName}.value[${leftPad(c.from)}]")
        .join("\n");
    var parentPrimaryKeyToOtherColumnsDescription = parentPrimaryKeyToOtherColumns
        .map((c) =>
            "${view.tableName}.value[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
        .join("\n");
    var parentOtherColumnToOtherColumnsDescription = parentOtherColumnToOtherColumns
        .map((c) =>
            "${view.tableName}.value[${leftPad(c.to)}] = ${parent.tableName}.value[${leftPad(c.from)}]")
        .join("\n");

    return """
${parent.tableName} -> ${view.tableName}
Keyの長さ: $primaryKeyLength
Valueの長さ: $otherColumnsLength
変換 
$parentPrimaryKeyToPrimaryKeyDescription

$parentOtherColumnToPrimaryKeyDescription
$parentPrimaryKeyToOtherColumnsDescription
$parentOtherColumnToOtherColumnsDescription
""";
  }
}

String leftPad(int i) {
  return i.toString().padLeft(3, "0");
}

class UnionViewMaterializeStrategyFromSecondaryTable
    implements MaterializeStrategy {
  TableOrView primaryTable;
  TableOrView parent; // secondary

  View view;

  int primaryTablePrimaryKeyLength;
  List<CopyU8int> secondaryTablePrimaryKeyToPrimaryTablePrimaryKey;
  List<CopyU8int> secondaryTableOtherColumnsToPrimaryTablePrimaryKey;

  int primaryKeyLength;
  int otherColumnsLength;

  // primaryKeyにデータがあった時
  List<CopyU8int> primaryTablePrimaryKeyToPrimaryKey;
  List<CopyU8int> primaryTableOtherColumnsToPrimaryKey;
  List<CopyU8int> primaryTablePrimaryKeyToOtherColumn;
  List<CopyU8int> primaryTableOtherColumnsToOtherColumn;

  // parentにしかなかった時
  List<CopyU8int> secondaryTablePrimaryKeyToPrimaryKey;
  List<CopyU8int> secondaryTableOtherColumnToPrimaryKey;
  List<CopyU8int> secondaryTablePrimaryKeyToOtherColumn;
  List<CopyU8int> secondaryTableOtherColumnToOtherColumn;

  String toString() {
    var secondaryTablePrimaryKeyToPrimaryTablePrimaryKeyDescription =
        secondaryTablePrimaryKeyToPrimaryTablePrimaryKey
            .map((c) =>
                "${primaryTable.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
            .join("\n");

    var primaryTablePrimaryKeyToPrimaryKeyDescription =
        primaryTablePrimaryKeyToPrimaryKey
            .map((c) =>
                "${view.tableName}.key[${leftPad(c.to)}] = ${primaryTable.tableName}.key[${leftPad(c.from)}]")
            .join("\n");
    var primaryTableOtherColumnsToPrimaryKeyDescription =
        primaryTablePrimaryKeyToPrimaryKey
            .map((c) =>
                "${view.tableName}.key[${leftPad(c.to)}] = ${primaryTable.tableName}.value[${leftPad(c.from)}]")
            .join("\n");
    var primaryTablePrimaryKeyToOtherColumnDescription =
        primaryTablePrimaryKeyToPrimaryKey
            .map((c) =>
                "${view.tableName}.value[${leftPad(c.to)}] = ${primaryTable.tableName}.key[${leftPad(c.from)}]")
            .join("\n");
    var primaryTableOtherColumnsToOtherColumnDescription =
        primaryTablePrimaryKeyToPrimaryKey
            .map((c) =>
                "${view.tableName}.value[${leftPad(c.to)}] = ${primaryTable.tableName}.value[${leftPad(c.from)}]")
            .join("\n");

    var secondaryTablePrimaryKeyToPrimaryKeyDescription =
        secondaryTablePrimaryKeyToPrimaryKey
            .map((c) =>
                "${view.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
            .join("\n");
    var secondaryTableOtherColumnToPrimaryKeyDescription =
        secondaryTableOtherColumnToPrimaryKey
            .map((c) =>
                "${view.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.value[${leftPad(c.from)}]")
            .join("\n");
    var secondaryTablePrimaryKeyToOtherColumnDescription =
        secondaryTablePrimaryKeyToOtherColumn
            .map((c) =>
                "${view.tableName}.value[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
            .join("\n");
    var secondaryTableOtherColumnToOtherColumnDescription =
        secondaryTableOtherColumnToOtherColumn
            .map((c) =>
                "${view.tableName}.value[${leftPad(c.to)}] = ${parent.tableName}.value[${leftPad(c.from)}]")
            .join("\n");

    return """
${parent.tableName} +
${primaryTable.tableName}(primary) +
   -> ${view.tableName}

secondary parentに変更があった時、それに対応する行がprimary parentにあるか確認する
Keyの長さ: $primaryTablePrimaryKeyLength
$secondaryTablePrimaryKeyToPrimaryTablePrimaryKeyDescription

${view.tableName}のKeyの長さ$primaryKeyLength
${view.tableName}のValueの長さ$otherColumnsLength

primary tableにデータがあった時
$primaryTablePrimaryKeyToPrimaryKeyDescription
$primaryTableOtherColumnsToPrimaryKeyDescription
$primaryTablePrimaryKeyToOtherColumnDescription
$primaryTableOtherColumnsToOtherColumnDescription

primary tableにデータがなかった時
$secondaryTablePrimaryKeyToPrimaryKeyDescription
$secondaryTableOtherColumnToPrimaryKeyDescription
$secondaryTablePrimaryKeyToOtherColumnDescription
$secondaryTableOtherColumnToOtherColumnDescription
""";
  }
}

class UnionViewMaterializeStrategyFromPrimaryTable
    implements MaterializeStrategy {
  TableOrView parent; // primary

  View view;

  int primaryKeyLength;
  int otherColumnsLength;
  List<CopyU8int> parentPrimaryKeyToPrimaryKey;
  List<CopyU8int> parentOtherColumnToPrimaryKey;
  List<CopyU8int> parentPrimaryKeyToOtherColumns;
  List<CopyU8int> parentOtherColumnToOtherColumns;

  String toString() {
    var parentPrimaryKeyToPrimaryKeyDescription = parentPrimaryKeyToPrimaryKey
        .map((c) =>
            "${view.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
        .join("\n");
    var parentOtherColumnToPrimaryKeyDescription = parentOtherColumnToPrimaryKey
        .map((c) =>
            "${view.tableName}.key[${leftPad(c.to)}] = ${parent.tableName}.value[${leftPad(c.from)}]")
        .join("\n");
    var parentPrimaryKeyToOtherColumnsDescription = parentPrimaryKeyToOtherColumns
        .map((c) =>
            "${view.tableName}.value[${leftPad(c.to)}] = ${parent.tableName}.key[${leftPad(c.from)}]")
        .join("\n");
    var parentOtherColumnToOtherColumnsDescription = parentOtherColumnToOtherColumns
        .map((c) =>
            "${view.tableName}.value[${leftPad(c.to)}] = ${parent.tableName}.value[${leftPad(c.from)}]")
        .join("\n");

    return """
${parent.tableName} 
   -> ${view.tableName}
   
${view.tableName}のKeyの長さ$primaryKeyLength
${view.tableName}のValueの長さ$otherColumnsLength

$parentPrimaryKeyToPrimaryKeyDescription
$parentOtherColumnToPrimaryKeyDescription
$parentPrimaryKeyToOtherColumnsDescription
$parentOtherColumnToOtherColumnsDescription
""";
  }
}

class UnionUseIndexStrategy implements UseIndexStrategy {
  Union view;
}

class SelectIndexStrategy implements UseIndexStrategy {
  SelectView view;
}

class InnerJoinIndexStrategy implements UseIndexStrategy {
  InnerJoin view;
  UsePrimaryKeyOrIndex onLeft;
  UsePrimaryKeyOrIndex onRight;
  UsePrimaryKeyOrIndex doOwn;
}

Map<V, K> mapReverse<K, V>(Map<K, V> map) {
  return Map.fromEntries(map.entries.map((e) => MapEntry(e.value, e.key)));
}

class SelectViewStrategyBuilder implements ViewStrategyBuilder {
  final SelectView view;

  SelectViewStrategyBuilder(this.view);

  List<UseIndexRequest> getUseIndex() {
    return <UseIndexRequest>[
      UseIndexRequest()
        ..table = view.parent
        ..columns = Set.from(view.parent.primaryKeys)
        ..consumer = view
        ..reason = UseIndexReason.parent
        ..parentToOwn = getParentToOwns()
        ..ownToParent = getOwnToParents()
    ];
  }

  Map<Column, Column> getParentToOwns() {
    return Map.fromEntries(
        view.parent.primaryKeys.map((c) => MapEntry(c, getParentToOwn(c))));
  }

  Map<Column, Column> getOwnToParents() {
    return Map.fromEntries(
        view.primaryKeys.map((c) => MapEntry(c, getOwnToParent(c))));
  }

  Column getParentToOwn(Column parentColumn) {
    return view.columns
        .firstWhere((ownColumn) => isSelectBy(ownColumn, parentColumn));
  }

  Column getOwnToParent(ViewColumn ownColumn) {
    return view.parent.columns
        .firstWhere((parentColumn) => isSelectBy(ownColumn, parentColumn));
  }

  bool isSelectBy(ViewColumn ownColumn, Column parentColumn) {
    if (ownColumn is SelectColumn) {
      if (ownColumn.from == parentColumn) {
        return true;
      }
    }
    return false;
  }

  @override
  List<MaterializeStrategy> createMaterializeStrategy(
      Iterable<ViewIndexMapping> viewIndexMappings) {
    // parentを指定してしまうと両親が同じの場合バグるのでreasonを使う
    var mapping = viewIndexMappings.firstWhere(
        (v) => v.consumer == view && v.reason == UseIndexReason.parent);

    var primaryKeys = getSortedPrimaryKeys(view);

    List<CopyU8int> parentPrimaryKeyToPrimaryKey =
        mapColumnsToCopyStrategy(primaryKeys, view.parent.primaryKeys, mapping);
    List<CopyU8int> parentOtherColumnToPrimaryKey = mapColumnsToCopyStrategy(
        primaryKeys, getOtherColumns(view.parent), mapping);
    List<CopyU8int> parentPrimaryKeyToOtherColumns = mapColumnsToCopyStrategy(
        getSortedOtherColumns(view), view.parent.primaryKeys, mapping);
    List<CopyU8int> parentOtherColumnToOtherColumns = mapColumnsToCopyStrategy(
        getSortedOtherColumns(view), getOtherColumns(view.parent), mapping);

    var strategy = SelectViewMaterializeStrategy()
      ..parent = view.parent
      ..view = view
      ..primaryKeyLength = columnsByteLength(view.primaryKeys)
      ..otherColumnsLength = columnsByteLength(getOtherColumns(view))
      ..parentPrimaryKeyToPrimaryKey = parentPrimaryKeyToPrimaryKey
      ..parentOtherColumnToPrimaryKey = parentOtherColumnToPrimaryKey
      ..parentPrimaryKeyToOtherColumns = parentPrimaryKeyToOtherColumns
      ..parentOtherColumnToOtherColumns = parentOtherColumnToOtherColumns;
    return [strategy];
  }
}

List<CopyU8int> mapColumnsToCopyStrategy(List<Column> ownColumns,
    Set<Column> parentColumns, ViewIndexMapping mapping) {
  var parentPrimaryKeyToPrimaryKey = List<CopyU8int>();
  //自分のPrimaryKeyを変換する
  var count = 0;
  for (var ownColumn in ownColumns) {
    var parentColumn = mapping.ownToParent[ownColumn];
    if (!parentColumns.contains(parentColumn)) {
      count += ownColumn.byteLength;
      continue;
    }
    var tableOffset = countColumnOffset(parentColumns, parentColumn);
    for (var x = 0; x < ownColumn.byteLength; x++) {
      var copy = CopyU8int()
        ..from = tableOffset + x
        ..to = count;
      parentPrimaryKeyToPrimaryKey.add(copy);
      count++;
    }
  }
  return parentPrimaryKeyToPrimaryKey;
}

int countColumnOffset(Set<Column> columns, Column column) {
  var sortedColumns = getSortedColumns(columns);
  var count = 0;
  for (var c in sortedColumns) {
    if (c == column) {
      return count;
    } else {
      count += c.byteLength;
    }
  }
  throw "カラムが見つからない";
}

class UnionViewStrategyBuilder implements ViewStrategyBuilder {
  final Union view;

  UnionViewStrategyBuilder(this.view);

  List<UseIndexRequest> getUseIndex() {
    var leftToOwns = getLeftToOwns();
    var ownsToLeft = mapReverse(leftToOwns);
    var rightToOwn = getRightToOwns();
    var ownToRight = mapReverse(rightToOwn);

    return <UseIndexRequest>[
      UseIndexRequest()
        ..table = view.leftTable
        ..columns = Set.from(view.leftTable.primaryKeys)
        ..consumer = view
        ..reason = UseIndexReason.left
        ..ownToParent = ownsToLeft
        ..parentToOwn = leftToOwns,
      UseIndexRequest()
        ..table = view.rightTable
        ..columns = Set.from(view.rightTable.primaryKeys)
        ..consumer = view
        ..reason = UseIndexReason.right
        ..ownToParent = ownToRight
        ..parentToOwn = rightToOwn,
    ];
  }

  Map<Column, Column> getLeftToOwns() {
    return Map.fromEntries(
        view.leftTable.primaryKeys.map((c) => MapEntry(c, getLeftToOwn(c))));
  }

  Column getLeftToOwn(Column c) {
    return view.columns.firstWhere((column) {
      if (column is UnionColumn) {
        if (column.left == c) {
          return true;
        }
      }
      return false;
    });
  }

  Map<Column, Column> getRightToOwns() {
    return Map.fromEntries(
        view.rightTable.primaryKeys.map((c) => MapEntry(c, getRightToOwn(c))));
  }

  Column getRightToOwn(Column c) {
    return view.columns.firstWhere((column) {
      if (column is UnionColumn) {
        if (column.right == c) {
          return true;
        }
      }
      return false;
    });
  }

  @override
  List<MaterializeStrategy> createMaterializeStrategy(
      Iterable<ViewIndexMapping> viewIndexMappings) {
    return [
      createLeftMaterializeStrategy(viewIndexMappings),
      createRightMaterializeStrategy(viewIndexMappings),
    ];
  }

  MaterializeStrategy createPrimaryMaterializeStrategyByParent(
      ViewIndexMapping mapping, TableOrView parent) {
    var primaryKeys = getSortedPrimaryKeys(view);

    var otherColumns = getSortedOtherColumns(view);
    var parentOtherColumns = getOtherColumns(parent);

    var parentPrimaryKeyToPrimaryKey =
        mapColumnsToCopyStrategy(primaryKeys, parent.primaryKeys, mapping);
    var parentOtherColumnToPrimaryKey =
        mapColumnsToCopyStrategy(primaryKeys, parentOtherColumns, mapping);
    var parentPrimaryKeyToOtherColumns =
        mapColumnsToCopyStrategy(otherColumns, parent.primaryKeys, mapping);
    var parentOtherColumnsToOtherColumns =
        mapColumnsToCopyStrategy(otherColumns, parentOtherColumns, mapping);

    return UnionViewMaterializeStrategyFromPrimaryTable()
      ..parent = parent
      ..view = view
      ..primaryKeyLength = columnsByteLength(view.primaryKeys)
      ..otherColumnsLength = columnsByteLength(getOtherColumns(view))
      ..parentPrimaryKeyToPrimaryKey = parentPrimaryKeyToPrimaryKey
      ..parentOtherColumnToPrimaryKey = parentOtherColumnToPrimaryKey
      ..parentPrimaryKeyToOtherColumns = parentPrimaryKeyToOtherColumns
      ..parentOtherColumnToOtherColumns = parentOtherColumnsToOtherColumns;
  }

  MaterializeStrategy createSecondaryMaterializeStrategyByParent(
      ViewIndexMapping parentMapping,
      ViewIndexMapping primaryMapping,
      TableOrView parent,
      TableOrView primaryTable) {
    var primaryKeys = getSortedPrimaryKeys(view);
    var otherColumns = getSortedOtherColumns(view);

    var parentOtherColumns = getOtherColumns(parent);
    var primaryTableOtherColumns = getOtherColumns(primaryTable);

    var secondaryTablePrimaryKeyToPrimaryTablePrimaryKey =
        mapSecondaryTableToCopyStrategy(parent.primaryKeys,
            primaryTable.primaryKeys, parentMapping, primaryMapping);
    var secondaryTableOtherColumnsToPrimaryTablePrimaryKey =
        mapSecondaryTableToCopyStrategy(parentOtherColumns,
            primaryTableOtherColumns, parentMapping, primaryMapping);

    return UnionViewMaterializeStrategyFromSecondaryTable()
      ..primaryTable = primaryTable
      ..parent = parent
      ..view = view
      // セカンダリテーブルに増減があったら、それが主テーブルに存在するかどうかお伺いを立てるためのもの
      ..primaryTablePrimaryKeyLength =
          columnsByteLength(primaryTable.primaryKeys)
      ..secondaryTablePrimaryKeyToPrimaryTablePrimaryKey =
          secondaryTablePrimaryKeyToPrimaryTablePrimaryKey
      ..secondaryTableOtherColumnsToPrimaryTablePrimaryKey =
          secondaryTableOtherColumnsToPrimaryTablePrimaryKey
      ..primaryKeyLength = columnsByteLength(view.primaryKeys)
      ..otherColumnsLength = columnsByteLength(otherColumns)
      ..primaryTablePrimaryKeyToPrimaryKey = mapColumnsToCopyStrategy(
          primaryKeys, primaryTable.primaryKeys, primaryMapping)
      ..primaryTableOtherColumnsToPrimaryKey = mapColumnsToCopyStrategy(
          primaryKeys, primaryTableOtherColumns, primaryMapping)
      ..primaryTablePrimaryKeyToOtherColumn = mapColumnsToCopyStrategy(
          otherColumns, primaryTable.primaryKeys, primaryMapping)
      ..primaryTableOtherColumnsToOtherColumn = mapColumnsToCopyStrategy(
          otherColumns, primaryTableOtherColumns, primaryMapping)
      ..secondaryTablePrimaryKeyToPrimaryKey = mapColumnsToCopyStrategy(
          primaryKeys, parent.primaryKeys, parentMapping)
      ..secondaryTableOtherColumnToPrimaryKey = mapColumnsToCopyStrategy(
          primaryKeys, parentOtherColumns, parentMapping)
      ..secondaryTablePrimaryKeyToOtherColumn = mapColumnsToCopyStrategy(
          otherColumns, parent.primaryKeys, parentMapping)
      ..secondaryTableOtherColumnToOtherColumn = mapColumnsToCopyStrategy(
          otherColumns, parent.primaryKeys, parentMapping);
  }

  MaterializeStrategy createLeftMaterializeStrategy(
      Iterable<ViewIndexMapping> viewIndexMappings) {
    var mapping = viewIndexMappings.firstWhere(
        (m) => m.consumer == view && m.reason == UseIndexReason.left);

    // 常に自分が優先
    if (view.whenDuplicate == WhenDuplicateStrategy.DontWorry ||
        view.whenDuplicate == WhenDuplicateStrategy.OverwriteRightWithLeft) {
      return createPrimaryMaterializeStrategyByParent(mapping, view.leftTable);
    } else {
      var primaryMapping = viewIndexMappings.firstWhere(
          (m) => m.consumer == view && m.reason == UseIndexReason.right);
      return createSecondaryMaterializeStrategyByParent(
          mapping, primaryMapping, view.leftTable, view.rightTable);
    }
  }

  MaterializeStrategy createRightMaterializeStrategy(
      Iterable<ViewIndexMapping> viewIndexMappings) {
    var mapping = viewIndexMappings.firstWhere(
        (m) => m.consumer == view && m.reason == UseIndexReason.right);

    // 常に自分が優先
    if (view.whenDuplicate == WhenDuplicateStrategy.DontWorry ||
        view.whenDuplicate == WhenDuplicateStrategy.OverwriteLeftWithRight) {
      return createPrimaryMaterializeStrategyByParent(mapping, view.rightTable);
    } else {
      var primaryMapping = viewIndexMappings.firstWhere(
          (m) => m.consumer == view && m.reason == UseIndexReason.left);
      return createSecondaryMaterializeStrategyByParent(
          mapping, primaryMapping, view.rightTable, view.leftTable);
    }
  }

  List<CopyU8int> mapSecondaryTableToCopyStrategy(
      Set<Column> secondaryTableColumns,
      Set<Column> primaryTableColumns,
      ViewIndexMapping secondaryMapping,
      ViewIndexMapping primaryMapping) {
    var sortedPrimaryTableColumns = getSortedColumns(primaryTableColumns);
    var list = List<CopyU8int>();
    var count = 0;
    for (var primaryKey in sortedPrimaryTableColumns) {
      var ownKey = primaryMapping.parentToOwn[primaryKey];
      var secondaryTableColumn = secondaryMapping.ownToParent[ownKey];
      if (!secondaryTableColumns.contains(secondaryTableColumn)) {
        count += secondaryTableColumn.byteLength;
        continue;
      }
      var offset =
          countColumnOffset(secondaryTableColumns, secondaryTableColumn);
      for (var i = 0; i < secondaryTableColumn.byteLength; i++) {
        var copy = CopyU8int()
          ..from = offset + i
          ..to = count;
        count++;
        list.add(copy);
      }
    }
    return list;
  }
}

class InnerJoinStrategyBuilder implements ViewStrategyBuilder {
  final InnerJoin view;

  InnerJoinStrategyBuilder(this.view);

  List<UseIndexRequest> getUseIndex() {
    // 片方のJoinColumnsがもう片方の何か

    var right = getJoinRightColumnsRecursive(view.on, Set());
    var left = getJoinLeftColumnsRecursive(view.on, Set());

    // 自分を消したりアップデートしたりするためのカラム
    // 元テーブルのprimary keyが自分のテーブルの何か？
    var ownFromLeft =
        Set<Column>.from(getLeftParentToOwnColumns(view.leftTable.primaryKeys));
    var ownFromRight = Set<Column>.from(
        getRightParentToOwnColumns(view.rightTable.primaryKeys));

    var rightToOwn = getRightToOwns();
    var ownToRight = mapReverse(rightToOwn);
    var leftToOwn = getLeftToOwns();
    var ownToLeft = mapReverse(leftToOwn);

    return <UseIndexRequest>[
      UseIndexRequest()
        ..table = view.rightTable
        ..columns = right
        ..consumer = view
        ..reason = UseIndexReason.right
        ..parentToOwn = rightToOwn
        ..ownToParent = ownToRight,
      UseIndexRequest()
        ..table = view.leftTable
        ..columns = left
        ..consumer = view
        ..reason = UseIndexReason.left
        ..parentToOwn = leftToOwn
        ..ownToParent = ownToLeft,
      UseIndexRequest()
        ..table = view
        ..columns = ownFromLeft
        ..consumer = view
        ..reason = UseIndexReason.ownFromLeft
        ..ownToParent = ownToOwn
        ..parentToOwn = ownToOwn,
    ];
  }

  Set<Column> getJoinRightColumnsRecursive(JoinOn on, Set<Column> results) {
    if (on is OnEqual) {
      return results..add(on.right);
    } else if (on is JoinOnOr) {
      var right = getJoinRightColumnsRecursive(on.right, results);
      var left = getJoinRightColumnsRecursive(on.left, results);
      return results..addAll(right)..addAll(left);
    } else if (on is JoinOnAnd) {
      var right = getJoinRightColumnsRecursive(on.right, results);
      var left = getJoinRightColumnsRecursive(on.left, results);
      return results..addAll(right)..addAll(left);
    }
    throw "知らないJoinOn";
  }

  Set<Column> getJoinLeftColumnsRecursive(JoinOn on, Set<Column> results) {
    if (on is OnEqual) {
      return results..add(on.left);
    } else if (on is JoinOnOr) {
      var right = getJoinLeftColumnsRecursive(on.right, results);
      var left = getJoinLeftColumnsRecursive(on.left, results);
      return results..addAll(right)..addAll(left);
    } else if (on is JoinOnAnd) {
      var right = getJoinLeftColumnsRecursive(on.right, results);
      var left = getJoinLeftColumnsRecursive(on.left, results);
      return results..addAll(right)..addAll(left);
    }
    throw "知らないJoinOn";
  }

  Set<JoinColumn> getJoinColumnsRecursive(JoinOn on, Set<JoinColumn> results) {
    if (on is OnEqual) {
      return results..add(getJoinColumn(on.left, on.right));
    } else if (on is JoinOnOr) {
      var right = getJoinColumnsRecursive(on.right, results);
      var left = getJoinColumnsRecursive(on.left, results);
      return results..addAll(right)..addAll(left);
    } else if (on is JoinOnAnd) {
      var right = getJoinColumnsRecursive(on.right, results);
      var left = getJoinColumnsRecursive(on.left, results);
      return results..addAll(right)..addAll(left);
    }
    throw "知らないJoinOn";
  }

  JoinColumn getJoinColumn(Column left, Column right) {
    for (var column in view.columns) {
      if (column is JoinColumn) {
        if (column.right == right && column.left == left) {
          return column;
        }
      }
    }
    throw "JoinColumnが足りない";
  }

  Map<Column, Column> getLeftToOwns() {
    return Map.fromEntries(view.columns
        .where((c) =>
            c is JoinColumn ||
            (c is SelectColumn && view.leftTable.columns.contains(c.from)))
        .map((c) => MapEntry(getOwnToLeft(c), c)));
  }

  Column getOwnToLeft(Column column) {
    if (column is JoinColumn) {
      return view.leftTable.columns.firstWhere((c) => column.left == c);
    } else if (column is SelectColumn) {
      return view.leftTable.columns.firstWhere((c) => column.from == c);
    }
    throw "ここにはこない";
  }

  Map<Column, Column> getRightToOwns() {
    return Map.fromEntries(view.columns
        .where((c) =>
            c is JoinColumn ||
            (c is SelectColumn && view.rightTable.columns.contains(c.from)))
        .map((c) => MapEntry(getOwnToRight(c), c)));
  }

  Column getOwnToRight(Column column) {
    if (column is JoinColumn) {
      return view.rightTable.columns.firstWhere((c) => column.right == c);
    } else if (column is SelectColumn) {
      return view.rightTable.columns.firstWhere((c) => column.from == c);
    }
    throw "ここにはこない";
  }

  @override
  List<MaterializeStrategy> createMaterializeStrategy(
      Iterable<ViewIndexMapping> viewIndexMappings) {
    return [
      createLeftMaterializeStrategy(viewIndexMappings),
      createRightMaterializeStrategy(viewIndexMappings),
    ];
  }

  MaterializeStrategy createLeftMaterializeStrategy(
      Iterable<ViewIndexMapping> viewIndexMappings) {
    var otherTableMapping = viewIndexMappings.firstWhere(
        (v) => v.consumer == view && v.reason == UseIndexReason.right);
    var parentMapping = viewIndexMappings.firstWhere(
        (v) => v.consumer == view && v.reason == UseIndexReason.left);
    var ownMapping = viewIndexMappings.firstWhere(
        (v) => v.consumer == view && v.reason == UseIndexReason.own);

    var sortedPrimaryKeys = getSortedPrimaryKeys(view);
    var sortedOtherColumns = getSortedOtherColumns(view);

    return InnerJoinMaterializeStrategy()
      ..parent = view.leftTable
      ..view = view
      ..otherParent = view.rightTable
      ..otherParentIndex = otherTableMapping.determinedIndex
      ..getOrSeek = otherTableMapping.getOrSeek
      ..otherParentPrimaryKeyLength =
          columnsByteLength(otherTableMapping.requestIndex)
      ..parentPrimaryKeyToOtherParentPrimaryKey = mapOtherColumnsCopyStrategy(
          otherTableMapping.requestIndex,
          view.leftTable.primaryKeys,
          otherTableMapping,
          parentMapping)
      ..parentOtherColumnsToOtherParentPrimaryKey = mapOtherColumnsCopyStrategy(
          otherTableMapping.requestIndex,
          getOtherColumns(view.leftTable),
          otherTableMapping,
          parentMapping)
      ..primaryKeyLength = columnsByteLength(view.primaryKeys)
      ..parentPrimaryKeyToPrimaryKey = mapColumnsToCopyStrategy(
          sortedPrimaryKeys, view.leftTable.primaryKeys, parentMapping)
      ..parentOtherColumnToPrimaryKey = mapColumnsToCopyStrategy(
          sortedPrimaryKeys, getOtherColumns(view.leftTable), parentMapping)
      ..otherParentPrimaryKeyToPrimaryKey = mapColumnsToCopyStrategy(
          sortedPrimaryKeys, view.rightTable.primaryKeys, otherTableMapping)
      ..otherParentOtherColumnToPrimaryKey = mapColumnsToCopyStrategy(
          sortedPrimaryKeys,
          getOtherColumns(view.rightTable),
          otherTableMapping)
      ..otherColumnsLength = columnsByteLength(getOtherColumns(view))
      ..parentPrimaryKeyToOtherColumn = mapColumnsToCopyStrategy(
          sortedOtherColumns, view.leftTable.primaryKeys, parentMapping)
      ..parentOtherColumnToOtherColumn = mapColumnsToCopyStrategy(
          sortedOtherColumns, getOtherColumns(view.leftTable), parentMapping)
      ..otherParentPrimaryKeyToOtherColumn = mapColumnsToCopyStrategy(
          sortedOtherColumns, view.rightTable.primaryKeys, otherTableMapping)
      ..otherParentOtherColumnToOtherColumn = mapColumnsToCopyStrategy(
          sortedOtherColumns,
          getOtherColumns(view.rightTable),
          otherTableMapping)

      // 左が変わった時に自分の行を特定するため
      ..ownIndex = ownMapping.determinedIndex
      ..ownIndexLength = columnsByteLength(ownMapping.requestIndex)
      ..parentPrimaryKeyToOwnIndex =
          mapColumnsToCopyStrategy(ownMapping.requestIndex, view.leftTable.primaryKeys, parentMapping)
      ..parentOtherColumnsToOwnIndex = mapColumnsToCopyStrategy(ownMapping.requestIndex, getOtherColumns(view.leftTable), parentMapping)
      ..parentIndexGetOrSeek = ownMapping.getOrSeek;
  }

  MaterializeStrategy createRightMaterializeStrategy(
      Iterable<ViewIndexMapping> viewIndexMappings) {
    var otherTableMapping = viewIndexMappings.firstWhere(
        (v) => v.consumer == view && v.reason == UseIndexReason.left);
    var parentMapping = viewIndexMappings.firstWhere(
        (v) => v.consumer == view && v.reason == UseIndexReason.right);
    var ownMapping = viewIndexMappings.firstWhere(
        (v) => v.consumer == view && v.reason == UseIndexReason.own);

    var sortedPrimaryKeys = getSortedPrimaryKeys(view);
    var sortedOtherColumns = getSortedOtherColumns(view);

    return InnerJoinMaterializeStrategy()
      ..parent = view.rightTable
      ..view = view
      ..otherParent = view.leftTable
      ..otherParentIndex = otherTableMapping.determinedIndex
      ..getOrSeek = otherTableMapping.getOrSeek
      ..otherParentPrimaryKeyLength =
          columnsByteLength(otherTableMapping.requestIndex)
      ..parentPrimaryKeyToOtherParentPrimaryKey = mapOtherColumnsCopyStrategy(
          otherTableMapping.requestIndex,
          view.rightTable.primaryKeys,
          otherTableMapping,
          parentMapping)
      ..parentOtherColumnsToOtherParentPrimaryKey = mapOtherColumnsCopyStrategy(
          otherTableMapping.requestIndex,
          getOtherColumns(view.rightTable),
          otherTableMapping,
          parentMapping)
      ..primaryKeyLength = columnsByteLength(view.primaryKeys)
      ..parentPrimaryKeyToPrimaryKey = mapColumnsToCopyStrategy(
          sortedPrimaryKeys, view.rightTable.primaryKeys, parentMapping)
      ..parentOtherColumnToPrimaryKey = mapColumnsToCopyStrategy(
          sortedPrimaryKeys, getOtherColumns(view.rightTable), parentMapping)
      ..otherParentPrimaryKeyToPrimaryKey = mapColumnsToCopyStrategy(
          sortedPrimaryKeys, view.leftTable.primaryKeys, otherTableMapping)
      ..otherParentOtherColumnToPrimaryKey = mapColumnsToCopyStrategy(
          sortedPrimaryKeys, getOtherColumns(view.leftTable), otherTableMapping)
      ..otherColumnsLength = columnsByteLength(getOtherColumns(view))
      ..parentPrimaryKeyToOtherColumn = mapColumnsToCopyStrategy(
          sortedOtherColumns, view.rightTable.primaryKeys, parentMapping)
      ..parentOtherColumnToOtherColumn = mapColumnsToCopyStrategy(
          sortedOtherColumns, getOtherColumns(view.rightTable), parentMapping)
      ..otherParentPrimaryKeyToOtherColumn = mapColumnsToCopyStrategy(
          sortedOtherColumns, view.leftTable.primaryKeys, otherTableMapping)
      ..otherParentOtherColumnToOtherColumn = mapColumnsToCopyStrategy(
          sortedOtherColumns,
          getOtherColumns(view.leftTable),
          otherTableMapping)

      // 左が変わった時に自分の行を特定するため
      ..ownIndex = ownMapping.determinedIndex
      ..ownIndexLength = columnsByteLength(ownMapping.requestIndex)
      ..parentPrimaryKeyToOwnIndex = mapColumnsToCopyStrategy(
          ownMapping.requestIndex, view.rightTable.primaryKeys, parentMapping)
      ..parentOtherColumnsToOwnIndex = mapColumnsToCopyStrategy(ownMapping.requestIndex, getOtherColumns(view.rightTable), parentMapping)
      ..parentIndexGetOrSeek = ownMapping.getOrSeek;
  }

  List<CopyU8int> mapOtherColumnsCopyStrategy(
      List<Column> otherParentIndex,
      Set<Column> candidateColumns,
      ViewIndexMapping otherTableMapping,
      ViewIndexMapping parentMapping) {
    var results = List<CopyU8int>();
    var offset = 0;
    for (var otherParentColumn in otherTableMapping.requestIndex) {
      var ownColumn = otherTableMapping.parentToOwn[otherParentColumn];
      var parentColumn = parentMapping.ownToParent[ownColumn];
      if (!candidateColumns.contains(parentColumn)) {
        offset += parentColumn.byteLength;
        continue;
      }
      var from = countColumnOffset(candidateColumns, parentColumn);
      for (var i = 0; i < otherParentColumn.byteLength; i++) {
        var copy = CopyU8int()
          ..from = from + i
          ..to = offset;
        results.add(copy);
        offset++;
      }
    }
    return results;
  }

  Iterable<Column> getLeftParentToOwnColumns(Set<Column> columns) {
    return columns.map((c) => getLeftParentToOwnColumn(c));
  }

  Column getLeftParentToOwnColumn(Column column) {
    return view.columns.firstWhere((c) {
      if (c is JoinColumn) {
        return c.left == column;
      } else if (c is SelectColumn && c.view == view.leftTable) {
        return c.from == column;
      }
      return false;
    });
  }

  Iterable<Column> getRightParentToOwnColumns(Set<Column> columns) {
    return columns.map((c) => getRightParentToOwnColumn(c));
  }

  Column getRightParentToOwnColumn(Column column) {
    return view.columns.firstWhere((c) {
      if (c is JoinColumn) {
        return c.right == column;
      } else if (c is SelectColumn && c.view == view.rightTable) {
        return c.from == column;
      }
      return false;
    });
  }
}
