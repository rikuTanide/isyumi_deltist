part of isyumi_deltist;

// あるViewが親のTableにどんなViewを必要としているか？
// Selectの場合はPrimaryKey
// Unionの場合は両方の親のPrimaryKey
// InnerJoin
// OuterJoin
// 　左のJOINColumnを洗い出し、それの右側の対応を取り、それを昇順に並べたもの
//   PKがA-B-CでOCがDで必要なインデックスがA-B-Dなら、データを一意にするためにA-B-D-Cがいる
//   よってprimary key以外はソートしてはいけない。必要なIndexとそれ以外を別々にソート
//
//   A-B-Cのインデックスが必要なものとA-Bが必要なものがあったらA-B-Cを使う
//     valueはPrimaryKeys
//  長さの比較はできない
//  Updateの時にPrimaryKeyが変わったら削除足す作成
//    A-旧-C を消してA-新-Cを作る
//    それ以外の場合、そのカラムがIndexに使われていたら
// 入力はtablesとviews、出力はstrategies
//   各テーブルがどのインデックスを持っているか
// 　Insertがあったらどのインデックスを更新するか
//　 削除があったらどのインデックスを削除するか
// 　どのUpdateがあったらどのIndexを更新するか
//   それをどのビューに伝えるか
// Optionalをどう表すか？　行全体がない場合とそこだけOptionalの場合
// coalesce とcoalesceOptionalがある。
// merge(Optional list) {}
// nullableはkeyになれない
// Optionalはそのまま転記
//   Coalesceがあった場合
//   判断の順番と値があった時の値を取り出す場所
//   ただし、そっち側の行が全部Nullならパス
//　直接転機するのは無理ゲー
//  Stringを読み出すかどうか
//  一旦左右を読み出し、生成列を作る
//   生成列をバイト配列にする方法
///  キーはColumnではないはず
///  殆どの列は問題ない
///  生成列を使いたい場合だけ変換・読み出しをする
///  Coalesceも生成列の一種と考える
///  単純な左優先OR右優先なら問題ない
///  ある場所に対する値を重ね合わせて、パスするかどうかと自分の優先順位を書く
///  この関数に入る前に生成列を計算しておく
///
///

/// Map<Column, U8int>をPrimaryKeyとValueの何番目にCopyするか
class RawDataUpdateStrategy {
  Table table;
  int primaryKeysLength;
  int otherColumnsLength;

  List<SetU8int> primaryKeysSetStrategy;
  List<SetU8int> otherColumnsSetStrategy;

  String toString() {
    var primaryKeysSetStrategyDescription = primaryKeysSetStrategy
        .map((pk) => "key[${pk.to}] = ${pk.column.name}[${pk.from}]")
        .join('\n');

    var otherColumnsSetStrategyDescription = otherColumnsSetStrategy
        .map((pk) => "value[${pk.to}] = ${pk.column.name}[${pk.from}]")
        .join('\n');
    return """

${table.tableName}
$primaryKeysSetStrategyDescription
$otherColumnsSetStrategyDescription
""";
  }
}

class BytesReadStrategy {
  TableOrView table;

  int primaryKeyLength;
  List<SetU8int> createPrimaryKeysStrategy;
  List<CreateColumn> readPrimaryKeysStrategy;
  List<CreateColumn> readOtherColumnsStrategy;
}

class CreateColumn {
  Column column;
  int length;
  List<CopyU8int> copies;

  @override
  String toString() {
    return column.name +
        copies.map((c) => "${c.to}:value[${c.from}]").join(" ");
  }
}

class SetU8int {
  Column column;
  int from;
  int to;

  String toString() {
    return "$to:${column.name}[${from}]";
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is SetU8int &&
          runtimeType == other.runtimeType &&
          column == other.column &&
          from == other.from &&
          to == other.to;

  @override
  int get hashCode => column.hashCode ^ from.hashCode ^ to.hashCode;
}

class CopyU8int {
  int from;
  int to;

  String toString() {
    return "from:$from to:$to";
  }

  @override
  bool operator ==(Object other) =>
      identical(this, other) ||
      other is CopyU8int &&
          runtimeType == other.runtimeType &&
          from == other.from &&
          to == other.to;

  @override
  int get hashCode => from.hashCode ^ to.hashCode;
}

// indexに使われているPrimaryKeyではないカラムが複数変更されたらどうするか？
// 一回で済ませたい

List<RawDataUpdateStrategy> createRawDataUpdateStrategies(List<Table> tables) {
  return tables
      .map((t) => createRawDataUpdateStrategy(t))
      .toList(growable: false);
}

RawDataUpdateStrategy createRawDataUpdateStrategy(Table table) {
  var primaryKeys = getSortedPrimaryKeys(table);
  var primaryKeyLength = columnsByteLength(primaryKeys);
  var otherColumns = getSortedOtherColumns(table);
  var otherColumnsLength = columnsByteLength(otherColumns);
  return RawDataUpdateStrategy()
    ..table = table
    ..primaryKeysLength = primaryKeyLength
    ..otherColumnsLength = otherColumnsLength
    ..primaryKeysSetStrategy =
        createSetStrategies(primaryKeyLength, primaryKeys)
    ..otherColumnsSetStrategy =
        createSetStrategies(otherColumnsLength, otherColumns);
}

List<SetU8int> createSetStrategies(int length, List<Column> primaryKeys) {
  var list = List<SetU8int>(length);
  var count = 0;

  for (var key in primaryKeys) {
    for (var i = 0; i < key.byteLength; i++) {
      list[count] = SetU8int()
        ..column = key
        ..from = i
        ..to = count;
      count++;
    }
  }
  return list;
}

int columnsByteLength(Iterable<Column> columns) {
  return columns.map((c) => c.byteLength).fold(0, (v, e) => v + e);
}

List<BytesReadStrategy> createBytesReadStrategies(List<TableOrView> tables) {
  return tables.map((t) => createBytesReadStrategy(t)).toList(growable: false);
}

BytesReadStrategy createBytesReadStrategy(TableOrView table) {
  var primaryKeys = getSortedPrimaryKeys(table);
  var primaryKeyLength = columnsByteLength(primaryKeys);
  var otherColumns = getSortedOtherColumns(table);
  return BytesReadStrategy()
    ..table = table
    ..primaryKeyLength = primaryKeyLength
    ..createPrimaryKeysStrategy =
        createSetStrategies(primaryKeyLength, primaryKeys)
    ..readPrimaryKeysStrategy = createReadColumns(primaryKeys)
    ..readOtherColumnsStrategy = createReadColumns(otherColumns);
}

List<CreateColumn> createReadColumns(List<Column> columns) {
  var list = <CreateColumn>[];
  var count = 0;
  for (var column in columns) {
    var copies = List<CopyU8int>(column.byteLength);
    for (var i = 0; i < column.byteLength; i++) {
      var copy = CopyU8int()
        ..from = count
        ..to = i;
      copies[i] = copy;
      count++;
    }

    var cc = CreateColumn()
      ..column = column
      ..length = column.byteLength
      ..copies = copies;
    list.add(cc);
  }
  return list;
}
