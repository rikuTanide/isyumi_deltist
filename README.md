非正規データの差分更新をするライブラリ

# 概要
Webアプリケーションを作る時、データの整合性のためにRDBを正にしたいがパフォーマンス向上のためKVSにいい感じにキャッシュしたい。  
しかし、そのコードを書くのが大変である。  
そこで、宣言的にRDBを集計してKVSに保存してくれるものを作った。  
RDBが１行変更される度にKVSを最短経路で差分更新する。  
JOIN、SUM、AVERAGE、UNIONなどに対応（予定のものもある）。  
どのような差分更新がなされたのか更新ログを生成してくれるので、その更新ログを使ってFirebaseを更新したりするといい感じになる。  


# インストール

## RocksDB
```
sudo apt install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev liblz4-dev libzstd-dev make cmake g++
git clone git@github.com:facebook/rocksdb.git
cd rocksdb
make shared_lib
sudo make install-shared
```

## Dart
```
sudo apt install apt-transport-https
sudo sh -c 'curl https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add -'
sudo sh -c 'curl https://storage.googleapis.com/download.dartlang.org/linux/debian/dart_stable.list > /etc/apt/sources.list.d/dart_stable.list'
sudo apt update
sudo apt install dart

export PATH=/usr/lib/dart/bin:$PATH # これは.bashrcにも書いておこう
```
# DBを作る
## テーブル定義

- カラムの型と名前、長さなどを指定
- どのカラムを主キーにするか
- その他のカラム
- テーブル名

```
class Users extends Table {
  final UintColumn userID = UintColumn.bit64("userID");

  final StringColumn name = StringColumn("name", Code.Utf8);

  Set<Column> get primaryKeys => {userID};

  Set<Column> get columns => {userID, name};

  Users() : super("Users", "user");
}
```

## ビューの指定


### Select
```
class FollowSelf extends SelectView<Users> {
  ViewColumn<int> get from => SelectColumn("from", this, parent.userID);

  ViewColumn<int> get to => SelectColumn("to", this, parent.userID);

  FollowSelf(Users users) : super("FollowSelfs", "FollowSelf", users);

  @override
  Set<ViewColumn> get columns => {from, to};

  @override
  Set<ViewColumn> get primaryKeys => {from, to};
}
```

### Union
```
class FollowAndSelfs extends Union<Follows, FollowSelf> {
  ViewColumn<int> get from =>
      UnionColumn("from", this, follows.from, followSelf.from);

  ViewColumn<int> get to => UnionColumn("to", this, follows.to, followSelf.to);

  FollowAndSelfs(this.follows, this.followSelf)
      : super("FollowAndSelfs", "FollowAndSelf", follows, followSelf,
            WhenDuplicateStrategy.DontWorry);

  final Follows follows;

  final FollowSelf followSelf;

  @override
  Set<ViewColumn> get primaryKeys => {from, to};

  @override
  Set<ViewColumn> get columns => {from, to};
}
```
  
### InnerJoin

```
class FolloweeTweets extends InnerJoin {
  final FollowAndSelfs followAndSelfs;
  final Tweets tweets;

  FolloweeTweets(this.followAndSelfs, this.tweets)
      : super("FolloweeTweets", "FolloweeTweet", followAndSelfs, tweets);

  @override
  View get leftTable => followAndSelfs;

  @override
  Table get rightTable => tweets;

  ViewColumn<int> get readerID =>
      SelectColumn("readerID", this, followAndSelfs.from);

  ViewColumn<int> get tweetID => SelectColumn("tweetID", this, tweets.tweetID);

  ViewColumn<int> get ownerID =>
      JoinColumn("ownerID", this, followAndSelfs.to, tweets.userID);

  ViewColumn<DateTime> get timestamp =>
      SelectColumn("timestamp", this, tweets.timestamp);

  ViewColumn<String> get content =>
      SelectColumn("content", this, tweets.content);

  @override
  Set<ViewColumn> get primaryKeys => {
        readerID,
        tweetID,
      };

  @override
  Set<ViewColumn> get columns => {
        readerID,
        tweetID,
        ownerID,
        timestamp,
        content,
      };

  @override
  JoinOn get on => OnEqual(this, followAndSelfs.to, tweets.userID);
}

```

## DBを立ちあげる
```
  var db = Database([
    users,
    tweets,
    follows,
  ], [
    followSelf,
    followAndSelfs,
    followeeTweets,
  ])
    ..create(dbPath)
    ..open(dbPath);
```

create()で既存のデータを消す
create()をせずにopen()すると前回のデータを引き継げる

# 操作
## Put
```
   var wr = WritableRow(users)
          ..set(users.userID, userID)
          ..set(users.name, name);
   var results = db.write(wr); // resutlsがこの書き込みによってViewがどのように変わったのかの履歴
```

## Get
```
 var readKey = ReadKey(tableF)..set(tableF.fID, 2);
    var readableRow = db.read(readKey);
    var fID = readableRow.get(tableF.fID);
    var fString = readableRow.get(tableF.fString);
    var fText = readableRow.get(tableF.fText);
    var fBool = readableRow.get(tableF.fBool);
    var fDate = readableRow.get(tableF.fDate);
```
