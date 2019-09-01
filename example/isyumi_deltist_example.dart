import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'dart:io';
import 'dart:math';

void main() {
  print("start");
  var users = Users();
  var tweets = Tweets();
  var follows = Follows();
  var followSelf = FollowSelf(users);
  var followAndSelfs = FollowAndSelfs(follows, followSelf);
  var followeeTweets = FolloweeTweets(followAndSelfs, tweets);

  var dbPath = "./test_db/example";
  var dbDir = Directory(dbPath);
  if (dbDir.existsSync()) {
    dbDir.deleteSync(recursive: true);
  }
  dbDir.createSync();

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
  print("on open");

  for (var i = 0; i < 10; i++) {
    var wr = WritableRow(users)..set(users.userID, i)..set(users.name, "name");
    db.write(wr);
    for (var tid = 0; tid < 10; tid++) {
      var wr2 = WritableRow(tweets)
        ..set(tweets.tweetID, i * 1000 + tid)
        ..set(tweets.userID, i)
        ..set(tweets.timestamp, DateTime.now())
        ..set(tweets.content, "こんにちは");
      db.write(wr2);
    }
  }

  for (var x = 0; x < 10; x++) {
    for (var y = 0; y < 10; y++) {
      if (x == y) {
        continue;
      }
      var w3 = WritableRow(follows)..set(follows.from, x)..set(follows.to, y);
      db.write(w3);
    }
  }

  var wr = WritableRow(tweets)
    ..set(tweets.tweetID, 1000000 + 1)
    ..set(tweets.userID, 1)
    ..set(tweets.timestamp, DateTime.now())
    ..set(tweets.content, "こんにちは");
  var ref = db.write(wr);

  for (var tweetRef in ref.map[followeeTweets]) {
    var row = tweetRef.row();
  }

  var dr = DeleteRow(tweets)..set(tweets.tweetID, 1000000 + 1);
  var ref2 = db.delete(dr);
//  for (var tweetRef in ref2.map[followeeTweets]) {
//    var row = tweetRef.row();
//    print([
//      "readerID: ${row.get(followeeTweets.readerID)}",
//      "tweetID: ${row.get(followeeTweets.tweetID)}",
//      "ownerID: ${row.get(followeeTweets.ownerID)}",
//      "timestamp: ${row.get(followeeTweets.timestamp)}",
//      "content: ${row.get(followeeTweets.content)}",
//    ]);
//  }
}

class Users extends Table {
  final UintColumn userID = UintColumn.bit64("userID");

  final StringColumn name = StringColumn("name", Code.Utf8);

  Set<Column> get primaryKeys => {userID};

  Set<Column> get columns => {userID, name};

  Users() : super("Users", "user");
}

class Tweets extends Table {
  final UintColumn tweetID = UintColumn.bit64("tweetID");

  final UintColumn userID = UintColumn.bit64("userID");

  final DateTimeColumn timestamp = DateTimeColumn("timestamp");

  final TextColumn content = TextColumn("name");

  Set<Column> get columns => {tweetID, userID, timestamp, content};

  Set<Column> get primaryKeys => {tweetID};

  Tweets() : super("Tweets", "Tweet");
}

class Follows extends Table {
  final UintColumn from = UintColumn.bit64("from");

  final UintColumn to = UintColumn.bit64("to");

  Set<Column> get columns => {from, to};

  Set<Column> get primaryKeys => {from, to};

  Follows() : super("Follows", "Follow");
}

// FollowsにUNIONするためのもの
class FollowSelf extends SelectView<Users> {
  ViewColumn<int> get from => SelectColumn("from", this, parent.userID);

  ViewColumn<int> get to => SelectColumn("to", this, parent.userID);

  FollowSelf(Users users) : super("FollowSelfs", "FollowSelf", users);

  @override
  Set<ViewColumn> get columns => {from, to};

  @override
  Set<ViewColumn> get primaryKeys => {from, to};
}

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
