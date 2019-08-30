import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'dart:io';

void main() {
  print("start");
  var users = Users();
  var tweets = Tweets();
  var follows = Follows();
  var followSelf = FollowSelf(users);
  var followAndSelfs = FollowAndSelfs(follows, followSelf);
  var followeeStories = FolloweeTweets(followAndSelfs, tweets);

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
    followeeStories,
  ])
    ..create(dbPath)
    ..open(dbPath);
  print("on open");

  var wr = WritableRow(users)..set(users.userID, 1)..set(users.name, "name");

  db.write(wr);
  var wr2 = WritableRow(tweets)
    ..set(tweets.tweetID, 1)
    ..set(tweets.userID, 1)
    ..set(tweets.timestamp, DateTime.now())
    ..set(tweets.content, "こんにちは");

  print("write");

  db.write(wr2);

  var readKey = ReadKey(users)..set(users.userID, 1);

  var readableRow = db.read(readKey);

  var name = readableRow.get(users.name);
  print(name);
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
