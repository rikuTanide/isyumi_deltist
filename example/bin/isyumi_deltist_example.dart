import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'dart:io';
import 'dart:math';
import 'dart:convert';
import 'dart:collection';
import 'dart:async';
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

  Process.start("node", ["example/bin/main.js"]).then((ps) {
    print("ooo");
    ps.stderr.listen((line) {
      print(["inline", Utf8Decoder().convert(line)]);
    });

    ps.stdout.listen((line) {
      print("pop");
      var lineStr = Utf8Decoder().convert(line).trim();
      if(lineStr == ""){
        return;
      }
      print(["linein",lineStr]);
      var json = JsonDecoder().convert(lineStr)
          as Map<String, dynamic>;
      //{"collection":"users","type":"added","value":{"name":"1さん","userID":"user1"}}
      var collection = json["collection"] as String;
      var type = json["type"] as String;
      dynamic value = json["value"];

      if (collection == "users") {
        var userID = value["userID"] as int;
        var name = value["name"] as String;
        var wr = WritableRow(users)
          ..set(users.userID, userID)
          ..set(users.name, name);
        db.write(wr);
      } else if (collection == "tweets") {
        var tweetID = value["tweetID"] as int;
        var userID = value["userID"] as int;
        var timestamp = DateTime.fromMillisecondsSinceEpoch(
            (value["timestap"]["seconds"] as int) * 100);
        var text = value["text"] as String;
        var wr2 = WritableRow(tweets)
          ..set(tweets.tweetID, tweetID)
          ..set(tweets.userID, userID)
          ..set(tweets.timestamp, timestamp)
          ..set(tweets.content, text);
        db.write(wr2);
      } else if (collection == "follows") {
        var from = value["from"] as int;
        var to = value["to"] as int;
        var w3 = WritableRow(follows)
          ..set(follows.from, from)
          ..set(follows.to, to);
        db.write(w3);
      }
      print(["end" , lineStr]);
    });
  });
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

  final TextColumn content = TextColumn("text");

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
