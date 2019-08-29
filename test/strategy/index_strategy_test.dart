// todo あとでUnionViewMaterializeStrategyFromSecondaryTableをList<Column> requestIndexを使う形に変える
import 'dart:ffi';

import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'package:test/test.dart';
import 'package:quiver/collection.dart';
import 'dart:io';

/// 各ビューが他のテーブルにどんなインデックスを求めているのか
///   そのインデックスがPrimaryKeyを全て含まない場合は後ろにつなげる
/// プライマリーキーで代替出来るものは除く
/// 他のインデックスで代替できるものも除く
/// 全ての必要なIndexにIDをふる
/// そのIndexがどのIndexに解決されるかIDを振る
/// それを受取り戦略を作る
/// ConsumerとReasonから必要なものをフィルター
/// Indexのアップデートだるい
///
///
/// 用途と必要なIndexをViewごとに列挙
/// 集約する
/// 後ろに余ったPrimaryKeyを付ける
/// 列挙したUseIndexにIndexIDを振る
///
///
void main() {
  test("Viewは自分に必要なIndexを列挙できる", () {
    var users = Users();
    var tweets = Tweets();
    var follows = Follows();
    var followSelf = FollowSelf(users);
    var followAndSelfs = FollowAndSelfs(follows, followSelf);
    var followeeStories = FolloweeTweets(followAndSelfs, tweets);
    var views = [followSelf, followAndSelfs, followeeStories];
    var tables = [users, tweets, follows];

    var tableOrViews = [
      users,
      tweets,
      follows,
      followSelf,
      followAndSelfs,
      followeeStories,
    ];
    var strategies = createIndexStrategies(views);
//    print(strategies);
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

class FolloweeTweets extends InnerJoin<FollowAndSelfs, Tweets> {
  final FollowAndSelfs followAndSelfs;
  final Tweets tweets;

  FolloweeTweets(this.followAndSelfs, this.tweets)
      : super("FolloweeTweets", "FolloweeTweet", followAndSelfs, tweets);

  @override
  FollowAndSelfs get leftTable => followAndSelfs;

  @override
  Tweets get rightTable => tweets;

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
