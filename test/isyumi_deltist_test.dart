import 'package:isyumi_deltist/isyumi_deltist.dart';
/*
viewに自己参照がない 不可能になった

テーブル名前かぶりがない

WritableRowの引数がviewではない
カラムが指定したテーブルのカラムである
全ての列が埋まっている
WritableRowの引数がDBの持っているTable
tableに書き込める

ReadKeyの引数がDBの持っているTableかView
Keyが埋まっていなければエラー
読み込める
getの引数が指定したテーブルのカラムである

テーブル
 primary keyが必ず含まれている
 primary key は全てcolumnsに含まれている
 primary keyにTextColumnが含まれていない
 カラムの順番をprimary keyを先頭に出来る 不要そう
 カラムの型ごとに保存の仕方があっている　
 　文字が長過ぎたらエラー
 nameの重複がない


View
　　そのビューの親を辿っていくと自分が出てこない
　　　FromがDBに登録されているTableかView
　　columnsが全てViewColumn
     nameの重複がない
     Viewが自分自身
   Tableは自分を参照しているテーブルを把握できている

　SelectView
    SelectColumnがそのViewのTableのColumnl
　　その名前でSelectした結果を保存できる
    ViewUpdateStrategy
      正しく生成できている

　　Insertがあったら１行生成し、その行があるかどうか確認しなかったら上書き
　　Updateがあったら
      前回が存在しないもので今回も存在しないものなら無視
      前回が存在しないもので今回は存在するものならInsert
      前回が存在するもので今回は存在しないものならDelete
      前回が存在するもので今回も存在するもので
        変更があったらUpdate
        変更がなかったら無視
　　Deleteがあったら１行のキーを生成し、削除

　UnionView DonwWorry
　　Insertがあったら１行生成し、その行があるかどうか確認しなかったら上書き
　　Updateがあったら１行生成し、その行と違いがあったら上書き
　　Deleteがあったら１行のキーを生成し、削除
  　UnionColumnでなければエラー
  　UnionColumnの方が一致しているか
  　
  　
  InnerJoin
　　Joinに使われているColumnがJoinColumnではなくSelectColumnになっていると警告
　　JoinColumnの型が一致しているか
　　JoinOnから辿れるOnEqualが全て自分のViewか
　　　LeftとRightはあっているか？
　　必要なOperation一覧を返す関数が正しく機能しているか
　　useParentTableIndexでKeySetを２つ正しく返しているか
  　片方にInsertがあったらInsertする
   useOwnViewIndexで２つ正しく返しているか


  Index
  　各Indexはカラムを全部名前順に並べる
  　useParentTableIndexとuseOwnIndexを全部足す
  　Squash
    　Length順に並べる
  　  自分に必要なIndexを先頭から順に全て含む他のIndexかPrimaryKeyがあればパスする

  useOwnViewIndex
    両方のKeyでどこまで絞れるか
  　JoinColumnに使われていたらそれに置き換える
  　
  SelectColumnはPrimaryKeyを全て含んでいなければエラー
  PrimaryKeyとOtherColumnsはSetにする
  　
 */

import 'package:test/test.dart';

/*
SelectOptionalColumn()
SelectColumnWithDefault()

JoinSet
GroupBy, Count, Set, Avg, Max, Min
 */