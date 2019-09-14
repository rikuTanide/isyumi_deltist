import 'dart:async';

import 'package:firebase/firebase.dart';
import 'package:firebase/firestore.dart' as fs;
import 'dart:html';


void main() {
  var app = initializeApp(
    apiKey: "AIzaSyA8EGHLdc2IAD-5BSa2jAQ9KixQ0OR0ytk",
    authDomain: "tweet-example-2e668.firebaseapp.com",
    databaseURL: "https://tweet-example-2e668.firebaseio.com",
    projectId: "tweet-example-2e668",
    storageBucket: "tweet-example-2e668.appspot.com",
    messagingSenderId: "409853597242",
  );

  var store = firestore(app);
  document.getElementById("delete-all").onClick.listen((_) async {
    await deleteCollection(store, "users");
    await deleteCollection(store, "tweets");
    await deleteCollection(store, "follows");
  });
  document.getElementById("insert-initialize").onClick.listen((_) async {
    var users = store.collection("users");
    users.doc("1").set(<String, dynamic>{"userID": 1, "name": "1さん"});
    users.doc("2").set(<String, dynamic>{"userID": 2, "name": "2さん"});
    users.doc("3").set(<String, dynamic>{"userID": 3, "name": "3さん"});

    var tweets = store.collection("tweets");
    tweets.doc("1").set(<String, dynamic>{
      "tweetID": 1,
      "userID": 1,
      "timestamp": DateTime.now(),
      "text": "こんにちは1",
    });
    tweets.doc("2").set(<String, dynamic>{
      "tweetID": 2,
      "userID": 2,
      "timestamp": DateTime.now(),
      "text": "こんにちは2",
    });
    tweets.doc("3").set(<String, dynamic>{
      "tweetID": 3,
      "userID": 3,
      "timestamp": DateTime.now(),
      "text": "こんにちは3",
    });
  });

  reflectIntoDiv(store, "users");
  reflectIntoDiv(store, "tweets");
  reflectIntoDiv(store, "follows");

  var users = store.collection("users");
  var select = document.getElementById("follow-to") as SelectElement;

  users.onSnapshot.listen((d) {
    select.children.clear();
    d.forEach((data) {
      var users = data.data();
      var row = OptionElement()
        ..text = users["name"].toString()
        ..value = users["userID"].toString();
      select.append(row);
    });
  });

  var followButton = document.getElementById("follow");
  followButton.onClick.listen((_){
    var to = int.parse(select.value);

    var follows = store.collection("follows");
    follows.doc("1_${to}").set(<String, dynamic>{
      "from": 1,
      "to": to,
    });

  });
}

void reflectIntoDiv(fs.Firestore store, String collectionName) {
  var collection = store.collection(collectionName);
  collection.onSnapshot.listen((d) {
    var div = document.getElementById(collectionName);
    div.children.clear();
    d.forEach((data) {
      var row = DivElement()..text = data.data().toString();
      div.append(row);
    });
  });
}

Future deleteCollection(fs.Firestore store, String collectionName) {
  var collection = store.collection(collectionName);

  var complete = Completer<dynamic>();

  collection.get().then((query) {
    query.forEach((data) {
      collection.doc(data.id).delete();
    });
    complete.complete();
  });
  return complete.future;
}
