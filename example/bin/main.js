const admin = require('firebase');
const readline = require('readline');
admin.initializeApp(    {
    apiKey: "AIzaSyA8EGHLdc2IAD-5BSa2jAQ9KixQ0OR0ytk",
    authDomain: "tweet-example-2e668.firebaseapp.com",
    databaseURL: "https://tweet-example-2e668.firebaseio.com",
    projectId: "tweet-example-2e668",
    storageBucket: "tweet-example-2e668.appspot.com",
    messagingSenderId: "409853597242",
});
var db = admin.firestore();


var reader = readline.createInterface({
    input: process.stdin,
    output: process.stdout
});

listen("users");
listen("tweets");
listen("follows");



function listen(collectionName) {
    db.collection(collectionName).onSnapshot(snapshot => {
        for(const change of snapshot.docChanges() ){
            var data = { collection: collectionName, type: change.type , value: change.doc.data()};
            process.stdout.write(JSON.stringify(data) + "\r\n");

        }
    });
}


reader.on('line', function (line) {

        return;

    var json = JSON.parse(line);
    var collection = json.collection;
    var type = json.type;
    var doc = json.doc;

    if(type == "delete") {
        db.collection(collection).doc(doc).delete();
    }else if(type == "put") {
        db.collection(collection).doc(doc).set(json.value)

    }else {
        throw  "deleteでもputでもない";
    }

});
process.stdin.on('end', function () {
    process.exit(0);
});



