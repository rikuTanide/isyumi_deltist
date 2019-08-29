import 'package:isyumi_deltist/isyumi_deltist.dart';
import 'package:test/test.dart';
import 'dart:typed_data';
import 'dart:convert';
import 'dart:io';
import 'package:rocksdb_interop/rocksdb_interop.dart' as rocksdb;

// 内部表現は特にテストせず、元に戻せるかをテストする

void main() {
  test("intをuint8で表現", () {
    var original = 432432897;
    var list = intToU8List(original);
    expect(list, equals([0, 0, 0, 0, 0x19, 0xc6, 0x67, 0x01]));
    expect(u8ListToInt(list), equals(original));
  });

  test("stringに代理IDを振る", () {
    var dbPath = "./test_db/write_string_expression_test";
    var dbDir = Directory(dbPath);
    if (dbDir.existsSync()) {
      dbDir.deleteSync(recursive: true);
    }
    var columnFamilies = ["strToId", "idToStr", "meta"];
    rocksdb.createDB(dbPath, columnFamilies);
    var handlers = rocksdb.open(dbPath, columnFamilies);
    var storeRocksDBPointer = StoreRocksDBPointer()
      ..dbPointer = handlers[0]
      ..strToIdPointer = handlers[2]
      ..idToStrPointer = handlers[3]
      ..metaTablePointer = handlers[4];

    var storeTuple =
        StoreTuple(RocksDB(), storeRocksDBPointer, StringStore(), TextStore());

    var strID1 = stringToU8List(storeTuple, Code.Ascii, "abcd");
    var strID2 = stringToU8List(storeTuple, Code.Ascii, "efgh");
    var strID3 = stringToU8List(storeTuple, Code.Ascii, "ijkl");
    var strID4 = stringToU8List(storeTuple, Code.Ascii, "abcd");

    expect(u8ListToString(storeTuple, Code.Ascii, strID1), equals("abcd"));
    expect(u8ListToString(storeTuple, Code.Ascii, strID2), equals("efgh"));
    expect(u8ListToString(storeTuple, Code.Ascii, strID3), equals("ijkl"));
    expect(u8ListToString(storeTuple, Code.Ascii, strID4), equals("abcd"));

    expect(strID1, equals(strID4));
  });

  test("Textに代理IDを振る", () {
    var dbPath = "./test_db/write_text_expression_test";
    var dbDir = Directory(dbPath);
    if (dbDir.existsSync()) {
      dbDir.deleteSync(recursive: true);
    }
    var columnFamilies = ["hashToText"];
    rocksdb.createDB(dbPath, columnFamilies);

    var handlers = rocksdb.open(dbPath, columnFamilies);
    var storeRocksDBPointer = StoreRocksDBPointer()
      ..dbPointer = handlers[0]
      ..hashToTextPointer = handlers[2];

    var storeTuple =
        StoreTuple(RocksDB(), storeRocksDBPointer, StringStore(), TextStore());

    var textID1 = textToU8List(storeTuple, "abcd");
    var textID2 = textToU8List(storeTuple, "efgh");
    var textID3 = textToU8List(storeTuple, "ijkl");
    var textID4 = textToU8List(storeTuple, "abcd");

    expect(u8ListToText(storeTuple, textID1), equals("abcd"));
    expect(u8ListToText(storeTuple, textID2), equals("efgh"));
    expect(u8ListToText(storeTuple, textID3), equals("ijkl"));
    expect(u8ListToText(storeTuple, textID4), equals("abcd"));

    expect(textID1, equals(textID1));
  });

  test("boolをuint8で表現", () {
    expect(u8ListToBool(boolToU8List(true)), isTrue);
    expect(u8ListToBool(boolToU8List(false)), isFalse);
  });

  test("DateTimeをuint8で表現", () {
    expect(u8ListToDateTime(dateTimeToU8List(DateTime(2019, 1, 1))),
        equals(DateTime(2019, 1, 1)));
  });
}
