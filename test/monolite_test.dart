// Created by Yanjunhui

import 'dart:io';
import 'dart:typed_data';

import 'package:test/test.dart';
import 'package:monolite/monolite.dart';

void main() {
  group('BSON Codec', () {
    test('encode and decode document', () {
      final doc = BsonDocument();
      doc['name'] = 'test';
      doc['count'] = 42;
      doc['active'] = true;
      doc['score'] = 3.14;

      final encoded = BsonCodec.encode(doc);
      final decoded = BsonCodec.decode(encoded);

      expect(decoded['name'], equals('test'));
      expect(decoded['count'], equals(42));
      expect(decoded['active'], equals(true));
      expect(decoded['score'], closeTo(3.14, 0.001));
    });

    test('encode and decode ObjectId', () {
      final id = ObjectId.generate();
      final doc = BsonDocument();
      doc['_id'] = id;

      final encoded = BsonCodec.encode(doc);
      final decoded = BsonCodec.decode(encoded);

      final decodedId = decoded['_id'] as ObjectId;
      expect(decodedId.toHex(), equals(id.toHex()));
    });

    test('encode and decode nested document', () {
      final inner = BsonDocument();
      inner['x'] = 1;
      inner['y'] = 2;

      final doc = BsonDocument();
      doc['point'] = inner;

      final encoded = BsonCodec.encode(doc);
      final decoded = BsonCodec.decode(encoded);

      final decodedInner = decoded['point'] as BsonDocument;
      expect(decodedInner['x'], equals(1));
      expect(decodedInner['y'], equals(2));
    });

    test('encode and decode array', () {
      final arr = BsonArray();
      arr.add(1);
      arr.add('two');
      arr.add(3.0);

      final doc = BsonDocument();
      doc['items'] = arr;

      final encoded = BsonCodec.encode(doc);
      final decoded = BsonCodec.decode(encoded);

      final decodedArr = decoded['items'] as BsonArray;
      expect(decodedArr.length, equals(3));
      expect(decodedArr[0], equals(1));
      expect(decodedArr[1], equals('two'));
      expect(decodedArr[2], closeTo(3.0, 0.001));
    });
  });

  group('Page', () {
    test('page header serialization', () {
      final page = Page.create(1, PageType.data);
      page.nextPageId = 2;
      page.prevPageId = 0;

      final bytes = page.marshal();
      expect(bytes.length, equals(pageSize));

      final restored = Page.unmarshal(bytes);
      expect(restored.id, equals(1));
      expect(restored.type, equals(PageType.data));
      expect(restored.nextPageId, equals(2));
      expect(restored.prevPageId, equals(0));
    });

    test('slotted page insert and read', () {
      final page = Page.create(1, PageType.data);
      final sp = SlottedPage.wrap(page);

      final record1 = Uint8List.fromList([1, 2, 3, 4, 5]);
      final record2 = Uint8List.fromList([10, 20, 30]);

      final slot1 = sp.insertRecord(record1);
      final slot2 = sp.insertRecord(record2);

      expect(slot1, greaterThanOrEqualTo(0));
      expect(slot2, greaterThanOrEqualTo(0));

      final read1 = sp.getRecord(slot1);
      final read2 = sp.getRecord(slot2);

      expect(read1, equals(record1));
      expect(read2, equals(record2));
    });
  });

  group('Database', () {
    late String testDbPath;

    setUp(() {
      testDbPath = '${Directory.systemTemp.path}/test_monolite_${DateTime.now().millisecondsSinceEpoch}.db';
    });

    tearDown(() async {
      try {
        final file = File(testDbPath);
        if (await file.exists()) {
          await file.delete();
        }
        final walFile = File('$testDbPath.wal');
        if (await walFile.exists()) {
          await walFile.delete();
        }
      } catch (_) {}
    });

    test('create and open database', () async {
      final db = await Database.open(testDbPath);
      expect(db.name, equals(testDbPath));
      await db.close();

      // Reopen
      final db2 = await Database.open(testDbPath);
      expect(db2.name, equals(testDbPath));
      await db2.close();
    });

    test('create collection', () async {
      final db = await Database.open(testDbPath);

      final col = await db.collection('users');
      expect(col.name, equals('users'));

      final collections = db.listCollections();
      expect(collections, contains('users'));

      await db.close();
    });

    test('insert and find documents', () async {
      final db = await Database.open(testDbPath);
      final col = await db.collection('users');

      final doc1 = BsonDocument();
      doc1['name'] = 'Alice';
      doc1['age'] = 30;

      final doc2 = BsonDocument();
      doc2['name'] = 'Bob';
      doc2['age'] = 25;

      await col.insert([doc1, doc2]);
      expect(col.count(), equals(2));

      // Find all
      final allDocs = await col.find(null);
      expect(allDocs.length, equals(2));

      // Find with filter
      final filter = BsonDocument();
      filter['name'] = 'Alice';
      final aliceDocs = await col.find(filter);
      expect(aliceDocs.length, equals(1));
      expect(aliceDocs[0]['age'], equals(30));

      await db.close();
    });

    test('update documents', () async {
      final db = await Database.open(testDbPath);
      final col = await db.collection('users');

      final doc = BsonDocument();
      doc['name'] = 'Alice';
      doc['age'] = 30;
      await col.insert([doc]);

      // Update
      final filter = BsonDocument();
      filter['name'] = 'Alice';

      final setDoc = BsonDocument();
      setDoc['age'] = 31;
      final update = BsonDocument();
      update['\$set'] = setDoc;

      final result = await col.update(filter, update);
      expect(result.matchedCount, equals(1));
      expect(result.modifiedCount, equals(1));

      // Verify
      final updated = await col.findOne(filter);
      expect(updated?['age'], equals(31));

      await db.close();
    });

    test('delete documents', () async {
      final db = await Database.open(testDbPath);
      final col = await db.collection('users');

      final doc1 = BsonDocument();
      doc1['name'] = 'Alice';
      final doc2 = BsonDocument();
      doc2['name'] = 'Bob';
      await col.insert([doc1, doc2]);

      expect(col.count(), equals(2));

      // Delete one
      final filter = BsonDocument();
      filter['name'] = 'Alice';
      final deleted = await col.deleteOne(filter);
      expect(deleted, equals(1));
      expect(col.count(), equals(1));

      await db.close();
    });

    test('persistence across close/reopen', () async {
      // Insert data
      var db = await Database.open(testDbPath);
      var col = await db.collection('users');

      final doc = BsonDocument();
      doc['name'] = 'Alice';
      doc['age'] = 30;
      await col.insert([doc]);

      await db.flush();
      await db.close();

      // Reopen and verify
      db = await Database.open(testDbPath);
      col = db.getCollection('users')!;

      expect(col.count(), equals(1));
      final found = await col.find(null);
      expect(found.length, equals(1));
      expect(found[0]['name'], equals('Alice'));
      expect(found[0]['age'], equals(30));

      await db.close();
    });
  });

  group('Filter Matcher', () {
    test('\$eq operator', () {
      final doc = BsonDocument();
      doc['x'] = 5;

      final eqFilter = BsonDocument();
      final eqCond = BsonDocument();
      eqCond['\$eq'] = 5;
      eqFilter['x'] = eqCond;

      expect(matchesFilter(doc, eqFilter), isTrue);

      eqCond['\$eq'] = 6;
      expect(matchesFilter(doc, eqFilter), isFalse);
    });

    test('\$gt, \$gte, \$lt, \$lte operators', () {
      final doc = BsonDocument();
      doc['x'] = 10;

      final gtFilter = BsonDocument();
      final gtCond = BsonDocument();
      gtCond['\$gt'] = 5;
      gtFilter['x'] = gtCond;
      expect(matchesFilter(doc, gtFilter), isTrue);

      gtCond['\$gt'] = 15;
      expect(matchesFilter(doc, gtFilter), isFalse);

      final lteFilter = BsonDocument();
      final lteCond = BsonDocument();
      lteCond['\$lte'] = 10;
      lteFilter['x'] = lteCond;
      expect(matchesFilter(doc, lteFilter), isTrue);
    });

    test('\$in operator', () {
      final doc = BsonDocument();
      doc['status'] = 'active';

      final inFilter = BsonDocument();
      final inCond = BsonDocument();
      final arr = BsonArray();
      arr.add('active');
      arr.add('pending');
      inCond['\$in'] = arr;
      inFilter['status'] = inCond;

      expect(matchesFilter(doc, inFilter), isTrue);

      doc['status'] = 'deleted';
      expect(matchesFilter(doc, inFilter), isFalse);
    });

    test('\$and and \$or operators', () {
      final doc = BsonDocument();
      doc['x'] = 5;
      doc['y'] = 10;

      // $and
      final andFilter = BsonDocument();
      final andArr = BsonArray();
      final cond1 = BsonDocument();
      cond1['x'] = 5;
      final cond2 = BsonDocument();
      cond2['y'] = 10;
      andArr.add(cond1);
      andArr.add(cond2);
      andFilter['\$and'] = andArr;

      expect(matchesFilter(doc, andFilter), isTrue);

      cond2['y'] = 20;
      expect(matchesFilter(doc, andFilter), isFalse);

      // $or
      final orFilter = BsonDocument();
      final orArr = BsonArray();
      final orCond1 = BsonDocument();
      orCond1['x'] = 5;
      final orCond2 = BsonDocument();
      orCond2['y'] = 100;
      orArr.add(orCond1);
      orArr.add(orCond2);
      orFilter['\$or'] = orArr;

      expect(matchesFilter(doc, orFilter), isTrue);
    });
  });

  group('Update Operators', () {
    test('\$set operator', () {
      final doc = BsonDocument();
      doc['x'] = 1;

      final setDoc = BsonDocument();
      setDoc['x'] = 10;
      setDoc['y'] = 20;
      final update = BsonDocument();
      update['\$set'] = setDoc;

      applyUpdate(doc, update);

      expect(doc['x'], equals(10));
      expect(doc['y'], equals(20));
    });

    test('\$inc operator', () {
      final doc = BsonDocument();
      doc['count'] = 5;

      final incDoc = BsonDocument();
      incDoc['count'] = 3;
      final update = BsonDocument();
      update['\$inc'] = incDoc;

      applyUpdate(doc, update);

      expect(doc['count'], equals(8));
    });

    test('\$unset operator', () {
      final doc = BsonDocument();
      doc['x'] = 1;
      doc['y'] = 2;

      final unsetDoc = BsonDocument();
      unsetDoc['y'] = '';
      final update = BsonDocument();
      update['\$unset'] = unsetDoc;

      applyUpdate(doc, update);

      expect(doc.containsKey('x'), isTrue);
      expect(doc.containsKey('y'), isFalse);
    });

    test('\$push operator', () {
      final doc = BsonDocument();
      final arr = BsonArray();
      arr.add(1);
      arr.add(2);
      doc['items'] = arr;

      final pushDoc = BsonDocument();
      pushDoc['items'] = 3;
      final update = BsonDocument();
      update['\$push'] = pushDoc;

      applyUpdate(doc, update);

      final items = doc['items'] as BsonArray;
      expect(items.length, equals(3));
      expect(items[2], equals(3));
    });
  });
}
