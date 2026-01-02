// Created by Yanjunhui

import 'dart:typed_data';

import '../bson/bson.dart';
import '../storage/storage.dart';
import 'database.dart';
import 'filter.dart';
import 'index.dart';
import 'query.dart';
import 'update.dart';

/// 最大写入批量大小
const int maxWriteBatchSize = 100000;

/// 集合信息
class CollectionInfo {
  String name;
  int firstPageId;
  int lastPageId;
  int documentCount;
  int indexPageId;
  List<IndexMeta> indexes;

  CollectionInfo({
    required this.name,
    required this.firstPageId,
    required this.lastPageId,
    required this.documentCount,
    required this.indexPageId,
    required this.indexes,
  });
}

/// 索引元数据
class IndexMeta {
  final String name;
  final Map<String, int> keys;
  final bool unique;
  final int rootPageId;

  IndexMeta({
    required this.name,
    required this.keys,
    required this.unique,
    required this.rootPageId,
  });
}

/// 更新结果
class UpdateResult {
  int matchedCount;
  int modifiedCount;
  int upsertedCount;
  dynamic upsertedId;

  UpdateResult({
    this.matchedCount = 0,
    this.modifiedCount = 0,
    this.upsertedCount = 0,
    this.upsertedId,
  });
}

/// 文档集合
class Collection {
  final CollectionInfo info;
  final Database db;
  IndexManager? _indexManager;

  Collection(this.info, this.db);

  /// 获取集合名称
  String get name => info.name;

  /// 获取文档数量
  int count() => info.documentCount;

  /// 获取索引管理器
  IndexManager? get indexManager => _indexManager;

  /// 获取或创建索引管理器
  IndexManager _getIndexManager() {
    _indexManager ??= IndexManager(this);
    return _indexManager!;
  }

  /// 恢复索引
  void restoreIndexes() {
    if (info.indexes.isEmpty) return;

    final im = _getIndexManager();
    for (final meta in info.indexes) {
      final tree = BTree.open(db.pager, meta.rootPageId, meta.name, unique: meta.unique);
      final idx = Index(
        info: IndexInfo(
          name: meta.name,
          keys: meta.keys,
          unique: meta.unique,
          rootPageId: meta.rootPageId,
        ),
        tree: tree,
        pager: db.pager,
      );
      im.indexes[meta.name] = idx;
    }
  }

  /// 插入文档
  Future<List<dynamic>> insert(List<BsonDocument> docs) async {
    if (docs.length > maxWriteBatchSize) {
      throw ArgumentError('Insert batch size exceeds maximum of $maxWriteBatchSize');
    }

    final ids = <dynamic>[];
    final insertedRecords = <_InsertedRecord>[];

    for (final doc in docs) {
      // 验证文档
      _validateDocument(doc);

      // 确保 _id 存在
      final id = _ensureId(doc);

      // 序列化文档
      final data = BsonCodec.encode(doc);

      // 检查文档大小 (16MB 限制)
      if (data.length > 16 * 1024 * 1024) {
        _rollbackInsertedRecords(insertedRecords);
        throw StateError('Document exceeds maximum size of 16MB');
      }

      // 检查唯一索引约束
      if (_indexManager != null) {
        final error = _indexManager!.checkUniqueConstraints(doc);
        if (error != null) {
          _rollbackInsertedRecords(insertedRecords);
          throw StateError('Duplicate key error: $error');
        }
      }

      // 写入文档
      final (pageId, slotIndex) = await _writeDocumentWithLocation(data);

      insertedRecords.add(_InsertedRecord(
        pageId: pageId,
        slotIndex: slotIndex,
        id: id,
      ));

      // 更新索引
      if (_indexManager != null) {
        final error = _indexManager!.insertDocument(doc);
        if (error != null) {
          _rollbackInsertedRecords(insertedRecords);
          throw StateError('Index update failed: $error');
        }
      }

      ids.add(id);
      info.documentCount++;
    }

    await db.saveCatalog();
    return ids;
  }

  /// 查询文档
  Future<List<BsonDocument>> find(BsonDocument? filter) async {
    final results = <BsonDocument>[];

    var currentPageId = info.firstPageId;
    while (currentPageId != 0) {
      final page = await db.pager.readPage(currentPageId);
      final docs = _readDocumentsFromPage(page);

      for (final doc in docs) {
        if (matchesFilter(doc, filter)) {
          results.add(doc);
        }
      }

      currentPageId = page.nextPageId;
    }

    return results;
  }

  /// 带选项的查询
  Future<List<BsonDocument>> findWithOptions(BsonDocument? filter, QueryOptions opts) async {
    var results = await find(filter);
    return applyOptions(results, opts);
  }

  /// 查询单个文档
  Future<BsonDocument?> findOne(BsonDocument? filter) async {
    var currentPageId = info.firstPageId;
    while (currentPageId != 0) {
      final page = await db.pager.readPage(currentPageId);
      final docs = _readDocumentsFromPage(page);

      for (final doc in docs) {
        if (matchesFilter(doc, filter)) {
          return doc;
        }
      }

      currentPageId = page.nextPageId;
    }

    return null;
  }

  /// 根据 _id 查询
  Future<BsonDocument?> findById(dynamic id) async {
    final filter = BsonDocument();
    filter['_id'] = id;
    return findOne(filter);
  }

  /// 更新文档
  Future<UpdateResult> update(BsonDocument? filter, BsonDocument update, {bool upsert = false}) async {
    final result = UpdateResult();

    var currentPageId = info.firstPageId;
    while (currentPageId != 0) {
      final page = await db.pager.readPage(currentPageId);
      final sp = SlottedPage.wrap(page);

      for (var i = 0; i < page.itemCount; i++) {
        final record = sp.getRecord(i);
        if (record == null || record.length < 5) continue;

        BsonDocument doc;
        try {
          doc = BsonCodec.decode(record);
        } catch (_) {
          continue;
        }

        if (matchesFilter(doc, filter)) {
          result.matchedCount++;

          // 保存原始文档
          final originalDoc = _copyDoc(doc);
          final originalData = record;

          // 应用更新
          applyUpdate(doc, update);

          // 重新序列化
          final newData = BsonCodec.encode(doc);

          // 检查数据是否改变
          if (!_bytesEqual(originalData, newData)) {
            // 检查唯一约束
            if (_indexManager != null) {
              final error = _indexManager!.checkUniqueConstraints(doc);
              if (error != null) {
                throw StateError('Duplicate key error: $error');
              }
            }

            // 更新记录
            sp.updateRecord(i, newData);

            // 更新索引
            if (_indexManager != null) {
              _indexManager!.deleteDocument(originalDoc);
              _indexManager!.insertDocument(doc);
            }

            db.pager.markDirty(page.id);
            result.modifiedCount++;
          }
        }
      }

      currentPageId = page.nextPageId;
    }

    // 处理 upsert
    if (result.matchedCount == 0 && upsert) {
      final newDoc = BsonDocument();

      // 复制 filter 中的非操作符字段
      if (filter != null) {
        for (final entry in filter.entries) {
          if (!entry.key.startsWith('\$')) {
            newDoc[entry.key] = entry.value;
          }
        }
      }

      // 应用 $setOnInsert
      for (final entry in update.entries) {
        if (entry.key == '\$setOnInsert' && entry.value is BsonDocument) {
          final setOnInsertDoc = entry.value as BsonDocument;
          for (final setEntry in setOnInsertDoc.entries) {
            newDoc[setEntry.key] = setEntry.value;
          }
        }
      }

      // 应用更新
      applyUpdate(newDoc, update);

      // 插入
      final insertedIds = await insert([newDoc]);
      result.upsertedCount = 1;
      result.upsertedId = insertedIds.first;
    }

    await db.saveCatalog();
    return result;
  }

  /// 删除文档
  Future<int> delete(BsonDocument? filter) async {
    var deletedCount = 0;

    var currentPageId = info.firstPageId;
    while (currentPageId != 0) {
      final page = await db.pager.readPage(currentPageId);
      final sp = SlottedPage.wrap(page);

      for (var i = 0; i < page.itemCount; i++) {
        final record = sp.getRecord(i);
        if (record == null || record.length < 5) continue;

        BsonDocument doc;
        try {
          doc = BsonCodec.decode(record);
        } catch (_) {
          continue;
        }

        if (matchesFilter(doc, filter)) {
          // 删除索引条目
          if (_indexManager != null) {
            _indexManager!.deleteDocument(doc);
          }

          // 删除记录
          sp.deleteRecord(i);
          db.pager.markDirty(page.id);
          deletedCount++;
          info.documentCount--;
        }
      }

      currentPageId = page.nextPageId;
    }

    await db.saveCatalog();
    return deletedCount;
  }

  /// 删除单个文档
  Future<int> deleteOne(BsonDocument? filter) async {
    var currentPageId = info.firstPageId;
    while (currentPageId != 0) {
      final page = await db.pager.readPage(currentPageId);
      final sp = SlottedPage.wrap(page);

      for (var i = 0; i < page.itemCount; i++) {
        final record = sp.getRecord(i);
        if (record == null || record.length < 5) continue;

        BsonDocument doc;
        try {
          doc = BsonCodec.decode(record);
        } catch (_) {
          continue;
        }

        if (matchesFilter(doc, filter)) {
          // 删除索引条目
          if (_indexManager != null) {
            _indexManager!.deleteDocument(doc);
          }

          // 删除记录
          sp.deleteRecord(i);
          db.pager.markDirty(page.id);
          info.documentCount--;

          await db.saveCatalog();
          return 1;
        }
      }

      currentPageId = page.nextPageId;
    }

    return 0;
  }

  /// 获取不重复值
  Future<List<dynamic>> distinct(String field, BsonDocument? filter) async {
    final docs = await find(filter);
    final seen = <String, dynamic>{};
    final result = <dynamic>[];

    for (final doc in docs) {
      final val = getDocField(doc, field);
      if (val == null) continue;

      final key = '${val.runtimeType}:$val';
      if (!seen.containsKey(key)) {
        seen[key] = val;
        result.add(val);
      }
    }

    return result;
  }

  /// 创建索引
  Future<String> createIndex(Map<String, int> keys, {String? name, bool unique = false}) async {
    return _getIndexManager().createIndex(keys, name: name, unique: unique);
  }

  /// 删除索引
  Future<void> dropIndex(String name) async {
    await _getIndexManager().dropIndex(name);
  }

  /// 列出索引
  List<BsonDocument> listIndexes() {
    return _getIndexManager().listIndexes();
  }

  /// 验证文档
  void _validateDocument(BsonDocument doc) {
    for (final entry in doc.entries) {
      if (entry.key.isEmpty) {
        throw ArgumentError('Field name cannot be empty');
      }
      if (entry.key.contains('\x00')) {
        throw ArgumentError('Field name cannot contain null character');
      }
    }
  }

  /// 确保文档有 _id
  dynamic _ensureId(BsonDocument doc) {
    if (doc.containsKey('_id')) {
      return doc['_id'];
    }

    final id = ObjectId.generate();
    // 将 _id 插入到开头
    final entries = doc.entries.toList();
    doc.clear();
    doc['_id'] = id;
    for (final entry in entries) {
      doc[entry.key] = entry.value;
    }
    return id;
  }

  /// 写入文档并返回位置
  Future<(int, int)> _writeDocumentWithLocation(Uint8List data) async {
    // 尝试在现有页面中插入
    if (info.lastPageId != 0) {
      final page = await db.pager.readPage(info.lastPageId);
      final sp = SlottedPage.wrap(page);
      final slotIndex = sp.insertRecord(data);
      if (slotIndex >= 0) {
        db.pager.markDirty(page.id);
        return (page.id, slotIndex);
      }
    }

    // 需要分配新页面
    final page = await db.pager.allocatePage(PageType.data);
    final sp = SlottedPage.wrap(page);
    final slotIndex = sp.insertRecord(data);
    if (slotIndex < 0) {
      throw StateError('Failed to insert record into new page');
    }

    // 更新链表
    if (info.firstPageId == 0) {
      info.firstPageId = page.id;
    } else {
      final lastPage = await db.pager.readPage(info.lastPageId);
      lastPage.nextPageId = page.id;
      page.prevPageId = info.lastPageId;
      db.pager.markDirty(info.lastPageId);
    }
    info.lastPageId = page.id;
    db.pager.markDirty(page.id);

    return (page.id, slotIndex);
  }

  /// 从页面读取文档
  List<BsonDocument> _readDocumentsFromPage(Page page) {
    final docs = <BsonDocument>[];
    final sp = SlottedPage.wrap(page);

    for (var i = 0; i < page.itemCount; i++) {
      final record = sp.getRecord(i);
      if (record == null || record.length < 5) continue;

      try {
        final doc = BsonCodec.decode(record);
        docs.add(doc);
      } catch (_) {
        continue;
      }
    }

    return docs;
  }

  /// 回滚已插入的记录
  void _rollbackInsertedRecords(List<_InsertedRecord> records) {
    for (final rec in records) {
      _rollbackDocument(rec.pageId, rec.slotIndex);
      if (_indexManager != null) {
        _indexManager!.rollbackDocumentById(rec.id);
      }
      info.documentCount--;
    }
  }

  /// 回滚单个文档
  Future<void> _rollbackDocument(int pageId, int slotIndex) async {
    try {
      final page = await db.pager.readPage(pageId);
      final sp = SlottedPage.wrap(page);
      sp.deleteRecord(slotIndex);
      db.pager.markDirty(pageId);
    } catch (e) {
      // 记录错误但继续回滚
      print('CRITICAL: rollback failed for page $pageId slot $slotIndex: $e');
    }
  }

  /// 复制文档
  BsonDocument _copyDoc(BsonDocument doc) {
    final data = BsonCodec.encode(doc);
    return BsonCodec.decode(data);
  }

  /// 比较字节数组
  bool _bytesEqual(Uint8List a, Uint8List b) {
    if (a.length != b.length) return false;
    for (var i = 0; i < a.length; i++) {
      if (a[i] != b[i]) return false;
    }
    return true;
  }
}

/// 已插入记录信息
class _InsertedRecord {
  final int pageId;
  final int slotIndex;
  final dynamic id;

  _InsertedRecord({
    required this.pageId,
    required this.slotIndex,
    required this.id,
  });
}
