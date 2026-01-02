// Created by Yanjunhui

import 'dart:typed_data';

import '../bson/bson.dart';
import '../storage/storage.dart';
import 'collection.dart';
import 'filter.dart';

/// 索引信息
class IndexInfo {
  final String name;
  final Map<String, int> keys;
  final bool unique;
  int rootPageId;

  IndexInfo({
    required this.name,
    required this.keys,
    required this.unique,
    required this.rootPageId,
  });
}

/// 索引
class Index {
  final IndexInfo info;
  final BTree tree;
  final Pager pager;

  Index({
    required this.info,
    required this.tree,
    required this.pager,
  });
}

/// 索引管理器
class IndexManager {
  final Collection collection;
  final Map<String, Index> indexes = {};

  IndexManager(this.collection);

  /// 创建索引
  Future<String> createIndex(
    Map<String, int> keys, {
    String? name,
    bool unique = false,
  }) async {
    // 生成索引名称
    final indexName = name ?? _generateIndexName(keys);

    // 检查索引是否已存在
    if (indexes.containsKey(indexName)) {
      return indexName;
    }

    // 创建 B+Tree
    final tree = await BTree.create(collection.db.pager, indexName, unique: unique);

    final info = IndexInfo(
      name: indexName,
      keys: keys,
      unique: unique,
      rootPageId: tree.rootPage,
    );

    final idx = Index(
      info: info,
      tree: tree,
      pager: collection.db.pager,
    );

    indexes[indexName] = idx;

    // 为现有文档建立索引
    await _buildIndex(idx);

    // 持久化索引元数据
    collection.info.indexes.add(IndexMeta(
      name: indexName,
      keys: keys,
      unique: unique,
      rootPageId: tree.rootPage,
    ));
    await collection.db.saveCatalog();

    return indexName;
  }

  /// 为现有文档建立索引
  Future<void> _buildIndex(Index idx) async {
    final docs = await collection.find(null);

    for (final doc in docs) {
      final key = _encodeIndexEntryKey(idx, doc);
      if (key == null) continue;

      final idVal = getDocField(doc, '_id');
      if (idVal == null) continue;

      final idDoc = BsonDocument();
      idDoc['_id'] = idVal;
      final idBytes = BsonCodec.encode(idDoc);

      try {
        await idx.tree.insert(key, idBytes);
      } catch (e) {
        if (idx.info.unique) {
          throw StateError('Duplicate key for index ${idx.info.name}');
        }
        rethrow;
      }
    }
  }

  /// 删除索引
  Future<void> dropIndex(String name) async {
    if (name == '_id_') {
      throw ArgumentError('Cannot drop _id index');
    }

    indexes.remove(name);

    // 从 CollectionInfo 中移除
    collection.info.indexes.removeWhere((meta) => meta.name == name);

    await collection.db.saveCatalog();
  }

  /// 列出所有索引
  List<BsonDocument> listIndexes() {
    final result = <BsonDocument>[];

    // 添加默认的 _id 索引
    final idIndex = BsonDocument();
    idIndex['name'] = '_id_';
    final idKey = BsonDocument();
    idKey['_id'] = 1;
    idIndex['key'] = idKey;
    idIndex['v'] = 2;
    result.add(idIndex);

    for (final idx in indexes.values) {
      final indexDoc = BsonDocument();
      indexDoc['name'] = idx.info.name;
      final keyDoc = BsonDocument();
      for (final entry in idx.info.keys.entries) {
        keyDoc[entry.key] = entry.value;
      }
      indexDoc['key'] = keyDoc;
      indexDoc['unique'] = idx.info.unique;
      indexDoc['v'] = 2;
      result.add(indexDoc);
    }

    return result;
  }

  /// 检查唯一约束
  String? checkUniqueConstraints(BsonDocument doc) {
    for (final idx in indexes.values) {
      if (!idx.info.unique) continue;

      final key = _encodeIndexEntryKey(idx, doc);
      if (key == null) continue;

      final exists = idx.tree.search(key);
      if (exists != null) {
        return idx.info.name;
      }
    }
    return null;
  }

  /// 插入文档时更新索引
  String? insertDocument(BsonDocument doc) {
    final insertedEntries = <(Index, Uint8List)>[];

    for (final idx in indexes.values) {
      final key = _encodeIndexEntryKey(idx, doc);
      if (key == null) continue;

      final idVal = getDocField(doc, '_id');
      if (idVal == null) continue;

      final idDoc = BsonDocument();
      idDoc['_id'] = idVal;
      final idBytes = BsonCodec.encode(idDoc);

      try {
        idx.tree.insert(key, idBytes);
        insertedEntries.add((idx, key));
      } catch (e) {
        // 回滚已成功的插入
        for (final entry in insertedEntries.reversed) {
          try {
            entry.$1.tree.delete(entry.$2);
          } catch (_) {}
        }
        return 'Failed to update index ${idx.info.name}: $e';
      }
    }
    return null;
  }

  /// 删除文档时更新索引
  String? deleteDocument(BsonDocument doc) {
    for (final idx in indexes.values) {
      final key = _encodeIndexEntryKey(idx, doc);
      if (key != null) {
        try {
          idx.tree.delete(key);
        } catch (e) {
          return 'Failed to delete from index ${idx.info.name}: $e';
        }
      }
    }
    return null;
  }

  /// 根据 _id 回滚索引条目
  void rollbackDocumentById(dynamic docId) {
    // 记录回滚意图
    print('Index rollback requested for docId: $docId');
  }

  /// 生成索引名称
  String _generateIndexName(Map<String, int> keys) {
    return keys.entries.map((e) => '${e.key}_${e.value}').join('_');
  }

  /// 编码索引条目键
  Uint8List? _encodeIndexEntryKey(Index idx, BsonDocument doc) {
    final base = _extractIndexKey(doc, idx.info.keys);
    if (base == null) return null;

    if (idx.info.unique) {
      return base;
    }

    // 非唯一索引：追加 _id
    final idVal = getDocField(doc, '_id');
    if (idVal == null) return base;

    final idDoc = BsonDocument();
    idDoc['_id'] = idVal;
    final idBytes = BsonCodec.encode(idDoc);

    final key = BytesBuilder();
    key.add(base);
    key.addByte(0x00);
    key.add(idBytes);
    return key.toBytes();
  }

  /// 提取索引键
  Uint8List? _extractIndexKey(BsonDocument doc, Map<String, int> keySpec) {
    final builder = BytesBuilder();

    for (final entry in keySpec.entries) {
      final val = getDocField(doc, entry.key);
      if (val == null) {
        // 字段不存在，跳过
        builder.addByte(0x00); // null 标记
        continue;
      }

      // 使用 BSON 编码值
      final valDoc = BsonDocument();
      valDoc['v'] = val;
      final encoded = BsonCodec.encode(valDoc);
      builder.add(encoded);
    }

    if (builder.isEmpty) return null;
    return builder.toBytes();
  }
}
