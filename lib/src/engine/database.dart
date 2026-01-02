// Created by Yanjunhui

import 'dart:typed_data';

import '../bson/bson.dart';
import '../storage/storage.dart';
import 'collection.dart';

/// Catalog 多页存储魔数
const int catalogMagic = 0x4D504354; // "MPCT"

/// Catalog 头长度
const int catalogHeaderLen = 12; // magic(4) + totalLen(4) + pageCount(4)

/// 数据库实例
class Database {
  final String name;
  final Pager _pager;
  final Map<String, Collection> _collections = {};
  final DateTime _startTime;

  Database._(this.name, this._pager) : _startTime = DateTime.now();

  /// 打开或创建数据库
  static Future<Database> open(String path) async {
    final pager = await Pager.open(path);
    final db = Database._(path, pager);
    await db._loadCatalog();
    return db;
  }

  /// 获取底层 Pager
  Pager get pager => _pager;

  /// 获取集合（不存在则返回 null）
  Collection? getCollection(String name) {
    return _collections[name];
  }

  /// 获取或创建集合
  Future<Collection> collection(String name) async {
    // 验证集合名
    _validateCollectionName(name);

    if (_collections.containsKey(name)) {
      return _collections[name]!;
    }

    // 创建新集合
    final info = CollectionInfo(
      name: name,
      firstPageId: 0,
      lastPageId: 0,
      documentCount: 0,
      indexPageId: 0,
      indexes: [],
    );

    final col = Collection(info, this);
    _collections[name] = col;

    // 保存目录
    await _saveCatalog();

    return col;
  }

  /// 删除集合
  Future<void> dropCollection(String name) async {
    final col = _collections[name];
    if (col == null) {
      return; // 不存在视为成功
    }

    // 释放所有数据页
    var currentPageId = col.info.firstPageId;
    while (currentPageId != 0) {
      final page = await _pager.readPage(currentPageId);
      final nextId = page.nextPageId;
      _pager.freePage(currentPageId);
      currentPageId = nextId;
    }

    _collections.remove(name);
    await _saveCatalog();
  }

  /// 列出所有集合
  List<String> listCollections() {
    return _collections.keys.toList();
  }

  /// 获取统计信息
  DatabaseStats stats() {
    int totalDocs = 0;
    final collectionStats = <String, CollectionStats>{};

    for (final entry in _collections.entries) {
      final count = entry.value.info.documentCount;
      totalDocs += count;
      collectionStats[entry.key] = CollectionStats(documentCount: count);
    }

    return DatabaseStats(
      collectionCount: _collections.length,
      documentCount: totalDocs,
      pageCount: _pager.pageCount,
      collections: collectionStats,
    );
  }

  /// 刷新所有更改到磁盘
  Future<void> flush() async {
    await _pager.flush();
  }

  /// 关闭数据库
  Future<void> close() async {
    await _pager.close();
  }

  /// 验证集合名称
  void _validateCollectionName(String name) {
    if (name.isEmpty) {
      throw ArgumentError('Collection name cannot be empty');
    }
    if (name.startsWith('system.')) {
      throw ArgumentError('Collection name cannot start with "system."');
    }
    if (name.contains('\$')) {
      throw ArgumentError('Collection name cannot contain "\$"');
    }
    if (name.contains('\x00')) {
      throw ArgumentError('Collection name cannot contain null character');
    }
  }

  /// 加载目录
  Future<void> _loadCatalog() async {
    final catalogPageId = _pager.catalogPageId;
    if (catalogPageId == 0) {
      return; // 空数据库
    }

    final page = await _pager.readPage(catalogPageId);
    final data = page.data;

    if (data.length < 5) {
      return;
    }

    // 检查是否是多页格式
    final magic = ByteData.view(data.buffer, data.offsetInBytes, 4)
        .getUint32(0, Endian.little);

    if (magic == catalogMagic) {
      await _loadCatalogMultiPage(page, data);
    } else {
      // 单页格式
      final bsonLen = ByteData.view(data.buffer, data.offsetInBytes, 4)
          .getUint32(0, Endian.little);
      if (bsonLen < 5 || bsonLen > data.length) {
        throw StateError('Invalid catalog: invalid BSON length $bsonLen');
      }

      final catalogDoc = BsonCodec.decode(Uint8List.fromList(data.sublist(0, bsonLen)));
      _restoreCollectionsFromCatalog(catalogDoc);
    }
  }

  /// 加载多页目录
  Future<void> _loadCatalogMultiPage(Page firstPage, Uint8List firstData) async {
    if (firstData.length < catalogHeaderLen) {
      throw StateError('Invalid multi-page catalog header');
    }

    final bd = ByteData.view(firstData.buffer, firstData.offsetInBytes, catalogHeaderLen);
    final totalLen = bd.getUint32(4, Endian.little);
    final pageCount = bd.getUint32(8, Endian.little);

    if (totalLen == 0 || pageCount == 0) {
      throw StateError('Invalid multi-page catalog: totalLen=$totalLen, pageCount=$pageCount');
    }

    // 读取所有数据
    final bsonData = BytesBuilder();

    // 第一页数据
    final firstPageDataCap = maxPageData - catalogHeaderLen;
    if (totalLen <= firstPageDataCap) {
      bsonData.add(firstData.sublist(catalogHeaderLen, catalogHeaderLen + totalLen));
    } else {
      bsonData.add(firstData.sublist(catalogHeaderLen));
    }

    // 读取后续页面
    var currentPageId = firstPage.nextPageId;
    while (bsonData.length < totalLen && currentPageId != 0) {
      final page = await _pager.readPage(currentPageId);
      final pageData = page.data;
      final remaining = totalLen - bsonData.length;
      final copyLen = remaining > pageData.length ? pageData.length : remaining;
      bsonData.add(pageData.sublist(0, copyLen));
      currentPageId = page.nextPageId;
    }

    if (bsonData.length < totalLen) {
      throw StateError('Incomplete multi-page catalog: got ${bsonData.length} bytes, expected $totalLen');
    }

    final catalogDoc = BsonCodec.decode(bsonData.toBytes());
    _restoreCollectionsFromCatalog(catalogDoc);
  }

  /// 从目录数据恢复集合
  void _restoreCollectionsFromCatalog(BsonDocument catalogDoc) {
    final collections = catalogDoc['collections'];
    if (collections == null || collections is! BsonArray) {
      return;
    }

    for (final colData in collections) {
      if (colData is! BsonDocument) continue;

      final name = colData['name'] as String?;
      if (name == null) continue;

      final indexes = <IndexMeta>[];
      final indexesData = colData['indexes'];
      if (indexesData is BsonArray) {
        for (final idxData in indexesData) {
          if (idxData is! BsonDocument) continue;
          indexes.add(IndexMeta(
            name: idxData['name'] as String? ?? '',
            keys: _parseKeys(idxData['keys']),
            unique: idxData['unique'] as bool? ?? false,
            rootPageId: (idxData['rootPageId'] as int?) ?? 0,
          ));
        }
      }

      final info = CollectionInfo(
        name: name,
        firstPageId: (colData['firstPageId'] as int?) ?? 0,
        lastPageId: (colData['lastPageId'] as int?) ?? 0,
        documentCount: (colData['documentCount'] as int?) ?? 0,
        indexPageId: (colData['indexPageId'] as int?) ?? 0,
        indexes: indexes,
      );

      final col = Collection(info, this);
      _collections[name] = col;

      // 恢复索引
      if (indexes.isNotEmpty) {
        col.restoreIndexes();
      }
    }
  }

  /// 解析索引键
  Map<String, int> _parseKeys(dynamic keysData) {
    final result = <String, int>{};
    if (keysData is BsonDocument) {
      for (final entry in keysData.entries) {
        result[entry.key] = entry.value is int ? entry.value : 1;
      }
    }
    return result;
  }

  /// 保存目录
  Future<void> _saveCatalog() async {
    // 构建目录数据
    final collections = BsonArray();

    for (final col in _collections.values) {
      final indexes = BsonArray();

      // 从索引管理器收集索引
      if (col.indexManager != null) {
        for (final idx in col.indexManager!.indexes.values) {
          final keysDoc = BsonDocument();
          for (final entry in idx.info.keys.entries) {
            keysDoc[entry.key] = entry.value;
          }

          final idxDoc = BsonDocument();
          idxDoc['name'] = idx.info.name;
          idxDoc['keys'] = keysDoc;
          idxDoc['unique'] = idx.info.unique;
          idxDoc['rootPageId'] = idx.info.rootPageId;
          indexes.add(idxDoc);
        }
      }

      // 也包含 CollectionInfo 中已持久化的索引
      for (final meta in col.info.indexes) {
        bool alreadyInManager = false;
        if (col.indexManager != null) {
          alreadyInManager = col.indexManager!.indexes.containsKey(meta.name);
        }
        if (!alreadyInManager) {
          final keysDoc = BsonDocument();
          for (final entry in meta.keys.entries) {
            keysDoc[entry.key] = entry.value;
          }

          final idxDoc = BsonDocument();
          idxDoc['name'] = meta.name;
          idxDoc['keys'] = keysDoc;
          idxDoc['unique'] = meta.unique;
          idxDoc['rootPageId'] = meta.rootPageId;
          indexes.add(idxDoc);
        }
      }

      final colDoc = BsonDocument();
      colDoc['name'] = col.info.name;
      colDoc['firstPageId'] = col.info.firstPageId;
      colDoc['lastPageId'] = col.info.lastPageId;
      colDoc['documentCount'] = col.info.documentCount;
      colDoc['indexPageId'] = col.info.indexPageId;
      colDoc['indexes'] = indexes;
      collections.add(colDoc);
    }

    final catalogDoc = BsonDocument();
    catalogDoc['collections'] = collections;
    final bsonData = BsonCodec.encode(catalogDoc);

    // 检查是否需要多页存储
    if (bsonData.length <= maxPageData) {
      await _saveCatalogSinglePage(bsonData);
    } else {
      await _saveCatalogMultiPage(bsonData);
    }
  }

  /// 单页保存目录
  Future<void> _saveCatalogSinglePage(Uint8List data) async {
    var catalogPageId = _pager.catalogPageId;
    Page page;

    if (catalogPageId == 0) {
      page = await _pager.allocatePage(PageType.catalog);
      _pager.setCatalogPageId(page.id);
    } else {
      page = await _pager.readPage(catalogPageId);
      // 清理可能存在的后续页面
      await _freeCatalogChain(page.nextPageId);
      page.nextPageId = 0;
    }

    page.setData(data);
    _pager.markDirty(page.id);
  }

  /// 多页保存目录
  Future<void> _saveCatalogMultiPage(Uint8List bsonData) async {
    // 计算需要的页数
    final firstPageDataCap = maxPageData - catalogHeaderLen;
    final subsequentPageCap = maxPageData;

    var pagesNeeded = 1;
    if (bsonData.length > firstPageDataCap) {
      final remaining = bsonData.length - firstPageDataCap;
      pagesNeeded += (remaining + subsequentPageCap - 1) ~/ subsequentPageCap;
    }

    // 准备多页头信息
    final header = Uint8List(catalogHeaderLen);
    final headerBd = ByteData.view(header.buffer);
    headerBd.setUint32(0, catalogMagic, Endian.little);
    headerBd.setUint32(4, bsonData.length, Endian.little);
    headerBd.setUint32(8, pagesNeeded, Endian.little);

    // 获取或分配第一页
    var catalogPageId = _pager.catalogPageId;
    Page firstPage;

    if (catalogPageId == 0) {
      firstPage = await _pager.allocatePage(PageType.catalog);
      _pager.setCatalogPageId(firstPage.id);
    } else {
      firstPage = await _pager.readPage(catalogPageId);
    }

    // 写入第一页
    final firstPageData = Uint8List(maxPageData);
    firstPageData.setRange(0, catalogHeaderLen, header);
    final dataForFirstPage = bsonData.length > firstPageDataCap
        ? bsonData.sublist(0, firstPageDataCap)
        : bsonData;
    firstPageData.setRange(catalogHeaderLen, catalogHeaderLen + dataForFirstPage.length, dataForFirstPage);
    firstPage.setData(firstPageData);
    _pager.markDirty(firstPage.id);

    // 写入后续页面
    var dataOffset = dataForFirstPage.length;
    var currentPage = firstPage;
    var existingNextId = currentPage.nextPageId;

    while (dataOffset < bsonData.length) {
      Page nextPage;
      if (existingNextId != 0) {
        try {
          nextPage = await _pager.readPage(existingNextId);
          existingNextId = nextPage.nextPageId;
        } catch (_) {
          nextPage = await _pager.allocatePage(PageType.catalog);
        }
      } else {
        nextPage = await _pager.allocatePage(PageType.catalog);
      }

      // 链接页面
      currentPage.nextPageId = nextPage.id;
      _pager.markDirty(currentPage.id);

      // 写入数据
      var endOffset = dataOffset + subsequentPageCap;
      if (endOffset > bsonData.length) {
        endOffset = bsonData.length;
      }

      final pageData = Uint8List(maxPageData);
      pageData.setRange(0, endOffset - dataOffset, bsonData.sublist(dataOffset, endOffset));
      nextPage.setData(pageData);
      _pager.markDirty(nextPage.id);

      dataOffset = endOffset;
      currentPage = nextPage;
    }

    // 清理多余的旧页面
    currentPage.nextPageId = 0;
    _pager.markDirty(currentPage.id);
    await _freeCatalogChain(existingNextId);
  }

  /// 释放目录页面链
  Future<void> _freeCatalogChain(int startPageId) async {
    var currentId = startPageId;
    while (currentId != 0) {
      try {
        final page = await _pager.readPage(currentId);
        final nextId = page.nextPageId;
        _pager.freePage(currentId);
        currentId = nextId;
      } catch (_) {
        break;
      }
    }
  }

  /// 保存 catalog（供 Collection 调用）
  Future<void> saveCatalog() async {
    await _saveCatalog();
  }
}

/// 数据库统计信息
class DatabaseStats {
  final int collectionCount;
  final int documentCount;
  final int pageCount;
  final Map<String, CollectionStats> collections;

  DatabaseStats({
    required this.collectionCount,
    required this.documentCount,
    required this.pageCount,
    required this.collections,
  });
}

/// 集合统计信息
class CollectionStats {
  final int documentCount;

  CollectionStats({required this.documentCount});
}
