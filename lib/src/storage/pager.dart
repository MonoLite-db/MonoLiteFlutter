// Created by Yanjunhui

import 'dart:io';
import 'dart:typed_data';

import 'page.dart';
import 'wal.dart';

/// 文件格式常量
const int magicNumber = 0x4D4F4E4F; // "MONO"
const int formatVersion = 1;
const int fileHeaderSize = 64;

/// 文件头结构（64 字节）
class FileHeader {
  /// 魔数 "MONO"
  int magic;

  /// 文件格式版本
  int version;

  /// 页面大小
  int pageSizeValue;

  /// 总页面数
  int pageCount;

  /// 空闲页链表头
  PageId freeListHead;

  /// 元数据页 ID
  PageId metaPageId;

  /// 目录页 ID
  PageId catalogPageId;

  /// 创建时间（Unix 毫秒）
  int createTime;

  /// 最后修改时间
  int modifyTime;

  /// 保留字段
  Uint8List reserved;

  FileHeader({
    required this.magic,
    required this.version,
    required this.pageSizeValue,
    required this.pageCount,
    required this.freeListHead,
    required this.metaPageId,
    required this.catalogPageId,
    required this.createTime,
    required this.modifyTime,
    Uint8List? reserved,
  }) : reserved = reserved ?? Uint8List(24);

  /// 序列化
  Uint8List marshal() {
    final buf = Uint8List(fileHeaderSize);
    final bd = ByteData.view(buf.buffer);

    bd.setUint32(0, magic, Endian.little);
    bd.setUint16(4, version, Endian.little);
    bd.setUint16(6, pageSizeValue, Endian.little);
    bd.setUint32(8, pageCount, Endian.little);
    bd.setUint32(12, freeListHead, Endian.little);
    bd.setUint32(16, metaPageId, Endian.little);
    bd.setUint32(20, catalogPageId, Endian.little);
    bd.setUint64(24, createTime, Endian.little);
    bd.setUint64(32, modifyTime, Endian.little);
    buf.setRange(40, 64, reserved);

    return buf;
  }

  /// 反序列化
  static FileHeader unmarshal(Uint8List data) {
    if (data.length < fileHeaderSize) {
      throw ArgumentError('invalid header size: ${data.length}');
    }

    final bd = ByteData.view(data.buffer);

    return FileHeader(
      magic: bd.getUint32(0, Endian.little),
      version: bd.getUint16(4, Endian.little),
      pageSizeValue: bd.getUint16(6, Endian.little),
      pageCount: bd.getUint32(8, Endian.little),
      freeListHead: bd.getUint32(12, Endian.little),
      metaPageId: bd.getUint32(16, Endian.little),
      catalogPageId: bd.getUint32(20, Endian.little),
      createTime: bd.getUint64(24, Endian.little),
      modifyTime: bd.getUint64(32, Endian.little),
      reserved: Uint8List.fromList(data.sublist(40, 64)),
    );
  }
}

/// 页面管理器
class Pager {
  RandomAccessFile? _file;
  final String path;
  FileHeader? _header;
  int _pageCount = 0;
  final List<PageId> _freePages = [];
  final Map<PageId, Page> _cache = {};
  final Set<PageId> _dirty = {};
  final Map<PageId, LSN> _pageLSN = {};
  final int _maxCached;
  WAL? _wal;
  final bool _walEnabled;

  Pager._({
    required this.path,
    int maxCached = 1000,
    bool walEnabled = true,
  })  : _maxCached = maxCached,
        _walEnabled = walEnabled;

  /// 打开或创建数据库文件
  static Future<Pager> open(String path, {bool enableWAL = true}) async {
    final pager = Pager._(path: path, walEnabled: enableWAL);

    final file = File(path);
    final exists = await file.exists();

    pager._file = await file.open(mode: FileMode.append);
    await pager._file!.setPosition(0);

    if (exists) {
      final stat = await file.stat();
      if (stat.size >= fileHeaderSize) {
        await pager._readHeader();
        await pager._loadFreeList();
      } else {
        await pager._initNewFile();
      }
    } else {
      await pager._initNewFile();
    }

    // 初始化 WAL
    if (enableWAL) {
      final walPath = WAL.walPath(path);
      pager._wal = await WAL.open(walPath);

      // 执行崩溃恢复
      if (exists) {
        await pager._recover();
      }
    }

    return pager;
  }

  /// 初始化新数据库文件
  Future<void> _initNewFile() async {
    final now = DateTime.now().millisecondsSinceEpoch;
    _header = FileHeader(
      magic: magicNumber,
      version: formatVersion,
      pageSizeValue: pageSize,
      pageCount: 1,
      freeListHead: 0,
      metaPageId: 0,
      catalogPageId: 0,
      createTime: now,
      modifyTime: now,
    );

    await _writeHeader();

    // 创建并写入初始元数据页
    final metaPage = Page.create(0, PageType.meta);
    await _writePage(metaPage);

    _pageCount = 1;
  }

  /// 读取文件头
  Future<void> _readHeader() async {
    await _file!.setPosition(0);
    final buf = await _file!.read(fileHeaderSize);
    _header = FileHeader.unmarshal(Uint8List.fromList(buf));

    if (_header!.magic != magicNumber) {
      throw StateError(
          'invalid magic number: ${_header!.magic.toRadixString(16)}');
    }
    if (_header!.version != formatVersion) {
      throw StateError('incompatible format version: ${_header!.version}');
    }
    if (_header!.pageSizeValue != pageSize) {
      throw StateError('incompatible page size: ${_header!.pageSizeValue}');
    }

    _pageCount = _header!.pageCount;
  }

  /// 写入文件头
  Future<void> _writeHeader() async {
    await _file!.setPosition(0);
    await _file!.writeFrom(_header!.marshal());
  }

  /// 加载空闲页列表
  Future<void> _loadFreeList() async {
    _freePages.clear();

    if (_header!.freeListHead == 0) {
      return;
    }

    PageId currentId = _header!.freeListHead;
    while (currentId != 0) {
      final page = await readPage(currentId);
      _freePages.add(currentId);
      currentId = page.nextPageId;
    }
  }

  /// 计算页面在文件中的偏移
  int _pageOffset(PageId id) {
    return fileHeaderSize + id * pageSize;
  }

  /// 读取页面
  Future<Page> readPage(PageId id) async {
    // 先检查缓存
    if (_cache.containsKey(id)) {
      return _cache[id]!;
    }

    // 从文件读取
    final offset = _pageOffset(id);
    await _file!.setPosition(offset);
    final buf = await _file!.read(pageSize);
    if (buf.length < pageSize) {
      throw StateError('page $id does not exist');
    }

    final page = Page.unmarshal(Uint8List.fromList(buf));
    _addToCache(page);

    return page;
  }

  /// 写入页面
  Future<void> _writePage(Page page) async {
    final data = page.marshal();

    // 先写 WAL
    if (_wal != null && _walEnabled) {
      final lsn = await _wal!.writePageRecord(page.id, data);
      _pageLSN[page.id] = lsn;
    }

    // 再写数据文件
    final offset = _pageOffset(page.id);
    await _file!.setPosition(offset);
    await _file!.writeFrom(data);

    page.clearDirty();
  }

  /// 分配新页面
  Future<Page> allocatePage(int pageType) async {
    PageId pageId;
    PageId oldFreeListHead = 0;
    PageId newFreeListHead = 0;
    int oldPageCount = 0;
    int newPageCount = 0;
    bool fromFreeList = false;

    // 优先从空闲列表分配
    if (_freePages.isNotEmpty) {
      fromFreeList = true;
      pageId = _freePages.first;

      final page = await readPage(pageId);
      oldFreeListHead = _header!.freeListHead;
      newFreeListHead = page.nextPageId;
    } else {
      pageId = _pageCount;
      oldPageCount = _pageCount;
      newPageCount = _pageCount + 1;
    }

    // WAL 先行
    if (_wal != null && _walEnabled) {
      await _wal!.writeAllocRecord(pageId, pageType);

      if (fromFreeList) {
        await _wal!.writeMetaRecord(
            MetaUpdateType.freeListHead, oldFreeListHead, newFreeListHead);
      } else {
        await _wal!.writeMetaRecord(
            MetaUpdateType.pageCount, oldPageCount, newPageCount);
      }

      await _wal!.sync();
    }

    // 更新内存状态
    if (fromFreeList) {
      _freePages.removeAt(0);
      _header!.freeListHead = newFreeListHead;
    } else {
      _pageCount++;
      _header!.pageCount = _pageCount;
    }
    _header!.modifyTime = DateTime.now().millisecondsSinceEpoch;

    // 创建新页面
    final page = Page.create(pageId, pageType);

    // 写入页面
    if (!fromFreeList) {
      await _writePage(page);
    }

    // 持久化 header
    await _writeHeader();

    _addToCache(page);
    _dirty.add(pageId);

    return page;
  }

  /// 释放页面
  Future<void> freePage(PageId id) async {
    final oldFreeListHead = _header!.freeListHead;
    final newFreeListHead = id;

    // WAL 先行
    if (_wal != null && _walEnabled) {
      await _wal!.writeFreeRecord(id);
      await _wal!.writeMetaRecord(
          MetaUpdateType.freeListHead, oldFreeListHead, newFreeListHead);
      await _wal!.sync();
    }

    final page = await readPage(id);
    page.pageType = PageType.free;
    page.nextPageId = oldFreeListHead;
    page.dirty = true;

    await _writePage(page);

    _header!.freeListHead = newFreeListHead;
    _header!.modifyTime = DateTime.now().millisecondsSinceEpoch;
    await _writeHeader();

    _freePages.insert(0, id);
    _dirty.add(id);
  }

  /// 标记页面为脏
  void markDirty(PageId id) {
    if (_cache.containsKey(id)) {
      _cache[id]!.markDirty();
      _dirty.add(id);
    }
  }

  /// 刷盘
  Future<void> flush() async {
    // 先刷 WAL
    if (_wal != null && _walEnabled) {
      await _wal!.sync();
    }

    // 写入所有脏页
    for (final id in _dirty.toList()) {
      if (_cache.containsKey(id) && _cache[id]!.dirty) {
        await _writePage(_cache[id]!);
      }
    }
    _dirty.clear();

    // 更新文件头
    _header!.modifyTime = DateTime.now().millisecondsSinceEpoch;
    await _writeHeader();

    // 同步数据文件
    await _file!.flush();

    // 创建检查点
    if (_wal != null && _walEnabled) {
      final currentLSN = _wal!.currentLSN;
      if (currentLSN > 1) {
        await _wal!.checkpoint(currentLSN - 1);
      }
    }
  }

  /// 关闭 Pager
  Future<void> close() async {
    await flush();

    if (_wal != null) {
      await _wal!.close();
    }

    await _file!.close();
  }

  /// 添加页面到缓存
  void _addToCache(Page page) {
    if (_cache.length >= _maxCached) {
      // 移除一个非脏页
      PageId? toEvict;
      for (final entry in _cache.entries) {
        if (!entry.value.dirty) {
          toEvict = entry.key;
          break;
        }
      }
      if (toEvict != null) {
        _cache.remove(toEvict);
      }
    }
    _cache[page.id] = page;
  }

  /// 执行崩溃恢复
  Future<void> _recover() async {
    if (_wal == null) return;

    final checkpointLSN = _wal!.checkpointLSN;

    // 记录分配页类型
    final allocPageTypes = <PageId, int>{};

    // 获取实际文件大小
    final stat = await File(path).stat();
    final actualSize = stat.size;

    // 读取 WAL 记录
    final records = await _wal!.readRecordsFrom(checkpointLSN + 1);
    if (records.isEmpty) return;

    // Redo: 回放所有记录
    for (final record in records) {
      switch (record.type) {
        case WalRecordType.pageWrite:
          if (record.dataLen > 0 && record.data!.length == pageSize) {
            final offset = _pageOffset(record.pageId);
            await _file!.setPosition(offset);
            await _file!.writeFrom(record.data!);
          }
          break;

        case WalRecordType.allocPage:
          if (record.pageId >= _pageCount) {
            _pageCount = record.pageId + 1;
            _header!.pageCount = _pageCount;
          }
          int allocPageType = PageType.data;
          if (record.dataLen >= 1 && record.data != null) {
            allocPageType = record.data![0];
            allocPageTypes[record.pageId] = allocPageType;
          }

          // 初始化页面
          final offset = _pageOffset(record.pageId);
          if (offset + pageSize <= actualSize) {
            final initPage = Page.create(record.pageId, allocPageType);
            await _file!.setPosition(offset);
            await _file!.writeFrom(initPage.marshal());
          }
          break;

        case WalRecordType.freePage:
          // 会在 MetaUpdate 中处理
          break;

        case WalRecordType.metaUpdate:
          if (record.dataLen >= 9 && record.data != null) {
            final metaType = record.data![0];
            final bd = ByteData.view(record.data!.buffer);
            final newValue = bd.getUint32(5, Endian.little);

            switch (metaType) {
              case MetaUpdateType.freeListHead:
                _header!.freeListHead = newValue;
                break;
              case MetaUpdateType.pageCount:
                _header!.pageCount = newValue;
                _pageCount = newValue;
                break;
              case MetaUpdateType.catalogPageId:
                _header!.catalogPageId = newValue;
                break;
            }
          }
          break;

        case WalRecordType.checkpoint:
          // 检查点记录
          break;
      }
    }

    // 同步数据文件
    await _file!.flush();

    // 确保文件大小与 PageCount 一致
    await _ensureFileSize(allocPageTypes);

    // 持久化 header
    await _writeHeader();

    // 重新加载 free list
    _freePages.clear();
    await _loadFreeList();
  }

  /// 确保文件大小与 PageCount 一致
  Future<void> _ensureFileSize(Map<PageId, int> allocPageTypes) async {
    final expectedSize = fileHeaderSize + _pageCount * pageSize;

    final stat = await File(path).stat();
    final actualSize = stat.size;

    if (actualSize < expectedSize) {
      int startOffset = actualSize;
      if (actualSize > fileHeaderSize) {
        final rel = actualSize - fileHeaderSize;
        final rem = rel % pageSize;
        if (rem != 0) {
          startOffset = actualSize - rem;
        }
      } else {
        startOffset = fileHeaderSize;
      }

      for (int offset = startOffset; offset < expectedSize; offset += pageSize) {
        final pageId = (offset - fileHeaderSize) ~/ pageSize;
        int allocPageType = PageType.data;
        if (allocPageTypes.containsKey(pageId)) {
          allocPageType = allocPageTypes[pageId]!;
        }
        final page = Page.create(pageId, allocPageType);
        await _file!.setPosition(offset);
        await _file!.writeFrom(page.marshal());
      }
    }
  }

  /// 获取总页面数
  int get pageCountValue => _pageCount;

  /// 获取空闲页面数
  int get freePageCount => _freePages.length;

  /// 获取文件头
  FileHeader get header => _header!;

  /// 设置目录页 ID
  void setCatalogPageId(PageId id) {
    _header!.catalogPageId = id;
  }

  /// 获取目录页 ID
  PageId get catalogPageId => _header!.catalogPageId;

  /// 获取页面总数
  int get pageCount => _pageCount;
}
