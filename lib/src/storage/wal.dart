// Created by Yanjunhui

import 'dart:io';
import 'dart:typed_data';

import 'page.dart';

/// WAL 常量
const int walMagic = 0x57414C4D; // "WALM"
const int walVersion = 1;
const int walHeaderSize = 32;
const int walRecordAlign = 8;

/// WAL 记录类型
class WalRecordType {
  /// 完整页面写入
  static const int pageWrite = 1;

  /// 分配页面
  static const int allocPage = 2;

  /// 释放页面
  static const int freePage = 3;

  /// 事务提交
  static const int commit = 4;

  /// 检查点标记
  static const int checkpoint = 5;

  /// 文件头元数据更新
  static const int metaUpdate = 6;
}

/// 元数据更新类型
class MetaUpdateType {
  static const int freeListHead = 1;
  static const int pageCount = 2;
  static const int catalogPageId = 3;
}

/// Log Sequence Number
typedef LSN = int;

/// WAL 文件头
class WalHeader {
  int magic;
  int version;
  int reserved1;
  LSN checkpointLSN;
  int fileSize;
  int checksum;
  int reserved2;

  WalHeader({
    required this.magic,
    required this.version,
    this.reserved1 = 0,
    required this.checkpointLSN,
    required this.fileSize,
    this.checksum = 0,
    this.reserved2 = 0,
  });

  /// 序列化
  Uint8List marshal() {
    final buf = Uint8List(walHeaderSize);
    final bd = ByteData.view(buf.buffer);

    bd.setUint32(0, magic, Endian.little);
    bd.setUint16(4, version, Endian.little);
    bd.setUint16(6, reserved1, Endian.little);
    bd.setUint64(8, checkpointLSN, Endian.little);
    bd.setUint64(16, fileSize, Endian.little);

    // 计算校验和
    checksum = _crc32(buf.sublist(0, 24));
    bd.setUint32(24, checksum, Endian.little);
    bd.setUint32(28, reserved2, Endian.little);

    return buf;
  }

  /// 反序列化
  static WalHeader unmarshal(Uint8List data) {
    if (data.length < walHeaderSize) {
      throw ArgumentError('invalid WAL header size: ${data.length}');
    }

    final bd = ByteData.view(data.buffer);

    final header = WalHeader(
      magic: bd.getUint32(0, Endian.little),
      version: bd.getUint16(4, Endian.little),
      reserved1: bd.getUint16(6, Endian.little),
      checkpointLSN: bd.getUint64(8, Endian.little),
      fileSize: bd.getUint64(16, Endian.little),
      checksum: bd.getUint32(24, Endian.little),
      reserved2: bd.getUint32(28, Endian.little),
    );

    if (header.magic != walMagic) {
      throw StateError('invalid WAL magic: ${header.magic.toRadixString(16)}');
    }
    if (header.version != walVersion) {
      throw StateError('unsupported WAL version: ${header.version}');
    }

    // 验证校验和
    final expectedChecksum = _crc32(data.sublist(0, 24));
    if (header.checksum != expectedChecksum) {
      throw StateError('WAL header checksum mismatch');
    }

    return header;
  }
}

/// WAL 记录头大小
const int walRecordHeaderSize = 20;

/// WAL 记录
class WalRecord {
  LSN lsn;
  int type;
  int flags;
  int dataLen;
  PageId pageId;
  int checksum;
  Uint8List? data;

  WalRecord({
    required this.lsn,
    required this.type,
    this.flags = 0,
    required this.dataLen,
    this.pageId = 0,
    this.checksum = 0,
    this.data,
  });

  /// 序列化
  Uint8List marshal() {
    // 计算记录大小（对齐）
    int recordSize = walRecordHeaderSize + dataLen;
    if (recordSize % walRecordAlign != 0) {
      recordSize += walRecordAlign - (recordSize % walRecordAlign);
    }

    final buf = Uint8List(recordSize);
    final bd = ByteData.view(buf.buffer);

    // 写入记录头
    bd.setUint64(0, lsn, Endian.little);
    buf[8] = type;
    buf[9] = flags;
    bd.setUint16(10, dataLen, Endian.little);
    bd.setUint32(12, pageId, Endian.little);

    // 写入数据
    if (dataLen > 0 && data != null) {
      buf.setRange(walRecordHeaderSize, walRecordHeaderSize + dataLen, data!);
    }

    // 计算校验和
    checksum = _crc32(buf.sublist(0, 16));
    if (dataLen > 0 && data != null) {
      checksum = _crc32Update(checksum, data!);
    }
    bd.setUint32(16, checksum, Endian.little);

    return buf;
  }

  /// 反序列化记录头
  static WalRecord unmarshalHeader(Uint8List headerBuf) {
    final bd = ByteData.view(headerBuf.buffer);

    return WalRecord(
      lsn: bd.getUint64(0, Endian.little),
      type: headerBuf[8],
      flags: headerBuf[9],
      dataLen: bd.getUint16(10, Endian.little),
      pageId: bd.getUint32(12, Endian.little),
      checksum: bd.getUint32(16, Endian.little),
    );
  }

  /// 计算记录大小（对齐后）
  int get alignedSize {
    int size = walRecordHeaderSize + dataLen;
    if (size % walRecordAlign != 0) {
      size += walRecordAlign - (size % walRecordAlign);
    }
    return size;
  }
}

/// Write-Ahead Log 管理器
class WAL {
  RandomAccessFile? _file;
  WalHeader? _header;
  LSN _currentLSN = 1;
  int _writeOffset = walHeaderSize;
  LSN _checkpointLSN = 0;
  bool _autoTruncate = false;

  WAL._();

  /// 创建或打开 WAL 文件
  static Future<WAL> open(String path) async {
    final wal = WAL._();

    final file = File(path);
    final exists = await file.exists();

    wal._file = await file.open(mode: FileMode.append);
    await wal._file!.setPosition(0);

    if (exists) {
      final stat = await file.stat();
      if (stat.size > 0) {
        await wal._readHeader();
      } else {
        await wal._initNewWAL();
      }
    } else {
      await wal._initNewWAL();
    }

    return wal;
  }

  /// 初始化新的 WAL 文件
  Future<void> _initNewWAL() async {
    _header = WalHeader(
      magic: walMagic,
      version: walVersion,
      checkpointLSN: 0,
      fileSize: walHeaderSize,
    );
    _currentLSN = 1;
    _writeOffset = walHeaderSize;
    _checkpointLSN = 0;

    await _writeHeader();
  }

  /// 读取 WAL 文件头
  Future<void> _readHeader() async {
    await _file!.setPosition(0);
    final buf = await _file!.read(walHeaderSize);
    if (buf.length < walHeaderSize) {
      await _initNewWAL();
      return;
    }

    try {
      _header = WalHeader.unmarshal(Uint8List.fromList(buf));
      _checkpointLSN = _header!.checkpointLSN;
      _writeOffset = _header!.fileSize;
      _currentLSN = _checkpointLSN + 1;
      await _scanForMaxLSN();
    } catch (e) {
      // 头部损坏，重新初始化
      await _initNewWAL();
    }
  }

  /// 写入 WAL 文件头
  Future<void> _writeHeader() async {
    await _file!.setPosition(0);
    await _file!.writeFrom(_header!.marshal());
  }

  /// 扫描 WAL 找到最大 LSN
  Future<void> _scanForMaxLSN() async {
    final stat = await File(_file!.path).stat();
    final actualFileSize = stat.size;

    int offset = walHeaderSize;
    final headerBuf = Uint8List(walRecordHeaderSize);
    int lastValidOffset = offset;

    while (offset < actualFileSize) {
      await _file!.setPosition(offset);
      final n = await _file!.readInto(headerBuf);
      if (n < walRecordHeaderSize) {
        break;
      }

      final record = WalRecord.unmarshalHeader(headerBuf);

      // 验证校验和
      int expectedChecksum = _crc32(headerBuf.sublist(0, 16));
      if (record.dataLen > 0) {
        if (offset + walRecordHeaderSize + record.dataLen > actualFileSize) {
          break;
        }
        final data = await _file!.read(record.dataLen);
        expectedChecksum = _crc32Update(expectedChecksum, Uint8List.fromList(data));
      }

      if (record.checksum != expectedChecksum) {
        break;
      }

      if (record.lsn >= _currentLSN) {
        _currentLSN = record.lsn + 1;
      }

      final recordSize = record.alignedSize;
      lastValidOffset = offset + recordSize;
      offset = lastValidOffset;
    }

    _writeOffset = lastValidOffset;
    _header!.fileSize = lastValidOffset;
  }

  /// 写入页面更新记录
  Future<LSN> writePageRecord(PageId pageId, Uint8List data) async {
    final record = WalRecord(
      lsn: _currentLSN,
      type: WalRecordType.pageWrite,
      dataLen: data.length,
      pageId: pageId,
      data: data,
    );

    await _writeRecord(record);
    final lsn = _currentLSN;
    _currentLSN++;
    return lsn;
  }

  /// 写入页面分配记录
  Future<LSN> writeAllocRecord(PageId pageId, int pageType) async {
    final data = Uint8List(1);
    data[0] = pageType;

    final record = WalRecord(
      lsn: _currentLSN,
      type: WalRecordType.allocPage,
      dataLen: 1,
      pageId: pageId,
      data: data,
    );

    await _writeRecord(record);
    final lsn = _currentLSN;
    _currentLSN++;
    return lsn;
  }

  /// 写入页面释放记录
  Future<LSN> writeFreeRecord(PageId pageId) async {
    final record = WalRecord(
      lsn: _currentLSN,
      type: WalRecordType.freePage,
      dataLen: 0,
      pageId: pageId,
    );

    await _writeRecord(record);
    final lsn = _currentLSN;
    _currentLSN++;
    return lsn;
  }

  /// 写入元数据更新记录
  Future<LSN> writeMetaRecord(int metaType, int oldValue, int newValue) async {
    final data = Uint8List(9);
    final bd = ByteData.view(data.buffer);
    data[0] = metaType;
    bd.setUint32(1, oldValue, Endian.little);
    bd.setUint32(5, newValue, Endian.little);

    final record = WalRecord(
      lsn: _currentLSN,
      type: WalRecordType.metaUpdate,
      dataLen: 9,
      pageId: 0,
      data: data,
    );

    await _writeRecord(record);
    final lsn = _currentLSN;
    _currentLSN++;
    return lsn;
  }

  /// 写入提交记录
  Future<LSN> writeCommitRecord() async {
    final record = WalRecord(
      lsn: _currentLSN,
      type: WalRecordType.commit,
      dataLen: 0,
      pageId: 0,
    );

    await _writeRecord(record);
    final lsn = _currentLSN;
    _currentLSN++;
    return lsn;
  }

  /// 写入单条 WAL 记录
  Future<void> _writeRecord(WalRecord record) async {
    final buf = record.marshal();
    await _file!.setPosition(_writeOffset);
    await _file!.writeFrom(buf);
    _writeOffset += buf.length;
    _header!.fileSize = _writeOffset;
  }

  /// 刷盘
  Future<void> sync() async {
    await _writeHeader();
    await _file!.flush();
  }

  /// 创建检查点
  Future<void> checkpoint(LSN lsn) async {
    // 写入检查点记录
    final data = Uint8List(8);
    final bd = ByteData.view(data.buffer);
    bd.setUint64(0, lsn, Endian.little);

    final record = WalRecord(
      lsn: _currentLSN,
      type: WalRecordType.checkpoint,
      dataLen: 8,
      pageId: 0,
      data: data,
    );

    await _writeRecord(record);
    _currentLSN++;

    _header!.checkpointLSN = lsn;
    _checkpointLSN = lsn;

    await _writeHeader();
    await _file!.flush();

    // 自动截断
    if (_autoTruncate && _writeOffset > 64 * 1024 * 1024) {
      await _truncateAfterCheckpoint();
    }
  }

  /// 截断 checkpoint 之前的记录
  Future<void> _truncateAfterCheckpoint() async {
    await _file!.truncate(walHeaderSize);
    _writeOffset = walHeaderSize;
    _header!.fileSize = walHeaderSize;
    await _writeHeader();
    await _file!.flush();
  }

  /// 获取检查点 LSN
  LSN get checkpointLSN => _checkpointLSN;

  /// 获取当前 LSN
  LSN get currentLSN => _currentLSN;

  /// 从指定 LSN 开始读取记录
  Future<List<WalRecord>> readRecordsFrom(LSN startLSN) async {
    final records = <WalRecord>[];
    int offset = walHeaderSize;
    final headerBuf = Uint8List(walRecordHeaderSize);

    while (offset < _writeOffset) {
      await _file!.setPosition(offset);
      final n = await _file!.readInto(headerBuf);
      if (n < walRecordHeaderSize) {
        break;
      }

      final record = WalRecord.unmarshalHeader(headerBuf);

      // 读取数据
      if (record.dataLen > 0) {
        record.data = Uint8List.fromList(await _file!.read(record.dataLen));
      }

      // 验证校验和
      int expectedChecksum = _crc32(headerBuf.sublist(0, 16));
      if (record.dataLen > 0 && record.data != null) {
        expectedChecksum = _crc32Update(expectedChecksum, record.data!);
      }

      if (record.checksum != expectedChecksum) {
        throw StateError('WAL record checksum mismatch at offset $offset');
      }

      if (record.lsn >= startLSN) {
        records.add(record);
      }

      offset += record.alignedSize;
    }

    return records;
  }

  /// 截断 WAL
  Future<void> truncate() async {
    await _file!.truncate(walHeaderSize);
    _writeOffset = walHeaderSize;
    _header!.fileSize = walHeaderSize;
    await _writeHeader();
    await _file!.flush();
  }

  /// 关闭 WAL
  Future<void> close() async {
    await _writeHeader();
    await _file!.flush();
    await _file!.close();
  }

  /// 设置自动截断
  void setAutoTruncate(bool enable) {
    _autoTruncate = enable;
  }

  /// 获取 WAL 文件路径
  static String walPath(String dbPath) => '$dbPath.wal';
}

/// CRC32 计算（IEEE 多项式）
int _crc32(Uint8List data) {
  return _crc32Update(0xFFFFFFFF, data) ^ 0xFFFFFFFF;
}

int _crc32Update(int crc, Uint8List data) {
  for (int i = 0; i < data.length; i++) {
    crc ^= data[i];
    for (int j = 0; j < 8; j++) {
      if ((crc & 1) != 0) {
        crc = (crc >> 1) ^ 0xEDB88320;
      } else {
        crc >>= 1;
      }
    }
  }
  return crc & 0xFFFFFFFF;
}
