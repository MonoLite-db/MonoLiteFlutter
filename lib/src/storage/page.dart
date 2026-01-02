// Created by Yanjunhui

import 'dart:typed_data';

/// 页面大小常量
const int pageSize = 4096;

/// 页面头大小
const int pageHeaderSize = 24;

/// 最大页面数据大小
const int maxPageData = pageSize - pageHeaderSize;

/// 页面类型
class PageType {
  /// 空闲页
  static const int free = 0x00;

  /// 元数据页
  static const int meta = 0x01;

  /// 目录页（Collection/Index 目录）
  static const int catalog = 0x02;

  /// 数据页
  static const int data = 0x03;

  /// 索引页
  static const int index = 0x04;

  /// 溢出页（存储大文档）
  static const int overflow = 0x05;

  /// 空闲列表页
  static const int freeList = 0x06;
}

/// 页面唯一标识
typedef PageId = int;

/// 页面结构
/// 页头结构（24 字节）：
///   - PageId (4 bytes)
///   - Type (1 byte)
///   - Flags (1 byte)
///   - ItemCount (2 bytes)
///   - FreeSpace (2 bytes)
///   - NextPageId (4 bytes) - 用于链表
///   - PrevPageId (4 bytes) - 用于双向链表
///   - Checksum (4 bytes)
///   - Reserved (2 bytes)
class Page {
  PageId id;
  int pageType;
  int flags;
  int itemCount;
  int freeSpace;
  PageId nextPageId;
  PageId prevPageId;
  int checksum;
  Uint8List data;
  bool dirty;

  Page({
    required this.id,
    required this.pageType,
    this.flags = 0,
    this.itemCount = 0,
    int? freeSpace,
    this.nextPageId = 0,
    this.prevPageId = 0,
    this.checksum = 0,
    Uint8List? data,
    this.dirty = true,
  })  : freeSpace = freeSpace ?? maxPageData,
        data = data ?? Uint8List(maxPageData);

  /// 页面类型（pageType 的别名）
  int get type => pageType;

  /// 创建一个新页面
  factory Page.create(PageId id, int pageType) {
    return Page(
      id: id,
      pageType: pageType,
      flags: 0,
      itemCount: 0,
      freeSpace: maxPageData,
      nextPageId: 0,
      prevPageId: 0,
      checksum: 0,
      data: Uint8List(maxPageData),
      dirty: true,
    );
  }

  /// 标记页面为脏
  void markDirty() {
    dirty = true;
  }

  /// 清除脏标记
  void clearDirty() {
    dirty = false;
  }

  /// 设置数据
  void setData(Uint8List newData) {
    if (newData.length > maxPageData) {
      throw ArgumentError('data too large: ${newData.length} > $maxPageData');
    }
    data.setRange(0, newData.length, newData);
    dirty = true;
  }

  /// 将页面序列化为字节
  Uint8List marshal() {
    final buf = Uint8List(pageSize);
    final bd = ByteData.view(buf.buffer);

    // 写入页头
    bd.setUint32(0, id, Endian.little);
    buf[4] = pageType;
    buf[5] = flags;
    bd.setUint16(6, itemCount, Endian.little);
    bd.setUint16(8, freeSpace, Endian.little);
    bd.setUint32(10, nextPageId, Endian.little);
    bd.setUint32(14, prevPageId, Endian.little);
    // checksum 位置 18:22，稍后计算
    // reserved 22:24

    // 写入数据
    buf.setRange(pageHeaderSize, pageHeaderSize + data.length, data);

    // 计算校验和
    checksum = _calculateChecksum(buf.sublist(pageHeaderSize));
    bd.setUint32(18, checksum, Endian.little);

    return buf;
  }

  /// 从字节反序列化页面
  static Page unmarshal(Uint8List data) {
    if (data.length != pageSize) {
      throw ArgumentError('invalid page size: ${data.length}');
    }

    final bd = ByteData.view(data.buffer);

    final page = Page(
      id: bd.getUint32(0, Endian.little),
      pageType: data[4],
      flags: data[5],
      itemCount: bd.getUint16(6, Endian.little),
      freeSpace: bd.getUint16(8, Endian.little),
      nextPageId: bd.getUint32(10, Endian.little),
      prevPageId: bd.getUint32(14, Endian.little),
      checksum: bd.getUint32(18, Endian.little),
      data: Uint8List.fromList(data.sublist(pageHeaderSize)),
      dirty: false,
    );

    // 验证校验和
    final expectedChecksum = _calculateChecksum(page.data);
    if (page.checksum != expectedChecksum) {
      throw StateError(
          'page checksum mismatch: expected ${expectedChecksum.toRadixString(16)}, got ${page.checksum.toRadixString(16)}');
    }

    return page;
  }

  /// 计算校验和
  static int _calculateChecksum(Uint8List data) {
    int sum = 0;
    int i = 0;
    while (i + 4 <= data.length) {
      sum ^= (data[i] |
          (data[i + 1] << 8) |
          (data[i + 2] << 16) |
          (data[i + 3] << 24));
      i += 4;
    }
    // 处理尾部不足 4 字节的情况
    if (i < data.length) {
      int last = 0;
      for (int j = i; j < data.length; j++) {
        last |= data[j] << (8 * (j - i));
      }
      sum ^= last;
    }
    return sum & 0xFFFFFFFF;
  }
}

/// 槽结构
class Slot {
  /// 记录在页面中的偏移
  int offset;

  /// 记录长度
  int length;

  /// 标志（删除标记等）
  int flags;

  Slot({
    required this.offset,
    required this.length,
    this.flags = 0,
  });

  /// 是否已删除
  bool get isDeleted => (flags & slotFlagDeleted) != 0;

  /// 标记为已删除
  void markDeleted() {
    flags |= slotFlagDeleted;
  }
}

/// 每个槽占用 6 字节
const int slotSize = 6;

/// 槽删除标记
const int slotFlagDeleted = 0x01;

/// 槽页面 - 支持可变长度记录的页面结构
class SlottedPage {
  final Page page;
  final List<Slot> slots;

  SlottedPage(this.page) : slots = [] {
    // 从页面数据中恢复 slots
    _restoreSlots();
  }

  /// 创建一个新的槽页面
  factory SlottedPage.create(PageId id) {
    return SlottedPage(Page.create(id, PageType.data));
  }

  /// 从现有 Page 包装为 SlottedPage
  factory SlottedPage.wrap(Page page) {
    return SlottedPage(page);
  }

  /// 从页面数据恢复槽目录
  void _restoreSlots() {
    final numSlots = page.itemCount;
    if (numSlots > 0 && page.data.length >= numSlots * slotSize) {
      final bd = ByteData.view(page.data.buffer);
      for (int i = 0; i < numSlots; i++) {
        final pos = i * slotSize;
        slots.add(Slot(
          offset: bd.getUint16(pos, Endian.little),
          length: bd.getUint16(pos + 2, Endian.little),
          flags: bd.getUint16(pos + 4, Endian.little),
        ));
      }
    }
  }

  /// 插入一条记录，返回槽索引
  int insertRecord(Uint8List data) {
    final recordLen = data.length;
    final totalNeeded = recordLen + slotSize;

    // 计算可用空间
    final slotDirEnd = (slots.length + 1) * slotSize;
    int minRecordOffset = maxPageData;
    for (final slot in slots) {
      if (!slot.isDeleted && slot.offset < minRecordOffset) {
        minRecordOffset = slot.offset;
      }
    }

    // 检查空间
    if (slotDirEnd + recordLen > minRecordOffset) {
      throw StateError(
          'not enough space: need $totalNeeded, have ${minRecordOffset - slotDirEnd}');
    }

    // 计算新记录的偏移（从页面尾部向前增长）
    final offset = minRecordOffset - recordLen;

    // 写入数据
    page.data.setRange(offset, offset + recordLen, data);

    // 添加槽
    final slot = Slot(
      offset: offset,
      length: recordLen,
      flags: 0,
    );
    slots.add(slot);

    // 将新 slot 写入页面数据
    _writeSlotToPage(slots.length - 1, slot);

    // 更新页面元数据
    page.itemCount++;
    page.freeSpace = minRecordOffset - slotDirEnd - recordLen;
    page.dirty = true;

    return slots.length - 1;
  }

  /// 将槽写入页面数据
  void _writeSlotToPage(int slotIndex, Slot slot) {
    final pos = slotIndex * slotSize;
    final bd = ByteData.view(page.data.buffer);
    bd.setUint16(pos, slot.offset, Endian.little);
    bd.setUint16(pos + 2, slot.length, Endian.little);
    bd.setUint16(pos + 4, slot.flags, Endian.little);
  }

  /// 获取指定槽的记录
  /// 如果槽不存在或已删除，返回 null
  Uint8List? getRecord(int slotIndex) {
    if (slotIndex < 0 || slotIndex >= slots.length) {
      return null;
    }

    final slot = slots[slotIndex];
    if (slot.isDeleted) {
      return null;
    }

    // 边界检查
    final endOffset = slot.offset + slot.length;
    if (slot.offset < 0 || endOffset > page.data.length) {
      return null;
    }

    return Uint8List.fromList(page.data.sublist(slot.offset, endOffset));
  }

  /// 删除指定槽的记录
  void deleteRecord(int slotIndex) {
    if (slotIndex < 0 || slotIndex >= slots.length) {
      throw RangeError('invalid slot index: $slotIndex');
    }

    final slot = slots[slotIndex];
    if (slot.isDeleted) {
      throw StateError('slot $slotIndex already deleted');
    }

    slot.markDeleted();

    // 更新页面数据中的 slot 目录
    final pos = slotIndex * slotSize;
    final bd = ByteData.view(page.data.buffer);
    bd.setUint16(pos + 4, slot.flags, Endian.little);

    page.dirty = true;
  }

  /// 更新指定槽的记录
  void updateRecord(int slotIndex, Uint8List data) {
    if (slotIndex < 0 || slotIndex >= slots.length) {
      throw RangeError('invalid slot index: $slotIndex');
    }

    final slot = slots[slotIndex];
    if (slot.isDeleted) {
      throw StateError('slot $slotIndex is deleted');
    }

    final newLen = data.length;

    // 如果新数据能放入原位置
    if (newLen <= slot.length) {
      page.data.setRange(slot.offset, slot.offset + newLen, data);
      slot.length = newLen;
      _writeSlotToPage(slotIndex, slot);
      page.dirty = true;
      return;
    }

    // 需要更多空间
    final extraSpace = newLen - slot.length;
    if (extraSpace > page.freeSpace) {
      throw StateError(
          'not enough space for update: need $extraSpace extra, have ${page.freeSpace}');
    }

    // 标记旧记录为删除，插入新记录到新位置
    slot.markDeleted();
    page.freeSpace += slot.length;

    // 找到新位置
    int minOffset = maxPageData;
    for (final s in slots) {
      if (!s.isDeleted && s.offset < minOffset) {
        minOffset = s.offset;
      }
    }
    final newOffset = minOffset - newLen;

    page.data.setRange(newOffset, newOffset + newLen, data);
    slot.offset = newOffset;
    slot.length = newLen;
    slot.flags = 0;
    page.freeSpace -= newLen;

    _writeSlotToPage(slotIndex, slot);
    page.dirty = true;
  }

  /// 返回槽总数（包括已删除的槽）
  int get slotCount => slots.length;

  /// 返回活跃记录数
  int get liveCount => slots.where((s) => !s.isDeleted).length;

  /// 检查指定槽是否已删除
  bool isSlotDeleted(int slotIndex) {
    if (slotIndex < 0 || slotIndex >= slots.length) {
      return true;
    }
    return slots[slotIndex].isDeleted;
  }

  /// 压缩页面，回收已删除记录的空间
  /// 返回旧索引到新索引的映射
  Map<int, int> compact() {
    final mapping = <int, int>{};

    // 收集所有活跃记录
    final records = <({Uint8List data, int oldIndex})>[];
    for (int i = 0; i < slots.length; i++) {
      final slot = slots[i];
      if (!slot.isDeleted) {
        final data =
            Uint8List.fromList(page.data.sublist(slot.offset, slot.offset + slot.length));
        records.add((data: data, oldIndex: i));
      }
    }

    // 清空数据区
    for (int i = 0; i < page.data.length; i++) {
      page.data[i] = 0;
    }

    // 创建新的槽目录
    slots.clear();

    // 重新写入记录
    int offset = maxPageData;
    for (int newIndex = 0; newIndex < records.length; newIndex++) {
      final r = records[newIndex];
      final recordLen = r.data.length;
      offset -= recordLen;
      page.data.setRange(offset, offset + recordLen, r.data);

      final slot = Slot(
        offset: offset,
        length: recordLen,
        flags: 0,
      );
      slots.add(slot);

      mapping[r.oldIndex] = newIndex;

      _writeSlotToPage(newIndex, slot);
    }

    // 更新页面元数据
    page.itemCount = slots.length;

    // 更新空闲空间
    int usedSpace = 0;
    for (final slot in slots) {
      usedSpace += slot.length;
    }
    final slotSpace = slots.length * slotSize;
    page.freeSpace = maxPageData - usedSpace - slotSpace;
    page.dirty = true;

    return mapping;
  }
}
