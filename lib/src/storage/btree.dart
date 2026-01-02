// Created by Yanjunhui

import 'dart:typed_data';

import 'page.dart';
import 'pager.dart';

/// B+Tree 阶数
const int btreeOrder = 50;

/// 索引键值大小限制
const int maxIndexKeyBytes = maxPageData ~/ 4; // ~1KB
const int maxIndexValueBytes = 256;
const int maxIndexEntryBytes = maxIndexKeyBytes + maxIndexValueBytes;

/// 节点头部固定大小
const int btreeNodeHeaderSize = 11;

/// 节点数据区最大字节数
const int btreeNodeMaxBytes = maxPageData - 64;

/// 触发分裂的字节阈值
const int btreeNodeSplitThreshold = btreeNodeMaxBytes * 3 ~/ 4;

/// 记录标识符
class RecordId {
  final PageId pageId;
  final int slotIndex;

  const RecordId({required this.pageId, required this.slotIndex});

  /// RecordId 字节大小
  static const int size = 6;

  /// 序列化
  Uint8List marshal() {
    final buf = Uint8List(size);
    final bd = ByteData.view(buf.buffer);
    bd.setUint32(0, pageId, Endian.little);
    bd.setUint16(4, slotIndex, Endian.little);
    return buf;
  }

  /// 反序列化
  static RecordId unmarshal(Uint8List data) {
    final bd = ByteData.view(data.buffer);
    return RecordId(
      pageId: bd.getUint32(0, Endian.little),
      slotIndex: bd.getUint16(4, Endian.little),
    );
  }

  @override
  String toString() => 'RecordId(pageId: $pageId, slot: $slotIndex)';

  @override
  bool operator ==(Object other) =>
      other is RecordId &&
      other.pageId == pageId &&
      other.slotIndex == slotIndex;

  @override
  int get hashCode => Object.hash(pageId, slotIndex);
}

/// B+Tree 节点
class BTreeNode {
  PageId pageId;
  bool isLeaf;
  int keyCount;
  List<Uint8List> keys;
  List<Uint8List> values; // 仅叶子节点使用
  List<PageId> children; // 仅内部节点使用
  PageId next;
  PageId prev;

  BTreeNode({
    required this.pageId,
    required this.isLeaf,
    this.keyCount = 0,
    List<Uint8List>? keys,
    List<Uint8List>? values,
    List<PageId>? children,
    this.next = 0,
    this.prev = 0,
  })  : keys = keys ?? [],
        values = values ?? [],
        children = children ?? [];

  /// 计算节点序列化后的字节大小
  int get byteSize {
    int size = btreeNodeHeaderSize;

    // 键的大小
    for (final key in keys) {
      size += 2 + key.length;
    }

    if (isLeaf) {
      // 值的大小
      for (final value in values) {
        size += 2 + value.length;
      }
    } else {
      // 子节点指针大小
      size += children.length * 4;
    }

    return size;
  }

  /// 是否需要分裂
  bool get needsSplit => byteSize > btreeNodeSplitThreshold;

  /// 是否能容纳新的键值对
  bool canAccommodate(Uint8List key, Uint8List value) {
    int additionalSize = 2 + key.length;
    if (isLeaf) {
      additionalSize += 2 + value.length;
    } else {
      additionalSize += 4;
    }
    return byteSize + additionalSize <= btreeNodeMaxBytes;
  }
}

/// B+Tree 索引结构
class BTree {
  final Pager pager;
  PageId _rootPage;
  final String name;
  final bool unique;

  BTree._({
    required this.pager,
    required PageId rootPage,
    required this.name,
    required this.unique,
  }) : _rootPage = rootPage;

  /// 创建新的 B+Tree
  static Future<BTree> create(
    Pager pager,
    String name, {
    bool unique = false,
  }) async {
    // 分配根节点页面
    final rootPage = await pager.allocatePage(PageType.index);

    // 初始化为空的叶子节点
    final root = BTreeNode(
      pageId: rootPage.id,
      isLeaf: true,
      keyCount: 0,
    );

    final tree = BTree._(
      pager: pager,
      rootPage: rootPage.id,
      name: name,
      unique: unique,
    );

    await tree._writeNode(root);

    return tree;
  }

  /// 打开已存在的 B+Tree
  static BTree open(
    Pager pager,
    PageId rootPage,
    String name, {
    bool unique = false,
  }) {
    return BTree._(
      pager: pager,
      rootPage: rootPage,
      name: name,
      unique: unique,
    );
  }

  /// 获取根页面 ID
  PageId get rootPage => _rootPage;

  /// 插入键值对
  Future<void> insert(Uint8List key, Uint8List value) async {
    // 检查键值大小
    if (key.length > maxIndexKeyBytes) {
      throw ArgumentError(
          'index key too large: ${key.length} bytes (max: $maxIndexKeyBytes)');
    }
    if (value.length > maxIndexValueBytes) {
      throw ArgumentError(
          'index value too large: ${value.length} bytes (max: $maxIndexValueBytes)');
    }

    final root = await _readNode(_rootPage);

    // 如果根节点满了，需要分裂
    if (root.keyCount >= btreeOrder - 1) {
      final newRootPage = await pager.allocatePage(PageType.index);

      final newRoot = BTreeNode(
        pageId: newRootPage.id,
        isLeaf: false,
        keyCount: 0,
        children: [root.pageId],
      );

      _rootPage = newRoot.pageId;

      await _splitChild(newRoot, 0);
      await _insertNonFull(newRoot, key, value);
    } else {
      await _insertNonFull(root, key, value);
    }
  }

  /// 在非满节点中插入
  Future<void> _insertNonFull(
      BTreeNode node, Uint8List key, Uint8List value) async {
    int i = node.keyCount - 1;

    if (node.isLeaf) {
      // 在叶子节点中找到插入位置
      while (i >= 0 && _compareKeys(key, node.keys[i]) < 0) {
        i--;
      }

      // 检查唯一约束
      if (unique) {
        final checkPos = i + 1;
        if (checkPos < node.keyCount &&
            _compareKeys(key, node.keys[checkPos]) == 0) {
          throw StateError('duplicate key');
        }
        if (i >= 0 && _compareKeys(key, node.keys[i]) == 0) {
          throw StateError('duplicate key');
        }
      }

      i++;

      // 插入键值对
      node.keys.insert(i, key);
      node.values.insert(i, value);
      node.keyCount++;

      await _writeNode(node);
    } else {
      // 内部节点：找到子节点
      while (i >= 0 && _compareKeys(key, node.keys[i]) < 0) {
        i--;
      }
      i++;

      var child = await _readNode(node.children[i]);

      // 如果子节点满了，先分裂
      if (child.keyCount >= btreeOrder - 1) {
        await _splitChild(node, i);

        // 决定走哪个子节点
        if (_compareKeys(key, node.keys[i]) > 0) {
          i++;
        }
        child = await _readNode(node.children[i]);
      }

      await _insertNonFull(child, key, value);
    }
  }

  /// 分裂子节点
  Future<void> _splitChild(BTreeNode parent, int index) async {
    final child = await _readNode(parent.children[index]);

    final newPage = await pager.allocatePage(PageType.index);

    // 字节驱动的分裂点
    final mid = _findByteDrivenSplitPoint(child);

    final newNode = BTreeNode(
      pageId: newPage.id,
      isLeaf: child.isLeaf,
      next: child.next,
      prev: child.pageId,
    );

    final nodesToWrite = <BTreeNode>[];

    if (child.isLeaf) {
      // 叶子节点：复制右半部分
      newNode.keys = child.keys.sublist(mid);
      newNode.values = child.values.sublist(mid);
      newNode.keyCount = newNode.keys.length;

      child.next = newNode.pageId;
      if (newNode.next != 0) {
        final nextNode = await _readNode(newNode.next);
        nextNode.prev = newNode.pageId;
        nodesToWrite.add(nextNode);
      }

      final midKey = Uint8List.fromList(child.keys[mid]);

      child.keys = child.keys.sublist(0, mid);
      child.values = child.values.sublist(0, mid);
      child.keyCount = child.keys.length;

      parent.keys.insert(index, midKey);
      parent.children.insert(index + 1, newNode.pageId);
      parent.keyCount++;
    } else {
      // 内部节点
      final midKey = Uint8List.fromList(child.keys[mid]);

      newNode.keys = child.keys.sublist(mid + 1);
      newNode.children = child.children.sublist(mid + 1);
      newNode.keyCount = newNode.keys.length;

      child.keys = child.keys.sublist(0, mid);
      child.children = child.children.sublist(0, mid + 1);
      child.keyCount = child.keys.length;

      parent.keys.insert(index, midKey);
      parent.children.insert(index + 1, newNode.pageId);
      parent.keyCount++;
    }

    nodesToWrite.addAll([child, newNode, parent]);

    for (final node in nodesToWrite) {
      await _writeNode(node);
    }
  }

  /// 计算字节驱动的分裂点
  int _findByteDrivenSplitPoint(BTreeNode node) {
    if (node.keyCount <= 1) {
      return 0;
    }

    final totalSize = node.byteSize;
    final targetSize = totalSize ~/ 2;

    int leftSize = btreeNodeHeaderSize;
    int bestMid = node.keyCount ~/ 2;

    for (int i = 0; i < node.keyCount; i++) {
      leftSize += 2 + node.keys[i].length;
      if (node.isLeaf) {
        leftSize += 2 + node.values[i].length;
      } else if (i < node.children.length) {
        leftSize += 4;
      }

      if (leftSize >= targetSize) {
        if (i < 1) {
          bestMid = 1;
        } else if (i >= node.keyCount - 1) {
          bestMid = node.keyCount - 1;
        } else {
          bestMid = i;
        }
        break;
      }
    }

    if (bestMid < 1) bestMid = 1;
    if (bestMid >= node.keyCount) bestMid = node.keyCount - 1;

    return bestMid;
  }

  /// 搜索键
  Future<Uint8List?> search(Uint8List key) async {
    var node = await _readNode(_rootPage);

    while (!node.isLeaf) {
      int i = 0;
      while (i < node.keyCount && _compareKeys(key, node.keys[i]) >= 0) {
        i++;
      }
      node = await _readNode(node.children[i]);
    }

    // 在叶子节点中查找
    for (int i = 0; i < node.keyCount; i++) {
      final cmp = _compareKeys(key, node.keys[i]);
      if (cmp == 0) {
        return node.values[i];
      }
      if (cmp < 0) {
        break;
      }
    }

    return null;
  }

  /// 范围搜索
  Future<List<Uint8List>> searchRange(
    Uint8List? minKey,
    Uint8List? maxKey, {
    bool includeMin = true,
    bool includeMax = true,
    int limit = -1,
  }) async {
    final results = <Uint8List>[];

    // 找到起始叶子节点
    var node = await _readNode(_rootPage);

    while (!node.isLeaf) {
      int i = 0;
      if (minKey != null) {
        while (i < node.keyCount && _compareKeys(minKey, node.keys[i]) >= 0) {
          i++;
        }
      }
      node = await _readNode(node.children[i]);
    }

    // 遍历叶子节点链表
    while (true) {
      for (int i = 0; i < node.keyCount; i++) {
        if (limit >= 0 && results.length >= limit) {
          return results;
        }

        final key = node.keys[i];

        // 检查下界
        if (minKey != null) {
          final cmp = _compareKeys(key, minKey);
          if (cmp < 0 || (!includeMin && cmp == 0)) {
            continue;
          }
        }

        // 检查上界
        if (maxKey != null) {
          final cmp = _compareKeys(key, maxKey);
          if (cmp > 0 || (!includeMax && cmp == 0)) {
            return results;
          }
        }

        results.add(node.values[i]);
      }

      if (node.next == 0) break;
      node = await _readNode(node.next);
    }

    return results;
  }

  /// 删除键
  Future<void> delete(Uint8List key) async {
    await _deleteInternal(_rootPage, key, null, -1);
  }

  /// 递归删除
  Future<void> _deleteInternal(
    PageId nodeId,
    Uint8List key,
    BTreeNode? parent,
    int childIndex,
  ) async {
    var node = await _readNode(nodeId);

    if (node.isLeaf) {
      await _deleteFromLeaf(node, key, parent, childIndex);
      return;
    }

    // 内部节点：找到子节点
    int i = 0;
    while (i < node.keyCount && _compareKeys(key, node.keys[i]) >= 0) {
      i++;
    }

    if (i >= node.children.length) {
      i = node.children.length - 1;
    }
    if (i < 0) return;

    await _deleteInternal(node.children[i], key, node, i);

    // 重新读取节点
    node = await _readNode(nodeId);

    // 检查子节点是否需要修复
    if (i < node.children.length) {
      await _fixAfterDelete(node, i);
    }
  }

  /// 从叶子节点删除
  Future<void> _deleteFromLeaf(
    BTreeNode node,
    Uint8List key,
    BTreeNode? parent,
    int childIndex,
  ) async {
    int found = -1;
    for (int i = 0; i < node.keyCount; i++) {
      if (_compareKeys(key, node.keys[i]) == 0) {
        found = i;
        break;
      }
    }

    if (found == -1) return;

    node.keys.removeAt(found);
    node.values.removeAt(found);
    node.keyCount--;

    await _writeNode(node);

    if (parent != null && node.keyCount < _minKeys()) {
      await _fixUnderflow(node, parent, childIndex);
    }
  }

  /// 删除后修复节点
  Future<void> _fixAfterDelete(BTreeNode parent, int childIndex) async {
    if (childIndex < 0 || childIndex >= parent.children.length) {
      return;
    }

    final child = await _readNode(parent.children[childIndex]);

    if (child.keyCount >= _minKeys()) {
      return;
    }

    if (parent.pageId == _rootPage && parent.keyCount == 0) {
      if (parent.children.isNotEmpty) {
        _rootPage = parent.children[0];
      }
      return;
    }

    await _fixUnderflow(child, parent, childIndex);
  }

  /// 修复下溢节点
  Future<void> _fixUnderflow(
    BTreeNode node,
    BTreeNode parent,
    int childIndex,
  ) async {
    // 尝试从左兄弟借键
    if (childIndex > 0) {
      final leftSibling = await _readNode(parent.children[childIndex - 1]);
      if (leftSibling.keyCount > _minKeys()) {
        await _borrowFromLeft(node, leftSibling, parent, childIndex);
        return;
      }
    }

    // 尝试从右兄弟借键
    if (childIndex < parent.children.length - 1) {
      final rightSibling = await _readNode(parent.children[childIndex + 1]);
      if (rightSibling.keyCount > _minKeys()) {
        await _borrowFromRight(node, rightSibling, parent, childIndex);
        return;
      }
    }

    // 需要合并
    if (childIndex > 0) {
      final leftSibling = await _readNode(parent.children[childIndex - 1]);
      await _mergeNodes(leftSibling, node, parent, childIndex - 1);
    } else if (childIndex < parent.children.length - 1) {
      final rightSibling = await _readNode(parent.children[childIndex + 1]);
      await _mergeNodes(node, rightSibling, parent, childIndex);
    }
  }

  /// 从左兄弟借键
  Future<void> _borrowFromLeft(
    BTreeNode node,
    BTreeNode leftSibling,
    BTreeNode parent,
    int childIndex,
  ) async {
    if (node.isLeaf) {
      final borrowedKey = leftSibling.keys.removeLast();
      final borrowedVal = leftSibling.values.removeLast();
      leftSibling.keyCount--;

      node.keys.insert(0, borrowedKey);
      node.values.insert(0, borrowedVal);
      node.keyCount++;

      parent.keys[childIndex - 1] = Uint8List.fromList(node.keys[0]);
    } else {
      final separatorKey = Uint8List.fromList(parent.keys[childIndex - 1]);
      final borrowedChild = leftSibling.children.removeLast();

      final newParentKey = leftSibling.keys.removeLast();
      leftSibling.keyCount--;

      node.keys.insert(0, separatorKey);
      node.children.insert(0, borrowedChild);
      node.keyCount++;

      parent.keys[childIndex - 1] = newParentKey;
    }

    await _writeNode(leftSibling);
    await _writeNode(node);
    await _writeNode(parent);
  }

  /// 从右兄弟借键
  Future<void> _borrowFromRight(
    BTreeNode node,
    BTreeNode rightSibling,
    BTreeNode parent,
    int childIndex,
  ) async {
    if (node.isLeaf) {
      final borrowedKey = rightSibling.keys.removeAt(0);
      final borrowedVal = rightSibling.values.removeAt(0);
      rightSibling.keyCount--;

      node.keys.add(borrowedKey);
      node.values.add(borrowedVal);
      node.keyCount++;

      parent.keys[childIndex] = Uint8List.fromList(rightSibling.keys[0]);
    } else {
      final separatorKey = Uint8List.fromList(parent.keys[childIndex]);
      final borrowedChild = rightSibling.children.removeAt(0);

      final newParentKey = rightSibling.keys.removeAt(0);
      rightSibling.keyCount--;

      node.keys.add(separatorKey);
      node.children.add(borrowedChild);
      node.keyCount++;

      parent.keys[childIndex] = newParentKey;
    }

    await _writeNode(rightSibling);
    await _writeNode(node);
    await _writeNode(parent);
  }

  /// 合并两个节点
  Future<void> _mergeNodes(
    BTreeNode left,
    BTreeNode right,
    BTreeNode parent,
    int separatorIndex,
  ) async {
    final nodesToWrite = <BTreeNode>[];

    if (left.isLeaf) {
      left.keys.addAll(right.keys);
      left.values.addAll(right.values);
      left.keyCount = left.keys.length;

      left.next = right.next;
      if (right.next != 0) {
        final nextNode = await _readNode(right.next);
        nextNode.prev = left.pageId;
        nodesToWrite.add(nextNode);
      }
    } else {
      final separatorKey = Uint8List.fromList(parent.keys[separatorIndex]);
      left.keys.add(separatorKey);
      left.keys.addAll(right.keys);
      left.children.addAll(right.children);
      left.keyCount = left.keys.length;
    }

    parent.keys.removeAt(separatorIndex);
    parent.children.removeAt(separatorIndex + 1);
    parent.keyCount--;

    if (parent.pageId == _rootPage && parent.keyCount == 0) {
      _rootPage = left.pageId;
    }

    nodesToWrite.addAll([left, parent]);

    for (final node in nodesToWrite) {
      await _writeNode(node);
    }

    await pager.freePage(right.pageId);
  }

  /// 最小键数
  int _minKeys() => (btreeOrder - 1) ~/ 2;

  /// 读取节点
  Future<BTreeNode> _readNode(PageId pageId) async {
    final page = await pager.readPage(pageId);
    final data = page.data;

    if (data.length < 10) {
      throw StateError('invalid btree node page');
    }

    final node = BTreeNode(pageId: pageId, isLeaf: data[0] != 0);

    int pos = 0;

    // IsLeaf
    node.isLeaf = data[pos] != 0;
    pos++;

    // KeyCount
    final bd = ByteData.view(data.buffer);
    node.keyCount = bd.getUint16(pos, Endian.little);
    pos += 2;

    // Next
    node.next = bd.getUint32(pos, Endian.little);
    pos += 4;

    // Prev
    node.prev = bd.getUint32(pos, Endian.little);
    pos += 4;

    // 读取键
    for (int i = 0; i < node.keyCount; i++) {
      if (pos + 2 > data.length) break;
      final keyLen = bd.getUint16(pos, Endian.little);
      pos += 2;
      if (pos + keyLen > data.length) break;
      node.keys.add(Uint8List.fromList(data.sublist(pos, pos + keyLen)));
      pos += keyLen;
    }

    if (node.isLeaf) {
      // 读取值
      for (int i = 0; i < node.keyCount; i++) {
        if (pos + 2 > data.length) break;
        final valLen = bd.getUint16(pos, Endian.little);
        pos += 2;
        if (pos + valLen > data.length) break;
        node.values.add(Uint8List.fromList(data.sublist(pos, pos + valLen)));
        pos += valLen;
      }
    } else {
      // 读取子节点指针
      final childCount = node.keyCount + 1;
      for (int i = 0; i < childCount; i++) {
        if (pos + 4 > data.length) break;
        node.children.add(bd.getUint32(pos, Endian.little));
        pos += 4;
      }
    }

    // 一致性验证
    if (node.keys.length != node.keyCount) {
      throw StateError(
          'corrupted node $pageId: key count mismatch (header=${node.keyCount}, actual=${node.keys.length})');
    }

    if (node.isLeaf && node.values.length != node.keyCount) {
      throw StateError(
          'corrupted leaf node $pageId: value count mismatch');
    }

    if (!node.isLeaf && node.children.length != node.keyCount + 1) {
      throw StateError(
          'corrupted internal node $pageId: child count mismatch');
    }

    return node;
  }

  /// 写入节点
  Future<void> _writeNode(BTreeNode node) async {
    final data = Uint8List(maxPageData);
    final bd = ByteData.view(data.buffer);
    int pos = 0;

    // IsLeaf
    data[pos] = node.isLeaf ? 1 : 0;
    pos++;

    // KeyCount
    bd.setUint16(pos, node.keyCount, Endian.little);
    pos += 2;

    // Next
    bd.setUint32(pos, node.next, Endian.little);
    pos += 4;

    // Prev
    bd.setUint32(pos, node.prev, Endian.little);
    pos += 4;

    // 写入键
    for (final key in node.keys) {
      if (pos + 2 + key.length > data.length) {
        throw StateError('btree node ${node.pageId} too large for page');
      }
      bd.setUint16(pos, key.length, Endian.little);
      pos += 2;
      data.setRange(pos, pos + key.length, key);
      pos += key.length;
    }

    if (node.isLeaf) {
      // 写入值
      for (final val in node.values) {
        if (pos + 2 + val.length > data.length) {
          throw StateError('btree node ${node.pageId} too large for page');
        }
        bd.setUint16(pos, val.length, Endian.little);
        pos += 2;
        data.setRange(pos, pos + val.length, val);
        pos += val.length;
      }
    } else {
      // 写入子节点指针
      for (final child in node.children) {
        if (pos + 4 > data.length) {
          throw StateError('btree node ${node.pageId} too large for page');
        }
        bd.setUint32(pos, child, Endian.little);
        pos += 4;
      }
    }

    final page = await pager.readPage(node.pageId);
    page.setData(data);
    pager.markDirty(node.pageId);
  }

  /// 比较两个键
  int _compareKeys(Uint8List a, Uint8List b) {
    final minLen = a.length < b.length ? a.length : b.length;
    for (int i = 0; i < minLen; i++) {
      if (a[i] != b[i]) {
        return a[i] - b[i];
      }
    }
    return a.length - b.length;
  }

  /// 获取 B+Tree 中的键总数
  Future<int> count() async {
    int count = 0;

    var node = await _readNode(_rootPage);
    while (!node.isLeaf) {
      if (node.children.isEmpty) break;
      node = await _readNode(node.children[0]);
    }

    while (true) {
      count += node.keyCount;
      if (node.next == 0) break;
      node = await _readNode(node.next);
    }

    return count;
  }

  /// 获取所有键
  Future<List<Uint8List>> getAllKeys() async {
    final keys = <Uint8List>[];

    var node = await _readNode(_rootPage);
    while (!node.isLeaf) {
      if (node.children.isEmpty) break;
      node = await _readNode(node.children[0]);
    }

    while (true) {
      keys.addAll(node.keys);
      if (node.next == 0) break;
      node = await _readNode(node.next);
    }

    return keys;
  }

  /// 获取树高度
  Future<int> height() async {
    int h = 0;
    var node = await _readNode(_rootPage);

    while (true) {
      h++;
      if (node.isLeaf) break;
      if (node.children.isEmpty) break;
      node = await _readNode(node.children[0]);
    }

    return h;
  }

  /// 验证 B+Tree 完整性
  Future<void> verify() async {
    await _verifyNode(_rootPage, null, null, 0);
    await _verifyLeafChain();
  }

  /// 递归验证节点
  Future<void> _verifyNode(
    PageId pageId,
    Uint8List? minKey,
    Uint8List? maxKey,
    int depth,
  ) async {
    final node = await _readNode(pageId);

    // 验证键数量
    if (pageId != _rootPage) {
      if (node.keyCount < _minKeys()) {
        throw StateError(
            'node $pageId has too few keys: ${node.keyCount} < ${_minKeys()}');
      }
    }
    if (node.keyCount > btreeOrder - 1) {
      throw StateError(
          'node $pageId has too many keys: ${node.keyCount} > ${btreeOrder - 1}');
    }

    // 验证键顺序
    for (int i = 1; i < node.keyCount; i++) {
      if (_compareKeys(node.keys[i - 1], node.keys[i]) >= 0) {
        throw StateError('node $pageId keys not in order at index $i');
      }
    }

    // 验证键范围
    if (minKey != null && node.keyCount > 0) {
      if (_compareKeys(node.keys[0], minKey) < 0) {
        throw StateError('node $pageId first key less than min bound');
      }
    }
    if (maxKey != null && node.keyCount > 0) {
      if (_compareKeys(node.keys[node.keyCount - 1], maxKey) >= 0) {
        throw StateError('node $pageId last key exceeds max bound');
      }
    }

    // 递归验证子节点
    if (!node.isLeaf) {
      if (node.children.length != node.keyCount + 1) {
        throw StateError(
            'node $pageId has incorrect child count: ${node.children.length} vs ${node.keyCount + 1}');
      }

      for (int i = 0; i < node.children.length; i++) {
        Uint8List? childMin;
        Uint8List? childMax;
        if (i > 0) {
          childMin = node.keys[i - 1];
        } else {
          childMin = minKey;
        }
        if (i < node.keyCount) {
          childMax = node.keys[i];
        } else {
          childMax = maxKey;
        }

        await _verifyNode(node.children[i], childMin, childMax, depth + 1);
      }
    } else {
      // 验证值数量
      if (node.values.length != node.keyCount) {
        throw StateError(
            'leaf node $pageId has mismatched values count: ${node.values.length} vs ${node.keyCount}');
      }
    }
  }

  /// 验证叶子节点链表
  Future<void> _verifyLeafChain() async {
    var node = await _readNode(_rootPage);

    while (!node.isLeaf) {
      if (node.children.isEmpty) {
        throw StateError('internal node ${node.pageId} has no children');
      }
      node = await _readNode(node.children[0]);
    }

    BTreeNode? prevNode;
    Uint8List? lastKey;
    int count = 0;

    while (true) {
      count++;
      if (count > 1000000) {
        throw StateError('leaf chain too long, possible cycle');
      }

      // 验证 Prev 指针
      if (prevNode != null) {
        if (node.prev != prevNode.pageId) {
          throw StateError(
              'leaf node ${node.pageId} has incorrect Prev pointer: ${node.prev} vs ${prevNode.pageId}');
        }
      } else {
        if (node.prev != 0) {
          throw StateError(
              'first leaf node ${node.pageId} has non-zero Prev: ${node.prev}');
        }
      }

      // 验证键顺序
      if (lastKey != null && node.keyCount > 0) {
        if (_compareKeys(lastKey, node.keys[0]) >= 0) {
          throw StateError('leaf chain order violation at node ${node.pageId}');
        }
      }

      if (node.keyCount > 0) {
        lastKey = node.keys[node.keyCount - 1];
      }

      prevNode = node;
      if (node.next == 0) break;
      node = await _readNode(node.next);
    }
  }
}
