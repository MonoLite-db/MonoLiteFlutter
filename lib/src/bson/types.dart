// Created by Yanjunhui

import 'dart:typed_data';

/// BSON 类型常量
class BsonType {
  static const int double_ = 0x01;
  static const int string = 0x02;
  static const int document = 0x03;
  static const int array = 0x04;
  static const int binary = 0x05;
  static const int undefined = 0x06; // deprecated
  static const int objectId = 0x07;
  static const int boolean = 0x08;
  static const int datetime = 0x09;
  static const int null_ = 0x0A;
  static const int regex = 0x0B;
  static const int dbPointer = 0x0C; // deprecated
  static const int javascript = 0x0D;
  static const int symbol = 0x0E; // deprecated
  static const int javascriptWithScope = 0x0F;
  static const int int32 = 0x10;
  static const int timestamp = 0x11;
  static const int int64 = 0x12;
  static const int decimal128 = 0x13;
  static const int minKey = 0xFF;
  static const int maxKey = 0x7F;
}

/// ObjectId - 12 字节唯一标识符
class ObjectId implements Comparable<ObjectId> {
  final Uint8List _bytes;

  ObjectId._(this._bytes) {
    if (_bytes.length != 12) {
      throw ArgumentError('ObjectId must be 12 bytes');
    }
  }

  /// 从字节创建
  factory ObjectId.fromBytes(Uint8List bytes) {
    return ObjectId._(Uint8List.fromList(bytes));
  }

  /// 从十六进制字符串创建
  factory ObjectId.fromHex(String hex) {
    if (hex.length != 24) {
      throw ArgumentError('ObjectId hex string must be 24 characters');
    }
    final bytes = Uint8List(12);
    for (int i = 0; i < 12; i++) {
      bytes[i] = int.parse(hex.substring(i * 2, i * 2 + 2), radix: 16);
    }
    return ObjectId._(bytes);
  }

  /// 生成新的 ObjectId
  factory ObjectId.generate() {
    final bytes = Uint8List(12);
    final now = DateTime.now();

    // 4 字节时间戳（秒）
    final timestamp = now.millisecondsSinceEpoch ~/ 1000;
    bytes[0] = (timestamp >> 24) & 0xFF;
    bytes[1] = (timestamp >> 16) & 0xFF;
    bytes[2] = (timestamp >> 8) & 0xFF;
    bytes[3] = timestamp & 0xFF;

    // 5 字节随机值
    final random = DateTime.now().microsecondsSinceEpoch;
    bytes[4] = (random >> 32) & 0xFF;
    bytes[5] = (random >> 24) & 0xFF;
    bytes[6] = (random >> 16) & 0xFF;
    bytes[7] = (random >> 8) & 0xFF;
    bytes[8] = random & 0xFF;

    // 3 字节计数器
    _counter++;
    bytes[9] = (_counter >> 16) & 0xFF;
    bytes[10] = (_counter >> 8) & 0xFF;
    bytes[11] = _counter & 0xFF;

    return ObjectId._(bytes);
  }

  static int _counter = 0;

  /// 获取字节
  Uint8List get bytes => Uint8List.fromList(_bytes);

  /// 转换为十六进制字符串
  String toHex() {
    return _bytes
        .map((b) => b.toRadixString(16).padLeft(2, '0'))
        .join();
  }

  /// 获取时间戳
  DateTime get timestamp {
    final seconds =
        (_bytes[0] << 24) | (_bytes[1] << 16) | (_bytes[2] << 8) | _bytes[3];
    return DateTime.fromMillisecondsSinceEpoch(seconds * 1000);
  }

  @override
  String toString() => 'ObjectId("${toHex()}")';

  @override
  bool operator ==(Object other) {
    if (other is! ObjectId) return false;
    for (int i = 0; i < 12; i++) {
      if (_bytes[i] != other._bytes[i]) return false;
    }
    return true;
  }

  @override
  int get hashCode {
    int hash = 0;
    for (final b in _bytes) {
      hash = 31 * hash + b;
    }
    return hash;
  }

  @override
  int compareTo(ObjectId other) {
    for (int i = 0; i < 12; i++) {
      if (_bytes[i] != other._bytes[i]) {
        return _bytes[i] - other._bytes[i];
      }
    }
    return 0;
  }
}

/// BSON DateTime
class BsonDateTime implements Comparable<BsonDateTime> {
  final int millisecondsSinceEpoch;

  BsonDateTime(this.millisecondsSinceEpoch);

  factory BsonDateTime.fromDateTime(DateTime dt) {
    return BsonDateTime(dt.millisecondsSinceEpoch);
  }

  factory BsonDateTime.now() {
    return BsonDateTime(DateTime.now().millisecondsSinceEpoch);
  }

  DateTime toDateTime() {
    return DateTime.fromMillisecondsSinceEpoch(millisecondsSinceEpoch);
  }

  @override
  String toString() => 'BsonDateTime(${toDateTime().toIso8601String()})';

  @override
  bool operator ==(Object other) =>
      other is BsonDateTime &&
      other.millisecondsSinceEpoch == millisecondsSinceEpoch;

  @override
  int get hashCode => millisecondsSinceEpoch.hashCode;

  @override
  int compareTo(BsonDateTime other) =>
      millisecondsSinceEpoch.compareTo(other.millisecondsSinceEpoch);
}

/// BSON Timestamp
class BsonTimestamp implements Comparable<BsonTimestamp> {
  final int t; // 秒
  final int i; // 增量

  BsonTimestamp(this.t, this.i);

  @override
  String toString() => 'Timestamp($t, $i)';

  @override
  bool operator ==(Object other) =>
      other is BsonTimestamp && other.t == t && other.i == i;

  @override
  int get hashCode => Object.hash(t, i);

  @override
  int compareTo(BsonTimestamp other) {
    if (t != other.t) return t.compareTo(other.t);
    return i.compareTo(other.i);
  }
}

/// BSON Binary 子类型
class BinarySubtype {
  static const int generic = 0x00;
  static const int function = 0x01;
  static const int binaryOld = 0x02;
  static const int uuidOld = 0x03;
  static const int uuid = 0x04;
  static const int md5 = 0x05;
  static const int encrypted = 0x06;
  static const int userDefined = 0x80;
}

/// BSON Binary
class BsonBinary {
  final int subtype;
  final Uint8List data;

  BsonBinary(this.subtype, this.data);

  factory BsonBinary.fromBytes(Uint8List bytes) {
    return BsonBinary(BinarySubtype.generic, bytes);
  }

  @override
  String toString() => 'Binary(subtype: $subtype, length: ${data.length})';

  @override
  bool operator ==(Object other) {
    if (other is! BsonBinary) return false;
    if (other.subtype != subtype) return false;
    if (other.data.length != data.length) return false;
    for (int i = 0; i < data.length; i++) {
      if (other.data[i] != data[i]) return false;
    }
    return true;
  }

  @override
  int get hashCode => Object.hash(subtype, Object.hashAll(data));
}

/// BSON Regex
class BsonRegex {
  final String pattern;
  final String options;

  BsonRegex(this.pattern, [this.options = '']);

  @override
  String toString() => 'Regex(/$pattern/$options)';

  @override
  bool operator ==(Object other) =>
      other is BsonRegex && other.pattern == pattern && other.options == options;

  @override
  int get hashCode => Object.hash(pattern, options);
}

/// BSON MinKey
class BsonMinKey {
  const BsonMinKey();

  @override
  String toString() => 'MinKey';

  @override
  bool operator ==(Object other) => other is BsonMinKey;

  @override
  int get hashCode => 0;
}

/// BSON MaxKey
class BsonMaxKey {
  const BsonMaxKey();

  @override
  String toString() => 'MaxKey';

  @override
  bool operator ==(Object other) => other is BsonMaxKey;

  @override
  int get hashCode => 0x7FFFFFFF;
}

/// BSON 文档
class BsonDocument {
  final Map<String, dynamic> _fields = {};

  BsonDocument();

  factory BsonDocument.from(Map<String, dynamic> map) {
    final doc = BsonDocument();
    map.forEach((key, value) {
      doc[key] = value;
    });
    return doc;
  }

  /// 获取字段
  dynamic operator [](String key) => _fields[key];

  /// 设置字段
  void operator []=(String key, dynamic value) {
    _fields[key] = value;
  }

  /// 包含字段
  bool containsKey(String key) => _fields.containsKey(key);

  /// 获取所有键
  Iterable<String> get keys => _fields.keys;

  /// 获取所有值
  Iterable<dynamic> get values => _fields.values;

  /// 获取条目
  Iterable<MapEntry<String, dynamic>> get entries => _fields.entries;

  /// 字段数
  int get length => _fields.length;

  /// 是否为空
  bool get isEmpty => _fields.isEmpty;

  /// 移除字段
  dynamic remove(String key) => _fields.remove(key);

  /// 清空
  void clear() => _fields.clear();

  /// 转换为 Map
  Map<String, dynamic> toMap() => Map.from(_fields);

  @override
  String toString() {
    final sb = StringBuffer('{');
    bool first = true;
    for (final entry in _fields.entries) {
      if (!first) sb.write(', ');
      first = false;
      sb.write('"${entry.key}": ${_valueToString(entry.value)}');
    }
    sb.write('}');
    return sb.toString();
  }

  String _valueToString(dynamic value) {
    if (value == null) return 'null';
    if (value is String) return '"$value"';
    if (value is List) {
      return '[${value.map(_valueToString).join(', ')}]';
    }
    if (value is BsonDocument) return value.toString();
    return value.toString();
  }

  @override
  bool operator ==(Object other) {
    if (other is! BsonDocument) return false;
    if (other._fields.length != _fields.length) return false;
    for (final key in _fields.keys) {
      if (!other._fields.containsKey(key)) return false;
      if (_fields[key] != other._fields[key]) return false;
    }
    return true;
  }

  @override
  int get hashCode => Object.hashAll(_fields.values);
}

/// BSON 数组
class BsonArray extends Iterable<dynamic> {
  final List<dynamic> _elements = [];

  BsonArray();

  factory BsonArray.from(List<dynamic> list) {
    final arr = BsonArray();
    arr._elements.addAll(list);
    return arr;
  }

  /// 获取元素
  dynamic operator [](int index) => _elements[index];

  /// 设置元素
  void operator []=(int index, dynamic value) {
    _elements[index] = value;
  }

  /// 添加元素
  void add(dynamic element) => _elements.add(element);

  /// 添加多个元素
  void addAll(Iterable<dynamic> elements) => _elements.addAll(elements);

  /// 移除元素
  bool remove(dynamic element) => _elements.remove(element);

  /// 移除指定位置元素
  dynamic removeAt(int index) => _elements.removeAt(index);

  /// 清空
  void clear() => _elements.clear();

  @override
  int get length => _elements.length;

  @override
  bool get isEmpty => _elements.isEmpty;

  @override
  Iterator<dynamic> get iterator => _elements.iterator;

  /// 转换为 List
  @override
  List<dynamic> toList({bool growable = true}) => List.from(_elements, growable: growable);

  @override
  String toString() {
    return '[${_elements.map((e) => e.toString()).join(', ')}]';
  }

  @override
  bool operator ==(Object other) {
    if (other is! BsonArray) return false;
    if (other._elements.length != _elements.length) return false;
    for (int i = 0; i < _elements.length; i++) {
      if (_elements[i] != other._elements[i]) return false;
    }
    return true;
  }

  @override
  int get hashCode => Object.hashAll(_elements);
}
