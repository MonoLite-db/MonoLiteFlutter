// Created by Yanjunhui

import 'dart:convert';
import 'dart:typed_data';

import 'types.dart';

/// BSON 编解码器
class BsonCodec {
  /// 序列化文档
  static Uint8List encode(BsonDocument doc) {
    final writer = _BsonWriter();
    writer.writeDocument(doc);
    return writer.toBytes();
  }

  /// 反序列化文档
  static BsonDocument decode(Uint8List data) {
    final reader = _BsonReader(data);
    return reader.readDocument();
  }

  /// 序列化任意值
  static Uint8List encodeValue(dynamic value) {
    final doc = BsonDocument();
    doc['v'] = value;
    return encode(doc);
  }

  /// 反序列化任意值
  static dynamic decodeValue(Uint8List data) {
    final doc = decode(data);
    return doc['v'];
  }
}

/// BSON 写入器
class _BsonWriter {
  final BytesBuilder _buffer = BytesBuilder();

  void writeDocument(BsonDocument doc) {
    // 先写入到临时缓冲区以计算长度
    final contentWriter = _BsonWriter();
    for (final entry in doc.entries) {
      contentWriter._writeElement(entry.key, entry.value);
    }
    contentWriter._writeByte(0); // 终止符

    final content = contentWriter.toBytes();
    _writeInt32(content.length + 4); // 总长度包括长度字段
    _buffer.add(content);
  }

  void _writeElement(String name, dynamic value) {
    if (value == null) {
      _writeByte(BsonType.null_);
      _writeCString(name);
    } else if (value is double) {
      _writeByte(BsonType.double_);
      _writeCString(name);
      _writeDouble(value);
    } else if (value is String) {
      _writeByte(BsonType.string);
      _writeCString(name);
      _writeString(value);
    } else if (value is BsonDocument) {
      _writeByte(BsonType.document);
      _writeCString(name);
      writeDocument(value);
    } else if (value is Map<String, dynamic>) {
      _writeByte(BsonType.document);
      _writeCString(name);
      writeDocument(BsonDocument.from(value));
    } else if (value is BsonArray) {
      _writeByte(BsonType.array);
      _writeCString(name);
      _writeArray(value);
    } else if (value is List) {
      _writeByte(BsonType.array);
      _writeCString(name);
      _writeArray(BsonArray.from(value));
    } else if (value is BsonBinary) {
      _writeByte(BsonType.binary);
      _writeCString(name);
      _writeBinary(value);
    } else if (value is Uint8List) {
      _writeByte(BsonType.binary);
      _writeCString(name);
      _writeBinary(BsonBinary.fromBytes(value));
    } else if (value is ObjectId) {
      _writeByte(BsonType.objectId);
      _writeCString(name);
      _buffer.add(value.bytes);
    } else if (value is bool) {
      _writeByte(BsonType.boolean);
      _writeCString(name);
      _writeByte(value ? 1 : 0);
    } else if (value is BsonDateTime) {
      _writeByte(BsonType.datetime);
      _writeCString(name);
      _writeInt64(value.millisecondsSinceEpoch);
    } else if (value is DateTime) {
      _writeByte(BsonType.datetime);
      _writeCString(name);
      _writeInt64(value.millisecondsSinceEpoch);
    } else if (value is BsonRegex) {
      _writeByte(BsonType.regex);
      _writeCString(name);
      _writeCString(value.pattern);
      _writeCString(value.options);
    } else if (value is int) {
      if (value >= -2147483648 && value <= 2147483647) {
        _writeByte(BsonType.int32);
        _writeCString(name);
        _writeInt32(value);
      } else {
        _writeByte(BsonType.int64);
        _writeCString(name);
        _writeInt64(value);
      }
    } else if (value is BsonTimestamp) {
      _writeByte(BsonType.timestamp);
      _writeCString(name);
      _writeInt32(value.i);
      _writeInt32(value.t);
    } else if (value is BsonMinKey) {
      _writeByte(BsonType.minKey);
      _writeCString(name);
    } else if (value is BsonMaxKey) {
      _writeByte(BsonType.maxKey);
      _writeCString(name);
    } else {
      throw ArgumentError('Unsupported BSON type: ${value.runtimeType}');
    }
  }

  void _writeArray(BsonArray array) {
    final doc = BsonDocument();
    for (int i = 0; i < array.length; i++) {
      doc[i.toString()] = array[i];
    }
    writeDocument(doc);
  }

  void _writeBinary(BsonBinary binary) {
    _writeInt32(binary.data.length);
    _writeByte(binary.subtype);
    _buffer.add(binary.data);
  }

  void _writeByte(int byte) {
    _buffer.addByte(byte);
  }

  void _writeInt32(int value) {
    final bytes = Uint8List(4);
    final bd = ByteData.view(bytes.buffer);
    bd.setInt32(0, value, Endian.little);
    _buffer.add(bytes);
  }

  void _writeInt64(int value) {
    final bytes = Uint8List(8);
    final bd = ByteData.view(bytes.buffer);
    bd.setInt64(0, value, Endian.little);
    _buffer.add(bytes);
  }

  void _writeDouble(double value) {
    final bytes = Uint8List(8);
    final bd = ByteData.view(bytes.buffer);
    bd.setFloat64(0, value, Endian.little);
    _buffer.add(bytes);
  }

  void _writeCString(String str) {
    _buffer.add(utf8.encode(str));
    _buffer.addByte(0);
  }

  void _writeString(String str) {
    final bytes = utf8.encode(str);
    _writeInt32(bytes.length + 1);
    _buffer.add(bytes);
    _buffer.addByte(0);
  }

  Uint8List toBytes() => _buffer.toBytes();
}

/// BSON 读取器
class _BsonReader {
  final Uint8List _data;
  int _pos = 0;

  _BsonReader(this._data);

  BsonDocument readDocument() {
    final length = _readInt32();
    final endPos = _pos + length - 4;

    final doc = BsonDocument();

    while (_pos < endPos - 1) {
      final type = _readByte();
      if (type == 0) break;

      final name = _readCString();
      final value = _readValue(type);
      doc[name] = value;
    }

    // 跳过终止符
    if (_pos < _data.length && _data[_pos] == 0) {
      _pos++;
    }

    return doc;
  }

  dynamic _readValue(int type) {
    switch (type) {
      case BsonType.double_:
        return _readDouble();
      case BsonType.string:
        return _readString();
      case BsonType.document:
        return readDocument();
      case BsonType.array:
        return _readArray();
      case BsonType.binary:
        return _readBinary();
      case BsonType.objectId:
        return _readObjectId();
      case BsonType.boolean:
        return _readByte() != 0;
      case BsonType.datetime:
        return BsonDateTime(_readInt64());
      case BsonType.null_:
        return null;
      case BsonType.regex:
        final pattern = _readCString();
        final options = _readCString();
        return BsonRegex(pattern, options);
      case BsonType.int32:
        return _readInt32();
      case BsonType.timestamp:
        final i = _readInt32();
        final t = _readInt32();
        return BsonTimestamp(t, i);
      case BsonType.int64:
        return _readInt64();
      case BsonType.minKey:
        return const BsonMinKey();
      case BsonType.maxKey:
        return const BsonMaxKey();
      default:
        throw StateError('Unknown BSON type: $type');
    }
  }

  BsonArray _readArray() {
    final doc = readDocument();
    final array = BsonArray();
    // 按数字顺序读取元素
    final keys = doc.keys.toList()
      ..sort((a, b) => int.parse(a).compareTo(int.parse(b)));
    for (final key in keys) {
      array.add(doc[key]);
    }
    return array;
  }

  BsonBinary _readBinary() {
    final length = _readInt32();
    final subtype = _readByte();
    final data = _readBytes(length);
    return BsonBinary(subtype, data);
  }

  ObjectId _readObjectId() {
    return ObjectId.fromBytes(_readBytes(12));
  }

  int _readByte() {
    return _data[_pos++];
  }

  Uint8List _readBytes(int length) {
    final bytes = Uint8List.fromList(_data.sublist(_pos, _pos + length));
    _pos += length;
    return bytes;
  }

  int _readInt32() {
    final bd = ByteData.view(_data.buffer, _pos, 4);
    _pos += 4;
    return bd.getInt32(0, Endian.little);
  }

  int _readInt64() {
    final bd = ByteData.view(_data.buffer, _pos, 8);
    _pos += 8;
    return bd.getInt64(0, Endian.little);
  }

  double _readDouble() {
    final bd = ByteData.view(_data.buffer, _pos, 8);
    _pos += 8;
    return bd.getFloat64(0, Endian.little);
  }

  String _readCString() {
    final start = _pos;
    while (_data[_pos] != 0) {
      _pos++;
    }
    final str = utf8.decode(_data.sublist(start, _pos));
    _pos++; // 跳过终止符
    return str;
  }

  String _readString() {
    final length = _readInt32();
    final str = utf8.decode(_data.sublist(_pos, _pos + length - 1));
    _pos += length; // 包括终止符
    return str;
  }
}

/// BSON 比较器
class BsonCompare {
  /// 比较两个 BSON 值
  static int compare(dynamic a, dynamic b) {
    // MongoDB 类型排序顺序
    final typeOrderA = _typeOrder(a);
    final typeOrderB = _typeOrder(b);

    if (typeOrderA != typeOrderB) {
      return typeOrderA.compareTo(typeOrderB);
    }

    // 同类型比较
    if (a == null && b == null) return 0;
    if (a == null) return -1;
    if (b == null) return 1;

    if (a is num && b is num) {
      return a.compareTo(b);
    }

    if (a is String && b is String) {
      return a.compareTo(b);
    }

    if (a is ObjectId && b is ObjectId) {
      return a.compareTo(b);
    }

    if (a is BsonDateTime && b is BsonDateTime) {
      return a.compareTo(b);
    }

    if (a is DateTime && b is DateTime) {
      return a.compareTo(b);
    }

    if (a is bool && b is bool) {
      return (a ? 1 : 0).compareTo(b ? 1 : 0);
    }

    if (a is BsonTimestamp && b is BsonTimestamp) {
      return a.compareTo(b);
    }

    // 默认相等
    return 0;
  }

  static int _typeOrder(dynamic value) {
    if (value == null) return 1;
    if (value is BsonMinKey) return 0;
    if (value is num) return 2;
    if (value is String) return 3;
    if (value is BsonDocument || value is Map) return 4;
    if (value is BsonArray || value is List) return 5;
    if (value is BsonBinary || value is Uint8List) return 6;
    if (value is ObjectId) return 7;
    if (value is bool) return 8;
    if (value is BsonDateTime || value is DateTime) return 9;
    if (value is BsonTimestamp) return 10;
    if (value is BsonRegex) return 11;
    if (value is BsonMaxKey) return 127;
    return 128;
  }
}
