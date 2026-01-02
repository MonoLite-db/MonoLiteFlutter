// Created by Yanjunhui

import '../bson/bson.dart';

/// 检查文档是否匹配过滤器
bool matchesFilter(BsonDocument doc, BsonDocument? filter) {
  if (filter == null || filter.isEmpty) {
    return true;
  }

  final matcher = FilterMatcher(filter);
  return matcher.match(doc);
}

/// 过滤器匹配器
class FilterMatcher {
  final BsonDocument filter;

  FilterMatcher(this.filter);

  /// 检查文档是否匹配
  bool match(BsonDocument doc) {
    for (final elem in filter.entries) {
      if (!_matchElement(doc, elem.key, elem.value)) {
        return false;
      }
    }
    return true;
  }

  /// 匹配单个元素
  bool _matchElement(BsonDocument doc, String key, dynamic value) {
    // 处理逻辑运算符
    switch (key) {
      case '\$and':
        return _matchAnd(doc, value);
      case '\$or':
        return _matchOr(doc, value);
      case '\$not':
        return _matchNot(doc, value);
      case '\$nor':
        return _matchNor(doc, value);
    }

    // 获取文档字段值
    final docVal = getDocField(doc, key);

    // 如果 value 是 BsonDocument，可能包含比较运算符
    if (value is BsonDocument) {
      return _matchOperators(docVal, value);
    }

    // 直接相等比较
    return BsonCompare.compare(docVal, value) == 0;
  }

  /// 匹配比较运算符
  bool _matchOperators(dynamic docVal, BsonDocument operators) {
    for (final op in operators.entries) {
      if (!_matchOperator(docVal, op.key, op.value)) {
        return false;
      }
    }
    return true;
  }

  /// 匹配单个运算符
  bool _matchOperator(dynamic docVal, String operator, dynamic operand) {
    switch (operator) {
      case '\$eq':
        return BsonCompare.compare(docVal, operand) == 0;

      case '\$ne':
        return BsonCompare.compare(docVal, operand) != 0;

      case '\$gt':
        return BsonCompare.compare(docVal, operand) > 0;

      case '\$gte':
        return BsonCompare.compare(docVal, operand) >= 0;

      case '\$lt':
        return BsonCompare.compare(docVal, operand) < 0;

      case '\$lte':
        return BsonCompare.compare(docVal, operand) <= 0;

      case '\$in':
        return _matchIn(docVal, operand);

      case '\$nin':
        return !_matchIn(docVal, operand);

      case '\$exists':
        final exists = docVal != null;
        if (operand is bool) {
          return exists == operand;
        }
        return exists;

      case '\$type':
        return _matchType(docVal, operand);

      case '\$regex':
        return _matchRegex(docVal, operand);

      case '\$size':
        return _matchSize(docVal, operand);

      case '\$all':
        return _matchAll(docVal, operand);

      case '\$elemMatch':
        return _matchElemMatch(docVal, operand);

      case '\$mod':
        return _matchMod(docVal, operand);

      default:
        return false;
    }
  }

  /// 处理 $and
  bool _matchAnd(BsonDocument doc, dynamic value) {
    if (value is! BsonArray) return false;

    for (final item in value) {
      if (item is BsonDocument) {
        final subMatcher = FilterMatcher(item);
        if (!subMatcher.match(doc)) {
          return false;
        }
      }
    }
    return true;
  }

  /// 处理 $or
  bool _matchOr(BsonDocument doc, dynamic value) {
    if (value is! BsonArray) return false;

    for (final item in value) {
      if (item is BsonDocument) {
        final subMatcher = FilterMatcher(item);
        if (subMatcher.match(doc)) {
          return true;
        }
      }
    }
    return false;
  }

  /// 处理 $not
  bool _matchNot(BsonDocument doc, dynamic value) {
    if (value is BsonDocument) {
      final subMatcher = FilterMatcher(value);
      return !subMatcher.match(doc);
    }
    return true;
  }

  /// 处理 $nor
  bool _matchNor(BsonDocument doc, dynamic value) {
    return !_matchOr(doc, value);
  }

  /// 处理 $in
  bool _matchIn(dynamic docVal, dynamic operand) {
    if (operand is! BsonArray) return false;

    for (final item in operand) {
      if (BsonCompare.compare(docVal, item) == 0) {
        return true;
      }
    }
    return false;
  }

  /// 处理 $type
  bool _matchType(dynamic docVal, dynamic operand) {
    int expectedType;
    if (operand is int) {
      expectedType = operand;
    } else if (operand is String) {
      expectedType = _typeNameToNumber(operand);
    } else {
      return false;
    }

    final actualType = _getBsonType(docVal);
    return actualType == expectedType;
  }

  /// 处理 $regex
  bool _matchRegex(dynamic docVal, dynamic operand) {
    if (docVal is! String) return false;

    String pattern;
    if (operand is String) {
      pattern = operand;
    } else if (operand is BsonRegex) {
      pattern = operand.pattern;
    } else {
      return false;
    }

    try {
      final re = RegExp(pattern);
      return re.hasMatch(docVal);
    } catch (_) {
      return false;
    }
  }

  /// 处理 $size
  bool _matchSize(dynamic docVal, dynamic operand) {
    if (docVal is! BsonArray) return false;

    int expectedSize;
    if (operand is int) {
      expectedSize = operand;
    } else {
      return false;
    }

    return docVal.length == expectedSize;
  }

  /// 处理 $all
  bool _matchAll(dynamic docVal, dynamic operand) {
    if (docVal is! BsonArray) return false;
    if (operand is! BsonArray) return false;

    for (final req in operand) {
      var found = false;
      for (final item in docVal) {
        if (BsonCompare.compare(item, req) == 0) {
          found = true;
          break;
        }
      }
      if (!found) return false;
    }
    return true;
  }

  /// 处理 $elemMatch
  bool _matchElemMatch(dynamic docVal, dynamic operand) {
    if (docVal is! BsonArray) return false;
    if (operand is! BsonDocument) return false;

    final subMatcher = FilterMatcher(operand);
    for (final item in docVal) {
      if (item is BsonDocument) {
        if (subMatcher.match(item)) {
          return true;
        }
      }
    }
    return false;
  }

  /// 处理 $mod
  bool _matchMod(dynamic docVal, dynamic operand) {
    if (operand is! BsonArray || operand.length != 2) return false;

    final divisor = _toDouble(operand[0]);
    final remainder = _toDouble(operand[1]);
    if (divisor == 0) return false;

    final docNum = _toDouble(docVal);
    final result = docNum.toInt() % divisor.toInt();
    return result == remainder.toInt();
  }

  /// 类型名转数字
  int _typeNameToNumber(String name) {
    switch (name) {
      case 'double':
        return 1;
      case 'string':
        return 2;
      case 'object':
        return 3;
      case 'array':
        return 4;
      case 'binData':
        return 5;
      case 'objectId':
        return 7;
      case 'bool':
        return 8;
      case 'date':
        return 9;
      case 'null':
        return 10;
      case 'regex':
        return 11;
      case 'int':
        return 16;
      case 'long':
        return 18;
      default:
        return -1;
    }
  }

  /// 获取 BSON 类型号
  int _getBsonType(dynamic val) {
    if (val == null) return 10;
    if (val is double) return 1;
    if (val is String) return 2;
    if (val is BsonDocument) return 3;
    if (val is BsonArray) return 4;
    if (val is BsonBinary) return 5;
    if (val is ObjectId) return 7;
    if (val is bool) return 8;
    if (val is BsonDateTime || val is DateTime) return 9;
    if (val is BsonRegex) return 11;
    if (val is int && val >= -2147483648 && val <= 2147483647) return 16;
    if (val is int) return 18;
    return -1;
  }

  /// 转换为 double
  double _toDouble(dynamic v) {
    if (v is int) return v.toDouble();
    if (v is double) return v;
    return 0.0;
  }
}

/// 从文档中获取字段值（支持点号路径）
dynamic getDocField(BsonDocument doc, String path) {
  return _getNestedValue(doc, path);
}

/// 递归获取嵌套字段值
dynamic _getNestedValue(dynamic value, String path) {
  if (path.isEmpty) return value;

  // 分割路径
  String key;
  String rest;
  final dotIndex = path.indexOf('.');
  if (dotIndex == -1) {
    key = path;
    rest = '';
  } else {
    key = path.substring(0, dotIndex);
    rest = path.substring(dotIndex + 1);
  }

  if (value is BsonDocument) {
    final fieldVal = value[key];
    if (fieldVal == null) return null;
    if (rest.isEmpty) return fieldVal;
    return _getNestedValue(fieldVal, rest);
  }

  if (value is BsonArray) {
    // 尝试解析数字索引
    final index = int.tryParse(key);
    if (index != null && index >= 0 && index < value.length) {
      if (rest.isEmpty) return value[index];
      return _getNestedValue(value[index], rest);
    }

    // 对数组中的每个元素查找
    for (final item in value) {
      if (item is BsonDocument) {
        final result = _getNestedValue(item, path);
        if (result != null) return result;
      }
    }
    return null;
  }

  return null;
}
