// Created by Yanjunhui

import '../bson/bson.dart';

/// 应用更新操作符到文档
void applyUpdate(BsonDocument doc, BsonDocument update) {
  for (final elem in update.entries) {
    switch (elem.key) {
      case '\$set':
        if (elem.value is BsonDocument) {
          final setDoc = elem.value as BsonDocument;
          for (final setElem in setDoc.entries) {
            _setField(doc, setElem.key, setElem.value);
          }
        }
        break;

      case '\$unset':
        if (elem.value is BsonDocument) {
          final unsetDoc = elem.value as BsonDocument;
          for (final unsetElem in unsetDoc.entries) {
            _removeField(doc, unsetElem.key);
          }
        }
        break;

      case '\$inc':
        if (elem.value is BsonDocument) {
          final incDoc = elem.value as BsonDocument;
          for (final incElem in incDoc.entries) {
            _incrementField(doc, incElem.key, incElem.value);
          }
        }
        break;

      case '\$mul':
        if (elem.value is BsonDocument) {
          final mulDoc = elem.value as BsonDocument;
          for (final mulElem in mulDoc.entries) {
            _multiplyField(doc, mulElem.key, mulElem.value);
          }
        }
        break;

      case '\$min':
        if (elem.value is BsonDocument) {
          final minDoc = elem.value as BsonDocument;
          for (final minElem in minDoc.entries) {
            _updateFieldMin(doc, minElem.key, minElem.value);
          }
        }
        break;

      case '\$max':
        if (elem.value is BsonDocument) {
          final maxDoc = elem.value as BsonDocument;
          for (final maxElem in maxDoc.entries) {
            _updateFieldMax(doc, maxElem.key, maxElem.value);
          }
        }
        break;

      case '\$rename':
        if (elem.value is BsonDocument) {
          final renameDoc = elem.value as BsonDocument;
          for (final renameElem in renameDoc.entries) {
            if (renameElem.value is String) {
              _renameField(doc, renameElem.key, renameElem.value as String);
            }
          }
        }
        break;

      case '\$push':
        if (elem.value is BsonDocument) {
          final pushDoc = elem.value as BsonDocument;
          for (final pushElem in pushDoc.entries) {
            _pushToArray(doc, pushElem.key, pushElem.value);
          }
        }
        break;

      case '\$pop':
        if (elem.value is BsonDocument) {
          final popDoc = elem.value as BsonDocument;
          for (final popElem in popDoc.entries) {
            _popFromArray(doc, popElem.key, popElem.value);
          }
        }
        break;

      case '\$pull':
        if (elem.value is BsonDocument) {
          final pullDoc = elem.value as BsonDocument;
          for (final pullElem in pullDoc.entries) {
            _pullFromArray(doc, pullElem.key, pullElem.value);
          }
        }
        break;

      case '\$addToSet':
        if (elem.value is BsonDocument) {
          final addDoc = elem.value as BsonDocument;
          for (final addElem in addDoc.entries) {
            _addToSet(doc, addElem.key, addElem.value);
          }
        }
        break;

      case '\$pullAll':
        if (elem.value is BsonDocument) {
          final pullAllDoc = elem.value as BsonDocument;
          for (final pullElem in pullAllDoc.entries) {
            _pullAllFromArray(doc, pullElem.key, pullElem.value);
          }
        }
        break;

      case '\$currentDate':
        if (elem.value is BsonDocument) {
          final cdDoc = elem.value as BsonDocument;
          for (final cdElem in cdDoc.entries) {
            _setCurrentDate(doc, cdElem.key, cdElem.value);
          }
        }
        break;

      case '\$setOnInsert':
        // $setOnInsert 仅在 upsert 时生效，普通更新中忽略
        break;

      default:
        // 非操作符字段，直接设置
        if (!elem.key.startsWith('\$')) {
          _setField(doc, elem.key, elem.value);
        }
    }
  }
}

/// 设置字段值
void _setField(BsonDocument doc, String key, dynamic value) {
  doc[key] = value;
}

/// 移除字段
void _removeField(BsonDocument doc, String key) {
  doc.remove(key);
}

/// 增加字段值
void _incrementField(BsonDocument doc, String key, dynamic incVal) {
  final incAmount = _toDouble(incVal);
  final current = doc[key];
  if (current != null) {
    final currentVal = _toDouble(current);
    doc[key] = currentVal + incAmount;
  } else {
    doc[key] = incVal;
  }
}

/// 乘法更新
void _multiplyField(BsonDocument doc, String key, dynamic mulVal) {
  final mulAmount = _toDouble(mulVal);
  final current = doc[key];
  if (current != null) {
    final currentVal = _toDouble(current);
    doc[key] = currentVal * mulAmount;
  } else {
    doc[key] = 0.0;
  }
}

/// 取最小值更新
void _updateFieldMin(BsonDocument doc, String key, dynamic minVal) {
  final current = doc[key];
  if (current != null) {
    if (BsonCompare.compare(minVal, current) < 0) {
      doc[key] = minVal;
    }
  } else {
    doc[key] = minVal;
  }
}

/// 取最大值更新
void _updateFieldMax(BsonDocument doc, String key, dynamic maxVal) {
  final current = doc[key];
  if (current != null) {
    if (BsonCompare.compare(maxVal, current) > 0) {
      doc[key] = maxVal;
    }
  } else {
    doc[key] = maxVal;
  }
}

/// 重命名字段
void _renameField(BsonDocument doc, String oldName, String newName) {
  if (doc.containsKey(oldName)) {
    final value = doc[oldName];
    doc.remove(oldName);
    doc[newName] = value;
  }
}

/// 向数组追加元素
void _pushToArray(BsonDocument doc, String key, dynamic value) {
  var arr = doc[key];
  if (arr == null) {
    arr = BsonArray();
    doc[key] = arr;
  }
  if (arr is! BsonArray) {
    throw ArgumentError('Field $key is not an array');
  }

  // 检查是否有 $each 修饰符
  if (value is BsonDocument) {
    final each = value['\$each'];
    if (each != null && each is BsonArray) {
      arr.addAll(each);
      return;
    }
  }

  arr.add(value);
}

/// 从数组头部或尾部移除元素
void _popFromArray(BsonDocument doc, String key, dynamic value) {
  final arr = doc[key];
  if (arr is! BsonArray || arr.isEmpty) return;

  final pos = _toDouble(value);
  if (pos >= 0) {
    // 移除尾部
    arr.removeAt(arr.length - 1);
  } else {
    // 移除头部
    arr.removeAt(0);
  }
}

/// 从数组移除匹配的元素
void _pullFromArray(BsonDocument doc, String key, dynamic value) {
  final arr = doc[key];
  if (arr is! BsonArray) return;

  final newArr = BsonArray();
  for (final item in arr) {
    if (BsonCompare.compare(item, value) != 0) {
      newArr.add(item);
    }
  }
  doc[key] = newArr;
}

/// 从数组移除所有指定的元素
void _pullAllFromArray(BsonDocument doc, String key, dynamic value) {
  final arr = doc[key];
  if (arr is! BsonArray) return;
  if (value is! BsonArray) return;

  final newArr = BsonArray();
  for (final item in arr) {
    var shouldKeep = true;
    for (final v in value) {
      if (BsonCompare.compare(item, v) == 0) {
        shouldKeep = false;
        break;
      }
    }
    if (shouldKeep) {
      newArr.add(item);
    }
  }
  doc[key] = newArr;
}

/// 向数组添加唯一元素
void _addToSet(BsonDocument doc, String key, dynamic value) {
  var arr = doc[key];
  if (arr == null) {
    arr = BsonArray();
    doc[key] = arr;
  }
  if (arr is! BsonArray) return;

  // 检查是否有 $each 修饰符
  if (value is BsonDocument) {
    final each = value['\$each'];
    if (each != null && each is BsonArray) {
      for (final v in each) {
        if (!_arrayContains(arr, v)) {
          arr.add(v);
        }
      }
      return;
    }
  }

  if (!_arrayContains(arr, value)) {
    arr.add(value);
  }
}

/// 设置字段为当前日期
void _setCurrentDate(BsonDocument doc, String key, dynamic spec) {
  final now = DateTime.now();

  if (spec is bool && spec) {
    doc[key] = BsonDateTime.fromDateTime(now);
  } else if (spec is BsonDocument) {
    final type = spec['\$type'];
    if (type == 'timestamp') {
      doc[key] = BsonTimestamp(now.millisecondsSinceEpoch ~/ 1000, 0);
    } else {
      doc[key] = BsonDateTime.fromDateTime(now);
    }
  } else {
    doc[key] = BsonDateTime.fromDateTime(now);
  }
}

/// 检查数组是否包含指定值
bool _arrayContains(BsonArray arr, dynamic value) {
  for (final item in arr) {
    if (BsonCompare.compare(item, value) == 0) {
      return true;
    }
  }
  return false;
}

/// 转换为 double
double _toDouble(dynamic v) {
  if (v is int) return v.toDouble();
  if (v is double) return v;
  return 0.0;
}
