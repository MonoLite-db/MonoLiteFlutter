// Created by Yanjunhui

import '../bson/bson.dart';
import 'filter.dart';

/// 查询选项
class QueryOptions {
  final Map<String, int>? sort;
  final int? limit;
  final int? skip;
  final Map<String, int>? projection;

  QueryOptions({
    this.sort,
    this.limit,
    this.skip,
    this.projection,
  });
}

/// 应用查询选项到结果集
List<BsonDocument> applyOptions(List<BsonDocument> docs, QueryOptions opts) {
  var result = docs;

  // 排序
  if (opts.sort != null && opts.sort!.isNotEmpty) {
    result = _sortDocuments(result, opts.sort!);
  }

  // Skip
  if (opts.skip != null && opts.skip! > 0) {
    if (result.length <= opts.skip!) {
      return [];
    }
    result = result.sublist(opts.skip!);
  }

  // Limit
  if (opts.limit != null && opts.limit! > 0 && result.length > opts.limit!) {
    result = result.sublist(0, opts.limit!);
  }

  // Projection
  if (opts.projection != null && opts.projection!.isNotEmpty) {
    result = _applyProjection(result, opts.projection!);
  }

  return result;
}

/// 对文档排序
List<BsonDocument> _sortDocuments(List<BsonDocument> docs, Map<String, int> sortSpec) {
  final result = List<BsonDocument>.from(docs);

  result.sort((a, b) {
    for (final entry in sortSpec.entries) {
      final field = entry.key;
      final direction = entry.value;

      final valA = getDocField(a, field);
      final valB = getDocField(b, field);

      final cmp = BsonCompare.compare(valA, valB);
      if (cmp != 0) {
        return direction < 0 ? -cmp : cmp;
      }
    }
    return 0;
  });

  return result;
}

/// 应用投影
List<BsonDocument> _applyProjection(List<BsonDocument> docs, Map<String, int> projection) {
  // 判断是包含还是排除模式
  var includeMode = false;
  for (final entry in projection.entries) {
    if (entry.key == '_id') continue;
    if (entry.value == 1) {
      includeMode = true;
    }
    break;
  }

  final result = <BsonDocument>[];
  for (final doc in docs) {
    if (includeMode) {
      // 包含模式
      final newDoc = BsonDocument();

      // 默认包含 _id
      var includeId = true;
      if (projection['_id'] == 0) {
        includeId = false;
      }

      if (includeId) {
        final idVal = getDocField(doc, '_id');
        if (idVal != null) {
          newDoc['_id'] = idVal;
        }
      }

      for (final entry in projection.entries) {
        if (entry.key == '_id') continue;
        if (entry.value == 1) {
          final val = getDocField(doc, entry.key);
          if (val != null) {
            newDoc[entry.key] = val;
          }
        }
      }
      result.add(newDoc);
    } else {
      // 排除模式
      final newDoc = BsonDocument();
      final excludeFields = <String>{};

      for (final entry in projection.entries) {
        if (entry.value == 0) {
          excludeFields.add(entry.key);
        }
      }

      for (final entry in doc.entries) {
        if (!excludeFields.contains(entry.key)) {
          newDoc[entry.key] = entry.value;
        }
      }
      result.add(newDoc);
    }
  }

  return result;
}
