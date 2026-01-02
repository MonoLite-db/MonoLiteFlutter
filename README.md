# MonoLite for Dart/Flutter

[![Dart](https://img.shields.io/badge/Dart-3.0+-0175C2?logo=dart)](https://dart.dev)
[![Flutter](https://img.shields.io/badge/Flutter-3.0+-02569B?logo=flutter)](https://flutter.dev)
[![License](https://img.shields.io/badge/license-MIT-green.svg)](LICENSE)
[![Platform](https://img.shields.io/badge/platform-iOS%20%7C%20Android%20%7C%20macOS%20%7C%20Windows%20%7C%20Linux-lightgrey)]()

**MonoLite** 是一个轻量级、单文件的嵌入式文档数据库，专为移动端和嵌入式环境设计。Dart/Flutter 版本提供与 Go、Swift、TypeScript 版本 **100% 二进制兼容** 的存储格式。

---

## 项目愿景

MonoLite 的目标是成为 **嵌入式场景下的 MongoDB**：
- 单文件存储，零外部依赖
- MongoDB 兼容的查询语法
- 跨语言、跨平台的二进制兼容
- 专为移动端和 IoT 设备优化

---

## 为什么选择 MonoLite？

| 特性 | SQLite | MonoLite |
|------|--------|----------|
| 数据模型 | 关系型（表、行、列） | 文档型（JSON/BSON） |
| Schema | 固定，需要迁移 | 灵活，无需迁移 |
| 查询语法 | SQL | MongoDB 风格 |
| 嵌套数据 | 需要 JSON 函数 | 原生支持 |
| 数组操作 | 复杂 | 原生支持 |
| 学习曲线 | 需要 SQL 知识 | JSON 即查询 |

---

## 快速开始

### 安装

在 `pubspec.yaml` 中添加依赖：

```yaml
dependencies:
  monolite:
    git:
      url: https://github.com/nicklaus-dev/MonoLiteFlutter.git
```

### 基本使用

```dart
import 'package:monolite/monolite.dart';

void main() async {
  // 打开数据库
  final db = await MonoLite.open('myapp.monodb');

  // 获取集合
  final users = db.collection('users');

  // 插入文档
  final doc = BsonDocument()
    ..['name'] = 'Alice'
    ..['age'] = 30
    ..['email'] = 'alice@example.com';

  final result = await users.insertOne(doc);
  print('插入成功，ID: ${result.insertedId}');

  // 查询文档
  final query = BsonDocument()..['age'] = (BsonDocument()..['$gte'] = 25);
  final cursor = await users.find(query);

  for (final user in cursor) {
    print('找到用户: ${user['name']}');
  }

  // 更新文档
  final filter = BsonDocument()..['name'] = 'Alice';
  final update = BsonDocument()
    ..[r'$set'] = (BsonDocument()..['age'] = 31);

  await users.updateOne(filter, update);

  // 删除文档
  await users.deleteOne(filter);

  // 关闭数据库
  await db.close();
}
```

---

## 核心功能

### CRUD 操作
```dart
// 插入
await collection.insertOne(doc);
await collection.insertMany([doc1, doc2, doc3]);

// 查询
await collection.find(filter);
await collection.findOne(filter);

// 更新
await collection.updateOne(filter, update);
await collection.updateMany(filter, update);
await collection.replaceOne(filter, replacement);

// 删除
await collection.deleteOne(filter);
await collection.deleteMany(filter);
```

### 查询操作符
```dart
// 比较操作符
{'age': {r'$gt': 25}}      // 大于
{'age': {r'$gte': 25}}     // 大于等于
{'age': {r'$lt': 30}}      // 小于
{'age': {r'$lte': 30}}     // 小于等于
{'age': {r'$ne': 25}}      // 不等于
{'status': {r'$in': ['A', 'B']}}  // 在列表中

// 逻辑操作符
{r'$and': [{'age': {r'$gt': 25}}, {'status': 'active'}]}
{r'$or': [{'age': {r'$lt': 20}}, {'age': {r'$gt': 60}}]}

// 元素操作符
{'email': {r'$exists': true}}

// 数组操作符
{'tags': {r'$all': ['dart', 'flutter']}}
{'scores': {r'$elemMatch': {r'$gt': 80}}}
```

### 更新操作符
```dart
// 字段更新
{r'$set': {'name': 'Bob'}}
{r'$unset': {'tempField': ''}}
{r'$inc': {'count': 1}}
{r'$mul': {'price': 1.1}}
{r'$min': {'low': 5}}
{r'$max': {'high': 100}}
{r'$rename': {'oldName': 'newName'}}

// 数组更新
{r'$push': {'tags': 'new'}}
{r'$addToSet': {'tags': 'unique'}}
{r'$pop': {'array': 1}}     // 移除最后一个
{r'$pull': {'tags': 'old'}} // 移除指定值
```

### 索引支持
```dart
// 创建索引
await collection.createIndex(
  {'email': 1},
  CreateIndexOptions(unique: true, name: 'idx_email'),
);

// 列出索引
final indexes = await collection.listIndexes();

// 删除索引
await collection.dropIndex('idx_email');
```

### 聚合管道
```dart
final pipeline = [
  {r'$match': {'status': 'active'}},
  {r'$group': {'_id': r'$category', 'total': {r'$sum': 1}}},
  {r'$sort': {'total': -1}},
  {r'$limit': 10},
];
final results = await collection.aggregate(pipeline);
```

---

## 功能支持状态

### 查询操作符
| 操作符 | 状态 | 说明 |
|--------|------|------|
| `$eq`, `$ne` | ✅ | 等于/不等于 |
| `$gt`, `$gte`, `$lt`, `$lte` | ✅ | 比较操作 |
| `$in`, `$nin` | ✅ | 包含/不包含 |
| `$and`, `$or`, `$not`, `$nor` | ✅ | 逻辑操作 |
| `$exists` | ✅ | 字段存在 |
| `$all`, `$elemMatch`, `$size` | ✅ | 数组查询 |
| `$regex` | ✅ | 正则匹配 |

### 更新操作符
| 操作符 | 状态 | 说明 |
|--------|------|------|
| `$set`, `$unset` | ✅ | 设置/删除字段 |
| `$inc`, `$mul` | ✅ | 数值运算 |
| `$min`, `$max` | ✅ | 最小/最大值 |
| `$rename` | ✅ | 重命名字段 |
| `$push`, `$addToSet` | ✅ | 数组添加 |
| `$pop`, `$pull` | ✅ | 数组移除 |
| `$each`, `$position`, `$slice` | ✅ | 数组修饰符 |

### 聚合阶段
| 阶段 | 状态 | 说明 |
|------|------|------|
| `$match` | ✅ | 过滤 |
| `$sort` | ✅ | 排序 |
| `$limit`, `$skip` | ✅ | 分页 |
| `$project` | ✅ | 投影 |
| `$group` | ✅ | 分组 |
| `$count` | ✅ | 计数 |

---

## 存储引擎架构

```
┌─────────────────────────────────────────────────────────┐
│                    MonoLite API                         │
│              (Collection / Database)                    │
├─────────────────────────────────────────────────────────┤
│                   Query Engine                          │
│        (QueryMatcher / UpdateOperator)                  │
├─────────────────────────────────────────────────────────┤
│                   Storage Engine                        │
│     ┌─────────────┐  ┌─────────────┐  ┌─────────────┐  │
│     │  B+ Tree    │  │   Catalog   │  │    WAL      │  │
│     │   Index     │  │   Manager   │  │   (Write    │  │
│     │             │  │             │  │   Ahead     │  │
│     │             │  │             │  │   Log)      │  │
│     └─────────────┘  └─────────────┘  └─────────────┘  │
├─────────────────────────────────────────────────────────┤
│                   Page Manager                          │
│        (SlottedPage / BufferPool / FreeList)           │
├─────────────────────────────────────────────────────────┤
│                   File System                           │
│              (Single .monodb File)                      │
└─────────────────────────────────────────────────────────┘
```

---

## 项目结构

```
lib/
├── monolite.dart              # 公共 API 导出
└── src/
    ├── bson/                  # BSON 序列化
    │   ├── bson_document.dart
    │   ├── bson_array.dart
    │   ├── bson_codec.dart
    │   └── object_id.dart
    ├── storage/               # 存储引擎
    │   ├── page.dart          # 页面结构
    │   ├── pager.dart         # 页面管理器
    │   ├── freelist.dart      # 空闲列表
    │   ├── btree.dart         # B+ 树索引
    │   └── wal.dart           # WAL 日志
    ├── engine/                # 数据库引擎
    │   ├── storage_engine.dart
    │   ├── catalog.dart
    │   ├── collection_storage.dart
    │   └── index_storage.dart
    ├── query/                 # 查询引擎
    │   ├── query_matcher.dart
    │   ├── update_operator.dart
    │   └── aggregation.dart
    └── api/                   # 高层 API
        ├── monolite.dart
        ├── database.dart
        └── collection.dart
```

---

## 技术规格

### 文件格式
- **文件头**: 64 字节（魔数、版本、页面计数等）
- **页面大小**: 4096 字节
- **页面头**: 24 字节

### 存储限制
- **最大文档大小**: 16 MB（支持溢出页）
- **最大集合数**: 无硬性限制
- **最大索引数**: 每集合无限制

### WAL 格式
- **WAL 头**: 32 字节
- **记录格式**: 类型 + 事务ID + 页面ID + 数据

---

## 跨语言兼容

MonoLite 在以下语言间保持 100% 二进制兼容：

| 语言 | 仓库 | 状态 |
|------|------|------|
| Go | [MonoLite](https://github.com/nicklaus-dev/MonoLite) | ✅ 参考实现 |
| Swift | [MonoLiteSwift](https://github.com/nicklaus-dev/MonoLiteSwift) | ✅ |
| TypeScript | [MonoLiteTS](https://github.com/nicklaus-dev/MonoLiteTS) | ✅ |
| Dart/Flutter | [MonoLiteFlutter](https://github.com/nicklaus-dev/MonoLiteFlutter) | ✅ |

**兼容性保证**：
- 相同的文件格式（可跨语言读写）
- 相同的 BSON 编码
- 相同的 B+ 树结构
- 相同的查询语义

---

## 环境要求

- Dart SDK: >= 3.0.0
- Flutter: >= 3.0.0（如果用于 Flutter 项目）

## 开发

```bash
# 获取依赖
dart pub get

# 运行测试
dart test

# 分析代码
dart analyze
```

---

## 许可证

MIT License - 详见 [LICENSE](LICENSE) 文件

## 作者

Created by Yanjunhui
