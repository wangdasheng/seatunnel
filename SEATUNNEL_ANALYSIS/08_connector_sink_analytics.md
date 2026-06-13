# Apache SeaTunnel Elasticsearch / ClickHouse / StarRocks / Doris Connector 深度源码分析

> 代码位置：`seatunnel-connectors-v2/connector-elasticsearch/`, `connector-clickhouse/`, `connector-starrocks/`, `connector-doris/`
> 分析版本：基于 2.3.13-release 分支

---

## 目录

1. [Elasticsearch Connector](#1-elasticsearch-connector)
2. [ClickHouse Connector](#2-clickhouse-connector)
3. [StarRocks Connector](#3-starrocks-connector)
4. [Doris Connector](#4-doris-connector)
5. [四个 Connector 的对比分析](#5-四个-connector-的对比分析)
6. [总结与设计模式](#6-总结与设计模式)

---

## 1. Elasticsearch Connector

### 1.1 总体架构

```
┌─────────────────────────────────────────────────┐
│              ElasticsearchSource                 │
│  implements SeaTunnelSource<Row, Split, State>  │
│  SupportParallelism, SupportColumnProjection     │
└─────────────────┬───────────────────────────────┘
                  │
        ┌─────────┴─────────┐
        │                   │
        ▼                   ▼
┌───────────────┐  ┌────────────────────┐
│ SplitEnumerator│  │  SourceReader      │
│ 生成索引分片    │  │ Scroll/PIT 读取    │
└───────────────┘  └────────────────────┘
                            │
                            ▼
                   ┌────────────────────┐
                   │ EsRestClient       │
                   │ ES REST API 客户端  │
                   └────────────────────┘

┌─────────────────────────────────────────────────┐
│              ElasticsearchSink                   │
│  implements SeaTunnelSink                        │
│  SupportMultiTableSink, SupportSaveMode          │
│  SupportSchemaEvolutionSink                      │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│         ElasticsearchSinkWriter                  │
│  implements SinkWriter, SupportSchemaEvolution   │
│  _bulk API 批量写入 + 重试机制                    │
└─────────────────────────────────────────────────┘
```

### 1.2 ElasticsearchSource — 多模式读取

`ElasticsearchSource` 实现了 `SeaTunnelSource`、`SupportParallelism`、`SupportColumnProjection`，提供三种读取模式：

| 模式 | 枚举值 | 适用场景 |
|------|--------|---------|
| **Scroll** | `SearchTypeEnum.SCROLL` | 传统深分页，适合数据量不大的场景 |
| **Scroll + SQL** | `SearchTypeEnum.SQL` | 使用 Elasticsearch SQL 语法查询 |
| **PIT (Point-in-Time)** | `SearchApiTypeEnum.PIT` | 7.10+ 版本，轻量级时间点快照，避免 Scroll 上下文开销 |

**核心配置解析流程：**

```
ElasticsearchSource 构造函数
  ├── 判断多源/单源模式 (index_list vs index)
  ├── 解析查询配置 (query DSL)
  ├── 获取字段类型映射 (通过 _mappings API 或 _sql)
  │     ├── getFieldTypeMapping() → EsType 映射
  │     └── ElasticSearchTypeConverter.INSTANCE.convert()
  ├── 构建 CatalogTable (含 TableSchema)
  ├── 解析 runtime_fields (ES 7.11+)
  └── 生成 ElasticsearchConfig 列表
```

**分片策略：** Source 通过 `ElasticsearchSourceSplitEnumerator` 生成分片。每个分片对应一个索引的查询范围，通过 `splitId.hashCode() % parallelism` 分配到不同 Reader。

### 1.3 ElasticsearchSink — 批量写入机制

#### 写入链路

`ElasticsearchSinkWriter` 不使用 ES 官方的 `BulkProcessor`，而是基于 `EsRestClient` 直接调用 `/_bulk` API：

```
write(row)
  └── seaTunnelRowSerializer.serializeRow(row) → JSON 字符串
        └── requestEsList.add(indexRequestRow)
              └── 达到 maxBatchSize → bulkEsWithRetry()
```

#### 批量提交协议

```java
// 每行序列化为 NDJSON 格式 (index + doc 两行)
// {"index":{"_index":"my_index","_id":"xxx"}}
// {"field1":"value1","field2":"value2"}
```

累积到 `maxBatchSize` 后，调用 `esRestClient.bulk(requestBody)` 向 `/_bulk` 端点发送 POST 请求。

#### 重试机制

```java
RetryUtils.retryWithException(
    () -> {
        BulkResponse bulkResponse = esRestClient.bulk(requestBody);
        if (bulkResponse.isErrors()) {
            throw new ElasticsearchConnectorException(...);
        }
        return bulkResponse;
    },
    retryMaterial);  // maxRetryCount, 200ms sleep
```

使用 `RetryUtils.retryWithException` 实现，默认重试间隔 200ms，所有异常都重试（`exception -> true`）。

#### 序列化器设计

```
SeaTunnelRowSerializer (接口)
  └── ElasticsearchRowSerializer (实现)
        ├── 根据 ES 集群版本自适应序列化
        ├── 支持 Vectorization 字段 (DENSE_VECTOR)
        └── 处理 RowKind (UPDATE_BEFORE 跳过)
```

#### 索引管理器

`IndexSerializer` 体系支持灵活的索引路由：

```
IndexSerializer (接口)
  ├── FixedValueIndexSerializer    — 固定索引名
  └── VariableIndexSerializer     — 从行数据中提取索引名
        └── IndexSerializerFactory.create()
```

#### Schema 演进支持

`ElasticsearchSinkWriter` 通过 `applySchemaChange()` 支持 `ADD_COLUMN`：

```java
// 调用 ES _mapping API 动态添加字段
esRestClient.addField(indexInfo.getIndex(), reconvert);
// 重建 Serializer 以适应新 schema
this.seaTunnelRowSerializer = new ElasticsearchRowSerializer(...);
```

### 1.4 EsRestClient — REST API 客户端

基于 ES 官方 `RestClient`（低级客户端），封装了完整的 ES 操作：

| 方法 | HTTP 端点 | 用途 |
|------|----------|------|
| `bulk()` | `POST /_bulk` | 批量写入 |
| `searchByScroll()` | `POST /{index}/_search?scroll=` | Scroll 搜索 |
| `searchBySql()` | `POST /_sql?format=json` | SQL 搜索 |
| `searchWithPointInTime()` | `POST /_search` (PIT) | PIT 搜索 |
| `createIndex()` | `PUT /{index}` | 创建索引 |
| `dropIndex()` | `DELETE /{index}` | 删除索引 |
| `clearIndexData()` | `POST /{index}/_delete_by_query` | 清空索引数据 |
| `getFieldTypeMapping()` | `GET /{index}/_mappings` | 获取字段映射 |
| `addField()` | `PUT /{index}/_mapping` | 动态添加字段 |
| `checkIndexExist()` | `HEAD /{index}` | 检查索引存在 |

**连接配置：**
- 连接请求超时：10s
- Socket 超时：5min
- 认证：支持 Basic Auth / API Key / API Key Encoded（通过 `AuthenticationProvider` 策略模式）

### 1.5 核心类路径速查

| 类名 | 路径 | 职责 |
|------|------|------|
| `ElasticsearchSource` | `source/ElasticsearchSource.java` | Source 入口，三种读取模式 |
| `ElasticsearchSourceSplitEnumerator` | `source/ElasticsearchSourceSplitEnumerator.java` | 分片生成与分配 |
| `ElasticsearchSourceReader` | `source/ElasticsearchSourceReader.java` | Scroll/PIT 数据读取 |
| `ElasticsearchSink` | `sink/ElasticsearchSink.java` | Sink 入口 |
| `ElasticsearchSinkWriter` | `sink/ElasticsearchSinkWriter.java` | 批量写入 + 重试 |
| `EsRestClient` | `client/EsRestClient.java` | ES REST API 封装 |
| `ElasticsearchRowSerializer` | `serialize/ElasticsearchRowSerializer.java` | 行序列化为 NDJSON |
| `IndexSerializerFactory` | `serialize/index/IndexSerializerFactory.java` | 索引序列化器工厂 |
| `ElasticSearchTypeConverter` | `catalog/ElasticSearchTypeConverter.java` | ES 类型转换器 |

---

## 2. ClickHouse Connector

### 2.1 总体架构

ClickHouse Connector 提供了**两种写入模式**：

```
┌─────────────────────────────────────────────────┐
│              ClickhouseSink (JDBC 模式)           │
│  implements SeaTunnelSink<CKCommitInfo, ...>     │
│  SupportSaveMode, SupportMultiTableSink          │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│           ClickhouseSinkWriter                   │
│  ShardRouter → 分片路由                          │
│  JdbcBatchStatementExecutor → JDBC 批量写入      │
│  InsertOrUpdateBatchStatementExecutor → Upsert   │
└─────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────┐
│           ClickhouseFileSink (文件模式)           │
│  implements SeaTunnelSink<CKFileCommitInfo, ...> │
└─────────────────┬───────────────────────────────┘
                  │
       ┌──────────┴──────────┐
       │                     │
       ▼                     ▼
┌──────────────┐  ┌──────────────────────┐
│ FileSinkWriter│  │ FileSinkAggCommitter │
│ 本地文件生成    │  │ 文件分发 + ATTACH     │
│ clickhouse-   │  │ PART                 │
│ local 转换     │  │                      │
└──────────────┘  └──────────────────────┘
```

### 2.2 JDBC 模式：ClickhouseSinkWriter

#### 初始化流程

```
ClickhouseSink.createWriter()
  ├── ClickhouseUtil.createNodes(config) → ClickHouseNode 列表
  ├── ClickhouseProxy.getClickhouseTableSchema() → 表结构
  ├── ClickhouseProxy.getClickhouseTable() → 表引擎/排序键
  ├── 构建 ShardMetadata (分片键/排序键/引擎类型)
  ├── 构建 ReaderOption (bulkSize/supportUpsert/primaryKeys)
  └── new ClickhouseSinkWriter(option, context)
        └── initStatementMap()
              ├── ShardRouter.getShards() → 遍历所有分片
              ├── 为每个分片创建 ClickHouseConnectionImpl
              ├── JdbcBatchStatementExecutorBuilder.build()
              │     ├── InsertOrUpdateBatchStatementExecutor (Upsert)
              │     └── SimpleBatchStatementExecutor (普通 Insert)
              └── 构建 ClickhouseBatchStatement (connection + executor + counter)
```

#### ShardRouter — 分片路由

ClickHouse 分布式表的写入核心是 `ShardRouter`，负责将数据路由到正确的分片：

```
ShardRouter 构造函数
  ├── splitMode = true
  │     ├── proxy.getClickhouseDistributedTable() → 解析 Distributed 引擎
  │     │     └── 获取 clusterName, localTable, localTableEngine
  │     ├── proxy.getClusterShardList() → 获取集群分片列表
  │     └── 构建 TreeMap<Integer, Shard> (按权重累加)
  └── splitMode = false
        └── 使用默认分片
```

**路由算法：**

```java
public Shard getShard(Object shardValue) {
    if (!splitMode) return shards.firstEntry().getValue();
    if (shardValue == null) return randomShard();  // 随机路由
    // XXHash64 哈希取模
    int offset = (int)((XXHash64.hash(shardValue) & Long.MAX_VALUE) % shardWeightCount);
    return shards.lowerEntry(offset + 1).getValue();
}
```

使用 XXHash64 算法对分片键进行哈希，按照权重分配到对应分片，保证数据均匀分布。

#### 写入路径

```
write(row)
  ├── 提取分片键值 (如果有)
  ├── ShardRouter.getShard(shardKey) → 目标分片
  ├── statement.addToBatch(row)
  ├── sizeHolder++
  └── 达到 bulkSize → flush(statement)
        └── executeBatch() → JDBC PreparedStatement.executeBatch()
```

#### Upsert 支持

`InsertOrUpdateBatchStatementExecutor` 通过 ClickHouse 的 `ALTER TABLE ... DELETE` + `INSERT` 实现 Upsert：

- 先按主键 DELETE 旧数据
- 再 INSERT 新数据
- 支持 `allow_experimental_lightweight_delete` 配置（ClickHouse 22.8+）

### 2.3 文件模式：ClickhouseFileSinkWriter

文件模式专为大规模数据导入设计，避免 JDBC 瓶颈：

```
write(row)
  └── saveDataToFile()
        ├── 按字段分隔符拼接行数据
        └── MappedByteBuffer 写入本地文件 (128KB buffer)

prepareCommit()
  └── 对每个分片的临时文件:
        ├── generateClickhouseLocalFiles()
        │     ├── 调用 clickhouse-local 工具
        │     │     └── 将 CSV 转换为 ClickHouse 原生格式
        │     └── 输出到 data/_local/{tableName}/ 目录
        ├── moveClickhouseLocalFileToServer()
        │     ├── SCP/RSync 文件传输 (FileTransfer 策略)
        │     └── 移动到 detached/ 目录
        └── 返回 CKFileCommitInfo (含 detachedFiles 映射)

ClickhouseFileSinkAggCommitter.commit()
  └── attachFileToClickhouse()
        └── ALTER TABLE {localTable} ATTACH PART '{fileName}'
```

**关键设计点：**
- 使用 `clickhouse-local` 工具（无需启动 ClickHouse Server）将 CSV 转为原生格式
- `FileTransfer` 策略模式支持 SCP 和 RSync 两种传输方式
- 最终通过 `ATTACH PART` 将数据文件挂载到表中，效率极高

### 2.4 核心类路径速查

| 类名 | 路径 | 职责 |
|------|------|------|
| `ClickhouseSink` | `sink/client/ClickhouseSink.java` | JDBC 模式 Sink 入口 |
| `ClickhouseSinkWriter` | `sink/client/ClickhouseSinkWriter.java` | JDBC 批量写入 + 分片路由 |
| `ShardRouter` | `sink/client/ShardRouter.java` | 分布式表分片路由 |
| `ClickhouseFileSink` | `sink/file/ClickhouseFileSink.java` | 文件模式 Sink 入口 |
| `ClickhouseFileSinkWriter` | `sink/file/ClickhouseFileSinkWriter.java` | 本地文件生成 + 上传 |
| `ClickhouseFileSinkAggCommitter` | `sink/file/ClickhouseFileSinkAggCommitter.java` | ATTACH PART 聚合提交 |
| `JdbcBatchStatementExecutorBuilder` | `sink/client/executor/JdbcBatchStatementExecutorBuilder.java` | SQL 执行器构建 |
| `InsertOrUpdateBatchStatementExecutor` | `sink/client/executor/InsertOrUpdateBatchStatementExecutor.java` | Upsert 执行器 |
| `ClickhouseProxy` | `util/ClickhouseProxy.java` | ClickHouse 连接代理 |

---

## 3. StarRocks Connector

### 3.1 总体架构

```
┌─────────────────────────────────────────────────┐
│                StarRocksSink                     │
│  extends AbstractSimpleSink<Row, Void>           │
│  SupportSaveMode, SupportMultiTableSink          │
│  SupportSchemaEvolutionSink                      │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│            StarRocksSinkWriter                   │
│  extends AbstractSinkWriter<Row, Void>           │
│  StarRocksISerializer → CSV/JSON 序列化          │
│  StarRocksSinkManager → 缓冲区管理               │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│          StarRocksSinkManager                    │
│  批量缓冲 (batchMaxSize/batchMaxBytes)            │
│  flush() → StarRocksStreamLoadVisitor            │
│  重试机制 (指数退避 + Label 再生)                  │
└─────────────────┬───────────────────────────────┘
                  │
                  ▼
┌─────────────────────────────────────────────────┐
│        StarRocksStreamLoadVisitor                │
│  HTTP PUT /api/{db}/{table}/_stream_load        │
│  Label 状态轮询 (VISIBLE/COMMITTED/PREPARE)      │
│  HttpHelper → HTTP 通信层                        │
└─────────────────────────────────────────────────┘
```

### 3.2 Stream Load 机制详解

StarRocks 使用 **Stream Load** 协议进行数据写入，通过 HTTP PUT 请求将数据推送到 BE 节点：

#### 数据流

```
StarRocksSinkWriter.write(row)
  └── serializer.serialize(row) → byte[]
        └── StarRocksSinkManager.write(record)
              ├── batchList.add(record)
              ├── batchRowCount++, batchBytesSize += bytes
              └── 达到阈值 → flush()
                    ├── createBatchLabel() → UUID
                    ├── StarRocksStreamLoadVisitor.doStreamLoad(tuple)
                    │     ├── getAvailableHost() → 轮询 BE 节点
                    │     ├── joinRows() → 拼接 CSV/JSON 字节数组
                    │     ├── httpHelper.doHttpPut(loadUrl, data, headers)
                    │     │     └── PUT /api/{db}/{table}/_stream_load
                    │     └── 检查响应状态
                    └── batchList.clear()
```

#### Stream Load HTTP 请求

```
PUT /api/{database}/{table}/_stream_load HTTP/1.1
Authorization: Basic {base64(user:password)}
Content-Type: application/x-www-form-urlencoded
Expect: 100-continue
label: {uuid}
format: CSV|JSON
columns: `col1`,`col2`,...
strip_outer_array: true
[可选] row_delimiter: \n
[可选] column_separator: \t

{CSV行数据或JSON数组}
```

#### 序列化器

```
StarRocksISerializer (接口)
  ├── StarRocksCsvSerializer  — CSV 格式 (带分隔符)
  └── StarRocksJsonSerializer — JSON 格式 ([{...}, {...}])
```

### 3.3 重试与容错机制

#### StarRocksSinkManager 重试逻辑

```java
for (int i = 0; i <= maxRetries; i++) {
    Boolean success = starrocksStreamLoadVisitor.doStreamLoad(tuple);
    if (success) break;
    // 指数退避
    long backoff = Math.min(retryBackoffMultiplierMs * i, maxRetryBackoffMs);
    Thread.sleep(backoff);
    // 需要时重新生成 Label
    if (needReCreateLabel) {
        tuple.setLabel(createBatchLabel());
    }
}
```

#### Label 状态轮询

当 Stream Load 返回 `Label Already Exists` 时，`StarRocksStreamLoadVisitor` 进入状态轮询：

```
checkLabelState(label)
  └── 循环轮询 GET /api/{db}/get_load_state?label={label}
        ├── VISIBLE / COMMITTED → 成功返回
        ├── PREPARE → 继续等待 (最多 5 秒)
        ├── ABORTED → 抛异常
        └── UNKNOWN → 抛异常
```

### 3.4 与 AbstractSimpleSink 的关系

`StarRocksSink` 继承 `AbstractSimpleSink<SeaTunnelRow, Void>`，而非常见的 `SeaTunnelSink` 接口。这意味着：

- **无 Committer**：不需要两阶段提交，`prepareCommit()` 中直接 flush
- **无 State**：泛型 `Void` 表示不需要状态序列化
- **更简洁**：适合"发后即忘"的写入模式

### 3.5 核心类路径速查

| 类名 | 路径 | 职责 |
|------|------|------|
| `StarRocksSink` | `sink/StarRocksSink.java` | Sink 入口 |
| `StarRocksSinkWriter` | `sink/StarRocksSinkWriter.java` | Writer + Schema 演进 |
| `StarRocksSinkManager` | `client/StarRocksSinkManager.java` | 缓冲区管理 + 重试 |
| `StarRocksStreamLoadVisitor` | `client/StarRocksStreamLoadVisitor.java` | Stream Load HTTP 交互 |
| `HttpHelper` | `client/HttpHelper.java` | HTTP 通信工具 |
| `StarRocksFlushTuple` | `client/StarRocksFlushTuple.java` | 批量刷新数据元组 |
| `StarRocksCsvSerializer` | `serialize/StarRocksCsvSerializer.java` | CSV 序列化 |
| `StarRocksJsonSerializer` | `serialize/StarRocksJsonSerializer.java` | JSON 序列化 |

---

## 4. Doris Connector

### 4.1 总体架构

```
┌─────────────────────────────────────────────────┐
│                  DorisSink                       │
│  implements SeaTunnelSink<Row, State,            │
│    DorisCommitInfo, DorisCommitInfo>             │
│  SupportSaveMode, SupportMultiTableSink          │
│  SupportSchemaEvolutionSink                      │
└─────────────────┬───────────────────────────────┘
                  │
         ┌────────┴────────┐
         │                 │
         ▼                 ▼
┌────────────────┐  ┌──────────────────┐
│ DorisSinkWriter │  │  DorisCommitter  │
│ Stream Load     │  │  2PC Commit/Abort│
│ 异步 HTTP 流式   │  │  /_stream_load   │
│ 写入            │  │  _2pc 端点        │
└────────┬───────┘  └──────────────────┘
         │
         ▼
┌─────────────────────────────────────────────────┐
│              DorisStreamLoad                     │
│  RecordStream (自定义 InputStream)               │
│  HTTP PUT /api/{db}/{table}/_stream_load        │
│  异步上传 (ExecutorService)                      │
│  LabelGenerator → Label 管理                     │
└─────────────────────────────────────────────────┘
```

### 4.2 Stream Load 机制详解

Doris 的 Stream Load 与 StarRocks 类似（两者同源），但 SeaTunnel 中的实现有显著差异：

#### 异步流式写入

Doris 采用了**异步流式上传**，这是与 StarRocks 同步批量写入的最大区别：

```
DorisSinkWriter.write(row)
  └── serializer.serialize(row) → byte[]
        └── dorisStreamLoad.writeRecord(record)
              ├── recordStream.write(record)  // 写入自定义 InputStream
              ├── recordCount++
              └── 首次写入时 → startStreamLoad()
                    └── executorService.submit(() -> {
                          httpClient.execute(
                            new HttpPutBuilder()
                              .setEntity(new InputStreamEntity(recordStream))
                              .build()
                          )
                        })
```

**核心设计：RecordStream**

`RecordStream` 是一个自定义的 `InputStream`，作为数据管道连接 Writer 和 HTTP 上传：

```
Writer 线程                          HTTP 上传线程
    │                                     │
    ├─ writeRecord(bytes) ──► RecordStream │
    │                         (阻塞队列)    │
    ├─ writeRecord(bytes) ──►             ├─ httpClient.execute()
    │                                     │   └─ 从 InputStream 读取
    │                         ...         │      并上传到 BE
    ├─ stopLoad() ──────────►            │
    │  └─ endInput()                       │
    │     └─ 发送结束信号                   ├─ 等待 HTTP 响应
    │                                     │
    ▼                                     ▼
  prepareCommit()                  handlePreCommitResponse()
```

#### 数据格式

```java
// 每次写入：
// recordStream.write(lineDelimiter)  // 行分隔符 (默认 \n)
// recordStream.write(record)         // 行数据 (CSV 或 JSON)
```

### 4.3 两阶段提交 (2PC)

Doris 支持基于 Stream Load 的两阶段提交，实现 Exactly-Once 语义：

#### Phase 1: Pre-Commit（Writer 端）

```
prepareCommit()
  └── dorisStreamLoad.stopLoad()
        ├── recordStream.endInput()  // 结束输入流
        ├── pendingLoadFuture.get()  // 等待 HTTP 响应
        ├── 解析响应 → RespContent
        └── 返回 DorisCommitInfo(hostPort, db, txnId)
```

#### Phase 2: Commit / Abort（Committer 端）

```
DorisCommitter.commit(commitInfos)
  └── for each commitInfo:
        └── commitTransaction(commitInfo)
              └── HTTP PUT /api/{db}/_stream_load_2pc
                    headers: {txn_id: {txnId}, commit: true}
                    └── 重试 maxRetries 次

DorisCommitter.abort(commitInfos)
  └── for each commitInfo:
        └── abortTransaction(commitInfo)
              └── HTTP PUT /api/{db}/_stream_load_2pc
                    headers: {txn_id: {txnId}, commit: false}
```

#### 恢复时的 Abort

`initializeLoad()` 中调用 `dorisStreamLoad.abortPreCommit()` 清理上一次未完成的 Pre-Commit：

```java
// 恢复时逐 label 检查并 abort
while (true) {
    String label = labelGenerator.generateLabel(startChkID);
    // 发送 enable2PC + 空 body 检查 label 状态
    RespContent respContent = handlePreCommitResponse(...);
    if (LABEL_ALREADY_EXIST) {
        if (jobFinished) throw Exception;  // 需要从 savepoint 恢复
        abortTransaction(txnId);           // job 未完成，abort
    } else {
        abortTransaction(txnId);           // 正常 abort
        break;
    }
    startChkID++;
}
```

### 4.4 非 2PC 模式

当 `enable2PC = false` 时：

```
write(row)
  ├── dorisStreamLoad.writeRecord(record)
  └── recordCount >= batchSize → flush()
        └── stopLoad() → 等待本次 Stream Load 完成
        └── startLoad(nextLabel) → 开始下一批

snapshotState(checkpointId)
  └── startLoad(labelGenerator.generateLabel(checkpointId + 1))
  └── 保存 state {labelPrefix, checkpointId}
```

### 4.5 定时健康检查

`ScheduledExecutorService` 定时检查 Stream Load 是否异常：

```java
scheduledExecutorService.scheduleWithFixedDelay(
    this::checkDone, INITIAL_DELAY, intervalTime, MILLISECONDS);

// checkDone
if (pendingLoadFuture.isDone()) {
    // 异步上传已完成，检查是否有错误
    loadException = new DorisConnectorException(errorMsg);
}
```

### 4.6 核心类路径速查

| 类名 | 路径 | 职责 |
|------|------|------|
| `DorisSink` | `sink/DorisSink.java` | Sink 入口 |
| `DorisSinkWriter` | `sink/writer/DorisSinkWriter.java` | 异步流式 Writer |
| `DorisStreamLoad` | `sink/writer/DorisStreamLoad.java` | Stream Load 核心 + 异步上传 |
| `DorisCommitter` | `sink/committer/DorisCommitter.java` | 2PC 提交/回滚 |
| `RecordStream` | `sink/writer/RecordStream.java` | 自定义 InputStream 管道 |
| `LabelGenerator` | `sink/writer/LabelGenerator.java` | Label 生成器 |
| `HttpPutBuilder` | `sink/HttpPutBuilder.java` | HTTP PUT 请求构建器 |
| `DorisCommitInfo` | `sink/committer/DorisCommitInfo.java` | Commit 信息 (hostPort, db, txnId) |
| `DorisSinkState` | `sink/writer/DorisSinkState.java` | Sink 状态 (labelPrefix, checkpointId) |

---

## 5. 四个 Connector 的对比分析

### 5.1 写入方式对比

| 维度 | Elasticsearch | ClickHouse | StarRocks | Doris |
|------|-------------|-----------|-----------|-------|
| **写入协议** | `_bulk` REST API | JDBC / 本地文件 + ATTACH PART | Stream Load HTTP PUT | Stream Load HTTP PUT |
| **批量方式** | 内存累积 + 一次性 _bulk | JDBC batch execute / 文件批量 | 内存累积 + 一次性 PUT | 异步流式 InputStream |
| **序列化格式** | NDJSON (index + doc) | JDBC PreparedStatement / CSV | CSV / JSON | CSV / JSON |
| **连接方式** | ES RestClient (长连接) | JDBC Connection (连接池) | HTTP (短连接/轮询) | HTTP (异步长连接) |
| **最大批量** | maxBatchSize (行数) | bulkSize (行数) | batchMaxSize + batchMaxBytes | batchSize + bufferSize + bufferCount |

### 5.2 性能特点对比

| 维度 | Elasticsearch | ClickHouse | StarRocks | Doris |
|------|-------------|-----------|-----------|-------|
| **写入吞吐** | 中 (REST API 单次批量) | 高 (JDBC batch) / 极高 (文件模式) | 高 (HTTP 直接推送) | 高 (异步流式) |
| **延迟** | 低 (同步批量) | 低 (同步批量) | 低 (同步批量) | 中 (异步提交) |
| **数据倾斜** | 取决于分片策略 | ShardRouter XXHash 路由 | 无特殊处理 | 无特殊处理 |
| **资源消耗** | 内存 (累积请求列表) | 内存 + JDBC 连接 | 内存 (累积字节数组) | 内存 (RecordStream 缓冲区) |

**ClickHouse 文件模式**是四种方案中吞吐最高的，因为它绕过了 JDBC 和 HTTP 的开销，直接生成 ClickHouse 原生格式并通过 `ATTACH PART` 挂载。

### 5.3 一致性保证对比

| 维度 | Elasticsearch | ClickHouse | StarRocks | Doris |
|------|-------------|-----------|-----------|-------|
| **提交语义** | At-Least-Once (无事务) | At-Least-Once (JDBC) / Exactly-Once (文件+ATTACH) | At-Least-Once | Exactly-Once (2PC) / At-Least-Once (非2PC) |
| **Committer** | 无 | 无 (JDBC) / AggCommitter (文件) | 无 | 有 (DorisCommitter) |
| **Checkpoint** | 无状态 | 无状态 | 无状态 | 有状态 (DorisSinkState) |
| **故障恢复** | 重试同一批次 | 无恢复 | 重试 + Label 重生 | 恢复时 abort 未完成事务 |
| **幂等性** | 依赖 ES `_id` | 无内置 | 依赖 Label 去重 | 依赖 Label 去重 |

### 5.4 架构设计对比

| 维度 | Elasticsearch | ClickHouse | StarRocks | Doris |
|------|-------------|-----------|-----------|-------|
| **Sink 基类** | `SeaTunnelSink` | `SeaTunnelSink` (两种) | `AbstractSimpleSink` | `SeaTunnelSink` |
| **Writer 基类** | `SinkWriter` | `SinkWriter` | `AbstractSinkWriter` | `SinkWriter` |
| **Schema 演进** | ADD_COLUMN | 不支持 | ADD/DROP/RENAME/UPDATE | ADD/DROP/RENAME/UPDATE |
| **SaveMode** | 支持 (DefaultSaveModeHandler) | 支持 (DefaultSaveModeHandler) | 支持 (DefaultSaveModeHandler) | 支持 (DefaultSaveModeHandler) |
| **多表支持** | SupportMultiTableSink | SupportMultiTableSink | SupportMultiTableSink | SupportMultiTableSink |

### 5.5 StarRocks vs Doris Stream Load 实现差异

虽然两者都使用 Stream Load 协议，但 SeaTunnel 中的实现有显著差异：

| 维度 | StarRocks | Doris |
|------|-----------|-------|
| **上传模式** | 同步批量 PUT | 异步流式 PUT (InputStreamEntity) |
| **缓冲策略** | Manager 内存累积 | RecordStream 管道 |
| **2PC 支持** | 无 (flush 即完成) | 有 (enable2PC) |
| **Committer** | 无 | DorisCommitter |
| **Label 管理** | StarRocksSinkManager UUID | LabelGenerator (checkpointId 递增) |
| **状态保存** | 无 | DorisSinkState (labelPrefix + checkpointId) |
| **错误检测** | 同步响应检查 | 定时 ScheduledExecutorService |
| **Label 冲突处理** | 轮询状态等待 | 恢复时 abort + 重试时重设 label |

**差异原因：** Doris 的实现更复杂，因为它需要支持 2PC 和 Checkpoint 恢复。StarRocks 的实现更简洁，适合非事务性场景。

---

## 6. 总结与设计模式

### 6.1 设计模式

| 模式 | 体现 |
|------|------|
| **策略模式** | `AuthenticationProvider` (ES) — 多种认证策略；`FileTransfer` (CK) — SCP/RSync 传输；`StarRocksISerializer` — CSV/JSON 序列化 |
| **工厂模式** | `IndexSerializerFactory` (ES) — 索引序列化器；`SeaTunnelRowSerializerFactory` (Doris) — 行序列化器；`FileTransferFactory` (CK) — 文件传输 |
| **模板方法** | `AbstractSimpleSink` (StarRocks) — 简化 Sink 实现 |
| **管道模式** | `RecordStream` (Doris) — 异步数据管道 |
| **观察者模式** | `ScheduledExecutorService` (Doris) — 定时健康检查 |
| **建造者模式** | `HttpPutBuilder` (Doris) — HTTP 请求构建；`JdbcBatchStatementExecutorBuilder` (CK) — SQL 执行器构建 |

### 6.2 核心亮点

1. **Elasticsearch**：三种读取模式（Scroll/SQL/PIT）灵活适配不同场景，`EsRestClient` 封装完整的 ES REST API，支持 Runtime Fields 和 Vectorization 字段
2. **ClickHouse**：双模式设计（JDBC + 文件）覆盖不同数据量场景，`ShardRouter` 基于 XXHash64 实现分布式表数据均匀路由，文件模式通过 `ATTACH PART` 实现极致写入性能
3. **StarRocks**：继承 `AbstractSimpleSink` 实现最简洁的 Sink，Stream Load 协议直接推送，完善的 Label 状态轮询和重试机制
4. **Doris**：异步流式上传 + 2PC 实现 Exactly-Once，`RecordStream` 管道设计优雅地解耦了写入和上传，`ScheduledExecutorService` 提供异步错误检测

### 6.3 可改进方向

1. **Elasticsearch**：`_bulk` 请求体使用 `String.join` 拼接，大字符串拼接可能产生内存压力，可考虑流式写入
2. **ClickHouse**：JDBC 模式每个分片维护独立连接，连接数随分片数线性增长
3. **StarRocks**：无状态管理，Checkpoint 恢复时可能重复写入（依赖 Label 去重）
4. **Doris**：`RecordStream` 使用单线程 `ExecutorService`，高吞吐场景可能成为瓶颈
5. **统一性**：StarRocks 和 Doris 使用相同的 Stream Load 协议，但实现差异较大，可考虑提取公共抽象