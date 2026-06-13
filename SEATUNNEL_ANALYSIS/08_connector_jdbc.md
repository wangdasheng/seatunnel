# Apache SeaTunnel JDBC Connector 深度源码分析

> 代码位置：`seatunnel-connectors-v2/connector-jdbc/`
> 分析版本：基于 2.3.13-release 分支

---

## 目录

1. [总体架构概览](#1-总体架构概览)
2. [Source 端：Split → Reader → Enumerator 完整链路](#2-source-端split--reader--enumerator-完整链路)
3. [Sink 端：Writer → Committer 完整链路](#3-sink-端writer--committer-完整链路)
4. [连接池管理 (HikariCP)](#4-连接池管理-hikaricp)
5. [批量写入与 Upsert 机制](#5-批量写入与-upsert-机制)
6. [方言层 (Dialect) 设计](#6-方言层-dialect-设计)
7. [分片 (Split) 策略](#7-分片-split-策略)
8. [Schema 演进支持](#8-schema-演进支持)
9. [总结与关键设计模式](#9-总结与关键设计模式)

---

## 1. 总体架构概览

### 1.1 JDBC Connector 全链路处理流程

```
┌──────────────────────────────────────────────────┐
│                  JdbcSource                       │
│  implements SeaTunnelSource, SupportParallelism  │
│                   SupportColumnProjection         │
└─────────────┬────────────────────────────────────┘
              │
    ┌─────────┴─────────┐
    │                   │
    ▼                   ▼
┌───────────────┐  ┌──────────────────┐
│ SplitEnumerator│  │   SourceReader   │
│ 生成JDBC Split │  │  读取Split数据    │
└───────┬───────┘  └────────┬─────────┘
        │                   │
        ▼                   ▼
┌───────────────┐  ┌──────────────────┐
│ ChunkSplitter │  │  JdbcInputFormat │
│ 分片策略引擎    │  │  JDBC查询执行器   │
└───────────────┘  └──────────────────┘
                           │
                           ▼
                  ┌──────────────────┐
                  │  JdbcRowConverter │
                  │  ResultSet→Row    │
                  └──────────────────┘

┌──────────────────────────────────────────────────┐
│                   JdbcSink                       │
│  implements SeaTunnelSink, SupportSaveMode       │
│            SupportMultiTableSink                 │
│            SupportSchemaEvolutionSink            │
└─────────────┬────────────────────────────────────┘
              │
    ┌─────────┴──────────┐
    │                    │
    ▼                    ▼
┌──────────────┐  ┌─────────────────┐
│ JdbcSinkWriter│  │ JdbcExactlyOnce │
│ (普通写入)     │  │ SinkWriter (XA) │
└──────┬───────┘  └────────┬────────┘
       │                   │
       ▼                   ▼
┌──────────────────────────────────────┐
│         JdbcOutputFormat             │
│   批量 INSERT/UPDATE/DELETE 执行      │
└──────────────────────────────────────┘
              │
              ▼
┌──────────────────────────────────────┐
│         JdbcDialect                  │
│    SQL方言生成 + 类型转换 + 连接管理   │
└──────────────────────────────────────┘
```

### 1.2 核心类路径速查表

| 类名 | 路径 | 职责 |
|------|------|------|
| `JdbcSource` | `source/JdbcSource.java` | Source 入口，实现并行读取和列投影 |
| `JdbcSourceSplit` | `source/JdbcSourceSplit.java` | 数据分片，含 splitQuery/splitKey/start/end |
| `JdbcSourceSplitEnumerator` | `source/JdbcSourceSplitEnumerator.java` | 分片生成与分配 |
| `JdbcSourceReader` | `source/JdbcSourceReader.java` | 逐 Split 读取数据 |
| `ChunkSplitter` | `source/ChunkSplitter.java` | 分片策略引擎（按主键范围分片） |
| `JdbcInputFormat` | `internal/JdbcInputFormat.java` | JDBC 查询执行器，ResultSet → Row |
| `JdbcSink` | `sink/JdbcSink.java` | Sink 入口，支持多表/保存模式/Schema演进 |
| `JdbcSinkWriter` | `sink/JdbcSinkWriter.java` | 普通批量写入（非 XA） |
| `JdbcExactlyOnceSinkWriter` | `sink/JdbcExactlyOnceSinkWriter.java` | XA 两阶段提交写入 |
| `JdbcSinkAggregatedCommitter` | `sink/JdbcSinkAggregatedCommitter.java` | XA 事务聚合提交器 |
| `JdbcOutputFormat` | `internal/JdbcOutputFormat.java` | 批处理 SQL 执行器 |
| `JdbcDialect` | `internal/dialect/JdbcDialect.java` | 方言接口，定义 SQL 生成和类型转换 |
| `JdbcDialectLoader` | `internal/dialect/JdbcDialectLoader.java` | 方言加载器，根据 URL 自动匹配 |
| `JdbcRowConverter` | `internal/converter/JdbcRowConverter.java` | JDBC 类型 → SeaTunnel 内部类型 |
| `JdbcConnectionProvider` | `internal/connection/JdbcConnectionProvider.java` | 连接提供者接口 |
| `SimpleJdbcConnectionProvider` | `internal/connection/SimpleJdbcConnectionProvider.java` | 简单连接提供者实现 |
| `DataSourceUtils` | `internal/connection/DataSourceUtils.java` | HikariCP 数据源构建工具 |

---

## 2. Source 端：Split → Reader → Enumerator 完整链路

### 2.1 JdbcSource — 入口类

[`JdbcSource.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/source/JdbcSource.java#L44-L47)

```java
public class JdbcSource
        implements SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>,
                SupportParallelism,
                SupportColumnProjection {
```

**关键设计点：**

1. **泛型参数**：`SeaTunnelSource<SeaTunnelRow, JdbcSourceSplit, JdbcSourceState>` — 明确声明了输出类型、Split 类型和状态类型
2. **SupportParallelism**：支持并行读取，通过 Split 机制实现数据分区
3. **SupportColumnProjection**：支持列投影，只读取需要的列以优化性能
4. **Boundedness.BOUNDED**：JDBC 是有界数据源（批量同步）

**构造函数流程：**

```java
public JdbcSource(JdbcSourceConfig jdbcSourceConfig) {
    Class.forName(jdbcSourceConfig.getJdbcConnectionConfig().getDriverName());
    this.jdbcSourceConfig = jdbcSourceConfig;
    this.jdbcSourceTables = JdbcCatalogUtils.getTables(
        jdbcSourceConfig.getJdbcConnectionConfig(),
        jdbcSourceConfig.getTableConfigList());
}
```

- 通过 `Class.forName` 加载 JDBC 驱动
- 通过 `JdbcCatalogUtils.getTables` 获取表元数据（CatalogTable），自动发现表结构

**工厂方法：**

| 方法 | 返回值 | 职责 |
|------|--------|------|
| `createReader()` | `JdbcSourceReader` | 创建读取器，每次创建新连接 |
| `createEnumerator()` | `JdbcSourceSplitEnumerator` | 创建分片枚举器（首次） |
| `restoreEnumerator()` | `JdbcSourceSplitEnumerator` | 从 checkpoint 恢复分片枚举器 |

### 2.2 JdbcSourceSplit — 分片数据结构

[`JdbcSourceSplit.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/source/JdbcSourceSplit.java#L30-L44)

```java
@Data
@AllArgsConstructor
public class JdbcSourceSplit implements SourceSplit {
    private final TablePath tablePath;      // 表路径
    private final String splitId;           // 分片ID
    private final String splitQuery;        // 分片查询SQL
    private final String splitKeyName;      // 分片键名
    private final SeaTunnelDataType splitKeyType; // 分片键类型
    private final Object splitStart;        // 分片起始值
    private final Object splitEnd;          // 分片结束值
}
```

每个 Split 包含：
- **splitQuery**：具体的 SQL 查询（含 WHERE 范围条件）
- **splitStart/End**：分片键的起止范围 [start, end)
- **splitKeyType**：分片键的类型信息，用于正确构造 PreparedStatement 参数

### 2.3 JdbcSourceSplitEnumerator — 分片生成与分配

[`JdbcSourceSplitEnumerator.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/source/JdbcSourceSplitEnumerator.java#L40-L178)

**核心数据结构：**

```java
private final Map<TablePath, JdbcSourceTable> tables;        // 待处理的表
private final ConcurrentLinkedQueue<TablePath> pendingTables; // 待分片的表队列
private final Map<Integer, List<JdbcSourceSplit>> pendingSplits; // 待分配的 Split（按 Reader 分组）
private final ChunkSplitter splitter;                         // 分片策略引擎
```

**run() 方法流程：**

```
while (pendingTables 不为空) {
    1. 取出一个 TablePath
    2. 调用 ChunkSplitter.generateSplits() 生成所有 Split
    3. 按 Split.hashCode() % parallelism 分配到各 Reader
    4. assignSplit() 将 Split 分配给对应 Reader
    5. 如果还有 pendingTables，继续循环
}
发送 NoMoreSplitsEvent 给所有 Reader
```

**Split 分配算法：**

```java
private static int getSplitOwner(String tp, int numReaders) {
    return (tp.hashCode() & Integer.MAX_VALUE) % numReaders;
}
```

使用 splitId 的 hashCode 对并行度取模，保证同一 Split 总是分配给同一 Reader（对 checkpoint 恢复友好）。

**状态管理：**
- `snapshotState()` 返回 `JdbcSourceState`（pendingTables + pendingSplits）
- `restoreEnumerator()` 时从 checkpoint 恢复状态，支持断点续传

### 2.4 JdbcSourceReader — 数据读取

[`JdbcSourceReader.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/source/JdbcSourceReader.java#L38-L102)

**pollNext() 核心逻辑：**

```java
public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
    synchronized (output.getCheckpointLock()) {
        JdbcSourceSplit split = splits.poll();
        if (null != split) {
            try {
                inputFormat.open(split);        // 打开 Split 查询
                while (!inputFormat.reachedEnd()) {
                    SeaTunnelRow seaTunnelRow = inputFormat.nextRecord();
                    output.collect(seaTunnelRow);  // 逐行输出
                }
            } finally {
                inputFormat.close();            // 关闭当前 Split 的 ResultSet
            }
        } else if (noMoreSplit && splits.isEmpty()) {
            context.signalNoMoreElement();      // 通知 Source 已完成
        } else {
            Thread.sleep(1000L);                // 等待新 Split
        }
    }
}
```

**关键设计：**
- 使用 `synchronized (output.getCheckpointLock())` 保证 checkpoint 一致性
- 每个 Split 打开 → 读取 → 关闭，而非一次性打开所有 Split
- `snapshotState()` 返回尚未处理的 Split 列表，用于 checkpoint

### 2.5 JdbcInputFormat — JDBC 查询执行器

[`JdbcInputFormat.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/internal/JdbcInputFormat.java#L51-L171)

**核心流程：**

```
open(inputSplit)
  ├── 获取 splitTableSchema
  ├── statement = chunkSplitter.generateSplitStatement(inputSplit, tableSchema)
  └── resultSet = statement.executeQuery()
      hasNext = resultSet.next()

nextRecord()
  ├── jdbcRowConverter.toInternal(resultSet, tableSchema)
  ├── setTableId / setRowKind(INSERT)
  └── hasNext = resultSet.next()
```

- `JdbcRowConverter` 负责将 JDBC ResultSet 转换为 `SeaTunnelRow`
- 自动设置 `RowKind.INSERT`，因为 JDBC 批量读取都是 INSERT 类型

---

## 3. Sink 端：Writer → Committer 完整链路

### 3.1 JdbcSink — 入口类

[`JdbcSink.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/sink/JdbcSink.java#L67-L71)

```java
public class JdbcSink
        implements SeaTunnelSink<SeaTunnelRow, JdbcSinkState, XidInfo, JdbcAggregatedCommitInfo>,
                SupportSaveMode,
                SupportMultiTableSink,
                SupportSchemaEvolutionSink {
```

**实现的接口及其意义：**

| 接口 | 含义 |
|------|------|
| `SeaTunnelSink<T, StateT, CommitT, AggCommitT>` | 标准 Sink，支持状态和聚合提交 |
| `SupportSaveMode` | 支持建表/删表/重建等保存模式 |
| `SupportMultiTableSink` | 支持多表写入（一个 Sink 写多个表） |
| `SupportSchemaEvolutionSink` | 支持目标表 Schema 自动演进（ADD/DROP/RENAME/MODIFY COLUMN） |

**Writer 选择逻辑：**

```java
public AbstractJdbcSinkWriter createWriter(SinkWriter.Context context) {
    if (jdbcSinkConfig.isExactlyOnce()) {
        // XA 两阶段提交 → JdbcExactlyOnceSinkWriter
        return new JdbcExactlyOnceSinkWriter(...);
    } else {
        if (有主键) {
            // 有主键 → 支持 Upsert 的 JdbcSinkWriter
            return new JdbcSinkWriter(..., primaryKeyIndex);
        } else {
            // 无主键 → 普通 Insert JdbcSinkWriter
            return new JdbcSinkWriter(..., null);
        }
    }
}
```

### 3.2 JdbcSinkWriter — 普通批量写入

[`JdbcSinkWriter.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/sink/JdbcSinkWriter.java#L50-L227)

**继承关系：** `JdbcSinkWriter extends AbstractJdbcSinkWriter<ConnectionPoolManager>`

**核心方法：**

| 方法 | 职责 |
|------|------|
| `write(element)` | 写入单行数据，调用 `outputFormat.writeRecord()` |
| `flushData()` | 刷新缓冲区，调用 `outputFormat.flush()` |
| `prepareCommit()` | 两阶段提交的 prepare 阶段 |
| `snapshotState()` | 返回空列表（无状态需要保存） |
| `handleFlushEvent()` | 处理 Flush 事件，触发数据刷新 |
| `initMultiTableResourceManager()` | 初始化多表资源共享管理器（HikariCP 连接池） |

**写路径：**

```
write(row)
  └── outputFormat.writeRecord(row)
        └── JdbcBatchStatementExecutor.addToBatch(row)
              └── 达到 batchSize 时 executeBatch()
```

**连接池初始化（多表场景）：**

```java
public MultiTableResourceManager<ConnectionPoolManager> initMultiTableResourceManager(
        int tableSize, int queueSize) {
    HikariDataSource ds = new HikariDataSource();
    ds.setMaximumPoolSize(queueSize);
    ds.setJdbcUrl(url);
    ds.setUsername/password;
    ds.setAutoCommit(config.isAutoCommit());
    return new JdbcMultiTableResourceManager(new ConnectionPoolManager(ds));
}
```

### 3.3 JdbcExactlyOnceSinkWriter — XA 两阶段提交

当 `isExactlyOnce = true` 时启用，利用 XA 事务实现 Exactly-Once 语义：

- **Phase 1 (prepare)**：所有 Writer 调用 `prepareCommit()`，返回 `XidInfo`
- **Phase 2 (commit)**：`JdbcSinkAggregatedCommitter` 聚合所有 Xid 后统一提交

### 3.4 SaveMode 处理

[`JdbcSink.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/sink/JdbcSink.java#L259-L311)

`getSaveModeHandler()` 返回 `JdbcSaveModeHandler`（或 `IrisSaveModeHandler`），支持的保存模式：

- **CREATE_TABLE_WHEN_NOT_EXIST**：表不存在时自动建表
- **DROP_TABLE**：先删表再建表
- **RECREATE_SCHEMA**：重建 Schema
- **ERROR_WHEN_SCHEMA_NOT_EXIST**：表不存在时报错

---

## 4. 连接池管理 (HikariCP)

### 4.1 连接提供者体系

```
JdbcConnectionProvider (接口)
  ├── SimpleJdbcConnectionProvider        // 简单单连接（无连接池）
  ├── SimpleJdbcConnectionPoolProviderProxy  // 连接池代理（多表共享）
  └── DdsqlJdbcConnectionPoolProviderProxy   // DSQL 专用连接池
```

### 4.2 连接池生命周期

```
JdbcSinkWriter.initMultiTableResourceManager()
  → 创建 HikariDataSource (连接池)
  → 所有 Writer 共享同一个连接池
  → setMultiTableResourceManager() 为每个 Writer 分配连接池索引
  → close() 时关闭连接池
```

**HikariCP 配置参数：**

```java
ds.setIdleTimeout(30 * 1000);       // 空闲超时 30 秒
ds.setMaximumPoolSize(queueSize);   // 最大连接数 = 并发写入数
ds.setAutoCommit(autoCommit);       // 自动提交
```

### 4.3 DataSourceUtils

[`DataSourceUtils.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/internal/connection/DataSourceUtils.java)

工具类，负责根据 `JdbcConnectionConfig` 构建 HikariCP DataSource，支持：
- 连接属性注入
- 驱动类名自动加载
- 连接池参数配置

---

## 5. 批量写入与 Upsert 机制

### 5.1 JdbcBatchStatementExecutor

[`JdbcBatchStatementExecutor.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/internal/executor/JdbcBatchStatementExecutor.java)

批处理 SQL 执行器接口，定义了批量写入的核心契约：

- `addToBatch(row)` — 将行添加到批次
- `executeBatch()` — 执行批次
- `close()` — 关闭资源

### 5.2 Upsert 语句生成

[`JdbcDialect.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/internal/dialect/JdbcDialect.java#L240-L241)

```java
Optional<String> getUpsertStatement(
    String database, String tableName, String[] fieldNames, String[] uniqueKeyFields);
```

各数据库方言的 Upsert 实现：

| 数据库 | Upsert 语法 |
|--------|------------|
| MySQL | `INSERT INTO ... ON DUPLICATE KEY UPDATE ...` |
| PostgreSQL | `INSERT INTO ... ON CONFLICT ... DO UPDATE SET ...` |
| Oracle | `MERGE INTO ... WHEN MATCHED THEN UPDATE ...` |
| SQL Server | `MERGE INTO ... WHEN MATCHED THEN UPDATE ...` |
| 不支持方言 | 回退到 `SELECT EXISTS` + `INSERT`/`UPDATE` |

### 5.3 写入策略选择

```
有主键 + 支持 Upsert → 使用 Upsert 语句（高性能）
有主键 + 不支持 Upsert → SELECT EXISTS → INSERT 或 UPDATE
无主键 → 纯 INSERT
```

### 5.4 事务管理

```java
// prepareCommit() 中的事务提交
if (!connectionProvider.getConnection().getAutoCommit()) {
    connectionProvider.getConnection().commit();
}
```

- 非自动提交模式：在每个 checkpoint 时手动 commit
- 自动提交模式：每条 SQL 自动提交

---

## 6. 方言层 (Dialect) 设计

### 6.1 JdbcDialect 接口

[`JdbcDialect.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/internal/dialect/JdbcDialect.java#L67-L67)

这是整个 JDBC Connector 最核心的抽象，定义了数据库差异的统一接口：

```java
public interface JdbcDialect extends Serializable {
    String dialectName();                    // 方言名称
    JdbcRowConverter getRowConverter();     // 类型转换器
    JdbcDialectTypeMapper getJdbcDialectTypeMapper(); // 类型映射
    // SQL 生成
    String getInsertIntoStatement(...);     // INSERT 语句
    String getUpdateStatement(...);         // UPDATE 语句
    String getDeleteStatement(...);         // DELETE 语句
    Optional<String> getUpsertStatement(...); // UPSERT 语句
    String getRowExistsStatement(...);      // 行存在检查
    // 分片支持
    Object[] sampleDataFromColumn(...);     // 列采样
    Object queryNextChunkMax(...);          // 计算下一个分片边界
    // Schema 变更
    void applySchemaChange(...);            // 应用 Schema 变更
    // 连接管理
    JdbcConnectionProvider getJdbcConnectionProvider(...);
    // 标识符引用
    String quoteIdentifier(String identifier);
    String tableIdentifier(String database, String tableName);
}
```

### 6.2 支持的方言实现

| 方言类 | 路径 | 数据库 |
|--------|------|--------|
| `MysqlDialect` | `dialect/mysql/MysqlDialect.java` | MySQL / MariaDB |
| `PostgresDialect` | `dialect/postgresql/PostgresDialect.java` | PostgreSQL |
| `OracleDialect` | `dialect/oracle/OracleDialect.java` | Oracle |
| `SqlServerDialect` | `dialect/sqlserver/SqlServerDialect.java` | SQL Server |
| `Db2Dialect` | `dialect/db2/Db2Dialect.java` | IBM DB2 |
| `OceanBaseDialect` | `dialect/oceanbase/OceanBaseDialect.java` | OceanBase |
| `DamengDialect` | `dialect/dameng/DamengDialect.java` | 达梦数据库 |
| `Gbase8aDialect` | `dialect/gbase8a/Gbase8aDialect.java` | 南大通用 GBase |
| `KingbaseDialect` | `dialect/kingbase/KingbaseDialect.java` | 人大金仓 |
| `IrisDialect` | `dialect/iris/IrisDialect.java` | InterSystems IRIS |
| ... | ... | 还有更多 |

### 6.3 JdbcDialectLoader — 自动方言匹配

根据 JDBC URL 自动匹配相应的方言实现：

```java
JdbcDialect dialect = JdbcDialectLoader.load(url, driverClassName, compatibleMode);
```

匹配逻辑基于 URL 前缀（如 `jdbc:mysql://` → `MysqlDialect`）。

---

## 7. 分片 (Split) 策略

### 7.1 ChunkSplitter 分片引擎

`ChunkSplitter` 是 JDBC Source 的分片策略引擎，负责将大表拆分为多个并行读取的 Split。

**分片算法：**

```
1. 查询表的总行数 approximateRowCntStatement()
2. 对分片键列进行采样 sampleDataFromColumn()
3. 根据采样结果计算分片边界
4. 为每个分片生成 splitQuery（WHERE splitKey >= ? AND splitKey < ?）
```

### 7.2 分片键选择

```
优先级：
1. 用户指定的 partition_column
2. 主键的第一列
3. 唯一索引的第一列
4. 无合适列 → 单分片（不并行）
```

### 7.3 分片查询生成

```java
// 典型的分片查询
SELECT * FROM table_name
WHERE split_key >= ? AND split_key < ?
```

通过 `generateSplitStatement()` 将 Split 信息转换为 PreparedStatement。

---

## 8. Schema 演进支持

### 8.1 SupportSchemaEvolutionSink

[`JdbcSink.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/sink/JdbcSink.java#L333-L339)

```java
public List<SchemaChangeType> supports() {
    return Arrays.asList(
        SchemaChangeType.ADD_COLUMN,
        SchemaChangeType.DROP_COLUMN,
        SchemaChangeType.RENAME_COLUMN,
        SchemaChangeType.UPDATE_COLUMN);
}
```

JDBC Sink 支持四种 Schema 变更类型：
- **ADD_COLUMN**：自动添加新列
- **DROP_COLUMN**：自动删除列
- **RENAME_COLUMN**：自动重命名列
- **UPDATE_COLUMN**：自动修改列类型

### 8.2 applySchemaChange 实现

[`JdbcDialect.java`](file:///Users/wangzhijun/sourceCode/seatunnel/seatunnel-connectors-v2/connector-jdbc/src/main/java/org/apache/seatunnel/connectors/seatunnel/jdbc/internal/dialect/JdbcDialect.java#L476-L774)

统一的 Schema 变更处理流程：

```
事件分发
├── AlterTableColumnsEvent → 遍历子事件递归处理
├── AlterTableChangeColumnEvent → ALTER TABLE ... CHANGE COLUMN ...
├── AlterTableModifyColumnEvent → ALTER TABLE ... MODIFY COLUMN ...
├── AlterTableAddColumnEvent → ALTER TABLE ... ADD COLUMN ...
└── AlterTableDropColumnEvent → ALTER TABLE ... DROP COLUMN ...
```

跨数据库 Schema 变更时，通过 `TypeConverter.reconvert()` 将源类型转换为目标数据库类型。

---

## 9. 总结与关键设计模式

### 9.1 设计模式

| 模式 | 体现 |
|------|------|
| **策略模式** | `JdbcDialect` — 不同数据库的 SQL 生成策略 |
| **工厂模式** | `JdbcDialectLoader` — 根据 URL 自动创建方言实例 |
| **模板方法** | `JdbcDialect` 提供默认实现，子类按需覆盖 |
| **建造者模式** | `JdbcOutputFormatBuilder` — 构建 OutputFormat |
| **代理模式** | `SimpleJdbcConnectionPoolProviderProxy` — 连接池代理 |
| **观察者模式** | `SchemaChangeEvent` 传播机制 |

### 9.2 核心亮点

1. **方言层抽象**：通过 `JdbcDialect` 接口统一了 30+ 种数据库的差异，新增数据库只需实现一个方言类
2. **Exactly-Once**：通过 XA 两阶段提交实现端到端精确一次语义
3. **Schema 自动演进**：支持 DDL 变更自动传播到目标表，大幅减少运维成本
4. **智能分片**：基于采样数据动态计算分片边界，避免数据倾斜
5. **多表共享连接池**：通过 `MultiTableResourceManager` 减少连接数，提升资源利用率

### 9.3 可改进方向

1. **分片策略可扩展性**：当前 `ChunkSplitter` 为单一实现，可抽象为策略接口支持更多分片策略
2. **连接池配置**：HikariCP 参数硬编码，应支持通过配置文件调整
3. **方言测试覆盖**：30+ 方言的测试覆盖率不一致，需要统一测试框架