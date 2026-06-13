# Apache SeaTunnel CDC 同步机制深度源码分析

> 代码位置：`seatunnel-connectors-v2/connector-cdc/`
> 分析版本：基于 2.3.13-release 分支

---

## 目录

1. [总体架构概览](#1-总体架构概览)
2. [CDC 统一抽象层分析](#2-cdc-统一抽象层分析)
3. [增量快照 (Incremental Snapshot) 算法](#3-增量快照-incremental-snapshot-算法)
4. [MySQL CDC 详解](#4-mysql-cdc-详解)
5. [PostgreSQL CDC 详解](#5-postgresql-cdc-详解)
6. [其他 CDC 支持](#6-其他-cdc-支持)
7. [Schema 变更传播机制](#7-schema-变更传播机制)
8. [一致性保证](#8-一致性保证)
9. [关键数据结构与状态流转](#9-关键数据结构与状态流转)
10. [总结与设计模式](#10-总结与设计模式)

---

## 1. 总体架构概览

### 1.1 CDC 整体处理流程

SeaTunnel CDC 基于 Debezium 引擎构建，通过统一的抽象层支持多种数据库的 Change Data Capture。核心设计思想是“Incremental Snapshot（增量快照）”算法——先将存量数据按主键分片快照，然后从快照过程中记录的高低水位切换到增量日志流，确保数据不丢不重。

```
┌──────────────────────────────────────────────────────────────────────┐
│                       IncrementalSource<T, C>                        │
│        implements SeaTunnelSource<T, SourceSplitBase, ...>           │
│                        (connector-cdc-base)                          │
│                                                                      │
│   每个具体 CDC 连接器继承此类：                                        │
│   MySqlIncrementalSource / PostgresIncrementalSource /               │
│   OracleIncrementalSource / SqlServerIncrementalSource / ...         │
└───────────────────────────┬──────────────────────────────────────────┘
                            │
          ┌─────────────────┴─────────────────┐
          │                                   │
          ▼                                   ▼
┌──────────────────────┐         ┌──────────────────────────────┐
│ IncrementalSourceEnum│         │    IncrementalSourceReader   │
│      erator          │         │  (SingleThreadMultiplexSRB)  │
│  Split 生成与分配     │         │   Split 读取 + 事件发射      │
│     ┌──────────┐     │         │     ┌──────────────────┐     │
│     │HybridSplit│     │         │     │IncrementalSource │     │
│     │ Assigner  │     │         │     │  SplitReader     │     │
│     └─────┬─────┘     │         │     └───────┬──────────┘     │
│           │            │         │             │                │
│   ┌───────┴────────┐   │         │   ┌─────────┴──────────┐    │
│   │SnapshotSplit   │   │         │   │        │           │    │
│   │  Assigner      │   │         │   ▼        ▼           │    │
│   │  ┌───────────┐ │   │         │ ┌───────┐┌──────────┐│    │
│   │  │ChunkSplitte│ │   │         │ │Scan   ││Stream    ││    │
│   │  │r(分片策略) │ │   │         │ │Fetcher││Fetcher   ││    │
│   │  └───────────┘ │   │         │ │(快照) ││(增量日志)││    │
│   └────────────────┘   │         │ └───┬───┘└────┬─────┘│    │
│   ┌────────────────┐   │         │     │         │      │    │
│   │IncrementalSplit│   │         │     ▼         ▼      │    │
│   │  Assigner      │   │         │ ┌──────────────────┐ │    │
│   └────────────────┘   │         │ │  FetchTask(方言)  │ │    │
└──────────────────────┘         │ │ MySqlBinlogFetch  │ │    │
                                  │ │ PostgresWalFetch  │ │    │
                                  │ └──────────────────┘ │    │
                                  │           │           │    │
                                  │           ▼           │    │
                                  │ ┌──────────────────┐ │    │
                                  │ │IncrementalSource │ │    │
                                  │ │  RecordEmitter   │ │    │
                                  │ └────────┬─────────┘ │    │
                                  │          │            │    │
                                  │          ▼            │    │
                                  │ ┌──────────────────┐ │    │
                                  │ │DebeziumDeseriali │ │    │
                                  │ │ zationSchema     │ │    │
                                  │ └────────┬─────────┘ │    │
                                  └──────────┼───────────┘    │
                                             │                │
                                             ▼                │
                                  ┌──────────────────────┐    │
                                  │   Collector<T>       │    │
                                  │   (输出到下游 Sink)   │    │
                                  └──────────────────────┘    │
```

### 1.2 核心类路径速查表

| 类名 | 路径 (connector-cdc-base) | 职责 |
|------|------|------|
| `IncrementalSource` | `source/IncrementalSource.java` | CDC Source 入口基类，实现 Enumerator/Reader 创建 |
| `IncrementalSourceReader` | `reader/IncrementalSourceReader.java` | 多路复用读取器，管理 Snapshot→Incremental 切换 |
| `IncrementalSourceSplitReader` | `reader/IncrementalSourceSplitReader.java` | 分片读取器，按 Split 类型路由到 ScanFetcher/StreamFetcher |
| `IncrementalSourceEnumerator` | `enumerator/IncrementalSourceEnumerator.java` | Enumerator 实现，管理 Split 分配 |
| `HybridSplitAssigner` | `enumerator/HybridSplitAssigner.java` | 混合分片分配器，组合 Snapshot + Incremental |
| `SnapshotSplitAssigner` | `enumerator/SnapshotSplitAssigner.java` | 快照阶段分配器，管理表级分片 |
| `IncrementalSplitAssigner` | `enumerator/IncrementalSplitAssigner.java` | 增量阶段分配器，构建 IncrementalSplit |
| `IncrementalSourceScanFetcher` | `reader/external/IncrementalSourceScanFetcher.java` | 快照数据抓取器，支持 At-Least-Once 和 Exactly-Once |
| `IncrementalSourceStreamFetcher` | `reader/external/IncrementalSourceStreamFetcher.java` | 增量日志抓取器，含 SchemaChange 分流 |
| `IncrementalSourceRecordEmitter` | `reader/IncrementalSourceRecordEmitter.java` | 记录发射器，处理 Watermark + SchemaChange 事件 |
| `SnapshotSplit` | `split/SnapshotSplit.java` | 快照分片，含 splitStart/End、low/high Watermark |
| `IncrementalSplit` | `split/IncrementalSplit.java` | 增量分片，含 startupOffset/stopOffset/已完成快照信息 |
| `Offset` | `offset/Offset.java` | 偏移量抽象基类，支持 compareTo/equals |
| `BinlogOffset` | `mysql/.../BinlogOffset.java` | MySQL Binlog 偏移量 (文件名+位置+GTID) |
| `LsnOffset` | `postgres/.../LsnOffset.java` | PostgreSQL LSN 偏移量 |
| `WatermarkEvent` | `split/wartermark/WatermarkEvent.java` | Watermark 事件工具类 (LOW/HIGH/END/SCHEMA_CHANGE) |
| `DataSourceDialect` | `dialect/DataSourceDialect.java` | 方言接口，定义所有 CDC 数据库必须实现的方法 |
| `JdbcDataSourceDialect` | `dialect/JdbcDataSourceDialect.java` | JDBC 方言扩展接口，含主键和约束键查询 |
| `AbstractSchemaChangeResolver` | `schema/AbstractSchemaChangeResolver.java` | Schema 变更解析基类，DDL 捕获与传播 |
| `FetchTask` | `reader/external/FetchTask.java` | 抓取任务接口，定义 execute/isRunning/shutdown |
| `SnapshotPhaseState` | `enumerator/state/SnapshotPhaseState.java` | 快照阶段状态，checkpoint 序列化 |
| `IncrementalPhaseState` | `enumerator/state/IncrementalPhaseState.java` | 增量阶段状态 |
| `HybridPendingSplitsState` | `enumerator/state/HybridPendingSplitsState.java` | 混合状态 (Snapshot + Incremental) |

### 1.3 支持的数据库及对应实现

| 数据库 | 模块 | 方言类 | 日志采集机制 | 偏移量类型 |
|--------|------|--------|--------------|-----------|
| MySQL | `connector-cdc-mysql` | `MySqlDialect` | Binlog (BinaryLogClient) | `BinlogOffset` (file+pos+GTID) |
| PostgreSQL | `connector-cdc-postgres` | `PostgresDialect` | WAL (Logical Replication) | `LsnOffset` (LSN) |
| Oracle | `connector-cdc-oracle` | `OracleDialect` | RedoLog (LogMiner) | `RedoLogOffset` (SCN) |
| SQL Server | `connector-cdc-sqlserver` | `SqlServerDialect` | Transaction Log (CDC) | `LsnOffset` (LSN) |
| MongoDB | `connector-cdc-mongodb` | `MongodbDialect` | ChangeStream | `ChangeStreamOffset` (ResumeToken) |
| TiDB | `connector-cdc-tidb` | `TiDBSource` | TiKV CDC (独立实现) | `RowKeyWithTs` |
| OpenGauss | `connector-cdc-opengauss` | (基于 Postgres 方言扩展) | WAL | `LsnOffset` |

---

## 2. CDC 统一抽象层分析

### 2.1 IncrementalSource 基类 — CDC Source 入口

[`IncrementalSource.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/IncrementalSource.java)

```java
public abstract class IncrementalSource<T, C extends SourceConfig>
        implements SeaTunnelSource<T, SourceSplitBase, PendingSplitsState> {
```

**泛型参数：**
- `T`：输出数据类型（`SeaTunnelRow` 或 `String` for JSON）
- `C`：`SourceConfig` 的子类型（如 `MySqlSourceConfig`）

**关键设计点：**

1. **静态初始化块加载 DriverManager**：在 JDK 8 中避免 DriverManager 静态初始化与特定驱动类并发加载的死锁
2. **动态 Boundedness 判断**：
```java
public Boundedness getBoundedness() {
    return stopMode == StopMode.NEVER ? Boundedness.UNBOUNDED : Boundedness.BOUNDED;
}
```
- `StopMode.NEVER` → `UNBOUNDED`（流式模式）
- 其它模式 → `BOUNDED`（批模式）

**构造函数流程：**
```
IncrementalSource(ReadonlyConfig, List<CatalogTable>)
  ├── updateCatalogTableMetadata()  → 添加元数据列 (event_time, delay)
  ├── getStartupConfig()            → 启动模式配置 (INITIAL/EARLIEST/LATEST/SPECIFIC/TIMESTAMP)
  ├── getStopConfig()               → 停止模式配置 (NEVER/LATEST/SPECIFIC/TIMESTAMP)
  ├── createSourceConfigFactory()   → 抽象方法，子类实现
  ├── createDataSourceDialect()     → 抽象方法，子类实现
  ├── createDebeziumDeserializationSchema() → 抽象方法，子类实现
  └── createOffsetFactory()         → 抽象方法，子类实现
```

**抽象方法（子类必须实现）：**

| 方法 | 职责 |
|------|------|
| `getStartupModeOption()` | 返回 StartupMode 配置项，每个数据库有不同选项 |
| `getStopModeOption()` | 返回 StopMode 配置项 |
| `createSourceConfigFactory(ReadonlyConfig)` | 创建 SourceConfig 工厂 |
| `createDebeziumDeserializationSchema(ReadonlyConfig)` | 创建反序列化 Schema |
| `createDataSourceDialect(ReadonlyConfig)` | 创建方言（对 DatabaseIdentifier 决策） |
| `createOffsetFactory(ReadonlyConfig)` | 创建 Offset 工厂 |

**Enumerator 创建流程 (createEnumerator)：**
```java
// Step 1: 发现数据集合（表）
final List<TableId> remainingTables = dataSourceDialect.discoverDataCollections(sourceConfig);

// Step 2: 根据 startupMode 选择分片分配器
if (startupMode == StartupMode.INITIAL) {
    // 混合模式：先快照，后增量
    splitAssigner = new HybridSplitAssigner<>(...);
} else {
    // 纯增量模式：直接从指定位置读取日志
    splitAssigner = new IncrementalSplitAssigner<>(...);
}
return new IncrementalSourceEnumerator(enumeratorContext, splitAssigner);
```

**Reader 创建流程 (createReader)：**
```java
// Step 1: 加载 JDBC 驱动
Class.forName(driverName);

// Step 2: 创建单 SubTask 的 SourceConfig（含唯一 serverId）
C sourceConfig = configFactory.create(subtaskIndex);

// Step 3: 构建 SplitReader 供应器
Supplier<IncrementalSourceSplitReader<C>> splitReaderSupplier = () ->
    new IncrementalSourceSplitReader<>(subtaskId, dataSourceDialect, sourceConfig, schemaChangeResolver);

// Step 4: 创建 IncrementalSourceReader
return new IncrementalSourceReader<>(elementsQueue, splitReaderSupplier, recordEmitter, ...);
```

**恢复流程 (restoreEnumerator)：**
```java
if (checkpointState instanceof HybridPendingSplitsState) {
    // 快照未完成，从断点恢复剩余表和分片
    checkpointState = restore(capturedTables, (HybridPendingSplitsState) checkpointState);
    splitAssigner = new HybridSplitAssigner<>(...checkpointState...);
} else if (checkpointState instanceof IncrementalPhaseState) {
    // 快照已完成，直接进入增量
    splitAssigner = new IncrementalSplitAssigner<>(...);
}
```

`restore()` 方法处理表变更场景——新表加入、旧表移除时会自动调整 SnapshotPhaseState 中的 `remainingTables`、`remainingSplits`、`assignedSplits` 等字段。

### 2.2 IncrementalSourceReader — 多路复用读取器

[`IncrementalSourceReader.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/reader/IncrementalSourceReader.java)

```java
public class IncrementalSourceReader<T, C extends SourceConfig>
        extends SingleThreadMultiplexSourceReaderBase<
                SourceRecords, T, SourceSplitBase, SourceSplitStateBase> {
```

这是整个 CDC 读取链路的核心，负责将 Snapshot Splits（并行）和 Incremental Split（单并行度）统一管理。

**关键字段：**
```java
private final Map<String, SnapshotSplit> finishedUnackedSplits;  // 已完成但未确认的快照分片
private volatile boolean running = false;                          // 运行状态
private transient volatile Offset snapshotChangeLogOffset;         // 快照切换时的 offset
private final AtomicBoolean needSendSplitRequest;                  // 是否需请求新分片
```

**addSplits() — 分片添加逻辑：**
```
addSplits(List<SourceSplitBase>)
  ├── 遍历 splits
  │   ├── isSnapshotSplit → isSnapshotReadFinished?
  │   │   ├── true  → finishedUnackedSplits.put()  // 已完成快照
  │   │   └── false → unfinishedSplits.add()        // 未完成快照
  │   └── isIncrementalSplit → unfinishedSplits.add()
  ├── reportFinishedSnapshotSplitsIfNeed()          // 上报已完成快照
  └── super.addSplits(unfinishedSplits)             // 添加到基类处理
```

**onSplitFinished() — 分片完成回调：**
```
onSplitFinished(Map<String, SourceSplitStateBase>)
  ├── 所有完成的 split 加入 finishedUnackedSplits
  ├── reportFinishedSnapshotSplitsIfNeed()  → 发送 CompletedSnapshotSplitsReportEvent 给 Enumerator
  └── context.sendSplitRequest()            → 请求新分片
```

**snapshotState() — Checkpoint 状态快照：**
```java
public List<SourceSplitBase> snapshotState(long checkpointId) {
    List<SourceSplitBase> stateSplits = super.snapshotState(checkpointId);
    // 包含未完成 splits + finishedUnackedSplits
    // 进入增量阶段时，将当前 schema 信息序列化到 IncrementalSplit
    if (isIncrementalSplitPhase(unfinishedSplits)) {
        return snapshotCheckpointDataType(incrementalSplit);
    }
    return unfinishedSplits;
}
```

**initializedState() — 初始化分片状态：**
```java
protected SourceSplitStateBase initializedState(SourceSplitBase split) {
    if (split.isSnapshotSplit()) {
        return new SnapshotSplitState(split.asSnapshotSplit());
    } else {
        // 增量分片可能从 checkpoint 恢复 schema
        IncrementalSplitState splitState = new IncrementalSplitState(incrementalSplit);
        if (splitState.autoEnterPureIncrementPhaseIfAllowed()) {
            // 发送 CompletedSnapshotPhaseEvent 清理旧状态
            context.sendSourceEventToEnumerator(new CompletedSnapshotPhaseEvent(tableIds));
        }
        return splitState;
    }
}
```

### 2.3 IncrementalSourceSplitReader — 分片读取器（路由核心）

[`IncrementalSourceSplitReader.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/reader/IncrementalSourceSplitReader.java)

这是整个读取链路中最关键的“路由”类。根据 Split 类型将实际读取工作委托给不同的 Fetcher。

```java
public class IncrementalSourceSplitReader<C extends SourceConfig>
        implements SplitReader<SourceRecords, SourceSplitBase> {
    private Fetcher<SourceRecords, SourceSplitBase> currentFetcher;
    private String currentSplitId;
    private String emittedFinishedSplitId;
```

**核心路由逻辑 (checkSplitOrStartNext)：**
```
checkSplitOrStartNext()
  ├── 当前是 StreamFetcher? → return (流式Fetcher持久存活)
  ├── canAssignNextSplit()?  → 当前 Fetcher 为 null 或已完成
  │   ├── 取下一个 split
  │   └── split.isSnapshotSplit()?
  │       ├── true  → 首次创建 IncrementalSourceScanFetcher (可复用)
  │       └── false → 关闭快照 fetcher → 创建 IncrementalSourceStreamFetcher
  └── currentFetcher.submitTask(dataSourceDialect.createFetchTask(nextSplit))
```

**fetch() 方法流程：**
```
fetch()
  ├── checkSplitOrStartNext()              → 按需切换到下一个 split
  ├── checkNeedStopBinlogReader()          → (TODO) 检查是否需要停止
  ├── hasEmittedCurrentSplitFinished()?    → 已发射完成标记? 返回 NoSplitRecords
  ├── currentFetcher.pollSplitRecords()    → 从队列拉取数据
  │   ├── 有数据  → ChangeEventRecords.forRecords(splitId, dataIt)
  │   └── 无数据  → finishedSnapshotSplit() 发送完成标记
  └── return
```

**Fetcher 切换时机示意图：**

```
时间线 ─────────────────────────────────────────────────────►

Reader 0: [SnapshotSplit-A] ──► [SnapshotSplit-B] ──► [IncrementalSplit-0]
                        ScanFetcher 复用                StreamFetcher (单并行)
Reader 1: [SnapshotSplit-C] ──► [SnapshotSplit-D] ──► (空闲等待)
                        ScanFetcher 复用
Reader 2: [SnapshotSplit-E] ──► (空闲等待)
                        ScanFetcher
...

快照阶段 (多并行)              │  增量阶段 (单并行，由 Reader 0 执行)
```

- **快照阶段**：多个 Reader 并行消费 SnapshotSplit，共享一个 ScanFetcher（注意：实际每个 Reader 有独立的 SplitReader 实例，各自有独立的 ScanFetcher）
- **增量阶段**：只有 1 个 IncrementalSplit，由 Reader 0 消费，切换为 StreamFetcher

### 2.4 RecordEmitter — 事件发射与 Watermark 处理

[`IncrementalSourceRecordEmitter.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/reader/IncrementalSourceRecordEmitter.java)

**processElement() 事件处理矩阵：**

| 事件类型 | Split 状态 | 处理动作 |
|----------|------------|----------|
| LOW_WATERMARK | Snapshot | `splitState.setLowWatermark(watermark)` |
| HIGH_WATERMARK | Snapshot | `splitState.setHighWatermark(watermark)` |
| SCHEMA_CHANGE_BEFORE/AFTER | Incremental | `emitElement(element, output)` |
| SCHEMA_CHANGE | Incremental | 更新 `startupOffset` + `emitElement` |
| DATA_CHANGE / HEARTBEAT | Incremental | 更新 `startupOffset` + `emitElement` |
| DATA_CHANGE / HEARTBEAT | Snapshot | `emitElement(element, output)` |

**纯增量阶段进入检测 (markEnterPureIncrementPhase)：**

```java
if (incrementalSplitState.markEnterPureIncrementPhaseIfNeed(position)) {
    // 当前 offset 超过 maxSnapshotSplitsHighWatermark
    // 发送 CompletedSnapshotPhaseEvent 清理快照状态
    context.sendSourceEventToEnumerator(new CompletedSnapshotPhaseEvent(tableIds));
}
```

---

## 3. 增量快照 (Incremental Snapshot) 算法

### 3.1 核心思想

增量快照算法的核心目标是：**在不锁表的情况下，将存量数据与增量数据无缝衔接**。

**两步走：**
1. **Snapshot 阶段**：将表按主键范围 [min, max] 分片，并行读取每个分片的存量数据
2. **Incremental 阶段**：从快照开始前的日志位置启动增量消费，通过高低水位去重

**Watermark（水位线）机制：**
- **Low Watermark**：快照分片开始时当前日志位置标记 → 快照结束、增量开始时，此位置之前的数据需要被快照数据覆盖
- **High Watermark**：快照分片结束时当前日志位置标记 → 此位置之后的数据都是纯增量数据

### 3.2 SnapshotSplit — 快照分片数据结构

[`SnapshotSplit.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/split/SnapshotSplit.java)

```java
public class SnapshotSplit extends SourceSplitBase {
    private final TableId tableId;           // 所属表
    private final SeaTunnelRowType splitKeyType; // 分片键类型
    private final Object[] splitStart;       // 分片起始值（含）
    private final Object[] splitEnd;         // 分片结束值（含）
    private final Offset lowWatermark;        // 低水位
    private final Offset highWatermark;       // 高水位
}
```

**生命周期：**
```
创建时: {tableId, splitStart, splitEnd, lowWatermark=null, highWatermark=null}
  → 分配给 Reader 时，获取 lowWatermark（记录当前日志位置）
  → 读取完成后，获取 highWatermark（记录读取结束时的日志位置）
  → isSnapshotReadFinished() == true  → 返回给 Enumerator 确认
```

### 3.3 IncrementalSplit — 增量分片数据结构

[`IncrementalSplit.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/split/IncrementalSplit.java)

```java
public class IncrementalSplit extends SourceSplitBase {
    private final List<TableId> tableIds;                            // 需要捕获的表列表
    private final Offset startupOffset;                              // 启动 offset (最小快照 highWatermark)
    private final Offset stopOffset;                                 // 停止 offset (可能为空)
    private final List<CompletedSnapshotSplitInfo> completedSnapshotSplitInfos; // 已完成快照信息
    private List<CatalogTable> checkpointTables;                     // Checkpoint 时的 Schema
    private final Map<TableId, byte[]> historyTableChanges;          // Debezium 历史表变更
}
```

**startupOffset 的计算逻辑 (IncrementalSplitAssigner.createIncrementalSplit)：**
```java
// Exactly-Once 模式使用 highWatermark (确保快照数据后的所有变更都覆盖)
// 非 Exactly-Once 模式使用 lowWatermark (At-Least-Once)
Offset splitOffset = sourceConfig.isExactlyOnce()
    ? splitWatermark.getHighWatermark()
    : splitWatermark.getLowWatermark();

// 对所有已完成快照分片取最小 offset 作为启动位置
if (minOffset == null || splitOffset.isBefore(minOffset)) {
    minOffset = splitOffset;
}
```

### 3.4 HybridSplitAssigner — 混合分片分配器

[`HybridSplitAssigner.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/enumerator/HybridSplitAssigner.java)

```java
public class HybridSplitAssigner<C extends SourceConfig> implements SplitAssigner {
    private final SnapshotSplitAssigner<C> snapshotSplitAssigner;
    private final IncrementalSplitAssigner<C> incrementalSplitAssigner;
```

**getNext() — 分片分配策略：**
```
getNext()
  ├── !snapshotSplitAssigner.noMoreSplits()? → snapshotSplitAssigner.getNext()
  │   (快照阶段：逐个分配 SnapshotSplit)
  │
  ├── !snapshotSplitAssigner.isCompleted()? → Optional.empty()
  │   (快照分片已分配完但未确认完成 → 等待)
  │
  ├── !incrementalSplitAssigner.noMoreSplits()? → incrementalSplitAssigner.getNext()
  │   (增量阶段：分配 IncrementalSplit)
  │
  └── Optional.empty()  (全部完成)
```

**关键约束：Snapshot → Incremental 切换必须等待一个完整 Checkpoint**

在 `SnapshotSplitAssigner` 中：
```java
public void notifyCheckpointComplete(long checkpointId) {
    // 所有 snapshot-splits 都完成后，至少等待一次完整 checkpoint
    if (checkpointIdToFinish != null && !assignerCompleted && allSplitsCompleted()) {
        assignerCompleted = checkpointId >= checkpointIdToFinish;
    }
}
```

这确保了在切换到增量阶段之前，所有快照数据都已经通过 checkpoint 持久化。

### 3.5 SnapshotSplitAssigner — 快照阶段分配器

[`SnapshotSplitAssigner.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/enumerator/SnapshotSplitAssigner.java)

**核心数据结构：**
```java
private final List<TableId> alreadyProcessedTables;       // 已处理的表
private final Queue<SnapshotSplit> remainingSplits;       // 剩余分片
private final Map<String, SnapshotSplit> assignedSplits;  // 已分配但未确认的分片
private final Map<String, SnapshotSplitWatermark> splitCompletedOffsets; // 已完成分片的 Watermark
private final Deque<TableId> remainingTables;             // 剩余待处理的表
```

**getNext() 逻辑：**
```
getNext()
  ├── remainingSplits 非空? → 取一个分配 → 加入 assignedSplits
  └── remainingSplits 为空?
      ├── remainingTables 非空? → 取一个表 → ChunkSplitter.generateSplits(tableId)
      │                            → splits 加入 remainingSplits → 递归 getNext()
      └── 无剩余表? → Optional.empty()
```

**完成条件 (allSplitsCompleted)：**
```java
private boolean allSplitsCompleted() {
    return noMoreSplits() && assignedSplits.size() == splitCompletedOffsets.size();
}
```
- 所有表的所有分片都已生成 (`noMoreSplits`)
- 所有已分配的分片都已收到完成确认 (`assignedSplits.size() == splitCompletedOffsets.size()`)

### 3.6 IncrementalSplitAssigner — 增量阶段分配器

[`IncrementalSplitAssigner.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/enumerator/IncrementalSplitAssigner.java)

**分配策略：**
```java
public List<IncrementalSplit> createIncrementalSplits(boolean startWithSnapshotMinimumOffset) {
    // 将所有捕获表按 incrementalParallelism 均匀分配到多个 IncrementalSplit
    int i = 0;
    for (TableId tableId : allTables) {
        capturedTables[i % incrementalParallelism].add(tableId);
    }
    // 每个 IncrementalSplit 的 startupOffset = 所属表的所有已完成 snapshot split 的 min(offset)
}
```

`incrementalParallelism` 默认为 1，即通常只有一个 IncrementalSplit。如果需要，可以配置大于 1 的值来并行化增量消费（每个 Split 消费一部分表）。

### 3.7 Watermark 事件类型

[`WatermarkEvent.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/split/wartermark/WatermarkEvent.java)
[`WatermarkKind.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/split/wartermark/WatermarkKind.java)

| Watermark 类型 | 含义 | 产生时机 |
|----------------|------|----------|
| `LOW` | 低水位，记录快照分片开始时的 binlog/WAL 位置 | `IncrementalSourceScanFetcher` 开始读取前 |
| `HIGH` | 高水位，记录快照分片结束时的 binlog/WAL 位置 | `IncrementalSourceScanFetcher` 读取完毕后 |
| `END` | 结束标记，有界模式下到达 stopOffset 时发送 | `MySqlBinlogSplitReadTask` 到达 stopOffset |
| `SCHEMA_CHANGE_BEFORE` | Schema 变更前标记 | `SchemaChangeStreamSplitter` 遇到 DDL 事件前 |
| `SCHEMA_CHANGE_AFTER` | Schema 变更后标记 | `SchemaChangeStreamSplitter` 遇到 DDL 事件后 |

---

## 4. MySQL CDC 详解

### 4.1 MySqlDialect — 方言实现

[`MySqlDialect.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-mysql/src/main/java/org/apache/seatunnel/connectors/seatunnel/cdc/mysql/source/MySqlDialect.java)

```java
public class MySqlDialect implements JdbcDataSourceDialect {
```

**核心方法实现：**

| 方法 | 实现 |
|------|------|
| `getName()` | 返回 `"MySQL"` |
| `openJdbcConnection()` | 使用 `MySqlConnectionUtils.createMySqlConnection()` 创建 Debezium MySQL 连接 |
| `createChunkSplitter()` | 创建 `MySqlChunkSplitter`（基于主键范围分片） |
| `discoverDataCollections()` | 通过 `TableDiscoveryUtils.listTables()` 发现所有匹配的表 |
| `queryTableSchema()` | 通过 `MySqlSchema` 查询表结构 |
| `createFetchTaskContext()` | 创建 `MySqlSourceFetchTaskContext` |
| **`createFetchTask()`** | **快照**: `MySqlSnapshotFetchTask` / **增量**: `MySqlBinlogFetchTask` |

### 4.2 MySqlBinlogFetchTask — Binlog 采集

[`MySqlBinlogFetchTask.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-mysql/src/main/java/org/apache/seatunnel/connectors/seatunnel/cdc/mysql/source/reader/fetch/binlog/MySqlBinlogFetchTask.java)

**execute() 核心流程：**
```java
public void execute(FetchTask.Context context) throws Exception {
    // 1. 根据 startupMode 选择 ChangeEventSource
    if (startupMode == StartupMode.TIMESTAMP) {
        mySqlStreamingChangeEventSource = new TimestampFilterMySqlStreamingChangeEventSource(..., targetTimestamp);
    } else {
        mySqlStreamingChangeEventSource = new MySqlStreamingChangeEventSource(...);
    }

    // 2. 注册 BinaryLogClient lifecycle listener
    //    (连接成功后关闭空闲的 JDBC 连接以释放资源)
    sourceFetchContext.getBinaryLogClient().registerLifecycleListener(...);

    // 3. 启动 binlog 事件采集
    mySqlStreamingChangeEventSource.execute(changeEventSourceContext, partition, offsetContext);
}
```

**内部类职责：**

| 内部类 | 职责 |
|--------|------|
| `MySqlBinlogSplitReadTask` | 有界 Binlog 读取，到达 stopOffset 时发送 END watermark |
| `TimestampFilterMySqlStreamingChangeEventSource` | 按时间戳过滤 Binlog 事件（`startupMode=TIMESTAMP`） |
| `BinlogSplitChangeEventSourceContext` | 控制 Binlog reader 运行/停止 |

### 4.3 MySqlStreamingChangeEventSource — Binlog 事件解析

[`MySqlStreamingChangeEventSource.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-mysql/src/main/java/io/debezium/connector/mysql/MySqlStreamingChangeEventSource.java)

这是 SeaTunnel 对 Debezium MySQL Connector 的定制版本，核心改动是让 StreamingChangeEventSource 支持增量快照模式的上下文切换。

**支持的事件类型：**
- `TABLE_MAP`：表映射事件（表 ID → 表结构）
- `WRITE_ROWS / EXT_WRITE_ROWS`：INSERT 事件
- `UPDATE_ROWS / EXT_UPDATE_ROWS`：UPDATE 事件
- `DELETE_ROWS / EXT_DELETE_ROWS`：DELETE 事件
- `QUERY`：DDL 语句（如 ALTER TABLE）
- `GTID`：GTID 事务标识
- `ROTATE`：Binlog 文件轮转

### 4.4 MySqlSnapshotChangeEventSource — 快照读取

[`MySqlSnapshotChangeEventSource.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-mysql/src/main/java/io/debezium/connector/mysql/MySqlSnapshotChangeEventSource.java)

负责从 MySQL 表中读取存量数据，使用 JDBC 执行 `SELECT` 查询并按主键范围分片。

**与 Debezium 原生 Snapshot 的区别：**
- 原生 Debezium：全表锁定 → 全量导出
- SeaTunnel：无锁分片 → 按主键范围拆分为多个 SnapshotSplit

### 4.5 BinlogOffset — MySQL 偏移量

[`BinlogOffset.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-mysql/src/main/java/org/apache/seatunnel/connectors/seatunnel/cdc/mysql/source/offset/BinlogOffset.java)

```java
public class BinlogOffset extends Offset {
    public static final String BINLOG_FILENAME_OFFSET_KEY = "file";   // binlog 文件名
    public static final String BINLOG_POSITION_OFFSET_KEY = "pos";    // binlog 位置
    public static final String EVENTS_TO_SKIP_OFFSET_KEY = "event";   // 事务内跳过事件数
    public static final String ROWS_TO_SKIP_OFFSET_KEY = "row";       // 事件内跳过行数
    public static final String GTID_SET_KEY = "gtids";                // GTID 集合
    public static final String TIMESTAMP_KEY = "ts_sec";              // 时间戳
    public static final String SERVER_ID_KEY = "server_id";           // 服务器 ID
```

**compareTo() 比较逻辑（多级比较）：**
```
1. NO_STOPPING_OFFSET 处理（最大 offset）
2. GTID 比较（如果两边都有 GTID）
3. serverId 比较（不同服务器用时间戳对比）
4. filename 字典序比较
5. position 比较
6. restartSkipEvents 比较
7. restartSkipRows 比较
```

---

## 5. PostgreSQL CDC 详解

### 5.1 PostgresDialect — 方言实现

[`PostgresDialect.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-postgres/src/main/java/org/apache/seatunnel/connectors/seatunnel/cdc/postgres/source/PostgresDialect.java)

```java
public class PostgresDialect implements JdbcDataSourceDialect {
```

**与 MySQL 的关键差异：**

| 特性 | MySQL | PostgreSQL |
|------|-------|------------|
| 日志机制 | Binlog (BinaryLogClient) | WAL Logical Replication |
| 连接类型 | `MySqlConnection` | `PostgresConnection` (含 Replication) |
| Offset | `BinlogOffset` | `LsnOffset` |
| Offset 提交 | 不直接提交 (binlog 自动管理) | `commitChangeLogOffset()` 提交 LSN |
| 约束检查 | 无需特殊处理 | 必须 `REPLICA IDENTITY FULL` |

**commitChangeLogOffset() 实现：**

```java
@Override
public void commitChangeLogOffset(Offset offset) throws Exception {
    if (postgresWalFetchTask != null) {
        postgresWalFetchTask.commitCurrentOffset((LsnOffset) offset);
    }
}
```

PostgreSQL 需要通过 replication slot 提交已消费的 LSN，否则 WAL 日志会无限增长。

### 5.2 PostgresWalFetchTask — WAL 采集

[`PostgresWalFetchTask.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-postgres/src/main/java/org/apache/seatunnel/connectors/seatunnel/cdc/postgres/source/reader/wal/PostgresWalFetchTask.java)

```java
public class PostgresWalFetchTask implements FetchTask<SourceSplitBase> {
    private Long lastCommitLsn;

    public void commitCurrentOffset(LsnOffset offset) {
        Long commitLsn = offset.getLsn().asLong();
        if (commitLsn > lastCommitLsn) {
            lastCommitLsn = commitLsn;
            streamingChangeEventSource.commitOffset(offsets);
        }
    }
}
```

**execute() 流程：**
```java
public void execute(FetchTask.Context context) throws Exception {
    streamingChangeEventSource = new PostgresStreamingChangeEventSource(
        config, snapshotter, dataConnection, eventDispatcher,
        errorHandler, clock, databaseSchema, taskContext, replicationConnection);
    streamingChangeEventSource.execute(changeEventSourceContext, partition, offsetContext);
}
```

### 5.3 PostgresOffsetContext — 偏移管理

[`PostgresOffsetContext.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-postgres/src/main/java/io/debezium/connector/postgresql/PostgresOffsetContext.java)

SeaTunnel 复制并定制了 Debezium 的 `PostgresOffsetContext`，主要改动：

```java
// 原版只支持 Number 类型的 LSN → 修改为也支持 String 和 Lsn 格式
private Long readOptionalLong(Map<String, ?> offset, String key) {
    final Object obj = offset.get(key);
    if (obj instanceof Number) return ((Number) obj).longValue();
    try {
        return Long.parseLong(obj.toString());
    } catch (NumberFormatException ne) {
        return Lsn.valueOf((String) obj).asLong();
    }
}
```

**Offset 包含的关键字段：**

| 字段 | 含义 |
|------|------|
| `lsn_proc` (LAST_COMPLETELY_PROCESSED_LSN_KEY) | 最后一个完全处理完的 LSN |
| `lsn_commit` (LAST_COMMIT_LSN_KEY) | 最后一个提交的 LSN |
| `ts_usec` | 事件时间戳 (微秒) |
| `txId` | 事务 ID |
| `xmin` | 快照 xmin |
| `snapshot` | 是否为快照模式 |

---

## 6. 其他 CDC 支持

### 6.1 Oracle CDC (LogMiner)

**模块**: `connector-cdc-oracle`

| 关键类 | 职责 |
|--------|------|
| `OracleDialect` | 方言实现，与 MySQL/Postgres 类似的接口适配 |
| `OracleRedoLogFetchTask` | RedoLog 采集任务，基于 Debezium `LogMinerStreamingChangeEventSource` |
| `LogMinerStreamingChangeEventSource` | 通过 Oracle LogMiner 读取 RedoLog 增量数据 |
| `LogMinerAdapter` | LogMiner 适配器，连接 Oracle LogMiner 会话 |
| `RedoLogOffset` | Oracle SCN (System Change Number) 偏移量 |
| `RedoLogOffsetFactory` | 从 SCN 构造 RedoLogOffset |

**特色**：Oracle 使用 LogMiner（而非 Oracle GoldenGate）进行 CDC，通过定期查询 `V$LOGMNR_CONTENTS` 解析 RedoLog 中的 DML 事件。

### 6.2 SQL Server CDC

**模块**: `connector-cdc-sqlserver`

| 关键类 | 职责 |
|--------|------|
| `SqlServerDialect` | 方言实现 |
| `SqlServerTransactionLogFetchTask` | Transaction Log 采集任务 |
| `SqlServerStreamingChangeEventSource` | 基于 SQL Server CDC 功能的增量事件源 |
| `LsnOffset` | SQL Server LSN (Log Sequence Number) 偏移量 |

**特色**：依赖 SQL Server 的 `CDC` 功能（需在数据库和表级别启用），通过 `sys.sp_cdc_help_change_data_capture` 等存储过程获取变更数据。

### 6.3 MongoDB CDC (ChangeStream)

**模块**: `connector-cdc-mongodb`

| 关键类 | 职责 |
|--------|------|
| `MongodbDialect` | 非 JDBC 方言，实现 `DataSourceDialect<MongodbSourceConfig>` |
| `MongodbStreamFetchTask` | ChangeStream 采集任务 |
| `MongodbScanFetchTask` | 快照采集任务 |
| `ChangeStreamOffset` | 基于 MongoDB ResumeToken 的偏移量 |
| `MongodbChunkSplitter` | 分片策略，支持 Sharded/SplitVector/SampleBucket/Single 策略 |

**分片策略（SplitStrategy）：**

| 策略 | 适用场景 |
|------|----------|
| `ShardedSplitStrategy` | 分片集群，按 Chunk 范围分片 |
| `SplitVectorSplitStrategy` | 非分片集群，使用 `splitVector` 命令 |
| `SampleBucketSplitStrategy` | 基于采样桶的分片 |
| `SingleSplitStrategy` | 小集合，单分片 |

### 6.4 TiDB CDC (TiKV CDC)

**模块**: `connector-cdc-tidb`

TiDB CDC 采用了完全独立的实现路径，不基于 Debezium：

| 关键类 | 职责 |
|--------|------|
| `TiDBSource` | Source 入口，直接实现 `SeaTunnelSource` |
| `TiDBSourceReader` | 读取器，使用 `ScanIterator` 从 TiKV 读取 |
| `TiDBSourceSplitEnumerator` | 分片枚举器 |
| `RowKeyWithTs` | TiDB 行键 + 时间戳偏移量 |

TiDB CDC 直接通过 TiKV 的 RawKV API 读取数据，使用 `org.tikv:tikv-client-java` 库。

### 6.5 OpenGauss CDC

**模块**: `connector-cdc-opengauss`

基于 PostgreSQL CDC 的方言扩展，主要差异在于连接配置和复制协议适配。核心类 `PostgresConnection` 和 `PostgresReplicationConnection` 做了 OpenGauss 特定适配。

---

## 7. Schema 变更传播机制

### 7.1 AbstractSchemaChangeResolver — 抽象解析器

[`AbstractSchemaChangeResolver.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/schema/AbstractSchemaChangeResolver.java)

```java
public abstract class AbstractSchemaChangeResolver implements SchemaChangeResolver {
    protected static final List<String> SUPPORT_DDL = Lists.newArrayList("ALTER TABLE");
```

**当前仅支持 `ALTER TABLE` 类型的 DDL**。其他 DDL 语句（如 `CREATE TABLE`、`DROP TABLE`）会被静默忽略。

**支撑判定 (support)：**
```java
public boolean support(SourceRecord record) {
    String ddl = SourceRecordUtils.getDdl(record);
    // 1. 检查 DDL 是否非空
    // 2. 检查 tableChanges 非空（只处理已捕获表）
    // 3. 检查 DDL 是否包含 "ALTER TABLE"
    return StringUtils.isNotBlank(ddl) && SUPPORT_DDL.stream()
        .anyMatch(prefix -> ddl.toUpperCase().contains(prefix));
}
```

**resolve() 核心流程：**
```
resolve(SourceRecord, List<CatalogTable>)
  ├── 获取 TablePath 和 DDL 语句
  ├── 创建/复用 DdlParser (使用 Debezium Antlr 解析器)
  ├── ddlParser.setCurrentDatabase/Schema()
  ├── ddlParser.parse(ddl, tables)          → 解析 DDL 为 TableChanges
  ├── getAndClearParsedEvents()              → 获取解析后的 AlterTableColumnEvent 列表
  ├── completionEvent(parsedEvents, catalogTables) → 补充类型信息
  └── 返回 AlterTableColumnsEvent
```

**抽象方法（各数据库子类实现）：**
```java
protected abstract DdlParser createDdlParser(TablePath tablePath);
protected abstract List<AlterTableColumnEvent> getAndClearParsedEvents();
protected abstract String getSourceDialectName();
```

**MySQL 实现**: `MySqlSchemaChangeResolver` 使用 `CustomMySqlAntlrDdlParser` + `CustomMySqlAntlrDdlParserListener` 解析 DDL。

**Oracle 实现**: `OracleSchemaChangeResolver` 使用 `CustomOracleAntlrDdlParser`。

### 7.2 DDL 事件在流中的传播

[`IncrementalSourceStreamFetcher.SchemaChangeStreamSplitter`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/reader/external/IncrementalSourceStreamFetcher.java#L300-L391)

DDL 事件在 Binlog/WAL 流中与原数据事件混在一起，`SchemaChangeStreamSplitter` 负责将流拆分为独立的逻辑批次：

**拆分规则：**

```
输入流: [a, b, c, SchemaChangeEvent-1, SchemaChangeEvent-2, d, e]

输出流:
  [a, b, c, SCHEMA_CHANGE_BEFORE_Watermark]         → 批次 1: DDL 前的数据事件
  [SchemaChangeEvent-1, SchemaChangeEvent-2, SCHEMA_CHANGE_AFTER_Watermark] → 批次 2: DDL 变更事件
  [d, e]                                            → 批次 3: DDL 后的数据事件
```

这样设计的好处是：
- DDL 事件前后的数据事件被隔离到不同批次
- 下游可以通过 `SCHEMA_CHANGE_BEFORE`/`SCHEMA_CHANGE_AFTER` 标记做 checkpoint
- 支持 Schema 演进 Sink 在不同批次间切换表结构

**IncrementalSourceRecordEmitter 中的处理：**
```java
if (isSchemaChangeBeforeWatermarkEvent(element) || isSchemaChangeAfterWatermarkEvent(element)) {
    emitElement(element, output);  // 传递给下游作为 checkpoint 标记
}
if (isSchemaChangeEvent(element)) {
    splitState.setStartupOffset(position);  // 更新 offset
    emitElement(element, output);            // SchemaChangeEvent 传递给下游
}
```

---

## 8. 一致性保证

### 8.1 Exactly-Once 语义实现

SeaTunnel CDC 的 Exactly-Once 是端到端语义，需要在 Source 和 Sink 两侧配合。

**Source 侧保证：**

**1. Watermark 驱动的去重：**

```
快照期间：
  LowWatermark ──[快照数据]── HighWatermark
                     ↑
                     这期间的增量变更可能重复
                     通过主键覆盖来解决

增量期间 Exactly-Once 模式：
  从 HighWatermark 位置读取增量日志
  对于快照期间的 Binlog 变更：
    - 检查记录的主键是否在对应 SnapshotSplit 范围内
    - 如果在该范围内且 offset > HighWatermark，发射该记录（覆盖快照数据）
    - 如果 offset <= HighWatermark，丢弃（快照数据已经包含）
```

**IncrementalSourceScanFetcher 的 Exactly-Once 模式：**
```java
// pollSplitRecordsIfExactlyOnce()
// 数据输入: [low watermark] [snapshot] [high watermark] [change events] [end watermark]
// 处理:
// 1. 收集 low watermark
// 2. 收集 snapshot 记录到 outputBuffer (以主键为 key 去重)
// 3. 遇到 high watermark 后开始处理 change events
// 4. 如果 change event 属于当前分片范围，用 change event 覆盖 outputBuffer
// 5. 遇到 end watermark 后输出: low + normalized + high
```

**2. Checkpoint 机制：**

```java
// IncrementalSourceReader.snapshotState()
// 快照阶段：保存 unmatched + finishedUnackedSplits
// 增量阶段：保存当前 schema 信息到 IncrementalSplit

// IncrementalSplit 中的 checkpointTables 用于恢复时重建 DeserializationSchema
debeziumDeserializationSchema.restoreCheckpointProducedType(incrementalSplit.getCheckpointTables());
```

**3. Offset 提交（PostgreSQL）：**

```java
// DataSourceDialect.commitChangeLogOffset()
// MySQL: 空实现（binlog offset 由 BinaryLogClient 管理）
// PostgreSQL: 必须提交 LSN 到 replication slot
postgresWalFetchTask.commitCurrentOffset((LsnOffset) offset);
```

### 8.2 断点续传与故障恢复

**状态持久化链：**

```
IncrementalSourceEnumerator.snapshotState(checkpointId)
  → SplitAssigner.snapshotState(checkpointId)
    → HybridSplitAssigner.snapshotState(checkpointId)
      → HybridPendingSplitsState {
          snapshotPhaseState: {
            alreadyProcessedTables,      // 已完成表
            remainingSplits,             // 未分配分片
            assignedSplits,              // 已分配未确认分片
            splitCompletedOffsets,       // 已完成分片 Watermark
            isAssignerCompleted          // 快照阶段是否完成
          },
          incrementalPhaseState: { ... } // 增量阶段状态
        }
```

**恢复流程 (restoreEnumerator)：**

```
restoreEnumerator(checkpointState)
  ├── checkpointState instanceof HybridPendingSplitsState?
  │   └── restore(capturedTables, checkpointState)
  │       ├── 对比 capturedTables 与 checkpoint 中的表
  │       ├── 新增表 → 加入 remainingTables
  │       ├── 删除表 → 从 remainingTables/assignedSplits 移除
  │       ├── 如果剩余表/分片不为空但 assigner 已标记完成
  │       │   → 重置 assignerCompleted = false
  │       └── 返回调整后的 HybridPendingSplitsState
  │
  ├── checkpointState instanceof IncrementalPhaseState?
  │   └── 直接创建 IncrementalSplitAssigner（快照已完成）
  │
  └── 创建 IncrementalSourceEnumerator
```

### 8.3 Offset/Binlog Position 管理

**Offset 类型层次：**
```
Offset (抽象基类)
├── BinlogOffset ← MySQL (file + pos + GTID)
├── LsnOffset ← PostgreSQL, SQL Server (LSN)
├── RedoLogOffset ← Oracle (SCN)
└── ChangeStreamOffset ← MongoDB (ResumeToken)
```

**Offset 比较规则 (isBefore/isAfter/isAtOrBefore/isAtOrAfter)：**
```java
public boolean isBefore(Offset that) { return this.compareTo(that) < 0; }
public boolean isAtOrAfter(Offset that) { return this.compareTo(that) >= 0; }
```

Offset 的可比较性是实现 Exactly-Once 去重的基础。
- `isBefore(HighWatermark)` → 该事件在快照覆盖范围内，需要根据 Exactly-Once 策略判断
- `isAtOrAfter(HighWatermark)` → 该事件在快照覆盖范围外，是纯增量事件

---

## 9. 关键数据结构与状态流转

### 9.1 Split 层次结构

```
SourceSplit (接口)
└── SourceSplitBase (抽象类)
    ├── SnapshotSplit          ← 快照分片 (多并行)
    │     tableId, splitStart, splitEnd
    │     lowWatermark, highWatermark
    │
    └── IncrementalSplit       ← 增量分片 (通常单并行)
          tableIds, startupOffset, stopOffset
          completedSnapshotSplitInfos, checkpointTables
```

### 9.2 状态流转图

```
┌───────────────────────────────────────────────────────────────────────┐
│                        任务生命周期状态机                              │
├───────────────────────────────────────────────────────────────────────┤
│                                                                       │
│  StartupMode.INITIAL:                                                │
│                                                                       │
│  ┌─────────┐    ┌──────────────────┐    ┌─────────────────────┐      │
│  │ 开始     │───►│ Snapshot 阶段     │───►│ Incremental 阶段    │      │
│  │         │    │ (HybridSplitAssig│    │ (IncrementalSplitAs│      │
│  │         │    │  ner)            │    │  signer)            │      │
│  └─────────┘    └────────┬─────────┘    └──────────┬──────────┘      │
│                          │                         │                  │
│                          │  SnapshotSplit 进度:    │                  │
│                          │  ┌───────────────────┐  │                  │
│                          │  │ remainingTables    │  │                  │
│                          │  │ remainingSplits    │  │                  │
│                          │  │ assignedSplits     │  │                  │
│                          │  │ splitCompletedOffsets│ │                 │
│                          │  └───────────────────┘  │                  │
│                          │                         │                  │
│                          │  切换条件:               │                  │
│                          │  allSplitsCompleted()   │                  │
│                          │  + checkpoint 完成       │                  │
│                          └─────────┬───────────────┘                  │
│                                    │                                   │
│                                    ▼                                   │
│                          IncrementalSplit 进度:                        │
│                          ┌────────────────────┐                       │
│                          │ startupOffset 逐步前进│                     │
│                          │ completedSnapshotSplit│                    │
│                          │ Infos 逐步清空       │                     │
│                          └────────────────────┘                       │
│                                                                       │
│  StartupMode.LATEST/EARLIEST/SPECIFIC/TIMESTAMP:                      │
│                                                                       │
│  ┌─────────┐    ┌─────────────────────┐                               │
│  │ 开始     │───►│ Incremental 阶段     │                              │
│  │         │    │ (跳过快照)            │                              │
│  └─────────┘    └─────────────────────┘                               │
│                                                                       │
└───────────────────────────────────────────────────────────────────────┘
```

### 9.3 事件在流中的传递路径

```
SourceRecord (Kafka Connect)
  │
  ├── WatermarkEvent (LOW/HIGH/END/SCHEMA_CHANGE_BEFORE/AFTER)
  │     └── IncrementalSourceRecordEmitter.processElement()
  │           └── 更新 splitState 的 Watermark
  │
  ├── SchemaChangeEvent (DDL)
  │     └── IncrementalSourceRecordEmitter.processElement()
  │           ├── 更新 startupOffset
  │           └── debeziumDeserializationSchema.deserialize()
  │                 └── SchemaChangeResolver.resolve() → AlterTableColumnsEvent
  │
  ├── DataChangeEvent (INSERT/UPDATE/DELETE)
  │     └── IncrementalSourceRecordEmitter.processElement()
  │           ├── (增量模式) 更新 startupOffset
  │           └── debeziumDeserializationSchema.deserialize() → SeaTunnelRow
  │
  └── HeartbeatEvent
        └── IncrementalSourceRecordEmitter.processElement()
              └── debeziumDeserializationSchema.deserialize() → heartbeat row
```

### 9.4 CompletedSnapshotSplitInfo 的作用

[`CompletedSnapshotSplitInfo.java`](../../seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/split/CompletedSnapshotSplitInfo.java)

```java
public class CompletedSnapshotSplitInfo {
    private final String splitId;                  // 分片 ID
    private final TableId tableId;                 // 表 ID
    private final SeaTunnelRowType splitKeyType;   // 分片键类型
    private final Object[] splitStart;             // 分片起始值
    private final Object[] splitEnd;               // 分片结束值
    private final SnapshotSplitWatermark watermark;// 高低水位
}
```

**用途：** 增量阶段消费 Binlog/WAL 时，每收到一条变更事件，需要：
1. 判断该事件属于哪个表 (`tableId`)
2. 判断该事件的主键是否在某个已完成的 SnapshotSplit 范围内 (`splitStart` ~ `splitEnd`)
3. 如果在范围内且 offset > `highWatermark`，用该事件覆盖快照数据

这就是 `IncrementalSourceStreamFetcher.shouldEmit()` 的核心逻辑。

---

## 10. 总结与设计模式

### 10.1 核心设计模式

#### 策略模式 (Strategy Pattern)

**ChunkSplitter / SplitStrategy：**
```
ChunkSplitter (接口)
├── MySqlChunkSplitter         ← 按主键范围分片
├── PostgresChunkSplitter      ← 按主键范围分片
├── OracleChunkSplitter        ← 按主键范围分片
├── SqlServerChunkSplitter     ← 按主键范围分片
└── MongodbChunkSplitter       ← 多种策略
    ├── ShardedSplitStrategy
    ├── SplitVectorSplitStrategy
    ├── SampleBucketSplitStrategy
    └── SingleSplitStrategy
```

**FetchTask 策略：**
```
FetchTask<SourceSplitBase> (接口)
├── MySqlSnapshotFetchTask / MySqlBinlogFetchTask
├── PostgresSnapshotFetchTask / PostgresWalFetchTask
├── OracleSnapshotFetchTask / OracleRedoLogFetchTask
├── SqlServerSnapshotFetchTask / SqlServerTransactionLogFetchTask
└── MongodbScanFetchTask / MongodbStreamFetchTask
```

#### 观察者模式 (Observer Pattern)

**事件驱动架构：**
```
IncrementalSourceEnumerator (观察者)
  ← handleSourceEvent(CompletedSnapshotSplitsReportEvent)  ← IncrementalSourceReader
  ← handleSourceEvent(CompletedSnapshotPhaseEvent)         ← IncrementalSourceReader

IncrementalSourceReader (观察者/被观察者)
  → context.sendSourceEventToEnumerator(CompletedSnapshotSplitsReportEvent)
  → context.sendSourceEventToEnumerator(CompletedSnapshotPhaseEvent)
```

**Enumerator ↔ Reader 通信：**
- Reader → Enumerator: `CompletedSnapshotSplitsReportEvent`（快照分片完成）
- Reader → Enumerator: `CompletedSnapshotPhaseEvent`（进入纯增量阶段）
- Enumerator → Reader: `CompletedSnapshotSplitsAckEvent`（确认收到）

#### 模板方法模式 (Template Method)

**IncrementalSource 抽象类：**
```java
public abstract class IncrementalSource<T, C extends SourceConfig> {
    // 模板方法 (已实现)
    public SourceSplitEnumerator createEnumerator(...) { ... }
    public SourceSplitEnumerator restoreEnumerator(...) { ... }

    // 抽象方法 (子类实现)
    public abstract DataSourceDialect<C> createDataSourceDialect(ReadonlyConfig config);
    public abstract OffsetFactory createOffsetFactory(ReadonlyConfig config);
    public abstract DebeziumDeserializationSchema<T> createDebeziumDeserializationSchema(...);
    public abstract SourceConfig.Factory<C> createSourceConfigFactory(ReadonlyConfig config);
}
```

**DataSourceDialect 接口：**
```java
public interface DataSourceDialect<C extends SourceConfig> extends Serializable {
    String getName();
    List<TableId> discoverDataCollections(C sourceConfig);
    boolean isDataCollectionIdCaseSensitive(C sourceConfig);
    ChunkSplitter createChunkSplitter(C sourceConfig);
    FetchTask<SourceSplitBase> createFetchTask(SourceSplitBase sourceSplitBase);
    FetchTask.Context createFetchTaskContext(...);
}
```

**AbstractSchemaChangeResolver 模板方法：**
```java
public abstract class AbstractSchemaChangeResolver {
    // 模板方法 (已实现)
    public SchemaChangeEvent resolve(SourceRecord record, List<CatalogTable> catalogTables) {
        ddlParser.parse(ddl, tables);
        return new AlterTableColumnsEvent(...);
    }
    // 抽象方法 (子类实现)
    protected abstract DdlParser createDdlParser(TablePath tablePath);
    protected abstract List<AlterTableColumnEvent> getAndClearParsedEvents();
}
```

#### 组合模式 (Composite Pattern)

**HybridSplitAssigner** 组合了 `SnapshotSplitAssigner` 和 `IncrementalSplitAssigner`：
```java
public class HybridSplitAssigner<C extends SourceConfig> implements SplitAssigner {
    private final SnapshotSplitAssigner<C> snapshotSplitAssigner;
    private final IncrementalSplitAssigner<C> incrementalSplitAssigner;

    public Optional<SourceSplitBase> getNext() {
        // 先分配快照分片，再分配增量分片
    }
}
```

### 10.2 架构优势总结

1. **统一抽象**：通过 `DataSourceDialect` + `IncrementalSource` + `FetchTask` 三层抽象，六种数据库共用同一套增量快照框架
2. **无锁快照**：基于主键范围分片 + Watermark 机制，避免了传统全表锁定
3. **灵活启停**：支持 5 种启动模式 (INITIAL/EARLIEST/LATEST/SPECIFIC/TIMESTAMP) 和 4 种停止模式
4. **断点续传**：完整的 checkpoint 机制，支持任务中断后从断点恢复
5. **Schema 演进**：支持 ALTER TABLE 的实时捕获与传播
6. **Exactly-Once**：通过 Watermark + 主键覆盖 + checkpoint 实现端到端精确一次语义