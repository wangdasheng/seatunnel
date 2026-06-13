# SeaTunnel CDC 数据迁移调试指南

> 目的：通过日志插桩，真实追踪一条 CDC 数据从 Source 到 Sink 的完整流转路径，验证我们的架构理解是否正确
> 创建日期: 2026-06-13

---

## 一、调试目标

追踪以下 5 个核心问题：

1. **Job 提交链路**: 配置如何变成 ExecutionPlan → Pipeline → TaskGroup？
2. **CDC 数据来源**: `SeaTunnelRow` 的 `tableId` 和 `rowKind` 在哪个环节被设置？值是什么？
3. **数据流转链路**: 一条 Row 从 Source → Transform → Sink 的实际路径是怎样的？
4. **Checkpoint 机制**: Barrier 如何从 Source 触发，经过哪些节点，最终到达 Sink？
5. **Pipeline 拓扑**: 实际生成了几个 Pipeline？几个 TaskGroup？Source/Transform/Sink 是在同一个还是不同的 TaskGroup？

---

## 二、准备工作

### 2.1 需要一个可运行的 CDC 配置

创建最简单的端到端配置，不需要真实 MySQL，使用 **FakeSource** 模拟 CDC 数据：

```hocon
# 文件: config/debug_cdc_flow.conf
env {
  job.mode = "STREAMING"
  parallelism = 1
  checkpoint.interval = 10000
}

source {
  FakeSource {
    result_table_name = "fake_cdc"
    row.num = 100
    schema = {
      fields {
        id = "int"
        name = "string"
        age = "int"
      }
    }
    # 模拟 tableId 变化
    tables = ["mydb.orders", "mydb.users"]
  }
}

transform {
  # 添加一个简单 Transform 来追踪数据经过
  Sql {
    source_table_name = "fake_cdc"
    result_table_name = "transformed"
    query = "SELECT *, name as display_name FROM fake_cdc"
  }
}

sink {
  Console {
    source_table_name = "transformed"
  }
}
```

> **备选**: 如果有本地 MySQL，可以用 MySQL-CDC 配置（见附录 A）

### 2.2 启动方式

```bash
# 本地模式启动（最简单）
cd /Users/wangzhijun/sourceCode/seatunnel

# 编译
mvn clean package -pl seatunnel-dist -am -DskipTests -Dmaven.javadoc.skip=true

# 启动
./bin/seatunnel.sh -c config/debug_cdc_flow.conf
```

### 2.3 日志配置

在 `config/log4j2.properties` 中增加 DEBUG 级别：

```properties
# 把我们的调试日志设为 DEBUG
logger.cdc-debug.name = org.apache.seatunnel.engine
logger.cdc-debug.level = DEBUG
logger.cdc-debug.appenderRef.console.ref = STDOUT
```

---

## 三、插桩点布局（18 个关键断点）

按执行顺序排列，用 `[CDC-DEBUG]` 标签标记，方便 grep 过滤：

```
[CDC-DEBUG] 标签 = 所有调试日志的统一前缀
grep "CDC-DEBUG" 可以快速过滤所有调试输出
```

### 断点 1: JobMaster 初始化

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/master/JobMaster.java`

**插桩位置**: `init()` 方法，`logicalDag` 创建后

```java
// 在 logicalDag 创建之后 (约第 245 行)
LOGGER.info("[CDC-DEBUG] JobMaster.init() - JobId={}, JobName={}, LogicalDag vertices={}",
    jobImmutableInformation.getJobId(),
    jobImmutableInformation.getJobConfig().getName(),
    logicalDag.getLogicalVertexMap().size());
    
// 打印每个 LogicalVertex
for (Map.Entry<Long, LogicalVertex> entry : logicalDag.getLogicalVertexMap().entrySet()) {
    Action action = entry.getValue().getAction();
    LOGGER.info("[CDC-DEBUG] LogicalVertex: id={}, name={}, type={}, parallelism={}",
        entry.getKey(), action.getName(), action.getClass().getSimpleName(),
        action.getParallelism());
}
```

**预期输出**:
```
[CDC-DEBUG] JobMaster.init() - JobId=123456, JobName=debug_cdc_flow, LogicalDag vertices=3
[CDC-DEBUG] LogicalVertex: id=1, name=FakeSource, type=SourceAction, parallelism=1
[CDC-DEBUG] LogicalVertex: id=2, name=Sql, type=TransformAction, parallelism=1
[CDC-DEBUG] LogicalVertex: id=3, name=Console, type=SinkAction, parallelism=1
```

---

### 断点 2: PhysicalPlan 生成

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/master/JobMaster.java`

**插桩位置**: `init()` 方法，`PlanUtils.fromLogicalDAG` 返回后 (约第 290 行)

```java
// PlanUtils.fromLogicalDAG 返回后
LOGGER.info("[CDC-DEBUG] PhysicalPlan created - PipelineCount={}, SubPlanCount={}",
    physicalPlan.getPipelineList().size(),
    physicalPlan.getSubPlanList().size());

for (PipelineLocation pl : physicalPlan.getPipelineList()) {
    LOGGER.info("[CDC-DEBUG] Pipeline: pipelineId={}, pipelineLocation={}",
        pl.getPipelineId(), pl);
}
```

**预期输出**:
```
[CDC-DEBUG] PhysicalPlan created - PipelineCount=1, SubPlanCount=1
[CDC-DEBUG] Pipeline: pipelineId=1, pipelineLocation=...
```

---

### 断点 3: ExecutionPlan 生成

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/execution/ExecutionPlanGenerator.java`

**插桩位置**: `generate()` 方法

```java
// 在 generate() 方法开始处
LOGGER.info("[CDC-DEBUG] ExecutionPlanGenerator.generate() - START, logicalVertexCount={}",
    logicalDag.getLogicalVertexMap().size());

// 在 generate() 方法结束时
for (Pipeline pipeline : executionPlan.getPipelines()) {
    LOGGER.info("[CDC-DEBUG] ExecutionPlan.generate() - Pipeline: id={}, edges={}",
        pipeline.getId(), pipeline.getEdges().size());
    for (ExecutionEdge edge : pipeline.getEdges()) {
        LOGGER.info("[CDC-DEBUG]   Edge: {} -> {} (type={})",
            edge.getLeftVertexId(), edge.getRightVertexId(),
            edge.getClass().getSimpleName());
    }
}
```

**预期输出**:
```
[CDC-DEBUG] ExecutionPlanGenerator.generate() - START, logicalVertexCount=3
[CDC-DEBUG] ExecutionPlan.generate() - Pipeline: id=1, edges=2
[CDC-DEBUG]   Edge: FakeSource -> Sql (type=...)
[CDC-DEBUG]   Edge: Sql -> Console (type=...)
```

---

### 断点 4: PipelineGenerator 拆分

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/execution/PipelineGenerator.java`

**插桩位置**: 各种 generate 方法中

```java
// 在 generate 方法中，打印每个 Pipeline 的拓扑
LOGGER.info("[CDC-DEBUG] PipelineGenerator - Pipeline[{}]: {} vertices, {} edges",
    pipelineId, vertices.size(), edges.size());
LOGGER.info("[CDC-DEBUG] PipelineGenerator - Pipeline[{}] vertices: {}",
    pipelineId, vertices.stream()
        .map(v -> v.getClass().getSimpleName())
        .collect(Collectors.toList()));
```

---

### 断点 5: TaskGroup 分配

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/execution/TaskGroup.java` 或相关调度类

**插桩位置**: TaskGroup 创建后

```java
LOGGER.info("[CDC-DEBUG] TaskGroup created: taskGroupId={}, tasks={}",
    taskGroupLocation, taskList.size());
```

---

### 断点 6: SourceFlowLifeCycle 初始化

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/SourceFlowLifeCycle.java`

**插桩位置**: `init()` 方法 (第 115 行)

```java
// 在 init() 方法返回前
LOGGER.info("[CDC-DEBUG] SourceFlowLifeCycle.init() - sourceName={}, readerClass={}, indexID={}",
    sourceAction.getName(), reader.getClass().getSimpleName(), indexID);
```

**插桩位置**: `open()` 方法 (第 129 行)

```java
LOGGER.info("[CDC-DEBUG] SourceFlowLifeCycle.open() - reader opened, sourceName={}",
    sourceAction.getName());
```

---

### 断点 7: Source 数据产生（最关键！）

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/SeaTunnelSourceCollector.java`

**插桩位置**: `collect()` 方法 (第 93 行)

```java
// 在 collect(T row) 方法中，sendRecordToNext 之前
if (row instanceof SeaTunnelRow) {
    SeaTunnelRow seaTunnelRow = (SeaTunnelRow) row;
    LOGGER.info("[CDC-DEBUG] SourceCollector.collect() - tableId={}, rowKind={}, fields={}, " +
        "physicalTableId={}",
        seaTunnelRow.getTableId(),
        seaTunnelRow.getRowKind(),
        seaTunnelRow.getFieldCount(),
        seaTunnelRow.getOptions().get("tableId"));
}
```

**预期输出**:
```
[CDC-DEBUG] SourceCollector.collect() - tableId=mydb.orders, rowKind=+I, fields=3, physicalTableId=mydb.orders
[CDC-DEBUG] SourceCollector.collect() - tableId=mydb.users, rowKind=+I, fields=3, physicalTableId=mydb.users
```

---

### 断点 8: Source → Transform 数据传递

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/SeaTunnelSourceCollector.java`

**插桩位置**: `sendRecordToNext()` 方法 (第 191 行)

```java
// 在 sendRecordToNext 方法中
SeaTunnelRow seaTunnelRow = (SeaTunnelRow) record.getData();
LOGGER.info("[CDC-DEBUG] SourceCollector.sendRecordToNext() - tableId={}, rowKind={}, " +
    "outputCount={}, checkpointLock={}",
    seaTunnelRow.getTableId(),
    seaTunnelRow.getRowKind(),
    outputs.size(),
    System.identityHashCode(checkpointLock));
```

---

### 断点 9: TransformFlowLifeCycle 接收数据

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/TransformFlowLifeCycle.java`

**插桩位置**: `received()` 方法

```java
// 在 TransformFlowLifeCycle.received() 中
if (record.getData() instanceof SeaTunnelRow) {
    SeaTunnelRow seaTunnelRow = (SeaTunnelRow) record.getData();
    LOGGER.info("[CDC-DEBUG] TransformFlowLifeCycle.received() - tableId={}, rowKind={}, " +
        "transformName={}",
        seaTunnelRow.getTableId(),
        seaTunnelRow.getRowKind(),
        transformAction.getName());
}
```

---

### 断点 10: Transform 处理

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/TransformSeaTunnelTask.java`

**插桩位置**: `collect()` 方法 (第 71 行)

```java
// 在 collect() 方法中
LOGGER.info("[CDC-DEBUG] TransformSeaTunnelTask.collect() - indexID={}, startFlowLifeCycle={}",
    indexID, startFlowLifeCycle.getClass().getSimpleName());
```

---

### 断点 11: Transform → Sink 数据传递

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/SeaTunnelTransformCollector.java`

**插桩位置**: `collect()` 方法 (第 36 行)

```java
// 在 collect(Record<?> record) 方法中
if (record.getData() instanceof SeaTunnelRow) {
    SeaTunnelRow seaTunnelRow = (SeaTunnelRow) record.getData();
    LOGGER.info("[CDC-DEBUG] TransformCollector.collect() -> Sink - tableId={}, rowKind={}, " +
        "outputCount={}",
        seaTunnelRow.getTableId(),
        seaTunnelRow.getRowKind(),
        outputs.size());
}
```

---

### 断点 12: SinkFlowLifeCycle 接收数据

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/SinkFlowLifeCycle.java`

**插桩位置**: `received()` 方法 (第 191 行)，在非 Barrier 的分支中

```java
// 在 received() 方法的 else 分支 (第 265 行附近)
if (record.getData() instanceof SeaTunnelRow) {
    SeaTunnelRow seaTunnelRow = (SeaTunnelRow) record.getData();
    LOGGER.info("[CDC-DEBUG] SinkFlowLifeCycle.received() - tableId={}, rowKind={}, " +
        "sinkName={}, writerClass={}",
        seaTunnelRow.getTableId(),
        seaTunnelRow.getRowKind(),
        sinkAction.getName(),
        writer.getClass().getSimpleName());
}
```

---

### 断点 13: Sink Writer 实际写入

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/SinkFlowLifeCycle.java`

**插桩位置**: `received()` 第 270 行 `writer.write()` 调用前后

```java
// 在 writer.write((T) record.getData()) 之前
LOGGER.info("[CDC-DEBUG] SinkFlowLifeCycle - writer.write() called, tableId={}, sinkName={}",
    tableId, sinkAction.getName());
```

---

### 断点 14: CheckpointCoordinator 触发

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/checkpoint/CheckpointCoordinator.java`

**插桩位置**: `tryTriggerPendingCheckpoint` 方法中

```java
LOGGER.info("[CDC-DEBUG] CheckpointCoordinator - trigger checkpoint[{}], pipelineId={}, " +
    "taskCount={}",
    checkpointId, pipelineId, taskCount);
```

---

### 断点 15: CheckpointBarrier 在 Source 端

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/SourceFlowLifeCycle.java`

**插桩位置**: `triggerBarrier()` 方法中

```java
LOGGER.info("[CDC-DEBUG] SourceFlowLifeCycle.triggerBarrier() - checkpointId={}, " +
    "sourceName={}, schemaChangePhase={}",
    barrier.getId(), sourceAction.getName(), schemaChangePhase.get());
```

---

### 断点 16: CheckpointBarrier 在 Sink 端

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/flow/SinkFlowLifeCycle.java`

**插桩位置**: `received()` 方法中 Barrier 分支 (第 193 行)

```java
// 在 received() 的 Barrier 分支开始处
if (record.getData() instanceof Barrier) {
    Barrier barrier = (Barrier) record.getData();
    LOGGER.info("[CDC-DEBUG] SinkFlowLifeCycle.received(Barrier) - checkpointId={}, " +
        "sinkName={}, prepareClose={}, snapshot={}",
        barrier.getId(), sinkAction.getName(), barrier.prepareClose(this.taskLocation),
        barrier.snapshot());
}
```

---

### 断点 17: Debezium 反序列化（CDC 数据源专用）

**文件**: `seatunnel-connectors-v2/connector-cdc/connector-cdc-base/src/main/java/org/apache/seatunnel/connectors/cdc/base/source/reader/IncrementalSourceRecordEmitter.java`

**插桩位置**: `emitElement()` 方法 (第 199 行)

```java
// 在 emitElement() 中，Debezium 反序列化之前
SourceRecord record = ...;
LOGGER.info("[CDC-DEBUG] RecordEmitter.emitElement() - topic={}, key={}, offset={}",
    record.topic(), record.key(), record.sourceOffset());
```

**插桩位置**: `debeziumDeserializationSchema.deserialize()` 调用后，通过 OutputCollector 拦截

```java
// 修改 OutputCollector.collect() (第 207 行)
@Override
public void collect(T record) {
    if (record instanceof SeaTunnelRow) {
        SeaTunnelRow row = (SeaTunnelRow) record;
        LOGGER.info("[CDC-DEBUG] RecordEmitter.emitElement() -> SeaTunnelRow: " +
            "tableId={}, rowKind={}, fields={}",
            row.getTableId(), row.getRowKind(), row.getFieldCount());
    }
    output.collect(record);
}
```

---

### 断点 18: SeaTunnelTask 执行循环

**文件**: `seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/task/SeaTunnelTask.java`

**插桩位置**: `stateProcess()` 方法中

```java
// 打印每次 collect 循环
LOGGER.info("[CDC-DEBUG] SeaTunnelTask.stateProcess() - taskType={}, taskIndex={}, progress={}",
    this.getClass().getSimpleName(), indexID, progress.toState());
```

---

## 四、调试执行计划

### 4.1 第一轮：静默运行（验证配置）

目标：确认配置能正常运行，控制台有输出

```bash
# 先用 FakeSource 跑 10 条数据
# 修改 config/debug_cdc_flow.conf 中 row.num = 10
./bin/seatunnel.sh -c config/debug_cdc_flow.conf
```

**检查点**: 控制台输出应该有 10 条数据

### 4.2 第二轮：全链路追踪

目标：18 个插桩点全部生效，观察完整数据流

```bash
# 运行 5 条数据
./bin/seatunnel.sh -c config/debug_cdc_flow.conf 2>&1 | grep "CDC-DEBUG"
```

**预期输出顺序**:
```
[CDC-DEBUG] JobMaster.init() - JobId=..., LogicalDag vertices=3
[CDC-DEBUG] LogicalVertex: id=1, name=FakeSource, type=SourceAction
[CDC-DEBUG] LogicalVertex: id=2, name=Sql, type=TransformAction
[CDC-DEBUG] LogicalVertex: id=3, name=Console, type=SinkAction
[CDC-DEBUG] ExecutionPlanGenerator.generate() - START
[CDC-DEBUG] ExecutionPlan.generate() - Pipeline: id=1, edges=2
[CDC-DEBUG] PhysicalPlan created - PipelineCount=1, SubPlanCount=1
[CDC-DEBUG] SourceFlowLifeCycle.init() - sourceName=FakeSource, readerClass=...
[CDC-DEBUG] SourceFlowLifeCycle.open() - reader opened
[CDC-DEBUG] SeaTunnelTask.stateProcess() - taskType=SourceSeaTunnelTask, ...
[CDC-DEBUG] SourceCollector.collect() - tableId=mydb.orders, rowKind=+I, fields=3
[CDC-DEBUG] SourceCollector.sendRecordToNext() - tableId=mydb.orders, ...
[CDC-DEBUG] TransformFlowLifeCycle.received() - tableId=mydb.orders, ...
[CDC-DEBUG] TransformCollector.collect() -> Sink - tableId=mydb.orders, ...
[CDC-DEBUG] SinkFlowLifeCycle.received() - tableId=mydb.orders, ...
[CDC-DEBUG] SinkFlowLifeCycle - writer.write() called, tableId=mydb.orders
[CDC-DEBUG] CheckpointCoordinator - trigger checkpoint[1], pipelineId=1
[CDC-DEBUG] SourceFlowLifeCycle.triggerBarrier() - checkpointId=1
[CDC-DEBUG] SinkFlowLifeCycle.received(Barrier) - checkpointId=1
```

### 4.3 第三轮：CDC 真实场景（如果有 MySQL）

目标：验证 `tableId` 和 `rowKind` 在真实 CDC 数据中的值

**关键验证**:
- `tableId` 是 `database.table` 格式吗？
- `rowKind` 对 INSERT/UPDATE/DELETE 分别是什么值？
- 一条 UPDATE 是否产生两条 Row（UPDATE_BEFORE + UPDATE_AFTER）？
- `row.getOptions()` 中是否包含了 binlog 位点信息？

---

## 五、关键验证点

运行调试后，需要回答以下问题：

### 5.1 架构验证

| 问题 | 验证方法 | 期望答案 |
|------|---------|---------|
| 一个 Source 是否只生成一个 Pipeline？ | 断点 2, 3, 4 | 是，当前是线性 Pipeline |
| Source 和 Sink 在同一个 TaskGroup 吗？ | 断点 5, 18 | 取决于配置，通常分离 |
| Transform 链是串行还是并行？ | 断点 9, 10 | 串行执行 |
| Checkpoint 是否全局统一？ | 断点 14, 15, 16 | 同一个 checkpointId 贯穿所有节点 |

### 5.2 数据载体验证

| 问题 | 验证方法 | 期望答案 |
|------|---------|---------|
| `SeaTunnelRow.tableId` 格式？ | 断点 7 | `database.table` 格式 |
| `RowKind` 有哪些值？ | 断点 7 | `+I`, `-U`, `+U`, `-D` |
| binlog 位点在哪里？ | 断点 7, 17 | `row.getOptions()` 或 SourceRecord 中 |
| Transform 后 tableId 是否保留？ | 断点 11 | 应该保留（需要验证） |

### 5.3 分发器可行性验证

| 问题 | 验证方法 | 对我们的方案影响 |
|------|---------|----------------|
| `row.getTableId()` 在整个链路中是否始终可用？ | 断点 7 → 9 → 11 → 12 | 如果 Transform 后丢失，分发器必须在 Transform 之前 |
| 同一条 Row 能否被多个 Transform 处理？ | 分析断点 8, 11 的 sendRecordToNext | 当前是串行，需要改造 |
| Checkpoint 粒度到 Pipeline 还是 Task？ | 断点 14, 15, 16 | 决定路径 B 是否需要独立 Checkpoint |

---

## 六、调试脚本

### 6.1 一键加日志脚本

```bash
#!/bin/bash
# 文件: scripts/add_debug_logs.sh
# 用法: bash scripts/add_debug_logs.sh

SEATUNNEL_ROOT="/Users/wangzhijun/sourceCode/seatunnel"

# 1. JobMaster.init()
# 在 logicalDag 创建后添加日志
# (需要手动编辑，因为插入位置需要精确匹配)

# 2. ExecutionPlanGenerator.generate()
# ...

# 3. SeaTunnelSourceCollector.collect()
# ...

echo "All debug logs added. Search for [CDC-DEBUG] in code."
```

### 6.2 日志过滤脚本

```bash
#!/bin/bash
# 文件: scripts/watch_debug.sh
# 用法: bash scripts/watch_debug.sh <logfile>

LOGFILE=${1:-"logs/seatunnel-engine-server.log"}

echo "=== Watching CDC-DEBUG logs ==="
tail -f "$LOGFILE" | grep --color=auto "CDC-DEBUG\|CDC-RECORD\|ERROR\|WARN"
```

---

## 七、附录

### A. MySQL-CDC 真实配置

如果有本地 MySQL，替换为真实 CDC 配置：

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = 10000
}

source {
  MySQL-CDC {
    result_table_name = "cdc_source"
    server-id = 5652
    username = "root"
    password = "password"
    hostname = "localhost"
    port = 3306
    database-names = ["mydb"]
    table-names = ["mydb.orders", "mydb.users"]
    startup.mode = "initial"
  }
}

sink {
  Console {
    source_table_name = "cdc_source"
  }
}
```

### B. 插桩文件清单

| # | 文件 | 方法 | 行号附近 |
|---|------|------|---------|
| 1 | `JobMaster.java` | `init()` | ~245 |
| 2 | `JobMaster.java` | `init()` | ~290 |
| 3 | `ExecutionPlanGenerator.java` | `generate()` | 开始/结束 |
| 4 | `PipelineGenerator.java` | `generate()` | 各方法 |
| 5 | `TaskGroup.java` 或调度类 | 创建后 | - |
| 6 | `SourceFlowLifeCycle.java` | `init()` | ~115 |
| 7 | `SeaTunnelSourceCollector.java` | `collect(T)` | ~93 |
| 8 | `SeaTunnelSourceCollector.java` | `sendRecordToNext()` | ~191 |
| 9 | `TransformFlowLifeCycle.java` | `received()` | - |
| 10 | `TransformSeaTunnelTask.java` | `collect()` | ~71 |
| 11 | `SeaTunnelTransformCollector.java` | `collect(Record)` | ~36 |
| 12 | `SinkFlowLifeCycle.java` | `received()` | ~265 |
| 13 | `SinkFlowLifeCycle.java` | `received()` | ~270 |
| 14 | `CheckpointCoordinator.java` | `tryTriggerPendingCheckpoint` | - |
| 15 | `SourceFlowLifeCycle.java` | `triggerBarrier()` | - |
| 16 | `SinkFlowLifeCycle.java` | `received()` Barrier | ~193 |
| 17 | `IncrementalSourceRecordEmitter.java` | `emitElement()` | ~199, ~207 |
| 18 | `SeaTunnelTask.java` | `stateProcess()` | - |