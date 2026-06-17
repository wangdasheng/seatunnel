# SeaTunnel CDC 数据迁移链路深度解析

## 一、整体架构：三大阶段，七层对象

```
┌────────────────────────────────────────────────────────────────────┐
│                      Master (CoordinatorService)                    │
│  ┌──────────┐    ┌──────────────┐    ┌──────────────────────────┐  │
│  │ submitJob│───▶│   JobMaster   │───▶│  CheckpointManager       │  │
│  │          │    │  run/init     │    │  ├─CheckpointCoordinator │  │
│  │          │    │  LogicalDag   │    │  │ ├─barrier 定时触发     │  │
│  │          │    │  PhysicalPlan │    │  │ ├─ack 收集            │  │
│  │          │    │  SubPlan部署  │    │  │ └─状态持久化          │  │
│  └──────────┘    └──────────────┘    └──────────────────────────┘  │
├────────────────────────────────────────────────────────────────────┤
│                      Worker (TaskExecutionService)                  │
│                                                                     │
│  ┌───────────────────┐  ┌───────────────────┐                      │
│  │ SourceSeaTunnelTask│  │  SinkSeaTunnelTask │                     │
│  │ ┌───────────────┐ │  │  ┌───────────────┐ │                     │
│  │ │SourceFlowLife │ │  │  │SinkFlowLife   │ │                     │
│  │ │ .init()→      │ │  │  │ .received()→  │ │                     │
│  │ │   createReader│ │  │  │   writer.write│ │                     │
│  │ │ .collect()→   │─┼──┼─▶│   Barrier时    │ │                     │
│  │ │   pollNext()  │ │  │  │   prepareCommit│ │                     │
│  │ └───────────────┘ │  │  └───────────────┘ │                     │
│  └───────────────────┘  └───────────────────┘                      │
│             │                      │                                │
│      SeaTunnelSourceCollector      │                                │
│       (collect→sendRecordToNext)   │                                │
│             │                      │                                │
│       ┌─────▼──────────────────────▼─────┐                         │
│       │   IntermediateBlockingQueue      │  (跨 TaskGroup 传输)     │
│       │   ArrayBlockingQueue<Record<?>>  │                         │
│       │   CAPACITY = 2048                │                         │
│       └──────────────────────────────────┘                         │
└────────────────────────────────────────────────────────────────────┘
```

## 二、七大核心对象及职责

### 第 0 层：入口

| 对象 | 文件 | 作用 |
|------|------|------|
| `SeaTunnel.run()` | `seatunnel-core-starter/.../SeaTunnel.java` | 应用入口，解析命令行参数，创建执行器 |
| `ClientExecuteCommand.execute()` | `seatunnel-starter/.../ClientExecuteCommand.java` | 选择 MasterType(LOCAL/CLUSTER)，提交作业 |

### 第 1 层：作业编排 (Master 端)

| 对象 | 文件 | 行号 | 作用 |
|------|------|------|------|
| **CoordinatorService.submitJob()** | `seatunnel-engine/.../server/CoordinatorService.java` | ~619 | **总入口**：接收客户端提交的 JobImmutableInformation，创建 JobMaster，放入 pendingJobQueue |
| **JobMaster.init()** | `seatunnel-engine/.../server/master/JobMaster.java` | ~180 | **作业初始化**：反序列化 LogicalDag → PhysicalPlan + CheckpointPlan → 创建 CheckpointManager |
| **JobMaster.run()** | `seatunnel-engine/.../server/master/JobMaster.java` | ~220 | **启动执行**：调用 PhysicalPlan.startJob()，部署 SubPlan 到 Worker |
| **PhysicalPlan.stateProcess()** | `seatunnel-engine/.../server/dag/physical/PhysicalPlan.java` | ~120 | **状态机驱动**：SCHEDULED→RUNNING，部署所有 SubPlan 到 Worker |
| **SubPlan** | 同上文件 | | 代表一条 pipeline（source→transform→sink），包含多个 PhysicalVertex(Task) |

### 第 2 层：Checkpoint 协调

| 对象 | 文件 | 行号 | 作用 |
|------|------|------|------|
| **CheckpointManager** | `.../server/checkpoint/CheckpointManager.java` | | **总管理器**：管理 job 内所有 pipeline 的 checkpoint，为每个 pipeline 创建 CheckpointCoordinator |
| **CheckpointCoordinator** | `.../server/checkpoint/CheckpointCoordinator.java` | ~93 | **单 pipeline 协调器**：定时触发 barrier、收集各 Task 的 ack、完成/中止 checkpoint |
| **CheckpointBarrier** | `.../checkpoint/CheckpointBarrier.java` | | **Barrier 数据对象**：作为特殊 Record 在 Flow 链中传输，触发各节点快照 |
| **CheckpointCoordinator.reportedTask()** | 同文件 | ~256 | 接收 Worker 上报的 Task 状态，所有 Task READY_START 后开始定时触发 checkpoint |
| **CheckpointCoordinator.triggerCoordinator()** | 同文件 | ~380 | **触发 checkpoint**：生成 CheckpointBarrier，注入到 Source Task |

### 第 3 层：Worker 端 Task 执行

| 对象 | 文件 | 行号 | 作用 |
|------|------|------|------|
| **TaskExecutionService.deployTask()** | `.../server/TaskExecutionService.java` | ~70 | Worker 接收 Master 部署的 TaskGroup，创建 TaskGroup 实例 |
| **SourceSeaTunnelTask.init()** | `.../server/task/SourceSeaTunnelTask.java` | ~71 | **Source Task 初始化**：创建 SeaTunnelSourceCollector，绑定到 SourceFlowLifeCycle |
| **SourceSeaTunnelTask.call()** | 同文件 | ~131 | **驱动**：循环调用 stateProcess() → collect() |
| **SeaTunnelTask.stateProcess()** | `.../server/task/SeaTunnelTask.java` | ~137 | **状态机**：READY_START → RUNNING，驱动 Flow 链 |

### 第 4 层：Source 数据读取 (Connector 层)

| 对象 | 文件 | 行号 | 作用 |
|------|------|------|------|
| **IncrementalSource.createEnumerator()** | `connector-cdc/.../cdc/base/source/IncrementalSource.java` | ~259 | 创建 Enumerator：发现表 → 创建 HybridSplitAssigner(initial 模式) 或 IncrementalSplitAssigner(latest 模式) |
| **IncrementalSource.createReader()** | 同文件 | ~219 | **创建 Reader**：创建 IncrementalSourceReader + LinkedBlockingQueue(容量2) |
| **IncrementalSourceReader.pollNext()** | `connector-cdc/.../cdc/base/source/reader/IncrementalSourceReader.java` | | **核心读取**：从 elementsQueue 取 Debezium SourceRecords → RecordEmitter 转为 SeaTunnelRow → 调用 Collector.collect() |
| **MySqlSnapshotFetchTask** | `connector-cdc-mysql/.../reader/fetch/scan/MySqlSnapshotFetchTask.java` | | **全量快照**：JDBC 分 chunk 读取历史数据 |
| **MySqlBinlogFetchTask** | `connector-cdc-mysql/.../reader/fetch/binlog/MySqlBinlogFetchTask.java` | | **增量读取**：监听 MySQL binlog，解析 INSERT/UPDATE/DELETE 事件 |
| **IncrementalSourceRecordEmitter** | `connector-cdc/.../cdc/base/source/reader/IncrementalSourceRecordEmitter.java` | | **记录转换**：Debezium SourceRecord → SeaTunnelRow (含 RowKind 信息) |

### 第 5 层：Flow 生命周期 (Engine 层)

| 对象 | 文件 | 行号 | 作用 |
|------|------|------|------|
| **SourceFlowLifeCycle.init()** | `.../server/task/flow/SourceFlowLifeCycle.java` | ~115 | 调用 `source.createReader()` 创建 Reader |
| **SourceFlowLifeCycle.collect()** | 同文件 | ~150 | **核心循环**：调用 `reader.pollNext(collector)` 读数据，无数据时 sleep(100ms) |
| **SeaTunnelSourceCollector.collect()** | `.../server/task/SeaTunnelSourceCollector.java` | ~93 | **数据收集**：接收 Reader 产出的 SeaTunnelRow → 调用 `sendRecordToNext()` |
| **SeaTunnelSourceCollector.sendRecordToNext()** | 同文件 | ~191 | **数据投递**：遍历 outputs，调用 `output.received(record)` |
| **IntermediateQueueFlowLifeCycle** | `.../server/task/flow/IntermediateQueueFlowLifeCycle.java` | | **中间队列**：跨 TaskGroup 传输数据的桥梁 |
| **IntermediateBlockingQueue.received()** | `.../server/task/group/queue/IntermediateBlockingQueue.java` | ~42 | put 数据到 ArrayBlockingQueue(2048) |
| **IntermediateBlockingQueue.collect()** | 同文件 | ~52 | poll 数据从队列取走 |

### 第 6 层：Sink 数据写入

| 对象 | 文件 | 行号 | 作用 |
|------|------|------|------|
| **SinkFlowLifeCycle.received(Record)** | `.../server/task/flow/SinkFlowLifeCycle.java` | ~191 | **Sink 入口**：分三种分支：Barrier/Barrier → prepareCommit()；SchemaChangeEvent → applySchemaChange()；普通数据 → writer.write() |
| **JdbcSinkWriter.write()** | `connector-jdbc/.../jdbc/sink/JdbcSinkWriter.java` | ~150 | **逐行写入**：调用 outputFormat.writeRecord(element) 写入目标表 |
| **JdbcSinkWriter.prepareCommit()** | 同文件 | ~190 | **checkpoint 时提交**：flush + connection.commit() |
| **JdbcSinkWriter.flushData()** | 同文件 | ~178 | 刷新批量缓冲区 |

### 第 7 层：Barrier 与 Checkpoint 完整流程

```
CheckpointCoordinator
  │ 定时触发 (checkpoint.interval)
  │
  ├─1. triggerCoordinator() ──生成 CheckpointBarrier
  │     注入到 Source Task
  │
  ▼
SourceSeaTunnelTask.triggerBarrier()
  │
  ├─2. SourceFlowLifeCycle.triggerBarrier()
  │     等待 pollNext() 释放 checkpointLock
  │     reader.snapshotState() 获取 Source 状态
  │
  ├─3. SeaTunnelSourceCollector.sendRecordToNext(barrierRecord)
  │     把 Barrier 作为 Record 发送给下游
  │
  ▼
SinkFlowLifeCycle.received(Record)
  │ record.getData() instanceof Barrier ?
  │
  ├─4. YES → writer.prepareCommit(barrierId)
  │          flush + commit 事务
  │          writer.snapshotState(barrierId)
  │          runningTask.ack(barrier)
  │
  ▼
CheckpointCoordinator
  │ 收集所有 Task 的 ack
  │
  └─5. completePendingCheckpoint()
         存储到 CheckpointStorage
         通知所有 Task checkpoint 完成
```

## 三、推荐断点位置（按数据流顺序）

### 🔴 断点 1：作业提交入口
```
文件: CoordinatorService.java
方法: submitJob(long jobId, Data jobImmutableInformation, ...)
行号: ~619
```
**作用**：你 debug 的起点。在这里能看到整个 JobImmutableInformation 结构，包含配置、DAG、jar 包等。F9 跳过，F7 进入看 JobMaster 如何创建。

### 🔴 断点 2：JobMaster 初始化 - 看 DAG 转 PhysicalPlan
```
文件: JobMaster.java
方法: init()
行号: ~180 (PlanUtils.fromLogicalDAG 调用处)
```
**作用**：LogicalDag（配置解析出的逻辑 DAG）如何转换为 PhysicalPlan（可执行的任务图）。这里能看到 pipeline 被拆成哪些 TaskGroup。

### 🔴 断点 3：CDC 创建 Enumerator - 看分片策略
```
文件: IncrementalSource.java
方法: createEnumerator()
行号: ~259
```
**作用**：当 startup.mode=initial 时，创建 HybridSplitAssigner。这里能看到表发现、chunk 拆分策略（主键范围分片）、每个 Subtask 被分配哪些 split。

### 🔴 断点 4：CDC 创建 Reader - 看读取器装配
```
文件: IncrementalSource.java
方法: createReader()
行号: ~219
```
**作用**：创建 IncrementalSourceReader 实例。重点看 elementsQueue（容量 2 的 LinkedBlockingQueue）——这是 Debezium 引擎和 SeaTunnel 之间的桥梁。

### 🔴 断点 5：Worker 端 Task 初始化
```
文件: SourceSeaTunnelTask.java
方法: init()
行号: ~71
```
**作用**：SeaTunnelSourceCollector 的创建和 SourceFlowLifeCycle 的绑定。从这里可以跟踪 `source.createReader()` 被调用。

### 🔴 断点 6：SourceFlowLifeCycle 初始化
```
文件: SourceFlowLifeCycle.java
方法: init()
行号: ~115 (this.reader = sourceAction.getSource().createReader(context))
```
**作用**：Worker 端真正创建 Reader 的地方。可以 F7 进入看 IncrementalSourceReader 的内部构造。

### 🔴 断点 7：数据读取核心循环 ★★★
```
文件: SourceFlowLifeCycle.java
方法: collect()
行号: ~159 (reader.pollNext(collector))
```
**作用**：**最重要的断点！** 每一次循环从 Reader 拉取一条数据。F7 进入看 IncrementalSourceReader 如何从 Debezium 队列取数据、RecordEmitter 如何转换。

### 🔴 断点 8：Collector 接收数据 ★★★
```
文件: SeaTunnelSourceCollector.java
方法: collect(T row)
行号: ~93
```
**作用**：Reader 产出数据后的第一站。在这里能看到 SeaTunnelRow 的完整内容（id, name, email 等字段值）。F7 进入 `sendRecordToNext()` 看数据如何流向 Sink。

### 🔴 断点 9：数据写入 Sink ★★★
```
文件: SinkFlowLifeCycle.java
方法: received(Record<?> record)
行号: ~191
```
**作用**：Sink 端的唯一入口。三条分支：
- `record.getData() instanceof Barrier` → prepareCommit (行 203)
- `record.getData() instanceof SchemaChangeEvent` → applySchemaChange (行 258)
- 普通数据 → writer.write() (行 270)

### 🔴 断点 10：JDBC 逐行写入
```
文件: JdbcSinkWriter.java
方法: write(SeaTunnelRow element)
行号: ~150
```
**作用**：看到每一行数据最终如何通过 JDBC 写入目标表。F7 进入 `outputFormat.writeRecord()` 看 SQL 生成。

### 🔴 断点 11：JDBC prepareCommit - 批量提交
```
文件: JdbcSinkWriter.java
方法: prepareCommit()
行号: ~190
```
**作用**：Checkpoint 时触发。flush 所有缓冲数据 + connection.commit()。这是事务的边界。

### 🔴 断点 12：Checkpoint Coordinator 触发 Barrier
```
文件: CheckpointCoordinator.java
搜索: triggerCoordinator 方法 (约 ~380)
```
**作用**：看 CheckpointCoordinator 如何定时生成 CheckpointBarrier。可以看到 barrierId 递增、注入到 Source Task 的完整过程。

### 🔴 断点 13：Barrier 在 Source 端的处理
```
文件: SourceFlowLifeCycle.java
方法: triggerBarrier(Barrier barrier)
搜索: "triggerBarrier" (约 ~195)
```
**作用**：Barrier 到达 Source 端。等待 checkpointLock 释放后，调用 `reader.snapshotState()` 保存 binlog offset，然后通过 collector 发送 Barrier 到下游。

### 🔴 断点 14：Barrier ack 返回 Coordinator
```
文件: CheckpointCoordinator.java
方法: acknowledgeTask (约 ~540)
```
**作用**：收集所有 Task 对 checkpoint 的 ack。当所有 Task ack 到达，checkpoint 完成状态被持久化。

## 四、数据流完整链路总结

```
┌──────────────────┐
│ MySQL binlog     │  ← Debezium 引擎实时监听
│ (INSERT/UPDATE/  │
│  DELETE events)  │
└────────┬─────────┘
         │ Debezium SourceRecord
         ▼
┌──────────────────┐
│ LinkedBlockingQueue │  ← elementsQueue (容量2)
│ (IncrementalSource  │     解耦 Debezium 和 SeaTunnel
│  Reader 内部)       │
└────────┬─────────┘
         │ pollNext() 取出
         ▼
┌──────────────────┐
│ RecordEmitter     │  ← SourceRecord → SeaTunnelRow
│                    │     提取 RowKind(INSERT/UPDATE/DELETE)
└────────┬─────────┘
         │ collector.collect(SeaTunnelRow)
         ▼
┌──────────────────┐
│ SeaTunnelSource   │  ← sendRecordToNext()
│ Collector         │     包装为 Record<?>
└────────┬─────────┘
         │ output.received(record)
         ▼
┌──────────────────┐
│ Intermediate      │  ← ArrayBlockingQueue(2048)
│ BlockingQueue     │     跨 TaskGroup 传输
└────────┬─────────┘
         │ collect() poll
         ▼
┌──────────────────┐
│ SinkFlowLifeCycle │  ← received(record)
│                    │     分 Barrier / RowKind / SchemaChange
└────────┬─────────┘
         │ writer.write(SeaTunnelRow)
         ▼
┌──────────────────┐
│ JdbcSinkWriter    │  ← outputFormat.writeRecord()
│                    │     生成 INSERT/UPDATE/DELETE SQL
└────────┬─────────┘
         │ JDBC execute
         ▼
┌──────────────────┐
│ MySQL (wangzhijun)│  ← 目标数据库
└──────────────────┘
```

## 五、调试方法

在 IDE 中运行 `SeaTunnelEngineLocalExample`，传入你建的配置文件路径：

```java
// VM options: 无需特殊配置
// Program arguments: 指向你的配置文件
// 例如: /Users/wangzhijun/sourceCode/seatunnel/test_cdc_mysql_to_mysql.conf
```

同时运行持续写入脚本：
```bash
./scripts/cdc_continuous_write.sh 2
```

然后按断点推荐顺序逐个打入断点，观察每一步中对象的创建和数据的流转。
