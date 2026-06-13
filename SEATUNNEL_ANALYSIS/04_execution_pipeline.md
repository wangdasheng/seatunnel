# 四、数据迁移核心主流程

## 完整执行链路总览 (Zeta引擎)

```
[1] 任务提交 (CLI)
  ↓
[2] 配置解析 (HOCON)
  ↓
[3] 插件加载 (SPI)
  ↓
[4] DAG构建 (Action→LogicalDag)
  ↓
[5] 任务提交到Master (JobImmutableInformation)
  ↓
[6] Master调度 (PhysicalPlan→TaskGroup分配)
  ↓
[7] Worker执行 (Source→Transform→Sink)
  ↓
[8] Checkpoint & 容错
  ↓
[9] 任务结束
```

---

## 步骤详解

### [1] 任务提交 — CLI 入口

**代码位置**: `seatunnel-core/seatunnel-starter/.../SeaTunnelClient.java:32`

```
SeaTunnelClient.main(args)
  → CommandLineUtils.parse(args, ClientCommandArgs)     // 解析CLI参数
  → clientCommandArgs.buildCommand()                     // 构建Command对象
  → SeaTunnel.run(command)                               // 执行命令
    → command.execute()                                  // ClientExecuteCommand.execute()
```

**关键类**:
- `SeaTunnelClient.main()`: `seatunnel-core/seatunnel-starter/src/main/java/.../SeaTunnelClient.java:32`
- `ClientCommandArgs`: `seatunnel-core/seatunnel-starter/src/main/java/.../args/ClientCommandArgs.java`
- `ClientExecuteCommand.execute()`: `seatunnel-core/seatunnel-starter/src/main/java/.../command/ClientExecuteCommand.java:85`

**execute() 核心逻辑**:
1. 加载 `SeaTunnelConfig` (从 seatunnel.yaml)
2. 判断 local/cluster 模式
3. local 模式: 本地启动 HazelcastInstance (内嵌 Server)
4. 创建 `SeaTunnelClient` (Hazelcast Client)
5. 调用 `engineClient.createExecutionContext()` → `ClientJobExecutionEnvironment`
6. 调用 `jobExecutionEnv.execute()` → `ClientJobProxy`
7. 调用 `clientJobProxy.waitForJobCompleteV2()` 阻塞等待完成

---

### [2] 配置文件解析 — HOCON 格式

**代码位置**: `seatunnel-core/seatunnel-core-starter/.../utils/ConfigBuilder.java`

**配置文件格式**:
```hocon
env {
  parallelism = 2
  job.mode = "BATCH"
}

source {
  FakeSource {
    result_table_name = "fake"
    row.num = 100
    schema = {
      fields { name = "string", age = "int" }
    }
  }
}

transform {
  Sql {
    source_table_name = "fake"
    result_table_name = "fake2"
    query = "select name, age from fake"
  }
}

sink {
  Console {
    source_table_name = "fake2"
  }
}
```

**解析流程**:
```
ConfigBuilder.of(filePath)
  → ConfigFactory.parseFile(filePath)           // Typesafe Config 解析
  → ConfigAdapter (SPI) 可选自定义格式适配器
  → backfillUserVariables()                      // 变量替换 ${var}
  → ConfigShadeUtils.decryptConfig()             // 敏感信息解密
  → 输出 Config 对象
```

---

### [3] 插件加载与实例化 — SPI 机制

**代码位置**: `seatunnel-plugin-discovery/.../AbstractPluginDiscovery.java`

**执行位置**: `seatunnel-engine/seatunnel-engine-core/.../parse/MultipleTableJobConfigParser.java`

**流程**:
```
MultipleTableJobConfigParser.parse()
  → 解析 source/transform/sink 配置块
  → 构建 PluginIdentifier (engine="seatunnel", type="source", name="FakeSource")
  → SeaTunnelSourcePluginDiscovery.createPluginInstance(pluginIdentifier)
    → ServiceLoader.load(getPluginBaseClass(), classLoader)   // Java SPI
    → 匹配 getPluginName() 找到对应实例
    → 或通过 plugin-mapping.properties → 查找 JAR → URLClassLoader 加载
  → 调用 Factory.createSource() 或 Source.prepare() 完成初始化
```

**插件发现的三种路径**:
1. **Classpath**: 插件JAR已在classpath中 (开发模式)
2. **PluginDir**: `SEATUNNEL_HOME/connectors/` 目录下的JAR (生产模式)
3. **Upload**: 客户端上传JAR到Master (connectorJarStorage.enable=true)

**plugin-mapping.properties 结构**:
```properties
--connectors-v2--
connector-fake
connector-jdbc
connector-kafka
...
```

---

### [4] DAG 构建 — Action → LogicalDag

**代码位置**:
- `MultipleTableJobConfigParser.parse()`: `seatunnel-engine/seatunnel-engine-core/.../parse/MultipleTableJobConfigParser.java`
- `LogicalDagGenerator`: `seatunnel-engine/seatunnel-engine-core/.../dag/logical/LogicalDagGenerator.java`
- `ClientJobExecutionEnvironment.getLogicalDag()`: `seatunnel-engine/seatunnel-engine-client/.../job/ClientJobExecutionEnvironment.java:121`

**DAG构建流程**:
```
MultipleTableJobConfigParser.parse()
  → 解析 env 配置 → JobConfig
  → 解析 source 块 → 创建 SourceAction (包含 SeaTunnelSource 实例)
  → 解析 transform 块 → 创建 TransformAction/TransformChainAction
  → 解析 sink 块 → 创建 SinkAction (包含 SeaTunnelSink 实例)
  → 通过 source_table_name/result_table_name 建立上下游关系
  → 返回 List<Action> (Action DAG)

LogicalDagGenerator.generate()
  → Action DAG → LogicalVertex + LogicalEdge
  → 生成 LogicalDag
```

**Action 类型** (位于 `seatunnel-engine/seatunnel-engine-core/.../dag/actions/`):
| Action | 职责 |
|--------|------|
| `SourceAction` | 包装 SeaTunnelSource |
| `SinkAction` | 包装 SeaTunnelSink + SinkConfig |
| `TransformAction` | 包装单个 SeaTunnelTransform |
| `TransformChainAction` | 链式 Transform (多个串联优化) |

---

### [5] 任务提交到 Master

**代码位置**: `seatunnel-engine/seatunnel-engine-client/.../job/ClientJobExecutionEnvironment.java:189`

```
ClientJobExecutionEnvironment.execute()
  → getLogicalDag()                            // 构建 LogicalDag
  → 构建 JobImmutableInformation (jobId, jobName, logicalDag, jarUrls, connectorJarIds)
  → jobClient.createJobProxy(jobImmutableInformation)
    → Hazelcast RPC 发送到 Master 节点
  → 返回 ClientJobProxy (任务句柄)
```

---

### [6] Master 调度 — PhysicalPlan 生成与 TaskGroup 分配

**核心代码位置**:
- `CoordinatorService`: `seatunnel-engine/seatunnel-engine-server/.../CoordinatorService.java`
- `JobMaster` (内部): 在 CoordinatorService 中管理 Job 生命周期
- `PhysicalPlanGenerator`: `seatunnel-engine/seatunnel-engine-server/.../dag/physical/PhysicalPlanGenerator.java`
- `DAGUtils`: `seatunnel-engine/seatunnel-engine-server/.../dag/DAGUtils.java`
- `ExecutionPlanGenerator`: `seatunnel-engine/seatunnel-engine-server/.../dag/execution/ExecutionPlanGenerator.java`

**调度流程**:
```
CoordinatorService.submitJob(jobImmutableInformation)
  → LogicalDag → ExecutionPlan (execution vertex/edge)
  → PipelineGenerator → Pipeline 列表
  → PhysicalPlanGenerator → PhysicalPlan (PhysicalVertex 列表)
  → 资源分配: 将 PhysicalVertex 分配到 Worker 节点
  → SubPlan 管理: 每个 Pipeline 一个 SubPlan
  → 部署 TaskGroup 到 Worker
```

**调度层级**:
```
Job
  └── Pipeline (多条并行Pipeline)
       └── PhysicalVertex (单个执行单元)
            └── TaskGroup (在单个Worker线程内执行)
                 └── Task (SourceTask/TransformTask/SinkTask)
```

---

### [7] Worker 执行 — Source→Transform→Sink 数据流

**核心代码位置**:
- `TaskGroupRunner`: `seatunnel-engine/seatunnel-engine-server/.../execution/TaskGroupRunner.java` (推测)
- `Task`: `seatunnel-engine/seatunnel-engine-server/.../execution/Task.java`
- `TaskExecutionContext`: `seatunnel-engine/seatunnel-engine-server/.../execution/TaskExecutionContext.java`

**数据流转**:
```
Worker 节点收到 TaskGroup
  → TaskGroupRunner 在独立线程中执行
  → Source Task:
     - 调用 SourceSplitEnumerator 分配 Split
     - 调用 SourceReader.pollNext() 读取数据
     - 产出 SeaTunnelRow
  → Transform Task:
     - 调用 SeaTunnelTransform.map()/flatMap()
     - 转换 SeaTunnelRow
  → Sink Task:
     - 调用 SinkWriter.write(row)
     - 定期 prepareCommit() 获取 CommitInfo
     - 调用 SinkCommitter.commit() 提交

中间队列: IntermediateQueue 在 Task 之间传递数据
```

---

### [8] Checkpoint & 容错

**核心代码位置**:
- `CheckpointCoordinator`: `seatunnel-engine/seatunnel-engine-server/.../checkpoint/CheckpointCoordinator.java`
- `CheckpointManager`: `seatunnel-engine/seatunnel-engine-server/.../checkpoint/CheckpointManager.java`
- `PendingCheckpoint`: `seatunnel-engine/seatunnel-engine-server/.../checkpoint/PendingCheckpoint.java`
- `CheckpointBarrier`: `seatunnel-engine/seatunnel-engine-server/.../checkpoint/CheckpointBarrier.java`

**Checkpoint 流程** (类似 Chandy-Lamport):
```
CheckpointCoordinator 定期触发:
  → 创建 CheckpointBarrier
  → 注入到 Source 数据流中
  → Barrier 流经 Source → Transform → Sink
  → 每个 Task 收到 Barrier:
     - 快照当前状态 (SourceSplitEnumerator state, SinkWriter state)
     - 上报 TaskAcknowledgeOperation 给 Master
  → Master 收集所有 Ack:
     - 构建 CompletedCheckpoint
     - 持久化到 Hazelcast IMap
  → 通知各 Task checkpoint 完成 (CheckpointFinishedOperation)
```

**容错恢复**:
```
Task 失败:
  → Master 检测到 TaskGroup 失败
  → 取消当前 Pipeline 所有 Task
  → 从最近 CompletedCheckpoint 恢复
  → SourceSplitEnumerator.restoreEnumerator(checkpointState)
  → SinkWriter.restoreWriter(states)
  → 重新部署 TaskGroup
```

---

### [9] 任务结束

```
所有 Source 数据读完 (NoMoreSplitsEvent):
  → SourceReader 完成
  → Transform 处理完所有数据
  → SinkWriter 写完 + 最终 commit
  → Pipeline 状态变为 FINISHED
  → Job 所有 Pipeline 完成 → Job 状态变为 FINISHED
  → ClientJobProxy.waitForJobCompleteV2() 返回 JobResult
  → ClientExecuteCommand 打印 Job 统计信息
```

---

## 三引擎执行流程差异

### Zeta 引擎 (默认)
| 阶段 | 实现 |
|------|------|
| 任务提交 | `ClientExecuteCommand` → `SeaTunnelClient` (Hazelcast Client) |
| 配置解析 | `MultipleTableJobConfigParser` → Action DAG |
| 调度执行 | `CoordinatorService` → `PhysicalPlan` → `TaskGroup` |
| 数据传输 | Hazelcast IMap + 内部 `IntermediateQueue` |
| Checkpoint | 自研 `CheckpointCoordinator` (Chandy-Lamport 变体) |
| 分布式 | Hazelcast IMDG 集群 |

### Spark 引擎
| 阶段 | 实现 |
|------|------|
| 任务提交 | `SparkTaskExecuteCommand` → `SparkSession` |
| 配置解析 | 复用 `MultipleTableJobConfigParser` |
| 调度执行 | `SparkExecution` → `SourceExecuteProcessor` → `SparkSourceReader` |
| 数据转换 | `translation-spark` 层将 SeaTunnel API → Spark DataSource V2 |
| 分布式 | Spark Cluster Manager (YARN/K8s/Standalone) |
| Checkpoint | Spark 原生 Checkpoint (HDFS) |

### Flink 引擎
| 阶段 | 实现 |
|------|------|
| 任务提交 | `FlinkTaskExecuteCommand` → `StreamExecutionEnvironment` |
| 配置解析 | 复用 `MultipleTableJobConfigParser` |
| 调度执行 | `FlinkExecution` → `SourceExecuteProcessor` → `FlinkSource` |
| 数据转换 | `translation-flink` 层将 SeaTunnel API → Flink Source/Sink API |
| 分布式 | Flink ResourceManager (YARN/K8s/Standalone) |
| Checkpoint | Flink 原生 Checkpoint (HDFS/RocksDB) |

**核心差异总结**: Zeta 完全自研分布式执行，Spark/Flink 通过 translation 层桥接到各自原生引擎。Connector 代码三引擎共用，差异只在 translation 层。

---

## 附录：Zeta 引擎深度解析

### A. Master 节点核心 — CoordinatorService

**路径**: `seatunnel-engine/seatunnel-engine-server/.../CoordinatorService.java`

CoordinatorService 是 Zeta 引擎的"大脑"，运行在 Master 节点上，管理所有 Job 的完整生命周期。

**核心职责**:
1. **Job 提交**: `submitJob()` → 创建 `JobMaster` → 加入 `pendingJobQueue`
2. **调度循环**: `pendingJobSchedule()` 单线程轮询 pending 队列，检查资源可用性
3. **资源分配**: 通过 `ResourceManager` 的 `SlotAllocationStrategy` 分配 Slot
4. **状态管理**: 通过 Hazelcast IMap 持久化所有 Job 状态（支持 Master 故障转移）
5. **容错**: `memberRemoved()` 处理 Worker 节点故障，标记故障节点上的 Task 为 FAILED
6. **Master 切换**: `restoreAllRunningJobFromMasterNodeSwitch()` 从 IMap 恢复所有运行中 Job

**调度策略** (ScheduleStrategy):
- `WAIT`: 资源不足时阻塞等待
- `FAIL`: 资源不足时直接失败

### B. DAG 转换管线

```
User Config (HOCON/SQL)
  ↓ MultipleTableJobConfigParser.parse()
List<Action> (SourceAction → TransformAction → SinkAction)
  ↓ LogicalDagGenerator.generate()
LogicalDag (LogicalVertex + LogicalEdge)
  ↓ ExecutionPlanGenerator (添加并行度)
ExecutionPlan (ExecutionVertex + ExecutionEdge + Pipeline列表)
  ↓ PipelineGenerator
Pipeline 列表
  ↓ PhysicalPlanGenerator (展开并行实例)
PhysicalPlan
  ├── SubPlan (Pipeline 1)
  │   ├── PhysicalVertex (task-group-1, parallelism=0)
  │   ├── PhysicalVertex (task-group-1, parallelism=1)
  │   └── ...
  └── SubPlan (Pipeline 2) ...
```

**关键路径**:
- `PlanUtils.fromLogicalDAG()`: `engine-server/.../dag/physical/PlanUtils.java`
- `ExecutionPlanGenerator`: `engine-server/.../dag/execution/ExecutionPlanGenerator.java`
- `PhysicalPlanGenerator`: `engine-server/.../dag/physical/PhysicalPlanGenerator.java`

### C. 三级状态机

```
PhysicalPlan (Job级)
  JobStatus: CREATED → PENDING → SCHEDULED → RUNNING → FINISHED/FAILED/CANCELED
  └── SubPlan (Pipeline级)
        PipelineStatus: CREATED → SCHEDULED → DEPLOYING → RUNNING → FINISHED/FAILED/CANCELED
        支持自动恢复 (最多 pipelineMaxRestoreNum 次)
        └── PhysicalVertex (TaskGroup级)
              ExecutionState: CREATED → DEPLOYING → RUNNING → FINISHED/FAILED/CANCELED
```

### D. Task 执行模型 — Cooperative vs Blocking

**路径**: `seatunnel-engine/seatunnel-engine-server/.../TaskExecutionService.java`

由 `ThreadShareMode` (ALL/OFF/PART) 配置：

| 模式 | 实现 | 适用场景 |
|------|------|----------|
| **Cooperative** (ALL) | `CooperativeTaskWorker` 从共享 `LinkedBlockingDeque<TaskTracker>` 轮询，`TaskCallTimer` 监控超时(50ms)自动扩容 | 大量轻量 Task |
| **Blocking** (OFF) | `BlockingWorker` 每个 Task 独立线程，`init() → call() 循环 → close()` | 少量重 Task |

**SeaTunnelTask 状态机** (engine-server/.../task/statemachine/):
```
CREATED → INIT → WAITING_RESTORE → READY_START → STARTING → RUNNING → PREPARE_CLOSE → CLOSED
```

### E. 分布式状态存储 — Hazelcast IMap

**路径**: `seatunnel-engine/seatunnel-engine-common/.../Constant.java`

| IMap 名称 | 内容 | 用途 |
|-----------|------|------|
| `runningJobInfo` | jobId → JobInfo (序列化 JobImmutableInformation) | 任务提交信息 |
| `runningJobState` | jobId/PipelineLocation/TaskGroupLocation → Status | 任务状态 |
| `stateTimestamps` | keys → Long[] 状态转换时间戳 | 状态追踪 |
| `ownedSlotProfiles` | PipelineLocation → Map<TaskGroupLocation, SlotProfile> | Slot 分配记录 |
| `runningJobMetrics` | 分区 Map 的 Task 指标 | 运行时指标 |
| `finishedJobState/Metrics/VertexInfo` | 已完成 Job 信息 | 历史查询 |

**Master 故障转移**: 新 Master 从 IMap 读取 `runningJobInfo`，重建所有 `JobMaster`。

### F. 网络通信 — Hazelcast Operation

所有节点间通信通过 Hazelcast Operation，核心 Operation 分类:

| 类别 | 关键 Operation | 方向 |
|------|---------------|------|
| **任务部署** | `DeployTaskOperation`, `CancelTaskOperation` | Master → Worker |
| **状态上报** | `NotifyTaskStatusOperation`, `ReportMetricsOperation` | Worker → Master |
| **Checkpoint** | `CheckpointBarrierTriggerOperation`, `TaskAcknowledgeOperation`, `BarrierFlowOperation` | Master↔Worker |
| **资源管理** | `RequestSlotOperation`, `ReleaseSlotOperation`, `WorkerHeartbeatOperation` | Master↔Worker |
| **Source** | `AssignSplitOperation`, `RequestSplitOperation`, `SourceNoMoreElementOperation` | Enumerator↔Reader |
| **Sink** | `SinkRegisterOperation`, `SinkPrepareCommitOperation` | Writer↔Committer |

### G. Source 执行模式 — 4种 Pattern

| Pattern | 基类 | 适用 Connector | 特点 |
|---------|------|---------------|------|
| **直接实现** | `SeaTunnelSource` | JDBC, Kafka, Iceberg, Doris | 完全自定义 Split/Reader |
| **单 Split** | `AbstractSingleSplitSource` | HTTP, Socket, Redis, Neo4j | parallelism=1 |
| **多线程 Fetcher** | `SourceReaderBase` | Kafka, CDC | 后台线程预取 + 阻塞队列 |
| **快照+增量** | `IncrementalSource` (CDC) | MySQL/PG/Oracle CDC | 双阶段: SnapshotSplit → IncrementalSplit |

### H. Sink 提交模式

| 模式 | 基类 | 适用 Connector | 特点 |
|------|------|---------------|------|
| **简单写入** | `AbstractSimpleSink` | Console, HTTP, DingTalk | 无 commit 协议 |
| **两阶段提交** | `SeaTunnelSink` + Committer | JDBC (XA), Kafka, Iceberg | Writer→Committer→AggregatedCommitter |
| **文件提交** | 写临时文件→AggregatedCommitter 原子提交 | Hive, S3, File | 先写后提交 |
