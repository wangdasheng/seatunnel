# Apache SeaTunnel Zeta 引擎深度源码分析

> 代码位置：`seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/`
> 分析版本：基于当前代码仓库最新版本

---

## 目录

1. [Master/Worker 架构](#1-masterworker-架构)
2. [任务调度核心](#2-任务调度核心)
3. [Checkpoint 机制](#3-checkpoint-机制)
4. [网络通信层](#4-网络通信层)
5. [关键数据结构与状态流转](#5-关键数据结构与状态流转)
6. [总结](#6-总结)

---

## 1. Master/Worker 架构

### 1.1 启动入口：SeaTunnelServer

Zeta 引擎的核心入口是 `SeaTunnelServer`，它实现了 Hazelcast 的 `ManagedService`、`MembershipAwareService` 和 `LiveOperationsTracker` 三个接口，作为 Hazelcast 成员服务运行。

**文件**：`SeaTunnelServer.java`（第 68-443 行）

```java
// SeaTunnelServer.java:68-69
public class SeaTunnelServer
        implements ManagedService, MembershipAwareService, LiveOperationsTracker {
```

#### 1.1.1 角色配置与初始化

引擎支持三种集群角色（`EngineConfig.ClusterRole`）：
- `MASTER` - 仅 Master 节点
- `WORKER` - 仅 Worker 节点
- `MASTER_AND_WORKER` - Master + Worker 混合模式（默认）

**启动流程**（`SeaTunnelServer.java:138-184`）：

```java
// SeaTunnelServer.java:139-184
@Override
public void init(NodeEngine engine, Properties hzProperties) {
    this.nodeEngine = (NodeEngineImpl) engine;

    // 1. 初始化 ClassLoader 服务
    classLoaderService = new DefaultClassLoaderService(...);

    // 2. 初始化事件服务
    eventService = new EventService(nodeEngine);

    // 3. 根据角色启动 Master/Worker
    if (MASTER_AND_WORKER == clusterRole) {
        startWorker();   // 先启动 Worker
        startMaster();   // 再启动 Master
    } else if (WORKER == clusterRole) {
        startWorker();
    } else {
        startMaster();
    }

    // 4. 启动健康监控
    seaTunnelHealthMonitor = new SeaTunnelHealthMonitor(...);

    // 5. 启动任务日志管理服务
    taskLogManagerService = new TaskLogManagerService(...);

    // 6. 启动 Jetty HTTP 服务（如果配置启用）
    if (httpConfig.isEnabled()) {
        jettyService = new JettyService(...);
        jettyService.createJettyServer();
    }
}
```

**关键设计**：先启动 Worker 再启动 Master，确保 Master 在初始化 ResourceManager 时能发现已注册的 Worker。

#### 1.1.2 Master 启动（startMaster）

**文件**：`SeaTunnelServer.java:186-198`

```java
private void startMaster() {
    coordinatorService = new CoordinatorService(nodeEngine, this, seaTunnelConfig.getEngineConfig());
    checkpointService = new CheckpointService(seaTunnelConfig.getEngineConfig().getCheckpointConfig());
    checkpointMonitorService = new CheckpointMonitorService(nodeEngine, 32);
    // 启动定时打印执行信息
    monitorService.scheduleAtFixedRate(this::printExecutionInfo, ...);
}
```

Master 启动时创建三个核心组件：
- **CoordinatorService** - 任务调度协调器
- **CheckpointService** - 检查点存储服务
- **CheckpointMonitorService** - 检查点监控服务

#### 1.1.3 Worker 启动（startWorker）

**文件**：`SeaTunnelServer.java:200-206`

```java
private void startWorker() {
    taskExecutionService = new TaskExecutionService(classLoaderService, nodeEngine, eventService);
    nodeEngine.getMetricsRegistry().registerDynamicMetricsProvider(taskExecutionService);
    taskExecutionService.start();
    getSlotService();  // 懒加载 SlotService
}
```

Worker 启动时创建 `TaskExecutionService` 并初始化 `SlotService`。注意 `SlotService` 采用懒加载模式（第 114-136 行），仅在 Worker 节点创建。

### 1.2 Worker 注册与心跳机制

#### 1.2.1 SlotService - Worker 资源管理

**文件**：`DefaultSlotService.java`（第 57-359 行）

SlotService 是 Worker 节点的核心服务，负责管理本地 Slot 的分配和释放。

**初始化**（第 90-154 行）：

```java
// DefaultSlotService.java:90-154
@Override
public void init() {
    slotServiceSequence = UUID.randomUUID().toString();  // 唯一标识此次 SlotService 生命周期
    contexts = new ConcurrentHashMap<>();
    assignedSlots = new ConcurrentHashMap<>();
    unassignedSlots = new ConcurrentHashMap<>();
    unassignedResource = new AtomicReference<>(new ResourceProfile());

    if (!config.isDynamicSlot()) {
        initFixedSlots();  // 固定模式：预分配 N 个 Slot
    }

    unassignedResource.set(getNodeResource());

    // 启动心跳定时器：每 5 秒发送一次心跳到 Master
    scheduledExecutorService.scheduleAtFixedRate(() -> {
        SystemLoadInfo systemLoadInfo = ...;  // 采集 CPU/内存负载
        WorkerProfile workerProfile = getWorkerProfile();
        sendToMaster(new WorkerHeartbeatOperation(workerProfile)).join();
    }, 0, DEFAULT_HEARTBEAT_TIMEOUT, TimeUnit.MILLISECONDS);
}
```

**固定 Slot 初始化**（第 285-297 行）：

```java
private void initFixedSlots() {
    long maxMemory = Runtime.getRuntime().maxMemory();
    for (int i = 0; i < config.getSlotNum(); i++) {
        unassignedSlots.put(i, new SlotProfile(
            nodeEngine.getThisAddress(),
            i,
            new ResourceProfile(CPU.of(0), Memory.of(maxMemory / config.getSlotNum())),
            slotServiceSequence));
    }
}
```

将 JVM 最大内存平均分配给 Slot。

#### 1.2.2 心跳流程

```
Worker                                     Master
  |                                         |
  |-- WorkerHeartbeatOperation(WorkerProfile) -->|
  |                                         |  heartbeat(workerProfile)
  |                                         |    registerWorker.put(address, profile)
  |                                         |    updateWorkerLoad(profile)
  |<---- ResetResourceOperation -----------|  (首次注册时)
```

**心跳发送**（`DefaultSlotService.java:111-153`）：

Worker 每 5 秒执行一次心跳：
1. 采集系统负载信息（CPU、内存百分比），每 2 次心跳采集一次（`SYSTEM_LOAD_SEND_INTERVAL = 2`）
2. 构建 `WorkerProfile`（包含地址、资源信息、已分配/未分配 Slot、节点属性）
3. 通过 `WorkerHeartbeatOperation` 发送到 Master

**心跳接收**（`AbstractResourceManager.java:258-268`）：

```java
// AbstractResourceManager.java:258-268
@Override
public void heartbeat(WorkerProfile workerProfile) {
    if (!registerWorker.containsKey(workerProfile.getAddress())) {
        // 新 Worker 首次注册
        log.info("received new worker register: " + workerProfile.getAddress());
        sendToMember(new ResetResourceOperation(), workerProfile.getAddress()).join();
    } else {
        log.debug("received worker heartbeat from: " + workerProfile.getAddress());
    }
    registerWorker.put(workerProfile.getAddress(), workerProfile);
    this.updateWorkerLoad(workerProfile);
}
```

#### 1.2.3 Master 初始化时的 Worker 发现

**文件**：`AbstractResourceManager.java:91-124`

```java
@Override
public void init() {
    log.info("Init ResourceManager");
    initWorker();  // 向所有存活节点发送 SyncWorkerProfileOperation
}

private void initWorker() {
    List<Address> aliveNode = nodeEngine.getClusterService().getMembers()...;
    aliveNode.stream()
        .map(node -> sendToMember(new SyncWorkerProfileOperation(), node)
            .thenAccept(p -> {
                if (p != null) {
                    registerWorker.put(node, (WorkerProfile) p);
                }
            }))
        .collect(Collectors.toList())
        .forEach(CompletableFuture::join);  // 等待所有节点响应
}
```

Master 启动时主动向集群中所有节点发送 `SyncWorkerProfileOperation`，收集 Worker 信息。

### 1.3 节点故障检测

**文件**：`SeaTunnelServer.java:242-251`

```java
@Override
public void memberRemoved(MembershipServiceEvent event) {
    if (isMasterNode()) {
        this.getCoordinatorService().memberRemoved(event);
    }
}
```

当 Hazelcast 检测到节点移除时，通过 `CoordinatorService.memberRemoved()` 处理：
1. 从 ResourceManager 注册表中移除该 Worker
2. 将该节点上所有运行中/部署中/取消中的 Task 标记为 FAILED

---

## 2. 任务调度核心

### 2.1 CoordinatorService - 任务调度协调器

**文件**：`CoordinatorService.java`（第 110-1166 行）

CoordinatorService 是 Zeta 引擎的任务调度核心，管理 Job 的提交、执行、状态监控和资源分配。

#### 2.1.1 核心数据结构

```java
// CoordinatorService.java:110-196
public class CoordinatorService {
    // 运行中 Job 信息（jobId -> JobInfo）
    private IMap<Long, JobInfo> runningJobInfoIMap;

    // 运行中 Job 状态（key 可以是 jobId/PipelineLocation/TaskGroupLocation，value 可以是 JobStatus/PipelineStatus/ExecutionState）
    private IMap<Object, Object> runningJobStateIMap;

    // 状态时间戳
    private IMap<Object, Long[]> runningJobStateTimestampsIMap;

    // Slot 占用信息（PipelineLocation -> {TaskGroupLocation -> SlotProfile}）
    private IMap<PipelineLocation, Map<TaskGroupLocation, SlotProfile>> ownedSlotProfilesIMap;

    // 运行中的 JobMaster
    private final Map<Long, JobMaster> runningJobMasterMap = new ConcurrentHashMap<>();

    // 待调度队列
    private final PeekBlockingQueue<PendingJobInfo> pendingJobQueue = new PeekBlockingQueue<>();

    // Master 活跃状态
    private volatile boolean isActive = false;
}
```

#### 2.1.2 Master 活跃检测与初始化

**文件**：`CoordinatorService.java:539-563`

```java
private void checkNewActiveMaster() {
    if (!isActive && this.seaTunnelServer.isMasterNode()) {
        // 当前节点成为新的 Active Master
        initCoordinatorService();
        isActive = true;
    } else if (isActive && !this.seaTunnelServer.isMasterNode()) {
        // 当前节点不再是 Active Master
        isActive = false;
        clearCoordinatorService();
    }
}
```

每 100 毫秒检查一次当前节点是否为 Master，实现 Master 故障自动切换。

#### 2.1.3 CoordinatorService 初始化

**文件**：`CoordinatorService.java:404-447`

```java
private void initCoordinatorService() {
    // 1. 获取 Hazelcast IMap
    runningJobInfoIMap = nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_INFO);
    runningJobStateIMap = nodeEngine.getHazelcastInstance().getMap(IMAP_RUNNING_JOB_STATE);
    runningJobStateTimestampsIMap = nodeEngine.getHazelcastInstance().getMap(IMAP_STATE_TIMESTAMPS);
    ownedSlotProfilesIMap = nodeEngine.getHazelcastInstance().getMap(IMAP_OWNED_SLOT_PROFILES);

    // 2. 初始化 JobHistoryService
    jobHistoryService = new JobHistoryService(...);

    // 3. 创建事件处理器
    eventProcessor = createJobEventProcessor(...);

    // 4. 创建连接器打包服务（可选）
    connectorPackageService = new ConnectorPackageService(seaTunnelServer);

    // 5. 异步恢复所有正在运行的 Job（Master 切换场景）
    restoreAllJobFromMasterNodeSwitchFuture = new PassiveCompletableFuture(
        CompletableFuture.runAsync(this::restoreAllRunningJobFromMasterNodeSwitch, executorService));
}
```

#### 2.1.4 Job 提交流程

**文件**：`CoordinatorService.java:619-692`

```
Client
  |
  |-- submitJob(jobId, jobImmutableInformation) -->
  |
  |  1. 创建 JobMaster
  |  2. jobMaster.init() - 生成 PhysicalPlan + CheckpointPlan
  |  3. 加入 pendingJobQueue
  |  4. 更新 Job 状态为 PENDING
  |
  |  pendingJobSchedule 线程：
  |    pendingJobQueue.peekBlocking()
  |    preApplyResources() - 预申请资源
  |    if (资源足够):
  |      runningJobMasterMap.put(jobId, jobMaster)
  |      pendingJobQueue.take()
  |      jobMaster.run()  --> 物理计划启动执行
  |    else:
  |      if (WAIT策略): 等待 3 秒后重试
  |      if (FAIL策略): 直接标记失败
```

#### 2.1.5 待调度队列调度

**文件**：`CoordinatorService.java:227-328`

```java
private void pendingJobSchedule() throws InterruptedException {
    PendingJobInfo pendingJobInfo = pendingJobQueue.peekBlocking();  // 阻塞等待

    // 1. 预检查资源是否足够
    boolean preApplyResources = jobMaster.preApplyResources();

    if (!preApplyResources) {
        if (isWaitStrategy) {
            Thread.sleep(3000);  // WAIT 模式：等待 3 秒后重试
            return;
        } else {
            completeFailJob(jobMaster);  // FAIL 模式：直接失败
            queueRemove(jobMaster);
            return;
        }
    }

    // 2. 资源足够，开始执行
    runningJobMasterMap.put(jobId, jobMaster);
    pendingJobQueue.take();  // 从队列取出

    // 3. 异步执行 jobMaster.run()
    mdcExecutorService.submit(() -> {
        finalJobMaster.run();
    });
}
```

#### 2.1.6 调度策略（ScheduleStrategy）

配置项 `engineConfig.getScheduleStrategy()`，支持两种策略：

- **WAIT**：资源不足时等待，持续重试直到资源足够
- **FAIL**：资源不足时直接标记 Job 为 FAILED

### 2.2 PhysicalPlanGenerator - 物理计划生成

**文件**：`PhysicalPlanGenerator.java`（第 86-630 行）

PhysicalPlanGenerator 将逻辑 DAG（ExecutionPlan）转换为物理执行计划（PhysicalPlan）。

#### 2.2.1 核心转换流程

**generate() 方法**（第 155-236 行）：

```java
public Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> generate() {
    // 1. 过滤已完成的 Pipeline
    List<Pipeline> unclosedPipelines = pipelines.stream()
        .filter(p -> !PipelineStatus.FINISHED.equals(runningJobStateIMap.get(pipelineLocation)))
        .collect(Collectors.toList());

    // 2. 为每个 Pipeline 生成物理计划
    for (Pipeline pipeline : unclosedPipelines) {
        // 2.1 获取 Source Action，创建 Enumerator 任务
        List<PhysicalVertex> coordinatorVertexList = getEnumeratorTask(sources, pipelineId, ...);
        // 2.2 获取 Sink Action，创建 AggregatedCommitter 任务
        coordinatorVertexList.addAll(getCommitterTask(edges, pipelineId, ...));
        // 2.3 创建 Source/Transform/Sink 任务
        List<PhysicalVertex> physicalVertexList = getSourceTask(edges, sources, pipelineId, ...);
        // 2.4 构建 CheckpointPlan
        checkpointPlans.put(pipelineId, CheckpointPlan.builder()...build());
        // 2.5 创建 SubPlan
        return new SubPlan(...);
    }

    // 3. 构建 PhysicalPlan
    return Tuple2.tuple2(physicalPlan, checkpointPlans);
}
```

#### 2.2.2 任务创建逻辑

**Source 任务创建**（第 329-376 行）：
- 为每个 SourceAction 创建一个 `SourceSplitEnumeratorTask`（Coordinator 任务）
- 记录 `enumeratorTaskIDMap` 用于后续 SourceTask 关联

**Committer 任务创建**（第 246-327 行）：
- 为有 `SinkAggregatedCommitter` 的 SinkAction 创建 `SinkAggregatedCommitterTask`（Coordinator 任务）

**SourceTask 创建**（第 378-511 行）：
- 按 SourceAction 的并行度创建 `SourceSeaTunnelTask`
- 支持 Source 到 Sink 无分区转换的场景（通过 IntermediateQueue 拆分）
- TaskGroup 类型根据队列类型选择：
  - `BLOCKINGQUEUE` → `TaskGroupWithIntermediateBlockingQueue`
  - `DISRUPTOR` → `TaskGroupWithIntermediateDisruptor`

#### 2.2.3 物理计划层级

```
PhysicalPlan
  ├── SubPlan (Pipeline 0)
  │   ├── CoordinatorVertexList
  │   │   ├── SourceSplitEnumeratorTask (parallelism=1)
  │   │   └── SinkAggregatedCommitterTask (parallelism=1)
  │   └── PhysicalVertexList
  │       ├── PhysicalVertex (SourceTask, parallelism=N)
  │       ├── PhysicalVertex (TransformTask, parallelism=M)
  │       └── PhysicalVertex (SinkTask, parallelism=M)
  ├── SubPlan (Pipeline 1)
  │   └── ...
  └── ...
```

### 2.3 JobMaster - Job 执行管理

**文件**：`JobMaster.java`（第 114-1066 行）

每个运行中的 Job 由一个 `JobMaster` 实例管理。

#### 2.3.1 JobMaster 初始化

**init() 方法**（第 213-312 行）：

```java
public synchronized void init(long initializationTimestamp, boolean restart) {
    // 1. 反序列化 Job 信息
    jobImmutableInformation = nodeEngine.getSerializationService().toObject(jobImmutableInformationData);

    // 2. 创建 Checkpoint 配置（合并 Engine 和 Job 级别配置）
    jobCheckpointConfig = createJobCheckpointConfig(engineConfig.getCheckpointConfig(), jobConfig);

    // 3. 恢复逻辑 DAG
    logicalDag = DAGUtils.restoreLogicalDag(...);

    // 4. 处理 SaveMode（如数据库的 CREATE TABLE IF NOT EXISTS）
    if (!restart && !isStartWithSavePoint) {
        handleSaveMode(sink, isStartWithSavePoint);
    }

    // 5. 生成物理计划
    final Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> planTuple =
            PlanUtils.fromLogicalDAG(...);
    this.physicalPlan = planTuple.f0();
    this.checkpointPlanMap = planTuple.f1();

    // 6. 初始化 CheckpointManager
    this.initCheckPointManager(restart);

    // 7. 初始化状态 Future
    this.initStateFuture();
}
```

#### 2.3.2 资源预申请

**preApplyResources() 方法**（第 390-503 行）：

```java
public boolean preApplyResources() {
    // 1. 重置 Slot 分配策略的 Worker 分配信息
    if (slotAllocationStrategy instanceof SlotRatioStrategy) {
        ((SlotRatioStrategy) slotAllocationStrategy).setWorkerAssignedSlots(new ConcurrentHashMap<>());
    }

    // 2. 为所有 TaskGroup 预申请资源
    Map<TaskGroupLocation, CompletableFuture<SlotProfile>> preApplyResourceFutures = new HashMap<>();
    for (SubPlan subPlan : physicalPlan.getPipelineList()) {
        preApplyResourcesForSubPlan(subPlan, preApplyResourceFutures);
    }

    // 3. 检查资源是否全部申请成功
    boolean enoughResource = preApplyResourceFutures.values().stream()
        .filter(value -> value != null && value.join() != null)
        .count() == preApplyResourceFutures.size();

    if (enoughResource) {
        physicalPlan.setPreApplyResourceFutures(preApplyResourceFutures);
    } else {
        // 资源不足，释放已申请的资源
        resourceManager.releaseResources(jobId, ...);
    }
    return enoughResource;
}
```

#### 2.3.3 Job 执行

**run() 方法**（第 540-561 行）：

```java
public void run() {
    try {
        physicalPlan.startJob();  // 启动物理计划
    } catch (Throwable e) {
        log.severe("Job run error: " + ...);
    } finally {
        jobMasterCompleteFuture.join();  // 等待 Job 完成
        // 清理连接器 JAR 包
        seaTunnelServer.getConnectorPackageService().cleanUpWhenJobFinished(...);
    }
}
```

#### 2.3.4 TaskGroup 部署

PhysicalVertex 在 `stateProcess()` 中处理 DEPLOYING 状态（`PhysicalVertex.java:546-606`）：

```java
case DEPLOYING:
    TaskDeployState deployState = deploy(jobMaster.getOwnedSlotProfiles(taskGroupLocation));
    if (!deployState.isSuccess()) {
        makeTaskGroupFailing(new TaskGroupDeployException(deployState.getThrowableMsg()));
    } else {
        updateTaskState(ExecutionState.RUNNING);
    }
```

**deploy() 方法**（第 308-318 行）：
- 如果目标 Worker 是本机：调用 `deployOnLocal()` → 直接调用 `TaskExecutionService.deployTask()`
- 如果目标 Worker 是远程：调用 `deployOnRemote()` → 发送 `DeployTaskOperation` 到远程节点

---

## 3. Checkpoint 机制

### 3.1 整体架构

Zeta 引擎的 Checkpoint 机制采用经典的 Coordinator-Worker 模式：

```
CheckpointCoordinator (Master 节点，每个 Pipeline 一个)
  |
  |-- CheckpointBarrierTriggerOperation --> Task (Worker 节点)
  |                                         |
  |<-- TaskAcknowledgeOperation ----------  |
  |                                         |
  |-- CheckpointFinishedOperation --------> Task
  |                                         |
  |-- CheckpointStorage.storeCheckPoint()  |
```

### 3.2 CheckpointCoordinator

**文件**：`CheckpointCoordinator.java`（第 93-1174 行）

每个 Pipeline 对应一个 CheckpointCoordinator 实例。

#### 3.2.1 Checkpoint 类型

**文件**：`CheckpointType.java`（第 20-98 行）

| 类型 | 说明 | 是否自动触发 |
|------|------|-------------|
| `CHECKPOINT_TYPE` | 常规 Checkpoint | 是（定时触发） |
| `SAVEPOINT_TYPE` | Savepoint（用户手动触发） | 否 |
| `COMPLETED_POINT_TYPE` | 完成 Checkpoint（Batch 作业结束） | 是（Task 触发） |
| `SCHEMA_CHANGE_BEFORE_POINT_TYPE` | Schema 变更前 Checkpoint | 是 |
| `SCHEMA_CHANGE_AFTER_POINT_TYPE` | Schema 变更后 Checkpoint | 是 |

其中 `isFinalCheckpoint()` 返回 true 的类型是 `COMPLETED_POINT_TYPE` 和 `SAVEPOINT_TYPE`，表示会触发任务关闭。

#### 3.2.2 Checkpoint 触发流程

**tryTriggerPendingCheckpoint() 方法**（第 500-582 行）：

```java
protected void tryTriggerPendingCheckpoint(CheckpointType checkpointType) {
    // 1. 检查是否所有 Task 已就绪
    if (!isAllTaskReady.get()) {
        return;  // 跳过
    }

    // 2. 检查时间间隔（最小间隔 + 最小暂停时间）
    long interval = currentTimestamp - latestTriggerTimestamp.get();
    if (interval < coordinatorConfig.getCheckpointInterval()) {
        scheduleTriggerPendingCheckpoint(checkpointType, checkpointConfig.getCheckpointInterval() - interval);
        return;
    }

    synchronized (lock) {
        // 3. 检查是否已完成/已关闭
        if (isCompleted() || isShutdown()) {
            return;
        }

        // 4. 检查是否有 Schema 变更进行中
        if (schemaChanging.get() && checkpointType.isGeneralCheckpoint()) {
            return;  // 跳过常规 Checkpoint
        }

        // 5. 检查是否有正在 Pending 的 Checkpoint
        if (pendingCounter.get() > 0) {
            scheduleTriggerPendingCheckpoint(checkpointType, 500L);
            return;
        }

        // 6. 创建 PendingCheckpoint 并触发
        CompletableFuture<PendingCheckpoint> pendingCheckpoint =
                createPendingCheckpoint(currentTimestamp, checkpointType);
        startTriggerPendingCheckpoint(pendingCheckpoint);

        // 7. 调度下一次 Checkpoint
        if (checkpointType.notFinalCheckpoint()) {
            scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
        }
    }
}
```

#### 3.2.3 Barrier 触发

**triggerCheckpoint() 方法**（第 839-851 行）：

```java
public InvocationFuture<?>[] triggerCheckpoint(CheckpointBarrier checkpointBarrier) {
    return plan.getStartingSubtasks().stream()
        .filter(taskLocation -> !CLOSED.equals(pipelineTaskStatus.get(taskLocation.getTaskID())))
        .map(taskLocation -> new CheckpointBarrierTriggerOperation(checkpointBarrier, taskLocation))
        .map(checkpointManager::sendOperationToMemberNode)
        .toArray(InvocationFuture[]::new);
}
```

仅向 Pipeline 的 Starting Tasks（即 SourceEnumerator）发送 Barrier 触发操作，Barrier 会通过数据流传播到下游 Task。

#### 3.2.4 Task ACK 与 Checkpoint 完成

**acknowledgeTask() 方法**（第 905-940 行）：

```java
protected void acknowledgeTask(TaskAcknowledgeOperation ackOperation) {
    final long checkpointId = ackOperation.getBarrier().getId();
    final PendingCheckpoint pendingCheckpoint = pendingCheckpoints.get(checkpointId);

    pendingCheckpoint.acknowledgeTask(
        location,
        ackOperation.getStates(),
        pendingCheckpoint.getCheckpointType().isSavepoint()
            ? SubtaskStatus.SAVEPOINT_PREPARE_CLOSE
            : SubtaskStatus.RUNNING);

    // 如果是最终 Checkpoint 且 Barrier 指示 prepareClose
    if (ackOperation.getBarrier().getCheckpointType().notFinalCheckpoint()
            && ackOperation.getBarrier().prepareClose(location)) {
        completedCloseIdleTask(location);
    }
}
```

**PendingCheckpoint.acknowledgeTask()**（`PendingCheckpoint.java:121-153`）：

```java
public void acknowledgeTask(TaskLocation taskLocation, List<ActionSubtaskState> states, SubtaskStatus status) {
    boolean exist = notYetAcknowledgedTasks.remove(taskLocation.getTaskID());
    if (!exist) return;  // 重复 ACK，跳过

    // 1. 更新 Action 状态
    for (ActionSubtaskState state : states) {
        ActionState actionState = actionStates.get(state.getStateKey());
        actionState.reportState(state.getIndex(), state);
    }

    // 2. 更新 Task 统计
    statistics.reportSubtaskStatistics(new SubtaskStatistics(...));

    // 3. 如果所有 Task 都已 ACK，标记 Checkpoint 完成
    if (isFullyAcknowledged()) {
        completableFuture.complete(toCompletedCheckpoint());
    }
}
```

#### 3.2.5 Checkpoint 存储与清理

**completePendingCheckpoint() 方法**（第 942-1012 行）：

```java
public synchronized void completePendingCheckpoint(CompletedCheckpoint completedCheckpoint) {
    // 1. 序列化并存储 Checkpoint
    byte[] states = serializer.serialize(completedCheckpoint);
    checkpointStorage.storeCheckPoint(PipelineState.builder()
        .checkpointId(checkpointId)
        .jobId(String.valueOf(jobId))
        .pipelineId(pipelineId)
        .states(states)
        .build());

    // 2. 清理超过最大保留数量的 Checkpoint
    if (completedCheckpointIds.size() % maxRetainedCheckpoints == 0) {
        List<String> needDeleteCheckpointId = new ArrayList<>();
        for (int i = 0; i < maxRetainedCheckpoints; i++) {
            needDeleteCheckpointId.add(completedCheckpointIds.removeFirst());
        }
        checkpointStorage.deleteCheckpoint(jobId, pipelineId, needDeleteCheckpointId);
    }

    // 3. 更新最新完成的 Checkpoint
    latestCompletedCheckpoint = completedCheckpoint;

    // 4. 通知所有 Task Checkpoint 已完成
    notifyCompleted(completedCheckpoint);
}
```

### 3.3 CheckpointBarrier - Barrier 对齐

**文件**：`CheckpointBarrier.java`（第 36-116 行）

CheckpointBarrier 是 Checkpoint 的核心传输机制，携带以下信息：
- `id` - Checkpoint ID
- `timestamp` - 触发时间戳
- `checkpointType` - Checkpoint 类型
- `prepareCloseTasks` - 需要准备关闭的 Task 集合
- `closedTasks` - 已关闭的 Task 集合

**Barrier 传播流程**：

```
CheckpointCoordinator
  |
  |-- CheckpointBarrierTriggerOperation -->
  |     SourceEnumerator
  |     task.triggerBarrier(barrier)  // 触发 Barrier
  |     |
  |     |-- Barrier 沿数据流传播 -->
  |     SourceTask -> TransformTask -> SinkTask
  |     |
  |<-- TaskAcknowledgeOperation ------ 每个 Task
```

### 3.4 Schema 变更支持

**文件**：`CheckpointCoordinator.java:1111-1158`

```java
protected void scheduleSchemaChangeBeforeCheckpoint() {
    if (schemaChanging.compareAndSet(false, true)) {
        // 暂停常规 Checkpoint，触发 Schema 变更前 Checkpoint
        scheduleTriggerPendingCheckpoint(SCHEMA_CHANGE_BEFORE_POINT_TYPE, 0);
    }
}

protected void scheduleSchemaChangeAfterCheckpoint() {
    if (schemaChanging.get()) {
        // Schema 变更完成后触发 Schema 变更后 Checkpoint
        scheduleTriggerPendingCheckpoint(SCHEMA_CHANGE_AFTER_POINT_TYPE, 0);
    }
}

protected void completeSchemaChangeAfterCheckpoint(CompletedCheckpoint checkpoint) {
    if (schemaChanging.compareAndSet(true, false)) {
        // 恢复常规 Checkpoint 调度
        scheduleTriggerPendingCheckpoint(coordinatorConfig.getCheckpointInterval());
    }
}
```

Schema 变更流程：
1. 设置 `schemaChanging = true`，暂停常规 Checkpoint
2. 触发 `SCHEMA_CHANGE_BEFORE_POINT_TYPE` Checkpoint
3. 执行 Schema 变更
4. 触发 `SCHEMA_CHANGE_AFTER_POINT_TYPE` Checkpoint
5. 恢复常规 Checkpoint

---

## 4. 网络通信层

### 4.1 Hazelcast Operation 机制

Zeta 引擎基于 Hazelcast 的 Operation 机制实现网络通信。所有 Operation 继承自 `com.hazelcast.spi.impl.operationservice.Operation`，通过 `IdentifiedDataSerializable` 进行序列化。

#### 4.1.1 Operation 分类

**资源管理类 Operation**（`resourcemanager/opeartion/`）：

| Operation | 说明 | 源文件行号 |
|-----------|------|-----------|
| `WorkerHeartbeatOperation` | Worker 心跳上报 | 第 31-71 行 |
| `SyncWorkerProfileOperation` | 同步 Worker Profile | — |
| `RequestSlotOperation` | 请求分配 Slot | 第 32-84 行 |
| `ReleaseSlotOperation` | 释放 Slot | — |
| `ResetResourceOperation` | 重置资源（首次注册） | — |
| `GetPendingJobsOperation` | 获取待调度 Job 列表 | — |
| `GetOverviewOperation` | 获取集群概览信息 | — |

**Checkpoint 类 Operation**（`checkpoint/operation/`）：

| Operation | 说明 | 源文件行号 |
|-----------|------|-----------|
| `CheckpointBarrierTriggerOperation` | 触发 Checkpoint Barrier | 第 39-107 行 |
| `TaskAcknowledgeOperation` | Task ACK | 第 40-87 行 |
| `CheckpointFinishedOperation` | 通知 Checkpoint 完成 | — |
| `CheckpointEndOperation` | 通知 Checkpoint 结束 | — |
| `CheckpointErrorReportOperation` | 上报 Checkpoint 错误 | — |
| `NotifyTaskStartOperation` | 通知 Task 启动 | — |
| `NotifyTaskRestoreOperation` | 通知 Task 恢复状态 | — |
| `TaskReportStatusOperation` | Task 状态上报 | — |
| `TriggerSchemaChangeBeforeCheckpointOperation` | 触发 Schema 变更前 Checkpoint | — |
| `TriggerSchemaChangeAfterCheckpointOperation` | 触发 Schema 变更后 Checkpoint | — |

**任务管理类 Operation**（`task/operation/`）：

| Operation | 说明 | 源文件行号 |
|-----------|------|-----------|
| `DeployTaskOperation` | 部署 TaskGroup 到 Worker | 第 34-86 行 |
| `CancelTaskOperation` | 取消 Task | — |
| `CheckTaskGroupIsExecutingOperation` | 检查 TaskGroup 是否正在执行 | — |
| `CleanTaskGroupContextOperation` | 清理 TaskGroup 上下文 | — |
| `GetTaskGroupMetricsOperation` | 获取 TaskGroup 指标 | — |
| `GetMetricsOperation` | 获取 Job 指标 | — |
| `NotifyTaskStatusOperation` | 通知 Task 状态变更 | — |
| `ReportMetricsOperation` | 上报指标 | — |

**Job 管理类 Operation**（`operation/`）：

| Operation | 说明 | 源文件行号 |
|-----------|------|-----------|
| `SubmitJobOperation` | 提交 Job | — |
| `CancelJobOperation` | 取消 Job | — |
| `SavePointJobOperation` | 执行 Savepoint | — |
| `WaitForJobCompleteOperation` | 等待 Job 完成 | — |
| `GetJobStatusOperation` | 获取 Job 状态 | — |
| `GetJobMetricsOperation` | 获取 Job 指标 | — |
| `GetJobInfoOperation` | 获取 Job 信息 | — |
| `GetCheckpointOverviewOperation` | 获取 Checkpoint 概览 | — |
| `GetCheckpointHistoryOperation` | 获取 Checkpoint 历史 | — |

### 4.2 数据传输 - Shuffle 机制

#### 4.2.1 IntermediateQueue 类型

PhysicalPlanGenerator 支持两种 IntermediateQueue 实现：

**TaskGroupWithIntermediateBlockingQueue**：
- 基于 Java `BlockingQueue` 实现
- 适合低吞吐场景
- 简单可靠，适合调试

**TaskGroupWithIntermediateDisruptor**：
- 基于 LMAX Disruptor 实现
- 适合高吞吐场景
- 无锁设计，性能更高

选择逻辑在 `PhysicalPlanGenerator.java:448-468`：

```java
if (queueType.equals(BLOCKINGQUEUE)) {
    taskGroup = new TaskGroupWithIntermediateBlockingQueue(...);
} else {
    taskGroup = new TaskGroupWithIntermediateDisruptor(...);
}
```

#### 4.2.2 数据流模型

```
SourceTask (Producer)
  |
  |-- 数据写入 IntermediateQueue/Disruptor -->
  |
  TransformTask (Consumer + Producer)
  |
  |-- 数据写入 IntermediateQueue/Disruptor -->
  |
SinkTask (Consumer)
```

每个 Task 由多个 FlowLifeCycle 组成：
- `SourceFlowLifeCycle` - Source 数据读取
- `TransformFlowLifeCycle` - 数据转换
- `SinkFlowLifeCycle` - Sink 数据写入
- `IntermediateQueueFlowLifeCycle` - 中间队列

### 4.3 背压机制

Zeta 引擎的背压主要通过以下机制实现：

1. **BlockingQueue 背压**：当使用 `BLOCKINGQUEUE` 类型时，`BlockingQueue.put()` 在队列满时会自动阻塞生产者。

2. **Disruptor 背压**：当使用 `DISRUPTOR` 类型时，RingBuffer 满时生产者会等待。

3. **Task 级别背压**：每个 TaskGroup 在同一个线程中顺序执行，下游消费慢会自然减慢上游生产。

### 4.4 Master-Worker 通信流程

```
Master (CoordinatorService)                 Worker (SlotService + TaskExecutionService)
  |                                         |
  |-- RequestSlotOperation -->              |  requestSlot(jobId, resourceProfile)
  |<-- SlotAndWorkerProfile --------------- |  返回分配的 Slot 信息
  |                                         |
  |-- DeployTaskOperation -->               |  deployTask(taskImmutableInformation)
  |<-- TaskDeployState -------------------- |  返回部署状态
  |                                         |
  |      Worker 按固定间隔发送心跳:          |
  |<-- WorkerHeartbeatOperation ----------- |  heartbeat(workerProfile)
  |-- ResetResourceOperation -->           |  (首次注册时)
  |                                         |
  |-- CheckpointBarrierTriggerOperation --> |  triggerBarrier(barrier)
  |<-- TaskAcknowledgeOperation ----------- |  acknowledgeTask(location, states)
  |                                         |
  |-- NotifyTaskRestoreOperation -->       |  恢复 Task 状态
  |<-- TaskReportStatusOperation ---------- |  reportStatus(WAITING_RESTORE/READY_START)
  |                                         |
  |-- NotifyTaskStartOperation -->         |  所有 Task 就绪后通知启动
```

---

## 5. 关键数据结构与状态流转

### 5.1 Job 状态流转

```
INITIALIZING --> CREATED --> PENDING --> SCHEDULED --> RUNNING --> FINISHED
                                                 |               |
                                                 |               +--> SAVEPOINT_DONE
                                                 |               |
                                                 +--> FAILING --> FAILED
                                                 |
                                                 +--> CANCELING --> CANCELED
```

### 5.2 Pipeline 状态流转

```
INITIALIZING --> CREATED --> SCHEDULED --> DEPLOYING --> RUNNING --> FINISHED
                                                     |               |
                                                     |               +--> (支持重试恢复)
                                                     +--> FAILING --> FAILED
                                                     |               |
                                                     |               +--> (如果可恢复: 重新调度)
                                                     +--> CANCELING --> CANCELED
```

### 5.3 TaskGroup (PhysicalVertex) 状态流转

```
INITIALIZING --> CREATED --> DEPLOYING --> RUNNING --> FINISHED
                                     |               |
                                     |               +--> (结束)
                                     +--> FAILING --> FAILED
                                     |               |
                                     +--> CANCELING --> CANCELED
```

### 5.4 CheckpointCoordinator 状态

```
RUNNING --> FINISHED (Batch 作业完成)
         --> SUSPEND  (Savepoint 完成)
         --> FAILED   (错误)
         --> CANCELED (用户取消)
```

### 5.5 IMap 持久化策略

Zeta 引擎利用 Hazelcast IMap 的分布式特性实现状态持久化：

| IMap | Key | Value | 用途 |
|------|-----|-------|------|
| `IMAP_RUNNING_JOB_INFO` | jobId | JobInfo | Job 基本信息 |
| `IMAP_RUNNING_JOB_STATE` | jobId/PipelineLocation/TaskGroupLocation | JobStatus/PipelineStatus/ExecutionState | 状态存储 |
| `IMAP_STATE_TIMESTAMPS` | 同上 | Long[] | 状态时间戳 |
| `IMAP_OWNED_SLOT_PROFILES` | PipelineLocation | Map<TaskGroupLocation, SlotProfile> | Slot 占用 |
| `IMAP_RUNNING_JOB_METRICS` | 分区 key | Map<TaskLocation, SeaTunnelMetricsContext> | 运行时指标 |
| `IMAP_FINISHED_JOB_STATE` | jobId | JobState | 已完成 Job 状态 |
| `IMAP_FINISHED_JOB_METRICS` | jobId | JobMetrics | 已完成 Job 指标 |
| `IMAP_FINISHED_JOB_VERTEX_INFO` | jobId | JobDAGInfo | 已完成 Job DAG 信息 |

所有状态更新都遵循 **先更新时间戳，再更新状态** 的原则，确保 Master 切换时能正确恢复。

---

## 6. 总结

### 6.1 架构特点

1. **基于 Hazelcast 的分布式架构**：利用 Hazelcast 的成员管理、分布式数据结构和 Operation 通信机制，实现无状态 Master + 有状态 Worker 的架构。

2. **角色灵活配置**：支持 Master、Worker、Master+Worker 三种角色模式，适应不同部署场景。

3. **自动 Master 切换**：通过 `checkNewActiveMaster()` 每 100ms 检测，结合 IMap 持久化状态，实现 Master 故障自动恢复。

4. **资源预申请机制**：Job 在提交前预申请所有资源，资源不足时根据策略选择等待或失败。

5. **Pipeline 级别恢复**：支持 Pipeline 级别的自动重试，可通过配置控制最大重试次数和重试间隔。

### 6.2 核心流程总结

```
Job 提交 --> CoordinatorService.submitJob()
  --> 创建 JobMaster + PhysicalPlan
  --> 加入 pendingJobQueue
  --> pendingJobSchedule 线程调度
    --> preApplyResources() 预申请资源
    --> resourceManager 分配 Slot
    --> jobMaster.run()
      --> PhysicalPlan.startJob()
        --> SubPlan.stateProcess()
          --> ResourceUtils.applyResourceForPipeline() 实际分配资源
          --> PhysicalVertex.deploy() 部署 TaskGroup
          --> TaskExecutionService 执行任务
          --> CheckpointCoordinator 协调 Checkpoint
```

### 6.3 关键设计决策

1. **Checkpoint 仅向 Starting Tasks 发送 Barrier**：Barrier 通过数据流自然传播到下游，简化了 Checkpoint 逻辑。

2. **Slot 分配策略可插拔**：支持 RANDOM、SLOT_RATIO、SYSTEM_LOAD 三种策略，适应不同场景。

3. **Dynamic Slot 支持**：支持动态创建和释放 Slot，适应云原生场景。

4. **多层级状态管理**：Job、Pipeline、TaskGroup 三层状态独立管理，通过回调机制联动。

5. **Schema 变更支持**：通过专门的 Checkpoint 类型支持 Schema 变更，确保数据一致性。
