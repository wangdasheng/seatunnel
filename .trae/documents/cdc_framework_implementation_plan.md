# CDC 数据处理框架 — 实施计划

> 基于 Apache SeaTunnel 2.3.13 架构，采用 **路径 B+（Pipeline 多路输出 + 精细进度追踪）** 方案
> 创建日期: 2026-06-13

---

## 一、摘要

在 SeaTunnel 现有 Pipeline 引擎基础上扩展 CDC 数据分发能力：一个 CDC Source 的数据流按表名路由到多个独立的处理链路，每个链路拥有独立的 Transform 链、Sink、Checkpoint 进度追踪、重试策略和死信队列。

## 二、当前架构分析（基于代码探索结论）

### 2.1 已确认的关键事实

| 维度 | 当前状态 | 关键代码位置 |
|------|---------|------------|
| 数据载体 | `SeaTunnelRow` 自带 `tableId: String` + `rowKind: RowKind`，天然支持路由 | `seatunnel-api/.../SeaTunnelRow.java` |
| Pipeline 拓扑 | 线性: Source → TransformChain → Sink，`PipelineGenerator` 生成 | `PipelineGenerator.java` |
| Transform 链 | `TransformChainAction` 封装 `List<SeaTunnelTransform<T>>` | `TransformChainAction.java` |
| Checkpoint | `CheckpointCoordinator` 按 `pipelineId` 维度管理，一个 Pipeline 一个 | `CheckpointCoordinator.java` |
| 执行计划生成 | `ExecutionPlanGenerator` 三阶段: edges → chain → pipelines | `ExecutionPlanGenerator.java` |
| CDC Source | `IncrementalSource` 抽象类 + Debezium, 支持 Snapshot + Incremental | `IncrementalSource.java` |
| SPI 发现 | `SeaTunnelTransformPluginDiscovery` 通过 SPI 加载 Transform | `SeaTunnelTransformPluginDiscovery.java` |

### 2.2 核心缺口

1. **无分支拓扑**: `PipelineGenerator` 只处理线性图，`collectChainedVertices` 遇到多目标顶点会停止
2. **无分发能力**: 没有按 `tableId` 路由到不同处理器的机制
3. **Checkpoint 耦合**: 所有处理共享一个 `CheckpointCoordinator`，无法独立进度追踪
4. **无故障隔离**: 一个 Transform 异常会影响整条 Pipeline

## 三、架构决策: Path B+（Pipeline 多路输出 + 精细进度）

```
                            ┌──────────────────────────┐
                            │   IncrementalSource       │
                            │   (MySQL Binlog / PG WAL) │
                            └────────────┬─────────────┘
                                         │ SeaTunnelRow (tableId + RowKind)
                                         ▼
                            ┌──────────────────────────┐
                            │   CdcDispatcherTransform  │  ← 新增: 分发器 Transform
                            │   ┌────────────────────┐  │
                            │   │ TableRoutingEngine  │  │
                            │   │ orders → pipeline_0 │  │
                            │   │ users  → pipeline_1 │  │
                            │   │ *      → pipeline_2 │  │
                            │   └────────────────────┘  │
                            └──┬──────────┬──────────┬──┘
                               │          │          │
                    ┌──────────┼──────────┼──────────┼──────────┐
                    ▼          ▼          ▼          ▼          ▼
              ┌──────────┐ ┌──────────┐ ┌──────────┐ ┌──────────┐
              │Pipeline 0│ │Pipeline 1│ │Pipeline 2│ │Pipeline N│ ← 每个独立 Pipeline
              │(orders)  │ │(users)   │ │(default) │ │(...)     │
              │          │ │          │ │          │ │          │
              │Transform │ │Transform │ │Transform │ │Transform │
              │Chain     │ │Chain     │ │Chain     │ │Chain     │
              │  ↓       │ │  ↓       │ │  ↓       │ │  ↓       │
              │Sink(JDBC)│ │Sink(Kfk) │ │Sink(ES)  │ │Sink(...) │
              │          │ │          │ │          │ │          │
              │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────┐ │ │ ┌──────┐ │
              │ │Check │ │ │ │Check │ │ │ │Check │ │ │ │Check │ │
              │ │point │ │ │ │point │ │ │ │point │ │ │ │point │ │
              │ │独立   │ │ │ │独立   │ │ │ │独立   │ │ │ │独立   │ │
              │ └──────┘ │ │ └──────┘ │ │ └──────┘ │ │ └──────┘ │
              └──────────┘ └──────────┘ └──────────┘ └──────────┘

    ┌──────────────────────────────────────────────────────────────┐
    │ 跨 Pipeline 组件 (新增模块 seatunnel-cdc-framework)           │
    │ ┌──────────────────┐ ┌──────────────────┐ ┌───────────────┐ │
    │ │ ProgressTracker  │ │ ErrorHandler     │ │ CdcConfig     │ │
    │ │ (精细进度)        │ │ (重试+死信队列)   │ │ (配置管理)     │ │
    │ └──────────────────┘ └──────────────────┘ └───────────────┘ │
    └──────────────────────────────────────────────────────────────┘
```

### 3.1 路径 B+ 的核心思路

1. **分发器作为特殊 Transform**: `CdcDispatcherTransform` 接收 CDC 数据，根据 `tableId` 路由规则，将数据写入对应下游 Pipeline 的缓冲区
2. **Pipeline 分支生成**: 修改 `ExecutionPlanGenerator`，在检测到 CDC 分发配置时，生成多个独立 Pipeline（每个路由目标一个）
3. **独立 Checkpoint**: 每个下游 Pipeline 拥有自己的 `CheckpointCoordinator`，按各自速度推进
4. **精细进度追踪**: 新增 `ProgressTracker` 组件，记录每条 ProcessorChain 的 binlog 位点
5. **异常处理**: 每个 ProcessorChain 有独立的 `ErrorHandler`，支持重试和死信队列

## 四、组件设计

### 4.1 新增模块: `seatunnel-cdc-framework`

```
seatunnel-cdc-framework/
├── pom.xml
└── src/main/java/org/apache/seatunnel/cdc/framework/
    ├── CdcProcessor.java              # 核心处理器接口
    ├── CdcRecord.java                 # CDC 变更记录
    ├── CdcDispatcherTransform.java    # 分发器 Transform (路径 A/B 通用)
    ├── CdcPipelineContext.java        # Pipeline 上下文 (offset/checkpoint)
    ├── routing/
    │   ├── TableRoutingRule.java      # 路由规则接口
    │   ├── GlobTableRoutingRule.java  # Glob 模式匹配规则
    │   └── TableRoutingEngine.java    # 路由引擎
    ├── processor/
    │   ├── AbstractCdcProcessor.java  # 处理器抽象基类
    │   ├── FilterProcessor.java        # 过滤处理器
    │   ├── EnrichProcessor.java        # 富化处理器
    │   ├── MaskProcessor.java          # 脱敏处理器
    │   └── TransformProcessor.java     # 转换处理器
    ├── progress/
    │   ├── ProgressTracker.java       # 进度追踪器接口
    │   ├── DefaultProgressTracker.java # 默认实现
    │   └── ProgressStore.java         # 进度持久化接口
    ├── error/
    │   ├── ErrorHandler.java          # 错误处理器接口
    │   ├── DefaultErrorHandler.java   # 默认实现
    │   ├── RetryPolicy.java           # 重试策略
    │   └── DeadLetterQueue.java       # 死信队列
    └── config/
        ├── CdcFrameworkConfig.java    # 框架配置类
        └── CdcConfigParser.java       # 配置解析器
```

### 4.2 核心接口定义

#### CdcProcessor

```java
package org.apache.seatunnel.cdc.framework;

/**
 * CDC 数据处理器接口。
 * 每个处理器接收 CDC 变更数据，处理后返回结果。
 */
public interface CdcProcessor extends Serializable {

    /** 处理器唯一标识 */
    String processorId();

    /** 初始化处理器 */
    void open() throws Exception;

    /**
     * 处理单条 CDC 变更记录
     * @param row CDC 变更数据（含 tableId, rowKind）
     * @return 处理后的数据，返回 null 表示过滤掉该记录
     */
    SeaTunnelRow process(SeaTunnelRow row) throws Exception;

    /** 获取处理器当前进度 */
    String getCurrentOffset();

    /** 关闭处理器 */
    void close() throws Exception;
}
```

#### ProgressTracker

```java
package org.apache.seatunnel.cdc.framework.progress;

/**
 * 进度追踪器 — 每个 ProcessorChain 独立追踪。
 * 全局进度 = min(所有链的进度)，确保故障恢复时数据不丢失。
 */
public interface ProgressTracker {

    /** 记录处理器进度 */
    void recordProgress(String chainId, String binlogFile, long binlogPosition);

    /** 获取指定链的进度 */
    Progress getProgress(String chainId);

    /** 获取全局进度（最慢链的进度） */
    Progress getGlobalProgress();

    /** 快照当前进度到持久化存储 */
    void snapshot();

    /** 从持久化存储恢复进度 */
    void restore();
}

class Progress implements Serializable {
    String chainId;
    String binlogFile;
    long binlogPosition;
    long processedCount;
    long errorCount;
    long lastUpdateTimestamp;
}
```

#### ErrorHandler

```java
package org.apache.seatunnel.cdc.framework.error;

/**
 * 异常处理 — 每个 ProcessorChain 独立。
 */
public interface ErrorHandler {

    /**
     * 处理异常
     * @param chainId 处理器链 ID
     * @param row 失败的记录
     * @param e 异常
     * @return 处理结果: RETRY_SUCCESS | RETRY_EXHAUSTED | DEAD_LETTER | FATAL
     */
    ErrorResult handle(String chainId, SeaTunnelRow row, Exception e);

    RetryPolicy getRetryPolicy();

    DeadLetterQueue getDeadLetterQueue();
}
```

### 4.3 配置格式

```hocon
env {
  job.mode = "STREAMING"
  checkpoint.interval = "60s"
}

source {
  MySQL-CDC {
    hostname = "localhost"
    port = 3306
    username = "root"
    password = "password"
    database-names = ["mydb"]
    table-names = ["mydb.orders", "mydb.users", "mydb.*"]
    startup.mode = "earliest"
  }
}

# ========== CDC 分发配置（新增） ==========
cdc-dispatch {
  enabled = true
  buffer-size = 10000

  # 路由规则: 按优先级匹配，优先匹配高优先级
  routing-rules = [
    {
      name = "orders_rule"
      priority = 100
      database = "mydb"
      table = "orders"
      pipeline = "orders_pipeline"
    },
    {
      name = "users_rule"
      priority = 50
      database = "mydb"
      table-pattern = "users*"
      pipeline = "users_pipeline"
    },
    {
      name = "default_rule"
      priority = 0
      database-pattern = "*"
      table-pattern = "*"
      pipeline = "default_pipeline"
    }
  ]

  # 独立 Pipeline 定义
  pipelines = {
    orders_pipeline {
      transform {
        # 订单特定的 Transform 链
        OrderEnricher { lookup-source = "redis" }
        OrderValidator { rules = [...] }
      }
      sink {
        Jdbc {
          url = "jdbc:mysql://target:3306/ods"
          table = "ods_orders"
        }
      }
      # 异常处理（独立配置）
      error-handling {
        retry { max-retries = 3; backoff = "1s" }
        dead-letter { enabled = true; storage = "kafka"; topic = "cdc-dlq-orders" }
      }
    }

    users_pipeline {
      transform {
        UserMasker { fields = ["phone", "email"] }
      }
      sink {
        Kafka {
          topic = "cdc_users"
          bootstrap.servers = "kafka:9092"
        }
      }
      error-handling {
        retry { max-retries = 5; backoff = "2s" }
        dead-letter { enabled = true; storage = "file"; path = "/tmp/cdc-dlq-users" }
      }
    }

    default_pipeline {
      transform {
        Passthrough {}
      }
      sink {
        Elasticsearch {
          hosts = ["es:9200"]
          index = "cdc_default"
        }
      }
    }
  }

  # 进度追踪配置
  progress-tracker {
    checkpoint-interval = "30s"
    storage-type = "jdbc"
    jdbc {
      url = "jdbc:mysql://localhost:3306/cdc_progress"
      table = "cdc_processor_progress"
    }
  }
}
```

## 五、实施步骤

### 步骤 1: 创建 `seatunnel-cdc-framework` 模块骨架

**文件**:
- `seatunnel-cdc-framework/pom.xml` — 模块 POM，依赖 `seatunnel-api`、`seatunnel-transforms-v2`
- `seatunnel-dist/pom.xml` — 将新模块加入发行版

**内容**: Maven 模块初始化，依赖声明，无业务逻辑

**验证**: `mvn clean compile -pl seatunnel-cdc-framework` 通过

---

### 步骤 2: 实现核心接口和数据结构

**文件** (全部在 `seatunnel-cdc-framework/src/main/java/org/apache/seatunnel/cdc/framework/` 下):

| 文件 | 职责 |
|------|------|
| `CdcProcessor.java` | 处理器接口 |
| `AbstractCdcProcessor.java` | 处理器抽象基类（含 open/close/错误计数模板） |
| `routing/TableRoutingRule.java` | 路由规则接口，定义 `match(database, table) → boolean` |
| `routing/GlobTableRoutingRule.java` | Glob 通配符路由规则实现 |
| `routing/TableRoutingEngine.java` | 路由引擎，维护规则链表，按优先级匹配 |
| `progress/Progress.java` | 进度数据结构 |
| `progress/ProgressTracker.java` | 进度追踪接口 |
| `progress/DefaultProgressTracker.java` | 基于 ConcurrentHashMap + 定时快照的进度追踪器 |
| `error/RetryPolicy.java` | 重试策略配置（maxRetries, backoffMs, multiplier） |
| `error/ErrorResult.java` | 错误处理结果枚举 |
| `error/ErrorHandler.java` | 错误处理接口 |
| `error/DefaultErrorHandler.java` | 默认实现（重试 + 死信队列） |
| `error/DeadLetterQueue.java` | 死信队列接口 + 本地文件实现 |
| `config/CdcFrameworkConfig.java` | 配置 POJO |
| `config/CdcConfigParser.java` | HOCON 配置解析器 |

**验证**: `mvn test -pl seatunnel-cdc-framework`（每个组件有对应的单元测试）

---

### 步骤 3: 实现内置处理器

**文件** (在 `processor/` 包下):

| 文件 | 处理器 | 功能 |
|------|--------|------|
| `FilterProcessor.java` | 过滤处理器 | 按 RowKind/字段条件过滤 |
| `EnrichProcessor.java` | 富化处理器 | 关联外部数据源补充字段 |
| `MaskProcessor.java` | 脱敏处理器 | 敏感字段脱敏（phone/email/id_card） |
| `TransformProcessor.java` | 转换处理器 | 字段映射/计算/类型转换 |
| `PassthroughProcessor.java` | 透传处理器 | 不做处理，原样返回 |

**验证**: 每个处理器有独立单元测试

---

### 步骤 4: 实现 `CdcDispatcherTransform`（分发器）

**文件**:
- `seatunnel-cdc-framework/.../CdcDispatcherTransform.java`
- `seatunnel-cdc-framework/.../CdcPipelineContext.java`

**CdcDispatcherTransform 职责**:
1. 初始化时加载路由规则和下游 Pipeline 定义
2. 在 `map(SeaTunnelRow row)` 方法中：
   a. 读取 `row.getTableId()`
   b. 通过 `TableRoutingEngine.match()` 找到目标 pipeline
   c. 将 row 写入对应 pipeline 的 `BlockingQueue<SeaTunnelRow>` 缓冲区
   d. 返回 null（分发器不产生下游数据，数据走独立 Pipeline）
3. 提供 `getPipelineBuffer(int index)` 返回各 pipeline 的缓冲区引用

**CdcPipelineContext 职责**:
- 持有 pipeline 的 buffer、offset、错误计数
- 提供 `nextRow()` 和 `hasMore()` 方法供下游读取

**验证**: 模拟多条不同 tableId 的数据，验证路由正确性和缓冲区分发

---

### 步骤 5: 修改 `ExecutionPlanGenerator` 支持分支 Pipeline

**文件**:
- `seatunnel-engine/.../dag/execution/ExecutionPlanGenerator.java`

**修改点**:
1. 在 `generateTransformChainEdges()` 方法中，当遇到 `CdcDispatcherTransform` 类型的 Transform 时，不进行链式合并，而是将其作为一个分叉点
2. 新增 `generateCdcDispatchEdges()` 方法，为每个下游 pipeline 创建独立的 `ExecutionEdge`
3. 新增 `CdcFanOutAction` 类（或复用现有 Action 体系），表示 Fan-Out 点

**关键逻辑**:
```java
// 伪代码
if (action is CdcDispatcherTransform) {
    // 不合并到 TransformChain，保留为独立顶点
    // 为每个下游 pipeline 创建独立的执行边
    for (PipelineConfig pipelineConfig : cdcConfig.getPipelines()) {
        // 创建 TransformChainAction (pipeline 的 transforms)
        // 创建 SinkAction
        // 创建 edges: CdcDispatcherTransform → TransformChain → Sink
    }
}
```

**PipelineGenerator 修改**: 在 `splitUnrelatedEdges()` 中支持 Fan-Out（一个顶点有多个目标顶点）场景，将每个 Fan-Out 分支拆分为独立 Pipeline。

**验证**: 配置一个 CDC Source + 3 个下游 pipeline，验证生成 3 个独立 Pipeline

---

### 步骤 6: 为每个 CDC Pipeline 创建独立 Checkpoint

**文件**:
- `seatunnel-engine/.../checkpoint/CheckpointManager.java`（可能涉及）
- `seatunnel-engine/.../dag/execution/ExecutionPlan.java`

**方案**: `CheckpointCoordinator` 已经是按 `pipelineId` 维度管理的，步骤 5 生成的每个独立 Pipeline 有不同的 `pipelineId`，因此会自动获得独立的 `CheckpointCoordinator`。主要工作：

1. 确保每个分支 Pipeline 的 `pipelineId` 唯一且稳定
2. 将 `ProgressTracker` 的进度信息纳入每个 Pipeline 的 Checkpoint 状态
3. 在 `CdcDispatcherTransform.checkpoint()` 中获取全局最小进度作为分发器的 checkpoint

**验证**: 验证不同 pipeline 的 checkpoint 独立推进，快速 pipeline 不受慢速 pipeline 影响

---

### 步骤 7: 实现进度追踪持久化

**文件**:
- `seatunnel-cdc-framework/.../progress/JdbcProgressStore.java`
- `seatunnel-cdc-framework/.../progress/FileProgressStore.java`

**实现**:
- `JdbcProgressStore`: 将每个 ProcessorChain 的 binlog 位点持久化到 MySQL
- `FileProgressStore`: 本地文件方案（开发/测试环境）
- Schema: `(chain_id, binlog_file, binlog_position, processed_count, error_count, updated_at)`

**验证**: 模拟写入进度 → 进程重启 → 验证正确恢复

---

### 步骤 8: 端到端集成测试

**测试场景** (基于用户提供):
1. MySQL CDC Source → orders_pipeline (Enrich+Validate → JDBC Sink)
2. 同一 Source → users_pipeline (Mask → Kafka Sink)
3. 同一 Source → default_pipeline (Passthrough → ES Sink)
4. 故障注入: 让 orders_pipeline 的 JDBC Sink 断开 → 验证 users_pipeline 不受影响
5. 进度验证: users_pipeline 快于 orders_pipeline → 验证独立的 checkpoint 进度
6. 重试验证: orders_pipeline 失败记录 → 经过重试 → 成功或进入死信队列

---

## 六、修改文件清单

| 文件路径 | 操作 | 说明 |
|---------|------|------|
| `seatunnel-cdc-framework/pom.xml` | **新增** | 模块 POM |
| `seatunnel-cdc-framework/src/main/java/.../CdcProcessor.java` | **新增** | 核心接口 |
| `seatunnel-cdc-framework/src/main/java/.../AbstractCdcProcessor.java` | **新增** | 抽象基类 |
| `seatunnel-cdc-framework/src/main/java/.../routing/*.java` | **新增** | 路由规则引擎 |
| `seatunnel-cdc-framework/src/main/java/.../processor/*.java` | **新增** | 5个内置处理器 |
| `seatunnel-cdc-framework/src/main/java/.../CdcDispatcherTransform.java` | **新增** | 分发器 |
| `seatunnel-cdc-framework/src/main/java/.../CdcPipelineContext.java` | **新增** | Pipeline 上下文 |
| `seatunnel-cdc-framework/src/main/java/.../progress/*.java` | **新增** | 进度追踪 |
| `seatunnel-cdc-framework/src/main/java/.../error/*.java` | **新增** | 异常处理 |
| `seatunnel-cdc-framework/src/main/java/.../config/*.java` | **新增** | 配置管理 |
| `seatunnel-engine/.../dag/execution/ExecutionPlanGenerator.java` | **修改** | 支持 Fan-Out 分支 |
| `seatunnel-engine/.../dag/execution/PipelineGenerator.java` | **修改** | 支持多目标顶点拆分 |
| `seatunnel-dist/pom.xml` | **修改** | 加入新模块 |

**总计**: 新增 ~20 文件，修改 ~3 文件。预估代码量 ~3000 行（含测试）。

## 七、验证步骤

1. **编译验证**: `mvn clean compile -pl seatunnel-cdc-framework,seatunnel-engine`
2. **单元测试**: `mvn test -pl seatunnel-cdc-framework`
3. **单元测试引擎**: `mvn test -pl seatunnel-engine -Dtest="ExecutionPlanGeneratorTest"`
4. **集成测试**: Docker 环境（MySQL + Kafka + ES），运行完整 CDC 分发配置
5. **故障测试**:
   - 模拟 Sink 断开 → 验证独立 Pipeline 不受影响
   - 模拟处理器异常 → 验证重试 + 死信队列
   - 进程重启 → 验证 Checkpoint 恢复 + 进度恢复
6. **性能测试**: 单 CDC Source → 5 个下游 Pipeline，验证背压和缓冲机制

## 八、假设与风险

| 假设 | 风险 | 缓解 |
|------|------|------|
| `IncrementalSource` 能正确设置 `SeaTunnelRow.tableId` | 如果 CDC Source 未设置 tableId，路由失效 | 在分发器中增加 fallback 机制 |
| Pipeline 引擎支持从单个 Source 创建多个独立 Pipeline | Engine 可能有单 Source 单 Pipeline 限制 | 在 PipelineGenerator 中显式处理 Fan-Out |
| Buffer 不会成为瓶颈 | 慢 Sink 可能导致内存溢出 | 配置 buffer-size + 背压机制 |
| JDBC Progress Store 可用 | 进度持久化失败 | Fallback 到 FileProgressStore |

## 九、配置管理器注册（SPI）

处理器通过 Java SPI 机制发现和注册:

```
src/main/resources/META-INF/services/
└── org.apache.seatunnel.cdc.framework.CdcProcessor
    # 内置处理器列表
    org.apache.seatunnel.cdc.framework.processor.FilterProcessor
    org.apache.seatunnel.cdc.framework.processor.EnrichProcessor
    org.apache.seatunnel.cdc.framework.processor.MaskProcessor
    org.apache.seatunnel.cdc.framework.processor.TransformProcessor
    org.apache.seatunnel.cdc.framework.processor.PassthroughProcessor
```

第三方可以实现 `CdcProcessor` 接口并注册 SPI 来扩展处理器类型。

---

## 十、与路径 A（低侵入方案）对比

> 详细路径 A 方案见：[cdc_framework_path_a_plan.md](./cdc_framework_path_a_plan.md)

### 10.1 核心差异

```
路径 A (低侵入):                    路径 B+ (Pipeline 多路):
                                    
┌──── Source ────┐                 ┌──── Source ────┐
│  CDC Binlog    │                 │  CDC Binlog    │
└───────┬────────┘                 └───────┬────────┘
        │                                  │
        ▼                                  ▼
┌───────────────────┐             ┌───────────────────┐
│ CdcDispatcher     │             │ CdcFanOutAction   │ ← 引擎级分支
│ Transform         │             └──┬────┬────┬──────┘
│ ┌───┐ ┌───┐ ┌───┐ │               │    │    │
│ │ A │ │ B │ │ C │ │  同线程      ┌▼──┐┌▼──┐┌▼──┐
│ └───┘ └───┘ └───┘ │             │A ││B ││C │    独立 Pipeline
│  ↑ 同线程 try-catch│             │独立││独立││独立│
└─────────┬─────────┘             │Sink││Sink││Sink│
          │                       └────┘└────┘└────┘
          ▼                           ↑    ↑    ↑
┌───────────────────┐          各自 CheckpointCoordinator
│ 单一 Sink         │
│ (需二次路由)       │
└───────────────────┘
```

### 10.2 对比矩阵

| 维度 | 路径 A (低侵入) | 路径 B+ (Pipeline 多路) |
|------|:---:|:---:|
| **引擎修改** | **0 行** | ~100 行 |
| **新增代码** | ~2000 行 | ~3000 行 |
| **开发周期** | **1-2 周** | 3-4 周 |
| **单元测试复杂度** | **低** (纯 Transform 测试) | 中 (需 mock Pipeline 引擎) |
| **集成测试** | 简单 (单进程) | 中等 (多 Pipeline 协调) |
| **故障隔离** | try-catch 级 | **Pipeline 级** |
| **Checkpoint 独立性** | 内部偏移追踪 + 幂等跳过 | **原生独立 Checkpoint** |
| **进度精确性** | 依赖幂等跳过 | **精确位点恢复** |
| **多 Sink 目标** | 二次路由 | **原生支持** |
| **异步处理** | 受限 | **原生支持** |
| **慢链影响** | 阻塞其他链 | **无影响** |
| **故障恢复** | 需重放 + 跳过 | **精确恢复** |
| **代码复用(两方案)** | 80% 共享: CdcProcessor、处理器、路由、错误处理 |

### 10.3 选择建议

| 条件 | 推荐方案 |
|------|---------|
| 链数量 2-5 条，速度差异小，快速上线 | **路径 A** |
| 链数量 5+，速度差异大，需独立 Sink | **路径 B+** |
| 链间处理速度差异大 | **路径 B+** (避免慢链拖累快链) |
| 需要真正的故障隔离 | **路径 B+** (Pipeline 级隔离) |
| 渐进式落地 (先验证再升级) | **路径 A → 路径 B+** (推荐) |