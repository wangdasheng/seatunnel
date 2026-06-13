# 方案 A：Transform 内嵌分发器（低侵入方案）— 详细实施计划

> 基于 Apache SeaTunnel 2.3.13 架构，**零侵入引擎核心代码**，全部在 Transform 层实现
> 创建日期: 2026-06-13

---

## 一、核心思想

**不修改 Pipeline 引擎、ExecutionPlanGenerator、CheckpointCoordinator 的任一行代码。** 所有 CDC 分发、多链处理、进度追踪、异常隔离全部在 `CdcDispatcherTransform` 这一个 Transform 内部完成。

```
┌──────────────────────────────────────────────────────────────────┐
│                     零侵入 CDC 分发框架                            │
│                    (全部在 Transform 内部)                         │
│                                                                  │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │              IncrementalSource (CDC Source) - 不动          │  │
│  │  MySQL / PG / Oracle → SeaTunnelRow (tableId + RowKind)    │  │
│  └──────────────────────────┬─────────────────────────────────┘  │
│                             │  ← 单一路径，流入一个 Transform      │
│                             ▼                                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │            CdcDispatcherTransform (唯一新增代码)              │  │
│  │                                                            │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │ ① TableRoutingEngine  (路由: tableId → chains[])     │  │  │
│  │  │   orders → [enrich_chain, validate_chain]             │  │  │
│  │  │   users  → [mask_chain]                               │  │  │
│  │  │   *      → [default_chain]                            │  │  │
│  │  └──────────────────────────────────────────────────────┘  │  │
│  │                          │                                  │  │
│  │         ┌────────────────┼────────────────┐                 │  │
│  │         ▼                ▼                 ▼                │  │
│  │  ┌──────────────┐ ┌──────────────┐ ┌──────────────┐        │  │
│  │  │ Processor    │ │ Processor    │ │ Processor    │        │  │
│  │  │ Chain A      │ │ Chain B      │ │ Chain C      │        │  │
│  │  │ ┌──────────┐ │ │ ┌──────────┐ │ │ ┌──────────┐ │        │  │
│  │  │ │Enrich    │ │ │ │Mask      │ │ │ │Pass      │ │        │  │
│  │  │ │(CdcProc) │ │ │ │(CdcProc) │ │ │ │(CdcProc) │ │        │  │
│  │  │ └────┬─────┘ │ │ └────┬─────┘ │ │ └────┬─────┘ │        │  │
│  │  │      ▼       │ │      ▼       │ │      ▼       │        │  │
│  │  │ ┌──────────┐ │ │ ┌──────────┐ │ │              │        │  │
│  │  │ │Validator │ │ │ │  (单处理器) │ │              │        │  │
│  │  │ │(CdcProc) │ │ │ └──────────┘ │ │              │        │  │
│  │  │ └──────────┘ │ │              │ │              │        │  │
│  │  └──────┬───────┘ └──────┬───────┘ └──────┬───────┘        │  │
│  │         │                │                 │                │  │
│  │         ▼                ▼                 ▼                │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │ ② 内部进度追踪                                       │  │  │
│  │  │ ┌────────────┐ ┌────────────┐ ┌────────────────────┐ │  │  │
│  │  │ │Chain A     │ │Chain B     │ │Global = min(A,B,C) │ │  │  │
│  │  │ │offset:1024 │ │offset:2048 │ │→ checkpoint pos=512 │ │  │  │
│  │  │ └────────────┘ └────────────┘ └────────────────────┘ │  │  │
│  │  │    Chain C: offset=512 (最慢)                         │  │  │
│  │  └──────────────────────────────────────────────────────┘  │  │
│  │                                                            │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │ ③ 异常隔离 + 重试                                    │  │  │
│  │  │ try { chainA.process(row) }                          │  │  │
│  │  │ catch → ErrorHandler.retry(row, 3 times)              │  │  │
│  │  │        → 仍失败 → DeadLetterQueue.write(row)          │  │  │
│  │  │        → chainB/C 不受影响，继续处理                    │  │  │
│  │  └──────────────────────────────────────────────────────┘  │  │
│  │                                                            │  │
│  │  ┌──────────────────────────────────────────────────────┐  │  │
│  │  │ ④ 结果合并 → 标记 processor_id，写入单一下游 Sink     │  │  │
│  │  │ 后续可通过普通 Transform 按 processor_id 二次路由      │  │  │
│  │  └──────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────┘  │
│                             │                                    │
│                             ▼                                    │
│  ┌────────────────────────────────────────────────────────────┐  │
│  │    后续 Transform (可选) → Sink (单一路径，不动)            │  │
│  │    通过 row.options["processor_id"] 在 Sink 中区分目标      │  │
│  └────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────┘
```

## 二、关键设计决策

### 2.1 内部进度追踪（不依赖引擎 Checkpoint）

因为整个框架在同一个 Pipeline 内，只有一个 `CheckpointCoordinator`，无法为每个 ProcessorChain 创建独立 checkpoint。解决方案：

```
Checkpoint 时:
  CdcDispatcherTransform.snapshotState(checkpointId)
    → 序列化所有链的内部偏移: {"chain_A": "mysql-bin.003:1024", "chain_B": "mysql-bin.003:2048", ...}
    → 全局偏移 = min(所有链偏移) = "mysql-bin.003:1024"
    → 全局偏移作为 Transform 的 checkpoint state 返回给引擎

Restore 时:
  CdcDispatcherTransform.restoreState(state)
    → 反序列化所有链偏移
    → Source 从全局偏移恢复 (最慢链的位点)
    → 快链已处理的记录通过偏移比对跳过 (幂等处理)
```

**幂等跳过机制**: 每条 `SeaTunnelRow` 带 binlog 位点（通过 `row.getOptions().get("binlog_position")`），快链在处理时对比自己的偏移，已处理的记录直接跳过。

### 2.2 异常隔离（同线程 try-catch）

所有 ProcessorChain 在 `map()` 方法的同一个线程中执行。异常隔离通过 try-catch 实现：

```java
public SeaTunnelRow map(SeaTunnelRow row) {
    String tableId = row.getTableId();
    List<ProcessorChain> chains = routingEngine.match(tableId);

    for (ProcessorChain chain : chains) {
        try {
            chain.process(row);
            progressTracker.recordProgress(chain.getId(), row.getBinlogPosition());
        } catch (Exception e) {
            errorHandler.handle(chain.getId(), row, e);
            // chain 进度不更新 → restart 时重试
        }
    }
    return row; // 原始行透传（或标记后传入下一级 Transform）
}
```

### 2.3 结果合并策略

因为所有数据最终流入同一个 Sink，需要区分不同 ProcessorChain 的输出：

```java
// 处理完成后在 row.options 中标记
row.getOptions().put("__processor_id", chain.getId());
row.getOptions().put("__target_sink", chain.getTargetSinkType());
```

后续的 Transform 或 Sink 可以通过这个标记做二次路由。

### 2.4 异步处理（内部线程池）

虽然是同步 Transform，但可以提供内部异步处理：

```java
// 异步模式的 ProcessorChain
public class AsyncProcessorChain extends ProcessorChain {
    private final ExecutorService executor;
    private final BlockingQueue<SeaTunnelRow> buffer;

    @Override
    public void process(SeaTunnelRow row) {
        buffer.put(row); // 放入缓冲队列
        executor.submit(() -> super.doProcess(row)); // 异步处理
    }
}
```

限制：异步模式下，`map()` 方法无法返回异步结果。如果需要异步处理的结果写入 Sink，需要 Sink 端支持从 buffer 读取。

> **注意**: 异步处理在路径 A 中是受限的。如果需要真正的异步处理，路径 B+ 是更好的选择。

---

## 三、组件结构

```
seatunnel-cdc-framework/        ← 新增模块，零侵入现有代码
├── pom.xml                     ← 仅依赖 seatunnel-api, seatunnel-transforms-v2
└── src/main/java/org/apache/seatunnel/cdc/framework/
    ├── CdcProcessor.java                    # 处理器接口 (同路径 B)
    ├── AbstractCdcProcessor.java            # 抽象基类
    ├── CdcDispatcherTransform.java          # ★ 核心: 分发器 Transform
    ├── ProcessorChain.java                  # ★ 处理器链容器
    ├── routing/
    │   ├── TableRoutingRule.java            # 路由规则接口
    │   ├── GlobTableRoutingRule.java        # Glob 匹配
    │   └── TableRoutingEngine.java          # 路由引擎
    ├── processor/
    │   ├── FilterProcessor.java             # 过滤处理器
    │   ├── EnrichProcessor.java             # 富化处理器
    │   ├── MaskProcessor.java               # 脱敏处理器
    │   ├── TransformProcessor.java          # 转换处理器
    │   └── PassthroughProcessor.java        # 透传处理器
    ├── progress/
    │   ├── InternalProgressTracker.java     # ★ 内部进度追踪器
    │   └── ProgressStore.java              # 进度持久化
    ├── error/
    │   ├── ErrorHandler.java               # 错误处理
    │   ├── RetryPolicy.java                # 重试策略
    │   └── DeadLetterQueue.java            # 死信队列
    └── config/
        ├── CdcFrameworkConfig.java          # 配置 POJO
        └── CdcConfigParser.java            # HOCON 解析
```

## 四、核心接口定义

### 4.1 CdcDispatcherTransform（分发器）

```java
/**
 * CDC 分发器 Transform - 路径 A 核心。
 * 实现 SeaTunnelTransform + Checkpoint 接口。
 * 这是唯一需要注册为 SeaTunnel Transform 插件的类。
 */
public class CdcDispatcherTransform
        extends AbstractSeaTunnelTransform<SeaTunnelRow> {

    // === 内部组件 ===
    private TableRoutingEngine routingEngine;       // 路由引擎
    private List<ProcessorChain> processorChains;   // 处理器链列表
    private InternalProgressTracker progressTracker; // 内部进度
    private ErrorHandler errorHandler;               // 错误处理

    // === Transform 生命周期 ===
    @Override
    public void init() {
        CdcFrameworkConfig config = CdcConfigParser.parse(pluginConfig);
        this.routingEngine = new TableRoutingEngine(config.getRoutingRules());
        this.processorChains = createChains(config);
        this.progressTracker = new InternalProgressTracker(config);
        this.errorHandler = new DefaultErrorHandler(config);
    }

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        String tableId = row.getTableId();
        List<ProcessorChain> matchedChains = routingEngine.match(tableId);

        for (ProcessorChain chain : matchedChains) {
            // 幂等跳过: 如果链已处理过此位点，跳过
            if (progressTracker.isAlreadyProcessed(chain.getId(), row)) {
                continue;
            }

            try {
                SeaTunnelRow result = chain.process(row);
                progressTracker.recordProgress(chain.getId(), row);
                // 标记输出
                if (result != null) {
                    result.getOptions().put("__chain_id", chain.getId());
                }
            } catch (Exception e) {
                ErrorResult result = errorHandler.handle(chain.getId(), row, e);
                if (result == ErrorResult.FATAL) {
                    throw new RuntimeException("Fatal error in chain " + chain.getId(), e);
                }
                // RETRY_EXHAUSTED / DEAD_LETTER: 进度不更新，restart 时重试
            }
        }
        return row; // 透传原始行（带着 chain_id 标记）
    }

    // === Checkpoint 接口 ===
    @Override  // 由 SeaTunnel Transform checkpoint 机制调用
    public List<ActionSubtaskState> snapshotState(long checkpointId) {
        byte[] state = progressTracker.snapshot();          // 序列化所有链偏移
        byte[] globalOffset = progressTracker.getGlobalOffsetBytes();
        // 返回给 CheckpointCoordinator
        return Collections.singletonList(
            new ActionSubtaskState(globalOffset, state));
    }

    @Override
    public void restoreState(List<ActionSubtaskState> states) {
        byte[] state = states.get(0).getState();
        progressTracker.restore(state); // 恢复各链偏移
    }
}
```

### 4.2 ProcessorChain（处理器链）

```java
/**
 * 处理器链 — 封装一组有序的 CdcProcessor。
 * 内部管理链的配置、异常处理和进度记录。
 */
public class ProcessorChain {
    private final String chainId;
    private final List<CdcProcessor> processors;
    private final ErrorHandler errorHandler;    // 链级错误处理
    private final InternalProgressTracker progress; // 链级进度

    /**
     * 处理单条记录 — 同步模式。
     * 链内 processors 依次执行, 任一返回 null 则终止。
     */
    public SeaTunnelRow process(SeaTunnelRow row) throws Exception {
        SeaTunnelRow current = row;
        for (CdcProcessor processor : processors) {
            current = processor.process(current);
            if (current == null) {
                return null; // 被过滤
            }
        }
        return current;
    }

    /**
     * 设置链级进度（由 Dispatcher 调用）。
     */
    public void recordProgress(String binlogFile, long position) {
        progress.recordProgress(chainId, binlogFile, position);
    }
}
```

### 4.3 InternalProgressTracker（内部进度追踪器）

```java
/**
 * 内部进度追踪器 — 路径 A 的核心挑战。
 *
 * 关键概念:
 *   - 每条链独立追踪 binlog 偏移
 *   - 全局偏移 = min(所有链偏移)
 *   - Checkpoint 时序列化所有链偏移
 *   - Restore 时恢复，快链通过偏移比对跳过已处理记录
 *
 * 这不是"独立 Checkpoint"，而是"独立进度记录 + 幂等处理"。
 */
public class InternalProgressTracker {
    // chainId → ChainProgress
    private final ConcurrentHashMap<String, ChainProgress> chainProgressMap;

    /** 记录链的进度 */
    public void recordProgress(String chainId, SeaTunnelRow row) {
        ChainProgress cp = chainProgressMap.get(chainId);
        cp.update(row.getBinlogFile(), row.getBinlogPosition());
    }

    /** 检查该链是否已处理过此位点 */
    public boolean isAlreadyProcessed(String chainId, SeaTunnelRow row) {
        ChainProgress cp = chainProgressMap.get(chainId);
        return cp.isAheadOf(row.getBinlogFile(), row.getBinlogPosition());
    }

    /** 全局偏移 = 最慢链 */
    public BinlogOffset getGlobalOffset() {
        return chainProgressMap.values().stream()
            .map(ChainProgress::getOffset)
            .min(BinlogOffset::compareTo)
            .orElse(BinlogOffset.ZERO);
    }

    /** 序列化所有链偏移 */
    public byte[] snapshot() { /* 序列化 chainProgressMap */ }

    /** 恢复所有链偏移 */
    public void restore(byte[] data) { /* 反序列化 chainProgressMap */ }
}
```

## 五、处理流程详解

### 5.1 正常处理流程

```
CDC Source 产生一条 row: {tableId="orders", binlog="mysql-bin.003", pos=1500}
         │
         ▼
CdcDispatcherTransform.map(row)
  ├── routingEngine.match("orders") → [Chain A, Chain D]  (2条链同时处理)
  │
  ├── Chain A (enrich_chain):
  │   ├── progressTracker.isAlreadyProcessed("A", pos=1500) → false (需处理)
  │   ├── chainA.process(row):
  │   │   ├── EnrichProcessor.process(row) → enrichedRow
  │   │   └── Validator.process(enrichedRow) → validRow
  │   ├── row.options["__chain_id"] = "chain_A"
  │   └── progressTracker.recordProgress("chain_A", "mysql-bin.003", 1500)
  │
  ├── Chain D (default_chain):
  │   ├── progressTracker.isAlreadyProcessed("D", pos=1500) → false
  │   ├── chainD.process(row) → passRow
  │   ├── row.options["__chain_id"] = "chain_D"  (覆盖，链D的标记)
  │   └── progressTracker.recordProgress("chain_D", "mysql-bin.003", 1500)
  │
  └── return row;  // 带着最后一个匹配链的标记
```

### 5.2 异常处理流程

```
row: {tableId="orders", pos=2000}
  │
Chain A: try { enrich(row) } → 抛异常!
  ├── errorHandler.handle("chain_A", row, SqlException)
  │   ├── retry: 第1次 (backoff 1s) → 仍失败
  │   ├── retry: 第2次 (backoff 2s) → 仍失败
  │   ├── retry: 第3次 (backoff 4s) → 仍失败
  │   └── → ErrorResult.RETRY_EXHAUSTED
  ├── DeadLetterQueue.write(row, exception)
  │   → 写入 Kafka topic "cdc-dead-letter" 或本地文件
  └── CHAIN_A 进度不更新 (pos 停留在 1500)
       → 其他链继续处理 (不受影响)
       → 下次 Checkpoint 时全局偏移 = min(1500, 2500, 3000) = 1500
       → 重启后 Chain A 从 pos=1500+1 开始重试
```

### 5.3 故障恢复流程

```
进程崩溃前状态:
  Chain A offset = "mysql-bin.003:1500"
  Chain B offset = "mysql-bin.003:3000"
  Chain C offset = "mysql-bin.003:5000"
  全局 Checkpoint offset = min = "mysql-bin.003:1500"

进程重启:
  1. Source 从 mysql-bin.003:1500 恢复 (最慢链的位点)
  2. CdcDispatcherTransform.restoreState() 恢复各链偏移
  3. Source 重放 pos 1501 ~ 5000 的数据
  4. pos=1501~2500:
     Chain A: isAlreadyProcessed? false → 处理 (重试)
     Chain B: isAlreadyProcessed? true  → 跳过 (已处理)
     Chain C: isAlreadyProcessed? true  → 跳过 (已处理)
  5. pos=2501~3000:
     Chain A: isAlreadyProcessed? false → 处理
     Chain B: isAlreadyProcessed? false → 处理
     Chain C: isAlreadyProcessed? true  → 跳过
  6. pos=3001~5000:
     Chain A: isAlreadyProcessed? false → 处理
     Chain B: isAlreadyProcessed? false → 处理
     Chain C: isAlreadyProcessed? false → 处理
```

## 六、配置格式

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
    table-names = ["mydb.*"]
    startup.mode = "earliest"
  }
}

transform {
  # ★ 唯一新增的 Transform 配置
  CdcDispatcher {
    # 路由规则
    routing_rules = [
      {
        name = "orders_rule"
        priority = 100
        database = "mydb"
        table = "orders"
        chains = ["orders_enrich_and_validate"]
      },
      {
        name = "users_rule"
        priority = 50
        database = "mydb"
        table = "users"
        chains = ["users_mask"]
      },
      {
        name = "default_rule"
        priority = 0
        database_pattern = "*"
        table_pattern = "*"
        chains = ["default_passthrough"]
      }
    ]

    # 处理器定义
    processors {
      order_enricher {
        type = "EnrichProcessor"
        config { ... }
      }
      order_validator {
        type = "ValidateProcessor"
        config { ... }
      }
      user_masker {
        type = "MaskProcessor"
        config { fields = ["phone", "email"] }
      }
      default_pass {
        type = "PassthroughProcessor"
      }
    }

    # 处理器链定义
    processor_chains = {
      orders_enrich_and_validate = {
        processors = ["order_enricher", "order_validator"]
        error_handling {
          retry { max_retries = 3; backoff_ms = 1000 }
          dead_letter { enabled = true; type = "file"; path = "/tmp/cdc-dlq/orders" }
        }
      }
      users_mask = {
        processors = ["user_masker"]
        async = false
      }
      default_passthrough = {
        processors = ["default_pass"]
      }
    }

    # 进度追踪
    progress_tracker {
      snapshot_interval_seconds = 30
      storage {
        type = "file"  # 或 "jdbc"
        path = "/tmp/cdc-progress/"
      }
    }
  }
}

sink {
  # 单一路径 Sink
  # 但因为 CdcDispatcher 已经在 row.options 中标记了 __chain_id
  # 后续 Transform 或 Sink 可以按需处理
  Console {}
}
```

## 七、实施步骤

### 步骤 1: 创建 `seatunnel-cdc-framework` 模块（不变）
与路径 B+ 相同，零侵入。

### 步骤 2: 实现核心接口（不变）
`CdcProcessor`、`AbstractCdcProcessor` 等接口，两条路径共享。

### 步骤 3: 实现内置处理器（不变）
5 个内置处理器，两条路径共享。

### 步骤 4: 实现 InternalProgressTracker + ProcessorChain
路径 A 独有：内部进度追踪 + 幂等跳过机制。

### 步骤 5: 实现 CdcDispatcherTransform
实现 `SeaTunnelTransform` + 内部 dispatch 逻辑。

### 步骤 6: 实现错误处理
`ErrorHandler`、`RetryPolicy`、`DeadLetterQueue`，两条路径共享。

### 步骤 7: 单元测试 + 集成测试
无需修改引擎代码，测试简单。

## 八、修改文件清单

| 文件 | 操作 | 说明 |
|------|------|------|
| `seatunnel-cdc-framework/` 下所有文件 | **新增** | ~18 个文件 |
| `seatunnel-dist/pom.xml` | **修改** | 1 行：加入新模块依赖 |

**总计: 新增 ~18 文件，修改 1 行（仅 dist/pom.xml）。引擎代码零修改。**

代码量约 ~2000 行（含测试）。

## 九、优点与局限

### 优点

| 优势 | 说明 |
|------|------|
| **零引擎侵入** | 不修改 PipelineGenerator / ExecutionPlanGenerator / CheckpointCoordinator |
| **快速落地** | 1-2 周可完成开发验证 |
| **完全复用** | 复用现有 SPI、Transform、Sink、Checkpoint 全生态 |
| **简单运维** | 单 Pipeline，一个配置文件，一个 JVM 进程 |
| **低风险** | 对现有系统零影响，可回滚 |

### 局限

| 局限 | 说明 | 影响 |
|------|------|------|
| **共享 Checkpoint** | 全局进度 = 最慢链 | 快链在 restart 时需跳过已处理数据 |
| **同步处理为主** | 所有链在同一线程 | 一条慢链会影响其他链的吞吐 |
| **故障恢复开销** | Restart 时快链需跳过已处理数据 | 重启后有短暂的跳过开销 |
| **无 Pipeline 级隔离** | 链间共享 JVM 线程 | 内存泄漏/OOM 会影响所有链 |
| **受限的异步** | 异步链的结果难以回流到 Sink | 需要 Sink 端特殊处理 |
| **单 Sink 路径** | 所有处理结果流向同一 Sink | 需要额外 Transform 做二次路由 |

---

## 十、与路径 B+ 的对比

| 维度 | 路径 A (低侵入) | 路径 B+ (Pipeline 多路) |
|------|:--------------:|:---------------------:|
| **引擎代码修改** | **0 行** | ~100 行 (PipelineGenerator + ExecutionPlanGenerator) |
| **新增代码量** | ~2000 行 | ~3000 行 |
| **开发周期** | **1-2 周** | 3-4 周 |
| **故障隔离** | try-catch 级 (同线程) | Pipeline 级 (独立线程/进程) |
| **Checkpoint** | 共享 (内部偏移追踪) | **独立** (每个 Pipeline 一个 CheckpointCoordinator) |
| **进度精度** | 幂等跳过机制 | **原生独立** |
| **异步处理** | 受限 (内部线程池) | **原生支持** (独立 Pipeline) |
| **多 Sink** | 需二次路由 (在一次 Sink 中处理) | **原生支持** (每 Pipeline 独立 Sink) |
| **运维复杂度** | **低** | 中 |
| **故障恢复** | 需跳过已处理数据 | **精确恢复** (无冗余处理) |
| **性能** | 受最慢链影响 | **独立** (链间无影响) |

---

## 十一、选择建议

### 选路径 A (低侵入) 当:

- 需求快速落地（1-2 周）
- ProcessorChain 数量少（2-5 条）
- 链间处理速度差异不大
- 可接受 restart 时的幂等跳过开销
- 不需要每个链独立的 Sink 目标

### 选路径 B+ (Pipeline 多路) 当:

- 链间处理速度差异大（需要独立 Checkpoint）
- 需要每个链独立的 Sink 目标（orders→JDBC, users→Kafka）
- 需要真正的故障隔离（一个链挂不影响其他）
- 链数量可能增长到 5+ 条
- 对性能和可靠性有更高要求

### 推荐演进策略:

```
Phase 1: 路径 A 快速验证 (1-2 周)
  → 在实际业务中验证分发逻辑和处理器设计
  → 过程中沉淀 Processor 接口和处理器实现

Phase 2: 迁移到路径 B+ (2-3 周)
  → Processor 接口和处理器实现可直接复用
  → 用 Pipeline 级隔离替换 try-catch 级隔离
  → 用独立 Checkpoint 替换内部进度追踪
```

两条路径共享 80% 的代码（CdcProcessor 接口、内置处理器、路由引擎、异常处理），只有 **分发器核心** 和 **进度追踪** 需要重新实现。