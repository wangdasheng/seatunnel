# CDC 数据处理框架 — 多条实现路径方案

> 基于 Apache SeaTunnel 2.3.13 现有架构的扩展设计
> 设计日期: 2026-06-13

---

## 目录

1. [现有架构回顾与缺口分析](#1-现有架构回顾与缺口分析)
2. [三条实现路径总览](#2-三条实现路径总览)
3. [路径 A：Transform 内嵌分发器（最小侵入）](#3-路径-atransform-内嵌分发器最小侵入)
4. [路径 B：Pipeline 多路输出（中等侵入）](#4-路径-bpipeline-多路输出中等侵入)
5. [路径 C：独立 CDC 处理框架（最大灵活性）](#5-路径-c独立-cdc-处理框架最大灵活性)
6. [三条路径对比矩阵](#6-三条路径对比矩阵)
7. [核心组件设计（跨路径复用）](#7-核心组件设计跨路径复用)
8. [推荐路径与演进策略](#8-推荐路径与演进策略)

---

## 1. 现有架构回顾与缺口分析

### 1.1 SeaTunnel 现有 CDC 数据流

```
┌──────────────────────────────────────────────────────────────┐
│                     SeaTunnel CDC Pipeline                   │
│                                                              │
│  ┌──────────┐    ┌──────────────┐    ┌────────────────────┐ │
│  │ MySQL    │    │ Incremental  │    │ IncrementalSource  │ │
│  │ Binlog ──┼───▶│ SourceReader │───▶│ RecordEmitter      │ │
│  │ PG WAL   │    │ (多路复用)    │    │ (Debezium→Row)     │ │
│  └──────────┘    └──────────────┘    └─────────┬──────────┘ │
│                                                │            │
│                          ┌─────────────────────┘            │
│                          ▼                                  │
│                   ┌─────────────┐                           │
│                   │  Transform  │  ← 单一路径，无分发能力      │
│                   │  Chain      │                           │
│                   └──────┬──────┘                           │
│                          ▼                                  │
│                   ┌─────────────┐                           │
│                   │    Sink     │  ← 单一路径，无多目标写入    │
│                   │  (JDBC/ES)  │                           │
│                   └─────────────┘                           │
└──────────────────────────────────────────────────────────────┘
```

### 1.2 现有架构的缺口

| 缺口 | 描述 | 影响 |
|------|------|------|
| **无数据复用** | CDC 数据流是单路径的，一次监听只能服务一个 Sink | 无法将同一份 CDC 数据同时用于多个下游 |
| **无分发机制** | 没有按表名/条件路由到不同处理器的能力 | 所有表的数据混在一起处理 |
| **无独立处理器** | Transform 是链式处理，无法为不同表配置不同处理逻辑 | 缺乏灵活性 |
| **无独立进度追踪** | 所有处理器共享同一个 checkpoint | 快慢处理器互相影响 |
| **无故障隔离** | 一个 Transform 失败会影响整个 Pipeline | 缺乏健壮性 |

---

## 2. 三条实现路径总览

```
┌─────────────────────────────────────────────────────────────────┐
│                    三条实现路径关系图                              │
│                                                                 │
│   侵入性: 低 ◀──────────────────────────────▶ 高                 │
│   灵活性: 低 ◀──────────────────────────────▶ 高                 │
│                                                                 │
│   ┌──────────────┐  ┌──────────────┐  ┌──────────────────────┐ │
│   │  路径 A      │  │  路径 B      │  │  路径 C              │ │
│   │  Transform   │  │  Pipeline    │  │  独立 CDC Framework  │ │
│   │  内嵌分发器   │  │  多路输出     │  │                      │ │
│   │              │  │              │  │                      │ │
│   │  改动量: ★   │  │  改动量: ★★  │  │  改动量: ★★★        │ │
│   │  复用度: ★★★ │  │  复用度: ★★  │  │  复用度: ★          │ │
│   │  隔离性: ★   │  │  隔离性: ★★  │  │  隔离性: ★★★        │ │
│   └──────────────┘  └──────────────┘  └──────────────────────┘ │
└─────────────────────────────────────────────────────────────────┘
```

---

## 3. 路径 A：Transform 内嵌分发器（最小侵入）

### 3.1 核心思想

在 SeaTunnel 现有 Transform 层实现一个特殊的 `CdcDispatcherTransform`，它作为 Transform 链中的一个节点，内部管理多个 `CdcProcessor`，将 CDC 数据按规则分发到不同的处理器。

### 3.2 架构图

```
┌──────────────────────────────────────────────────────────────────────┐
│                        路径 A: Transform 内嵌分发器                     │
│                                                                      │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │              IncrementalSource (CDC Source)                    │  │
│  │  MySQL Binlog / PG WAL / Oracle LogMiner                       │  │
│  └──────────────────────────┬─────────────────────────────────────┘  │
│                             │ SeaTunnelRow (含 table_id + RowKind)   │
│                             ▼                                        │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │            CdcDispatcherTransform (NEW)                        │  │
│  │  ┌──────────────────────────────────────────────────────────┐  │  │
│  │  │                   CdcDispatcher                           │  │  │
│  │  │  ┌─────────────────────────────────────────────────────┐ │  │  │
│  │  │  │              TableRoutingRule                       │ │  │  │
│  │  │  │  table: "orders"    → ProcessorChain: [A, B]        │ │  │  │
│  │  │  │  table: "users"     → ProcessorChain: [C]           │ │  │  │
│  │  │  │  table: "products"  → ProcessorChain: [A, D]        │ │  │  │
│  │  │  │  table: "*"         → ProcessorChain: [default]     │ │  │  │
│  │  │  └─────────────────────────────────────────────────────┘ │  │  │
│  │  │                          │                                │  │  │
│  │  │         ┌────────────────┼────────────────┐               │  │  │
│  │  │         ▼                ▼                 ▼              │  │  │
│  │  │  ┌────────────┐  ┌────────────┐  ┌────────────┐         │  │  │
│  │  │  │ Processor  │  │ Processor  │  │ Processor  │         │  │  │
│  │  │  │ Chain A    │  │ Chain B    │  │ Chain C    │         │  │  │
│  │  │  │ ┌────────┐ │  │ ┌────────┐ │  │ ┌────────┐ │         │  │  │
│  │  │  │ │Filter  │ │  │ │Enrich  │ │  │ │Map     │ │         │  │  │
│  │  │  │ │Processor│ │  │ │Processor│ │  │ │Processor│ │         │  │  │
│  │  │  │ └────┬───┘ │  │ └───┬────┘ │  │ └───┬────┘ │         │  │  │
│  │  │  │      ▼     │  │     ▼      │  │     ▼      │         │  │  │
│  │  │  │ ┌────────┐ │  │ ┌────────┐ │  │ ┌────────┐ │         │  │  │
│  │  │  │ │Transform│ │  │ │Validate│ │  │ │Enrich  │ │         │  │  │
│  │  │  │ │Processor│ │  │ │Processor│ │  │ │Processor│ │         │  │  │
│  │  │  │ └────────┘ │  │ └────────┘ │  │ └────────┘ │         │  │  │
│  │  │  └──────┬─────┘  └─────┬──────┘  └─────┬──────┘         │  │  │
│  │  │         │              │               │                 │  │  │
│  │  │         ▼              ▼               ▼                 │  │  │
│  │  │  ┌─────────────────────────────────────────────────┐    │  │  │
│  │  │  │            Result Merger                         │    │  │  │
│  │  │  │  合并所有 Processor 输出，添加 "__processor_id"    │    │  │  │
│  │  │  │  标记字段，统一传递给下游                          │    │  │  │
│  │  │  └─────────────────────────────────────────────────┘    │  │  │
│  │  └──────────────────────────────────────────────────────────┘  │  │
│  └────────────────────────────────────────────────────────────────┘  │
│                             │ SeaTunnelRow (含 processor_id)         │
│                             ▼                                        │
│  ┌────────────────────────────────────────────────────────────────┐  │
│  │                   后续 Transform / Sink                         │  │
│  │  (根据 processor_id 字段路由到不同 Sink)                         │  │
│  └────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────┘
```

### 3.3 核心接口定义

```java
/**
 * CDC 数据处理器接口 - 路径 A 版本
 * 每个处理器接收 CDC 变更数据，处理后返回结果
 */
public interface CdcProcessor extends Serializable {

    /** 处理器唯一标识 */
    String processorId();

    /** 初始化处理器 */
    void open() throws Exception;

    /**
     * 处理单条 CDC 变更记录
     * @param row    CDC 变更数据（含 table_id, RowKind）
     * @return 处理后的数据，返回 null 表示过滤掉该记录
     */
    SeaTunnelRow process(SeaTunnelRow row) throws Exception;

    /** 批量处理 */
    default List<SeaTunnelRow> processBatch(List<SeaTunnelRow> rows) throws Exception {
        return rows.stream()
            .map(row -> {
                try { return process(row); }
                catch (Exception e) { throw new RuntimeException(e); }
            })
            .filter(Objects::nonNull)
            .collect(Collectors.toList());
    }

    /** 关闭处理器 */
    void close() throws Exception;
}

/**
 * 表路由规则
 */
public interface TableRoutingRule extends Serializable {

    /** 匹配表名 */
    boolean matches(String database, String tableName);

    /** 获取目标处理器链 */
    List<String> getProcessorIds();
}

/**
 * CDC 分发器 Transform
 */
public class CdcDispatcherTransform implements SeaTunnelTransform<SeaTunnelRow> {

    // 配置
    private final ReadonlyConfig config;

    // 路由规则列表
    private final List<TableRoutingRule> routingRules;

    // 处理器实例 (processorId → processor)
    private final Map<String, CdcProcessor> processors;

    // 处理器链 (processorId → processor chain)
    private final Map<String, List<CdcProcessor>> processorChains;

    @Override
    public SeaTunnelRow map(SeaTunnelRow row) {
        String tableId = row.getTableId();
        // 1. 匹配路由规则
        List<CdcProcessor> chain = route(tableId);
        // 2. 依次执行处理器链
        SeaTunnelRow result = row;
        for (CdcProcessor processor : chain) {
            result = processor.process(result);
            if (result == null) return null; // 被过滤
        }
        return result;
    }
}
```

### 3.4 配置示例

```hocon
transform {
  CdcDispatcher {
    routing_rules = [
      {
        table_pattern = "mydb.orders"
        processors = ["order_enricher", "order_validator"]
      },
      {
        table_pattern = "mydb.users"
        processors = ["user_masker"]
      },
      {
        table_pattern = "*"
        processors = ["default_passthrough"]
      }
    ]

    processors = {
      order_enricher {
        type = "EnrichProcessor"
        lookup_table = "dim_products"
        join_key = "product_id"
      }
      order_validator {
        type = "ValidateProcessor"
        rules = [...]
      }
      user_masker {
        type = "MaskProcessor"
        fields = ["phone", "email"]
      }
      default_passthrough {
        type = "PassthroughProcessor"
      }
    }
  }
}
```

### 3.5 优缺点

| 维度 | 评价 |
|------|------|
| **侵入性** | ★☆☆☆☆ 极低 — 仅新增一个 Transform 实现 |
| **复用度** | ★★★★★ 完全复用现有 Pipeline、Checkpoint、SPI |
| **隔离性** | ★☆☆☆☆ 所有处理器在同一线程，故障会影响整个 Pipeline |
| **扩展性** | ★★★☆☆ 通过 SPI 扩展 Processor，但无法扩展现有架构 |
| **进度追踪** | ★★☆☆☆ 共享 checkpoint，无法独立追踪 |
| **异步处理** | ★☆☆☆☆ 难以支持，需要额外线程池管理 |

---

## 4. 路径 B：Pipeline 多路输出（中等侵入）

### 4.1 核心思想

扩展 SeaTunnel 的 Pipeline 引擎，使其支持从单个 Source 分叉出多个 Sink 路径。CDC Dispatcher 作为 Source 和 Sink 之间的路由层，将不同表的数据路由到不同的 Transform → Sink 链路。

### 4.2 架构图

```
┌──────────────────────────────────────────────────────────────────────────┐
│                     路径 B: Pipeline 多路输出                              │
│                                                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                    IncrementalSource (CDC)                         │  │
│  │             MySQL Binlog / PG WAL / Oracle LogMiner                │  │
│  └────────────────────────────┬───────────────────────────────────────┘  │
│                               │ SeaTunnelRow (含 table_id)               │
│                               ▼                                          │
│  ┌────────────────────────────────────────────────────────────────────┐  │
│  │                    CdcDataDispatcher (NEW)                         │  │
│  │  ┌──────────────────────────────────────────────────────────────┐  │  │
│  │  │  DispatcherConfig                                            │  │  │
│  │  │  ┌────────────┐  ┌────────────┐  ┌──────────────────────┐   │  │  │
│  │  │  │ Routing    │  │ Buffer     │  │ Checkpoint           │   │  │  │
│  │  │  │ Rules      │  │ Manager    │  │ Coordinator          │   │  │  │
│  │  │  │            │  │ ┌────────┐ │  │ ┌──────────────────┐ │   │  │  │
│  │  │  │ orders→[A] │  │ │Buffer A│ │  │ │Offset A: 1024    │ │   │  │  │
│  │  │  │ users→[B]  │  │ │Buffer B│ │  │ │Offset B: 2048    │ │   │  │  │
│  │  │  │ *→[C]      │  │ │Buffer C│ │  │ │Offset C: 512     │ │   │  │  │
│  │  │  └────────────┘  │ └────────┘ │  │ └──────────────────┘ │   │  │  │
│  │  │                  └────────────┘  └──────────────────────┘   │  │  │
│  │  └──────────────────────────────────────────────────────────────┘  │  │
│  │         │                    │                    │                  │  │
│  │         ▼                    ▼                    ▼                  │  │
│  └─────────┼────────────────────┼────────────────────┼──────────────────┘  │
│            │                    │                    │                      │
│   ┌────────┴────────┐  ┌───────┴────────┐  ┌───────┴────────┐             │
│   │  Pipeline A     │  │  Pipeline B    │  │  Pipeline C    │             │
│   │  (orders)       │  │  (users)       │  │  (default)     │             │
│   │ ┌────────────┐  │  │ ┌────────────┐ │  │ ┌────────────┐ │             │
│   │ │ Transform  │  │  │ │ Transform  │ │  │ │ Transform  │ │             │
│   │ │ Chain A    │  │  │ │ Chain B    │ │  │ │ Chain C    │ │             │
│   │ └─────┬──────┘  │  │ └─────┬──────┘ │  │ └─────┬──────┘ │             │
│   │       ▼         │  │       ▼        │  │       ▼        │             │
│   │ ┌────────────┐  │  │ ┌────────────┐ │  │ ┌────────────┐ │             │
│   │ │ Sink A     │  │  │ │ Sink B     │ │  │ │ Sink C     │ │             │
│   │ │ (JDBC)     │  │  │ │ (Kafka)    │ │  │ │ (ES)       │ │             │
│   │ └────────────┘  │  │ └────────────┘ │  │ └────────────┘ │             │
│   │                 │  │                │  │                │             │
│   │ ┌────────────┐  │  │ ┌────────────┐ │  │ ┌────────────┐ │             │
│   │ │ Independent│  │  │ │ Independent│ │  │ │ Independent│ │             │
│   │ │ Checkpoint │  │  │ │ Checkpoint │ │  │ │ Checkpoint │ │             │
│   │ └────────────┘  │  │ └────────────┘ │  │ └────────────┘ │             │
│   └─────────────────┘  └────────────────┘  └────────────────┘             │
└──────────────────────────────────────────────────────────────────────────┘
```

### 4.3 核心接口定义

```java
/**
 * CDC 数据分发器 - 路径 B 版本
 * 作为 SeaTunnel Pipeline 的中间组件，负责任务级别的数据分发
 */
public interface CdcDataDispatcher extends Serializable {

    /**
     * 初始化分发器
     * @param routingRules 路由规则配置
     * @param pipelineCount 下游 Pipeline 数量
     */
    void open(List<RoutingRule> routingRules, int pipelineCount);

    /**
     * 分发数据到对应的 Pipeline
     * @param row CDC 变更数据
     * @return 目标 Pipeline 索引列表
     */
    List<Integer> dispatch(SeaTunnelRow row);

    /**
     * 通知所有下游 Pipeline 不再有新数据
     */
    void signalNoMoreElement();

    /**
     * 获取指定 Pipeline 的消费位点
     */
    Offset getOffset(int pipelineIndex);

    /**
     * 关闭分发器
     */
    void close();
}

/**
 * 路由规则 - 路径 B 版本
 */
public class RoutingRule implements Serializable {
    private String tablePattern;        // 表名匹配模式 (支持 glob)
    private String databasePattern;     // 库名匹配模式
    private int targetPipelineIndex;    // 目标 Pipeline 索引
    private List<String> transformChain; // Transform 链配置
    private SinkConfig sinkConfig;      // Sink 配置
}

/**
 * 独立处理管道 - 路径 B 版本
 * 每个 Pipeline 有自己的 Transform 链和 Sink
 */
public class CdcPipeline implements Serializable {
    private final int pipelineIndex;
    private final List<SeaTunnelTransform<SeaTunnelRow>> transforms;
    private final SeaTunnelSink<...> sink;

    // 独立的 Checkpoint 状态
    private Offset consumedOffset;
    private List<SeaTunnelRow> pendingRows;

    public void processRow(SeaTunnelRow row) { ... }
    public void checkpoint(long checkpointId) { ... }
    public void restoreFromCheckpoint(PipelineState state) { ... }
}
```

### 4.4 配置示例

```hocon
env {
  job.mode = "STREAMING"
}

source {
  MySQL-CDC {
    hostname = "localhost"
    port = 3306
    database-names = ["mydb"]
    table-names = ["mydb.orders", "mydb.users", "mydb.products"]
  }
}

# 新增：多路输出配置
dispatch {
  enabled = true
  buffer_size = 10000

  pipelines = [
    {
      name = "orders_pipeline"
      table_pattern = "mydb.orders"
      transform {
        # 订单特定的 Transform 链
      }
      sink {
        Jdbc { ... }
      }
    },
    {
      name = "users_pipeline"
      table_pattern = "mydb.users"
      transform {
        # 用户特定的 Transform 链
      }
      sink {
        Kafka { ... }
      }
    },
    {
      name = "default_pipeline"
      table_pattern = "*"
      transform {
        # 默认处理
      }
      sink {
        Elasticsearch { ... }
      }
    }
  ]
}
```

### 4.5 优缺点

| 维度 | 评价 |
|------|------|
| **侵入性** | ★★★☆☆ 需修改 Pipeline 引擎和配置解析 |
| **复用度** | ★★★★☆ 复用现有 Transform/Sink 全部生态 |
| **隔离性** | ★★★☆☆ 独立 Pipeline，但共享 JVM 进程 |
| **扩展性** | ★★★★☆ 通过配置新增 Pipeline，无需改代码 |
| **进度追踪** | ★★★★☆ 每个 Pipeline 独立 checkpoint |
| **异步处理** | ★★★★☆ 每个 Pipeline 独立线程，天然支持异步 |

---

## 5. 路径 C：独立 CDC 处理框架（最大灵活性）

### 5.1 核心思想

构建一个完全独立的 CDC 处理框架，运行在 SeaTunnel 引擎之上但拥有自己的生命周期管理、Checkpoint 机制和监控体系。它作为 SeaTunnel 的一个新模块（`seatunnel-cdc-framework`），提供完整的 CDC 数据分发、处理、进度追踪能力。

### 5.2 架构图

```
┌──────────────────────────────────────────────────────────────────────────────┐
│                   路径 C: 独立 CDC 处理框架                                     │
│                   seatunnel-cdc-framework (新模块)                             │
│                                                                              │
│  ┌────────────────────────────────────────────────────────────────────────┐  │
│  │                        CdcFrameworkBootstrap                           │  │
│  │  (框架启动器，负责初始化所有组件，管理生命周期)                              │  │
│  └────────────────────────────────┬───────────────────────────────────────┘  │
│                                   │                                          │
│          ┌────────────────────────┼────────────────────────┐                 │
│          ▼                        ▼                         ▼                │
│  ┌───────────────┐    ┌───────────────────┐    ┌──────────────────────┐     │
│  │ CdcListener   │    │ CdcEventBus       │    │ CdcFrameworkConfig   │     │
│  │ (CDC 监听器)   │    │ (事件总线)         │    │ (框架配置)            │     │
│  │               │    │                   │    │                      │     │
│  │ 封装           │    │ 异步事件传递        │    │ - 监听表配置          │     │
│  │ Incremental   │    │ 支持广播/单播       │    │ - 分发策略            │     │
│  │ Source        │    │ 背压控制           │    │ - 处理器配置          │     │
│  └───────┬───────┘    └────────┬──────────┘    │ - 重试策略            │     │
│          │                     │               └──────────────────────┘     │
│          │                     │                                             │
│          ▼                     ▼                                             │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                     CdcDataDispatcher (核心)                           │  │
│  │                                                                       │  │
│  │  ┌─────────────────────────────────────────────────────────────────┐  │  │
│  │  │                    TableRoutingEngine                           │  │  │
│  │  │  ┌───────────────────────────────────────────────────────────┐  │  │  │
│  │  │  │  RouteTable                                              │  │  │  │
│  │  │  │  ┌───────────────┬───────────────┬───────────────────┐   │  │  │  │
│  │  │  │  │ PriorityRule  │ PatternRule   │ ExpressionRule    │   │  │  │  │
│  │  │  │  │ (精确匹配)     │ (glob 匹配)   │ (SpEL 表达式)     │   │  │  │  │
│  │  │  │  └───────────────┴───────────────┴───────────────────┘   │  │  │  │
│  │  │  └───────────────────────────────────────────────────────────┘  │  │  │
│  │  └─────────────────────────────────────────────────────────────────┘  │  │
│  │                                                                       │  │
│  │         ┌─────────────────────┼─────────────────────┐                 │  │
│  │         ▼                     ▼                      ▼                │  │
│  │  ┌─────────────────┐ ┌─────────────────┐ ┌─────────────────────┐     │  │
│  │  │ ProcessorChain  │ │ ProcessorChain  │ │ ProcessorChain      │     │  │
│  │  │ "orders"        │ │ "users"         │ │ "default"           │     │  │
│  │  │                 │ │                 │ │                     │     │  │
│  │  │ ┌─────────────┐ │ │ ┌─────────────┐ │ │ ┌─────────────────┐ │     │  │
│  │  │ │ CdcProcessor │ │ │ │ CdcProcessor │ │ │ │ CdcProcessor    │ │     │  │
│  │  │ │ (Interface)  │ │ │ │ (Interface)  │ │ │ │ (Interface)     │ │     │  │
│  │  │ └─────────────┘ │ │ └─────────────┘ │ │ └─────────────────┘ │     │  │
│  │  │       │         │ │       │         │ │         │           │     │  │
│  │  │  ┌────┴────┐    │ │  ┌────┴────┐    │ │  ┌──────┴──────┐    │     │  │
│  │  │  │ Sync    │    │ │  │ Sync    │    │ │  │ Async       │    │     │  │
│  │  │  │Processor│    │ │  │Processor│    │ │  │ Processor   │    │     │  │
│  │  │  └─────────┘    │ │  └─────────┘    │ │  └─────────────┘    │     │  │
│  │  └────────┬────────┘ └────────┬────────┘ └──────────┬──────────┘     │  │
│  │           │                   │                      │                │  │
│  │           ▼                   ▼                      ▼                │  │
│  │  ┌────────────────────────────────────────────────────────────────┐   │  │
│  │  │                  ResultCollector (结果收集器)                    │   │  │
│  │  │  收集所有 Processor 输出，路由到对应的 Sink                       │   │  │
│  │  └────────────────────────────────────────────────────────────────┘   │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
│                                                                              │
│  ┌───────────────────────────────────────────────────────────────────────┐  │
│  │                    辅助组件                                            │  │
│  │  ┌──────────────────┐  ┌──────────────────┐  ┌──────────────────┐     │  │
│  │  │ ProgressTracker  │  │ ErrorHandler     │  │ MetricsCollector │     │  │
│  │  │ (进度追踪)        │  │ (异常处理)        │  │ (指标收集)        │     │  │
│  │  │                  │  │                  │  │                  │     │  │
│  │  │ - 每处理器独立    │  │ - 重试策略        │  │ - 处理速率        │     │  │
│  │  │   Binlog Offset  │  │ - 死信队列        │  │ - 延迟监控        │     │  │
│  │  │ - 最慢处理器决定  │  │ - 降级策略        │  │ - 错误计数        │     │  │
│  │  │   全局进度        │  │ - 熔断机制        │  │ - 健康检查        │     │  │
│  │  └──────────────────┘  └──────────────────┘  └──────────────────┘     │  │
│  └───────────────────────────────────────────────────────────────────────┘  │
└──────────────────────────────────────────────────────────────────────────────┘
```

### 5.3 核心接口定义

```java
/**
 * CDC 处理器接口 - 路径 C 完整版本
 */
public interface CdcProcessor<T_IN, T_OUT> extends Serializable {

    /** 处理器唯一标识 */
    String processorId();

    /** 处理器名称（用于展示） */
    String processorName();

    /** 初始化 */
    void open(ProcessorContext context) throws Exception;

    /**
     * 同步处理单条记录
     * @param record CDC 变更记录
     * @return 处理结果，null 表示过滤
     */
    T_OUT process(CdcRecord<T_IN> record) throws Exception;

    /**
     * 异步处理（批量）
     * @param records 批量记录
     * @return CompletableFuture 包含处理结果
     */
    default CompletableFuture<List<CdcRecord<T_OUT>>> processAsync(
            List<CdcRecord<T_IN>> records) {
        return CompletableFuture.supplyAsync(() ->
            records.stream()
                .map(r -> {
                    try { return process(r); }
                    catch (Exception e) { throw new RuntimeException(e); }
                })
                .filter(Objects::nonNull)
                .collect(Collectors.toList())
        );
    }

    /** 是否支持异步处理 */
    default boolean supportsAsync() { return false; }

    /** 获取当前处理进度 */
    Progress getProgress();

    /** 从检查点恢复 */
    void restoreFromCheckpoint(Progress checkpoint);

    /** 关闭 */
    void close() throws Exception;
}

/**
 * CDC 变更记录
 */
public class CdcRecord<T> implements Serializable {
    private final String database;         // 数据库名
    private final String tableName;        // 表名
    private final RowKind rowKind;         // 变更类型 (INSERT/UPDATE/DELETE)
    private final T before;                // 变更前数据 (DELETE/UPDATE)
    private final T after;                 // 变更后数据 (INSERT/UPDATE)
    private final long timestamp;          // 事件时间戳
    private final String binlogFile;       // Binlog 文件名
    private final long binlogPosition;     // Binlog 位点
    private final Map<String, Object> headers; // 扩展头信息
}

/**
 * 分发器接口
 */
public interface CdcDataDispatcher extends AutoCloseable {

    /** 注册处理器链 */
    void registerProcessorChain(String chainId, ProcessorChain chain);

    /** 注销处理器链 */
    void unregisterProcessorChain(String chainId);

    /** 分发 CDC 记录 */
    void dispatch(CdcRecord<?> record);

    /** 获取全局进度（所有处理器中最慢的） */
    Progress getGlobalProgress();

    /** 获取指定处理器的进度 */
    Progress getProcessorProgress(String processorId);

    /** 启动分发器 */
    void start();

    /** 停止分发器 */
    void stop();
}

/**
 * 处理器链
 */
public class ProcessorChain implements Serializable {
    private final String chainId;
    private final List<CdcProcessor<?, ?>> processors;
    private final ErrorHandler errorHandler;
    private final ProgressTracker progressTracker;

    public void process(CdcRecord<?> record) {
        Object current = record;
        for (CdcProcessor processor : processors) {
            try {
                current = processor.process((CdcRecord) current);
                if (current == null) return; // 被过滤
                progressTracker.recordProgress(processor.processorId(), record);
            } catch (Exception e) {
                errorHandler.handle(processor.processorId(), record, e);
            }
        }
    }
}

/**
 * 异常处理策略
 */
public interface ErrorHandler {

    /** 处理异常 */
    void handle(String processorId, CdcRecord<?> record, Exception e);

    /** 重试策略 */
    RetryPolicy getRetryPolicy();

    /** 获取死信队列 */
    DeadLetterQueue getDeadLetterQueue();
}

/**
 * 重试策略
 */
public class RetryPolicy implements Serializable {
    private final int maxRetries;           // 最大重试次数
    private final long backoffMs;           // 退避时间
    private final double backoffMultiplier; // 退避倍增因子
    private final List<Class<? extends Exception>> retryableExceptions;
}

/**
 * 进度追踪器
 */
public interface ProgressTracker {

    /** 记录进度 */
    void recordProgress(String processorId, CdcRecord<?> record);

    /** 获取处理器进度 */
    Progress getProgress(String processorId);

    /** 获取全局进度（最慢处理器） */
    Progress getGlobalProgress();

    /** 保存检查点 */
    void saveCheckpoint(long checkpointId);

    /** 从检查点恢复 */
    void restoreFromCheckpoint(long checkpointId);
}

/**
 * 进度信息
 */
public class Progress implements Serializable {
    private final String binlogFile;
    private final long binlogPosition;
    private final long timestamp;
    private final long processedCount;
    private final long errorCount;
}
```

### 5.4 配置示例

```hocon
cdc-framework {
  # 监听器配置
  listener {
    type = "MySQL-CDC"
    hostname = "localhost"
    port = 3306
    database-names = ["mydb"]
    table-names = ["mydb.*"]
    startup-mode = "earliest"
  }

  # 事件总线配置
  event-bus {
    type = "disruptor"          # 或 "linked-blocking-queue"
    buffer-size = 16384
    wait-strategy = "blocking"
  }

  # 分发器配置
  dispatcher {
    # 路由规则
    routing-rules = [
      {
        name = "orders_rule"
        priority = 100
        match {
          database = "mydb"
          table = "orders"
        }
        target-chain = "orders_chain"
      },
      {
        name = "users_rule"
        priority = 50
        match {
          database = "mydb"
          table-pattern = "users*"
        }
        target-chain = "users_chain"
      },
      {
        name = "default_rule"
        priority = 0
        match {
          database-pattern = "*"
          table-pattern = "*"
        }
        target-chain = "default_chain"
      }
    ]
  }

  # 处理器链配置
  processor-chains = {
    orders_chain = {
      processors = [
        {
          name = "order_enricher"
          type = "EnrichProcessor"
          config {
            lookup-source = "redis"
            redis-key-pattern = "product:${product_id}"
          }
          async = false
          retry {
            max-retries = 3
            backoff-ms = 1000
          }
        },
        {
          name = "order_validator"
          type = "ValidateProcessor"
          config {
            rules = [
              { field = "amount", min = 0 }
              { field = "status", in = ["PENDING", "PAID", "SHIPPED"] }
            ]
          }
          on-error = "dead-letter"
        }
      ]
      sink {
        type = "Jdbc"
        config { ... }
      }
    }

    users_chain = {
      processors = [
        {
          name = "user_masker"
          type = "MaskProcessor"
          config {
            fields = ["phone", "email", "id_card"]
            mask-type = "PARTIAL"
          }
          async = true
          batch-size = 100
        }
      ]
      sink {
        type = "Kafka"
        config { ... }
      }
    }

    default_chain = {
      processors = [
        {
          name = "passthrough"
          type = "PassthroughProcessor"
        }
      ]
      sink {
        type = "Elasticsearch"
        config { ... }
      }
    }
  }

  # 进度追踪
  progress-tracker {
    checkpoint-interval = "30s"
    storage = "jdbc"           # 或 "file", "redis"
    jdbc {
      url = "jdbc:mysql://..."
      table = "cdc_progress"
    }
  }

  # 异常处理
  error-handling {
    dead-letter {
      enabled = true
      storage = "kafka"
      topic = "cdc-dead-letter"
    }
    alert {
      enabled = true
      channel = "feishu"
      webhook = "..."
    }
  }
}
```

### 5.5 优缺点

| 维度 | 评价 |
|------|------|
| **侵入性** | ★★★★★ 完全独立模块，不影响现有代码 |
| **复用度** | ★☆☆☆☆ 需要独立实现 Checkpoint、SPI 等 |
| **隔离性** | ★★★★★ 完全独立，故障不互相影响 |
| **扩展性** | ★★★★★ 架构设计之初就考虑完全可扩展 |
| **进度追踪** | ★★★★★ 独立、精细的进度追踪 |
| **异步处理** | ★★★★★ 原生支持异步，基于 Disruptor/事件总线 |

---

## 6. 三条路径对比矩阵

```
┌────────────────────┬─────────────────┬─────────────────┬─────────────────┐
│       维度          │   路径 A        │   路径 B        │   路径 C        │
│                    │ Transform 内嵌   │ Pipeline 多路    │ 独立框架        │
├────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ 代码改动量          │ ~500 行         │ ~2000 行        │ ~5000 行        │
│ 开发周期            │ 1-2 周          │ 3-4 周          │ 6-8 周          │
│ 对现有代码影响      │ 无              │ 修改 Pipeline   │ 无              │
│                    │                 │ 引擎 + 配置解析  │                │
├────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ 数据复用            │ ✅ 同一Transform │ ✅ 多Pipeline    │ ✅ 原生支持      │
│                    │    内分发        │    独立消费      │                 │
│ 表级路由            │ ✅ 可配置        │ ✅ 可配置        │ ✅ 多级路由      │
│ 动态添加处理器      │ ⚠️ 需重启        │ ⚠️ 需重启        │ ✅ 热加载        │
├────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ 同步处理            │ ✅ 原生支持      │ ✅ 原生支持      │ ✅ 原生支持      │
│ 异步处理            │ ❌ 困难          │ ✅ 天然支持      │ ✅ 原生支持      │
├────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ 独立进度追踪        │ ❌ 共享          │ ✅ 独立Checkpoint│ ✅ 精细追踪      │
│ 故障隔离            │ ❌ 影响全局      │ ⚠️ Pipeline 级   │ ✅ 处理器级      │
│ 重试机制            │ ⚠️ 需自行实现    │ ⚠️ 需自行实现    │ ✅ 内置支持      │
│ 死信队列            │ ❌ 不支持        │ ❌ 不支持        │ ✅ 内置支持      │
├────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ 复用现有 SPI        │ ✅ 完全复用      │ ✅ 完全复用      │ ⚠️ 需适配        │
│ 复用现有 Sink       │ ✅ 后续Sink处理  │ ✅ 每Pipeline    │ ⚠️ 需适配        │
│                    │                 │    独立Sink      │                 │
│ 复用现有 Checkpoint │ ✅ 完全复用      │ ✅ 扩展复用      │ ❌ 独立实现      │
├────────────────────┼─────────────────┼─────────────────┼─────────────────┤
│ 运维复杂度          │ 低              │ 中              │ 高              │
│ 监控能力            │ 弱              │ 中              │ 强              │
│ 适合场景            │ 简单分发场景     │ 中等复杂度场景   │ 复杂企业级场景   │
└────────────────────┴─────────────────┴─────────────────┴─────────────────┘
```

---

## 7. 核心组件设计（跨路径复用）

无论选择哪条路径，以下核心组件设计是通用的：

### 7.1 CdcProcessor 继承体系

```
                    ┌─────────────────────┐
                    │   CdcProcessor      │  ← 核心接口
                    │   <T_IN, T_OUT>     │
                    └──────────┬──────────┘
                               │
          ┌────────────────────┼────────────────────┐
          ▼                    ▼                     ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐
│ AbstractSync    │  │ AbstractAsync   │  │ AbstractBatch       │
│ Processor       │  │ Processor       │  │ Processor           │
│                 │  │                 │  │                     │
│ 同步单条处理     │  │ 异步批量处理     │  │ 同步批量处理         │
│ process(row)    │  │ processAsync()  │  │ processBatch(rows)  │
└────────┬────────┘  └────────┬────────┘  └──────────┬──────────┘
         │                    │                       │
         └────────────────────┼───────────────────────┘
                              │
         ┌────────────────────┼────────────────────────────┐
         ▼                    ▼                             ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐
│ FilterProcessor │  │ EnrichProcessor │  │ TransformProcessor  │
│ (过滤处理器)     │  │ (富化处理器)     │  │ (转换处理器)         │
│                 │  │                 │  │                     │
│ 按条件过滤记录   │  │ 关联外部数据源   │  │ 字段映射/计算        │
└─────────────────┘  └─────────────────┘  └─────────────────────┘
         │                    │                       │
         ▼                    ▼                       ▼
┌─────────────────┐  ┌─────────────────┐  ┌─────────────────────┐
│ Validate        │  │ MaskProcessor   │  │ RouteProcessor      │
│ Processor       │  │ (脱敏处理器)     │  │ (二级路由处理器)      │
│                 │  │                 │  │                     │
│ 数据校验/清洗    │  │ 敏感字段脱敏     │  │ 根据字段值再次路由    │
└─────────────────┘  └─────────────────┘  └─────────────────────┘
```

### 7.2 数据流关键流程

```
CDC Event (Binlog/WAL)
        │
        ▼
┌───────────────────┐
│ 1. CdcListener    │  监听 CDC 事件流
│    接收原始事件     │  封装为 CdcRecord
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│ 2. Dispatcher     │  表名匹配路由规则
│    路由分发        │  确定目标 ProcessorChain
└────────┬──────────┘
         │
    ┌────┴────┬────────────┐
    ▼         ▼             ▼
┌───────┐ ┌───────┐  ┌───────────┐
│Chain A│ │Chain B│  │Chain C    │
│ ┌───┐ │ │ ┌───┐ │  │ ┌───────┐ │
│ │ P1│ │ │ │ P3│ │  │ │ P5    │ │
│ │ P2│ │ │ │ P4│ │  │ │ P6    │ │
│ └───┘ │ │ └───┘ │  │ └───────┘ │
└───┬───┘ └───┬───┘  └─────┬─────┘
    │         │             │
    │    ┌────┴────┐        │
    │    │ 重试?    │        │
    │    │ 失败→DLQ │        │
    │    └─────────┘        │
    │         │             │
    ▼         ▼             ▼
┌───────────────────┐
│ 3. ResultCollector│  收集处理结果
│    结果收集        │  更新进度
└────────┬──────────┘
         │
         ▼
┌───────────────────┐
│ 4. Sink Router    │  路由到目标 Sink
│    写入目标        │  JDBC / Kafka / ES / ...
└───────────────────┘
```

### 7.3 进度追踪机制

```
                    ┌─────────────────────────────┐
                    │     ProgressTracker          │
                    │                             │
                    │  ┌───────────────────────┐  │
                    │  │ ProcessorProgress A   │  │
                    │  │ binlog: mysql-bin.003 │  │
                    │  │ position: 1024         │  │
                    │  │ processed: 50000       │  │
                    │  │ errors: 3              │  │
                    │  └───────────────────────┘  │
                    │                             │
                    │  ┌───────────────────────┐  │
                    │  │ ProcessorProgress B   │  │
                    │  │ binlog: mysql-bin.003 │  │
                    │  │ position: 2048         │  │  ← 最快
                    │  │ processed: 100000      │  │
                    │  │ errors: 0              │  │
                    │  └───────────────────────┘  │
                    │                             │
                    │  ┌───────────────────────┐  │
                    │  │ ProcessorProgress C   │  │
                    │  │ binlog: mysql-bin.003 │  │
                    │  │ position: 512          │  │  ← 最慢
                    │  │ processed: 10000       │  │
                    │  │ errors: 50             │  │
                    │  └───────────────────────┘  │
                    │                             │
                    │  GlobalProgress = min(ABC)  │  ← 全局进度 = 最慢的
                    │  = position 512             │
                    └─────────────────────────────┘
```

**关键设计原则：**
- 全局进度 = 所有处理器中最慢的消费位点
- 每个处理器独立记录 checkpoint
- 故障恢复时，回退到最慢处理器的 checkpoint
- 快处理器可以继续消费，但全局 checkpoint 不会超过最慢的

---

## 8. 推荐路径与演进策略

### 8.1 推荐：路径 B（Pipeline 多路输出）作为最终目标，路径 A 作为快速验证

```
Phase 1 (Week 1-2): 路径 A 快速验证
  ├── 实现 CdcDispatcherTransform
  ├── 实现 3-5 个基础 Processor
  ├── 在实际场景中验证分发逻辑
  └── 收集反馈，确定真实需求

Phase 2 (Week 3-5): 路径 B 引擎扩展
  ├── 扩展 Pipeline 引擎支持多路输出
  ├── 实现独立 Checkpoint 机制
  ├── 迁移 Phase 1 的 Processor 到新架构
  └── 生产环境灰度验证

Phase 3 (Week 6+): 路径 C 按需选装
  ├── 如需要热加载、死信队列等高级特性
  ├── 从路径 B 中提取可复用组件
  └── 构建独立模块，与路径 B 并存
```

### 8.2 路径 B 的优势总结

1. **平衡性最佳**：在侵入性、灵活性、复用度之间取得最佳平衡
2. **复用现有生态**：Transform/Sink/SPI 全部复用，无需重复造轮子
3. **独立 Checkpoint**：每个 Pipeline 独立管理进度，实现真正的故障隔离
4. **配置化驱动**：通过 HOCON 配置即可添加新的处理链路
5. **渐进式演进**：可以平滑升级到路径 C 的高级特性

---

## 附录：项目目录结构建议

```
seatunnel-cdc-framework/
├── cdc-framework-api/          # 核心 API 接口
│   └── src/main/java/.../
│       ├── CdcProcessor.java
│       ├── CdcRecord.java
│       ├── CdcDataDispatcher.java
│       ├── ProcessorChain.java
│       ├── ProgressTracker.java
│       ├── ErrorHandler.java
│       └── RoutingRule.java
│
├── cdc-framework-core/         # 核心实现
│   └── src/main/java/.../
│       ├── dispatcher/
│       │   ├── DefaultCdcDataDispatcher.java
│       │   └── TableRoutingEngine.java
│       ├── processor/
│       │   ├── AbstractSyncProcessor.java
│       │   ├── AbstractAsyncProcessor.java
│       │   └── ProcessorChainExecutor.java
│       ├── progress/
│       │   ├── DefaultProgressTracker.java
│       │   └── JdbcProgressStore.java
│       ├── error/
│       │   ├── DefaultErrorHandler.java
│       │   ├── RetryPolicy.java
│       │   └── DeadLetterQueue.java
│       └── event/
│           ├── CdcEventBus.java
│           └── DisruptorEventBus.java
│
├── cdc-framework-processors/   # 内置处理器
│   └── src/main/java/.../
│       ├── FilterProcessor.java
│       ├── EnrichProcessor.java
│       ├── MaskProcessor.java
│       ├── ValidateProcessor.java
│       └── TransformProcessor.java
│
└── cdc-framework-config/       # 配置解析
    └── src/main/java/.../
        ├── CdcFrameworkConfig.java
        └── CdcFrameworkConfigParser.java
```