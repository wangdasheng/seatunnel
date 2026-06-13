# 二、整体分层架构

## 架构分层总览

```
┌───────────────────────────────────────────────────────────────┐
│                   CLI / API 提交层                             │
│  seatunnel-core/seatunnel-starter (SeaTunnelClient)           │
├───────────────────────────────────────────────────────────────┤
│                   配置解析层                                    │
│  seatunnel-core-starter (ConfigBuilder) + seatunnel-common    │
├───────────────────────────────────────────────────────────────┤
│                   插件发现与加载层                               │
│  seatunnel-plugin-discovery (AbstractPluginDiscovery)         │
├───────────────────────────────────────────────────────────────┤
│                   API 抽象层                                    │
│  seatunnel-api (Source/Sink/Transform/Factory/Type)           │
├───────────────────────────────────────────────────────────────┤
│                   引擎适配层 (Translation)                      │
│  seatunnel-translation-base / flink / spark                   │
├───────────────────────────────────────────────────────────────┤
│                   执行引擎层                                    │
│  Zeta: seatunnel-engine-*                                     │
│  Spark: seatunnel-spark-starter + translation-spark           │
│  Flink: seatunnel-flink-starter + translation-flink           │
├───────────────────────────────────────────────────────────────┤
│                   插件层 (Connector/Transform)                  │
│  seatunnel-connectors-v2/ (70+ connectors)                    │
│  seatunnel-transforms-v2/ (SQL/Filter/Copy/Replace...)        │
└───────────────────────────────────────────────────────────────┘
```

---

## 第一层：API 抽象层 (seatunnel-api)

**路径**: `seatunnel-api/src/main/java/org/apache/seatunnel/api/`

**职责**: 定义所有数据源、目标、转换的统一接口，与具体引擎解耦

**核心接口**:
| 接口 | 路径 | 职责 |
|------|------|------|
| `SeaTunnelSource<T,SplitT,StateT>` | `source/SeaTunnelSource.java` | 数据源读取接口，工厂模式创建Reader+Enumerator |
| `SeaTunnelSink<IN,StateT,CommitInfoT,AggregatedCommitInfoT>` | `sink/SeaTunnelSink.java` | 数据写入接口，工厂模式创建Writer+Committer |
| `SeaTunnelTransform<T>` | `transform/SeaTunnelTransform.java` | 数据转换接口 |
| `TableSourceFactory` | `table/factory/TableSourceFactory.java` | SPI工厂接口，创建TableSource |
| `TableSinkFactory` | `table/factory/TableSinkFactory.java` | SPI工厂接口，创建TableSink |
| `TableTransformFactory` | `table/factory/TableTransformFactory.java` | SPI工厂接口，创建TableTransform |
| `Factory` | `table/factory/Factory.java` | 所有Factory的基接口 (factoryIdentifier + optionRule) |
| `CatalogTable` | `table/catalog/CatalogTable.java` | 表元数据模型 (含RowType、TablePath、Schema) |
| `SeaTunnelRow` | `table/type/SeaTunnelRow.java` | 统一行数据模型 |

**设计模式**:
- **工厂模式**: Factory接口体系 (TableSourceFactory/TableSinkFactory/TableTransformFactory)
- **SPI机制**: 通过 `ServiceLoader<Factory>` 发现插件
- **模板方法**: SeaTunnelSource/SeaTunnelSink 的 createReader/createWriter 模式
- **策略模式**: Boundedness (BOUNDED/UNBOUNDED) 区分批/流

**调用边界**: 只定义接口，不包含执行逻辑。引擎层实现这些接口，Connector层提供具体实现。

---

## 第二层：核心调度与生命周期管理层 (seatunnel-core)

**路径**: `seatunnel-core/`

**子模块**:
| 子模块 | 路径 | 职责 |
|--------|------|------|
| core-starter | `seatunnel-core/seatunnel-core-starter/` | 通用启动框架、Command模式、ConfigBuilder |
| starter | `seatunnel-core/seatunnel-starter/` | Zeta引擎启动器 |
| flink-starter | `seatunnel-core/seatunnel-flink-starter/` | Flink引擎启动器 |
| spark-starter | `seatunnel-core/seatunnel-spark-starter/` | Spark引擎启动器 |

**核心类**:
- `SeaTunnel.java` (`core-starter`) — 统一入口，执行 Command
- `Command<T>` 接口 — 命令抽象，实现 execute()
- `ConfigBuilder.java` — HOCON 配置文件解析
- `PluginExecuteProcessor` — 插件执行处理器 (抽象)
- `RuntimeEnvironment` — 运行时环境管理

**设计模式**:
- **命令模式**: Command 接口，不同引擎不同操作
- **模板方法**: AbstractCommandArgs 定义参数解析模板

**调用边界**: 向上接收CLI参数，向下调用引擎适配层

---

## 第三层：多引擎适配层 (Translation + Engine-Specific Starter)

### 3a. Translation 翻译层

**路径**: `seatunnel-translation/`

**职责**: 将 SeaTunnel 统一 API 适配为各引擎原生 API

| 子模块 | 适配方向 |
|--------|----------|
| `translation-base` | 通用抽象: ParallelSource/CoordinatedSource/BaseSourceFunction |
| `translation-flink` | SeaTunnelSource → Flink Source API (FlinkSource/FlinkSourceReader) |
| | SeaTunnelSink → Flink Sink API (FlinkSink/FlinkSinkWriter) |
| `translation-spark` | SeaTunnelSource → Spark DataSource V2 (SeaTunnelScan/SeaTunnelBatch) |
| | SeaTunnelSink → Spark Write API (SeaTunnelBatchWrite/SeaTunnelWrite) |

### 3b. 引擎专用 Starter

**Flink Starter** (`seatunnel-core/seatunnel-flink-starter/`):
- `FlinkExecution.java` — 基于 StreamExecutionEnvironment 构建 Flink DAG
- `SourceExecuteProcessor` — 创建 FlinkSource 包装 SeaTunnelSource
- `SinkExecuteProcessor` — 创建 FlinkSink 包装 SeaTunnelSink
- `TransformExecuteProcessor` — Transform 算子链

**Spark Starter** (`seatunnel-core/seatunnel-spark-starter/`):
- `SparkExecution.java` — 基于 SparkSession 构建 Spark DAG
- `SourceExecuteProcessor` — 创建 Spark Source
- `SinkExecuteProcessor` — 创建 Spark Sink

**调用边界**: 向上接收 Source/Sink/Transform 实例，向下调用引擎原生 API

---

## 第四层：Zeta 原生引擎层 (seatunnel-engine)

**路径**: `seatunnel-engine/`

| 子模块 | 职责 | 核心类 |
|--------|------|--------|
| `engine-core` | DAG模型、Checkpoint协议、Job协议 | `Action`, `LogicalDag`, `Job`, `MultipleTableJobConfigParser` |
| `engine-server` | Master调度、Worker执行、容错 | `CoordinatorService`, `TaskGroupRunner`, `CheckpointCoordinator` |
| `engine-client` | 客户端SDK | `SeaTunnelClient`, `ClientJobProxy`, `JobClient` |
| `engine-common` | 引擎公共配置 | `SeaTunnelConfig`, `EngineConfig` |
| `engine-storage` | 状态持久化 | (IMap-based) |

**核心设计**:
- **DAG**: Action(SourceAction→TransformAction→SinkAction) → LogicalDag → PhysicalPlan → TaskGroup
- **调度**: Master 节点 CoordinatorService 管理 Job 生命周期
- **容错**: CheckpointCoordinator 协调分布式 checkpoint (基于 Hazelcast Operation)
- **执行**: Worker 节点 TaskGroupRunner 在单线程内串行执行 Source→Transform→Sink

---

## 第五层：Connector 插件层

**路径**: `seatunnel-connectors-v2/`

**职责**: 实现具体数据源的读写逻辑

**架构模式** (以 JDBC Connector 为例):
```
connector-jdbc/
├── JdbcSourceFactory (implements TableSourceFactory)
│   └── createSource() → JdbcSource (implements SeaTunnelSource)
│       ├── JdbcSourceSplitEnumerator (分片枚举)
│       └── JdbcSourceReader (数据读取)
├── JdbcSinkFactory (implements TableSinkFactory)
│   └── createSink() → JdbcSink (implements SeaTunnelSink)
│       ├── JdbcSinkWriter (数据写入)
│       └── JdbcSinkCommitter (事务提交)
└── JdbcCatalogFactory (implements CatalogFactory)
    └── JdbcCatalog (元数据管理)
```

---

## 第六层：Transform 算子层

**路径**: `seatunnel-transforms-v2/src/main/java/`

**职责**: 实现数据转换逻辑

**内置算子**:
| 算子 | 类 | 能力 |
|------|-----|------|
| SQL | `SQLTransform` | SQL 表达式转换 (ZetaSQL引擎) |
| Copy | `CopyFieldTransform` | 字段复制 |
| Replace | `ReplaceTransform` | 正则替换 |
| Filter | `FilterFieldTransform` | 字段过滤 |
| FilterRowKind | `FilterRowKindTransform` | 按行类型过滤 |
| Split | `SplitTransform` | 字段拆分 |
| FieldMapper | `FieldMapperTransform` | 字段映射/重命名 |
| RegexExtract | `RegexExtractTransform` | 正则提取 |
| JsonPath | `JsonPathTransform` | JSON路径提取 |
| Metadata | `MetadataTransform` | 元数据注入 |
| DynamicCompile | `DynamicCompileTransform` | 动态编译(Groovy/Java/Scala) |
| LLM | `LLMTransform` | 大语言模型增强 |
| Embedding | `EmbeddingTransform` | 向量嵌入 |
| RowKind | `RowKindExtractorTransform` | 行类型提取 |
| Rename | `FieldRenameTransform`/`TableRenameTransform` | 字段/表重命名 |
| TableFilter | `TableFilterTransform` | 表过滤 |
| TableMerge | `TableMergeTransform` | 表合并 |
| Validator | `DataValidatorTransform` | 数据校验 |

---

## 第七层：配置解析层

**路径**: `seatunnel-core/seatunnel-core-starter/src/main/java/.../utils/ConfigBuilder.java`

**职责**: 解析 HOCON 格式配置文件为 Typesafe Config 对象

**流程**:
1. `ConfigBuilder.of(filePath)` → 读取文件
2. `ConfigAdapter` SPI → 支持自定义配置格式
3. 变量替换 → `${variable}` 语法
4. `ConfigShadeUtils.decryptConfig()` → 敏感信息解密
5. 输出 `Config` 对象 → 传给 JobConfigParser

---

## 层间调用边界

```
CLI参数 → [ConfigBuilder] → Config对象
                                    ↓
              [MultipleTableJobConfigParser] → Action DAG
                                                    ↓
              [PluginDiscovery] → 加载 Connector/Transform 实例
                                                    ↓
              [LogicalDagGenerator] → LogicalDag → PhysicalPlan
                                                    ↓
         ┌──────────────────────────────────────────────┐
         │ Zeta: CoordinatorService 调度 TaskGroup      │
         │ Flink: FlinkExecution → StreamGraph          │
         │ Spark: SparkExecution → SparkSession DAG     │
         └──────────────────────────────────────────────┘
```
