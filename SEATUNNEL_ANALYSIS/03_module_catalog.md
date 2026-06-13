# 三、全模块拆解手册 (AI代码导航核心)

## 核心框架模块

| 模块名称 | 完整源码路径 | 核心职责 | 入口类/核心接口 | 对外暴露能力 | 依赖的核心模块 |
|----------|-------------|----------|----------------|-------------|---------------|
| **seatunnel-api** | `seatunnel-api/` | API抽象定义：Source/Sink/Transform/Factory/Type/Catalog | `SeaTunnelSource`, `SeaTunnelSink`, `SeaTunnelTransform`, `Factory`, `TableSourceFactory`, `TableSinkFactory` | 插件开发接口、数据模型、配置Option体系 | 无 |
| **seatunnel-common** | `seatunnel-common/` | 公共工具类、异常体系、常量、配置校验 | `Common`, `CheckConfigUtil`, `SeaTunnelRuntimeException`, `Constants`, `EngineType`, `JobMode` | 异常码、工具方法、部署模式枚举 | 无 |
| **seatunnel-config** | `seatunnel-config/` | 引擎运行时配置 (seatunnel.yaml) | `SeaTunnelConfig`, `EngineConfig`, `ConfigProvider` | 引擎配置对象 | seatunnel-common |
| **seatunnel-core-starter** | `seatunnel-core/seatunnel-core-starter/` | 通用启动框架、Command模式、ConfigBuilder | `SeaTunnel.run()`, `Command`, `ConfigBuilder`, `RuntimeEnvironment`, `PluginExecuteProcessor` | 配置解析、命令执行框架 | seatunnel-api, seatunnel-common |
| **seatunnel-starter** | `seatunnel-core/seatunnel-starter/` | Zeta引擎CLI启动器 | `SeaTunnelClient.main()`, `SeaTunnelServer.main()`, `ClientExecuteCommand`, `ServerExecuteCommand` | Zeta引擎任务提交、服务启动 | seatunnel-core-starter, seatunnel-engine-client, seatunnel-engine-server |
| **seatunnel-plugin-discovery** | `seatunnel-plugin-discovery/` | SPI插件发现、JAR加载、ClassLoader管理 | `AbstractPluginDiscovery`, `SeaTunnelSourcePluginDiscovery`, `SeaTunnelSinkPluginDiscovery` | 插件实例化、JAR路径解析 | seatunnel-api, seatunnel-common |
| **seatunnel-formats** | `seatunnel-formats/` | 数据序列化/反序列化格式 | `JsonSerializationSchema`, `CsvSerializationSchema`, `AvroSerializationSchema`, `ProtobufSerializationSchema`, `TextSerializationSchema` | JSON/CSV/Avro/Protobuf/Text格式支持 | seatunnel-api |

---

## Zeta 引擎模块

| 模块名称 | 完整源码路径 | 核心职责 | 入口类/核心接口 | 对外暴露能力 | 依赖的核心模块 |
|----------|-------------|----------|----------------|-------------|---------------|
| **engine-core** | `seatunnel-engine/seatunnel-engine-core/` | DAG模型定义、Job/Pipeline协议、Checkpoint协议、配置解析 | `Action`, `SourceAction`, `SinkAction`, `TransformAction`, `LogicalDag`, `Job`, `MultipleTableJobConfigParser`, `Checkpoint` | DAG构建、Job提交协议、Checkpoint协议 | seatunnel-api, seatunnel-common |
| **engine-server** | `seatunnel-engine/seatunnel-engine-server/` | Master调度、Worker执行、Checkpoint协调、资源管理 | `SeaTunnelServer`, `CoordinatorService`, `TaskGroupRunner`, `CheckpointCoordinator`, `PhysicalPlan`, `SubPlan` | 集群管理、任务调度、容错恢复 | engine-core, engine-common |
| **engine-client** | `seatunnel-engine/seatunnel-engine-client/` | 客户端SDK、任务提交、状态查询 | `SeaTunnelClient`, `ClientJobExecutionEnvironment`, `ClientJobProxy`, `JobClient` | 任务提交API、状态查询API | engine-core, engine-common |
| **engine-common** | `seatunnel-engine/seatunnel-engine-common/` | 引擎公共配置、常量、运行时模式 | `SeaTunnelConfig`, `EngineConfig`, `ExecutionMode`, `Constant` | 配置解析、运行模式定义 | seatunnel-common |
| **engine-serializer** | `seatunnel-engine/seatunnel-engine-serializer/` | 引擎内部数据序列化 | (序列化框架) | 高效序列化 | engine-core |
| **engine-storage** | `seatunnel-engine/seatunnel-engine-storage/` | 状态持久化存储 | (IMap-based存储) | Checkpoint状态持久化 | engine-core |

---

## Flink 引擎适配模块

| 模块名称 | 完整源码路径 | 核心职责 | 入口类/核心接口 | 对外暴露能力 | 依赖的核心模块 |
|----------|-------------|----------|----------------|-------------|---------------|
| **flink-starter-common** | `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-starter-common/` | Flink启动框架、执行环境 | `FlinkExecution`, `AbstractFlinkRuntimeEnvironment`, `SourceExecuteProcessor`, `SinkExecuteProcessor`, `TransformExecuteProcessor` | Flink任务编排框架 | seatunnel-core-starter, translation-flink |
| **flink-13-starter** | `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-13-starter/` | Flink 1.13 适配 | `SeaTunnelFlink`, `FlinkStarter` | Flink 1.13 支持 | flink-starter-common |
| **flink-20-starter** | `seatunnel-core/seatunnel-flink-starter/seatunnel-flink-20-starter/` | Flink 1.20 适配 | `SeaTunnelFlink`, `FlinkStarter` | Flink 1.20 支持 | flink-starter-common |
| **translation-flink-common** | `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-common/` | Flink通用翻译层 | `FlinkSource`, `FlinkSourceReader`, `FlinkSourceEnumerator`, `FlinkSink`, `FlinkSinkWriter`, `FlinkGlobalCommitter` | SeaTunnel API → Flink API 翻译 | translation-base |
| **translation-flink-13** | `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-13/` | Flink 1.13 特殊适配 | `FlinkMetricContext` | Flink 1.13 Metric适配 | translation-flink-common |
| **translation-flink-20** | `seatunnel-translation/seatunnel-translation-flink/seatunnel-translation-flink-20/` | Flink 1.20 特殊适配 | `FlinkSink`, `FlinkCommitter` | Flink 1.20 Sink适配 | translation-flink-common |

---

## Spark 引擎适配模块

| 模块名称 | 完整源码路径 | 核心职责 | 入口类/核心接口 | 对外暴露能力 | 依赖的核心模块 |
|----------|-------------|----------|----------------|-------------|---------------|
| **spark-starter-common** | `seatunnel-core/seatunnel-spark-starter/seatunnel-spark-starter-common/` | Spark启动框架、执行环境 | `SparkExecution`, `SparkRuntimeEnvironment`, `SourceExecuteProcessor`, `SinkExecuteProcessor`, `TransformExecuteProcessor` | Spark任务编排框架 | seatunnel-core-starter, translation-spark |
| **spark-2-starter** | `seatunnel-core/seatunnel-spark-starter/seatunnel-spark-2-starter/` | Spark 2.4 适配 | `SeaTunnelSpark`, `SparkStarter` | Spark 2.x 支持 | spark-starter-common |
| **translation-spark-common** | `seatunnel-translation/seatunnel-translation-spark/seatunnel-translation-spark-common/` | Spark通用翻译层 | `InternalRowConverter`, `SeaTunnelRowConverter`, `MultiTableManager` | 数据类型转换、Row序列化 | translation-base |
| **translation-spark-2.4** | `seatunnel-translation/seatunnel-translation-spark/seatunnel-translation-spark-2.4/` | Spark 2.4 适配 | `SeaTunnelSourceSupport`, `SparkSink`, `BatchSourceReader`, `SparkDataWriter` | SeaTunnel API → Spark DataSource V2 | translation-spark-common |
| **translation-spark-3.3** | `seatunnel-translation/seatunnel-translation-spark/seatunnel-translation-spark-3.3/` | Spark 3.3 适配 | `SeaTunnelSparkSource`, `SeaTunnelSparkSink`, `SeaTunnelBatch`, `SeaTunnelBatchWrite` | Spark 3.x DataSource V2 适配 | translation-spark-common |

---

## Translation 基础抽象层

| 模块名称 | 完整源码路径 | 核心职责 | 入口类/核心接口 | 对外暴露能力 | 依赖的核心模块 |
|----------|-------------|----------|----------------|-------------|---------------|
| **translation-base** | `seatunnel-translation/seatunnel-translation-base/` | 引擎无关的通用执行抽象 | `ParallelSource`, `CoordinatedSource`, `BaseSourceFunction`, `SinkConverter`, `SinkWriterConverter`, `RowConverter` | 统一的Source执行模型 (Parallel/Coordinated两种模式) | seatunnel-api |

**关键设计**:
- `ParallelSource`: 每个Reader独立运行，无需Enumerator协调 (适用于可并行的批处理)
- `CoordinatedSource`: Enumerator统一分配Split给Reader (适用于需要协调的场景)

---

## Connector 插件模块 (seatunnel-connectors-v2)

**父路径**: `seatunnel-connectors-v2/`

### 全部 Connector 列表 (70+)

| 类别 | 连接器 | 路径 |
|------|--------|------|
| **数据库** | JDBC | `connector-jdbc/` |
| | ClickHouse | `connector-clickhouse/` |
| | Doris | `connector-doris/` |
| | StarRocks | `connector-starrocks/` |
| | SelectDB Cloud | `connector-selectdb-cloud/` |
| | Elasticsearch | `connector-elasticsearch/` |
| | MongoDB | `connector-mongodb/` |
| | Redis | `connector-redis/` |
| | Neo4j | `connector-neo4j/` |
| | InfluxDB | `connector-influxdb/` |
| | IoTDB / IoTDB-v2 | `connector-iotdb/`, `connector-iotdb-v2/` |
| | TDengine | `connector-tdengine/` |
| | HBase | `connector-hbase/` |
| | Kudu | `connector-kudu/` |
| | Cassandra | `connector-cassandra/` |
| | OpenMLDB | `connector-openmldb/` |
| | Druid | `connector-druid/` |
| | Easysearch | `connector-easysearch/` |
| | Tablestore | `connector-tablestore/` |
| | Databend | `connector-databend/` |
| | Milvus | `connector-milvus/` |
| | Qdrant | `connector-qdrant/` |
| | HugeGraph | `connector-hugegraph/` |
| | Lance | `connector-lance/` |
| **消息队列** | Kafka | `connector-kafka/` |
| | Pulsar | `connector-pulsar/` |
| | RocketMQ | `connector-rocketmq/` |
| | RabbitMQ | `connector-rabbitmq/` |
| | ActiveMQ | `connector-activemq/` |
| | Amazon SQS | `connector-amazonsqs/` |
| **CDC** | CDC (MySQL/PostgreSQL/Oracle/SQL Server/MongoDB/TiDB/OpenGauss) | `connector-cdc/` |
| **数据湖** | Iceberg | `connector-iceberg/` |
| | Hudi | `connector-hudi/` |
| | Paimon | `connector-paimon/` |
| **文件系统** | Local File | `connector-file/` (含local/hadoop/s3/oss/ftp/sftp/obs/jindo) |
| **云服务** | Amazon DynamoDB | `connector-amazondynamodb/` |
| | Google Sheets | `connector-google-sheets/` |
| | Google Firestore | `connector-google-firestore/` |
| | MaxCompute | `connector-maxcompute/` |
| | S3 Redshift | `connector-s3-redshift/` |
| | SLS (阿里云日志) | `connector-sls/` |
| **HTTP** | HTTP Base / 飞书 / GitLab / GitHub / Jira 等 | `connector-http/` |
| **通讯** | DingTalk | `connector-dingtalk/` |
| | Slack | `connector-slack/` |
| | Email | `connector-email/` |
| | Socket | `connector-socket/` |
| **其他** | Console | `connector-console/` |
| | Fake | `connector-fake/` |
| | Assert | `connector-assert/` |
| | Datahub | `connector-datahub/` |
| | GraphQL | `connector-graphql/` |
| | Sentry | `connector-sentry/` |
| | Web3j | `connector-web3j/` |
| | Prometheus | `connector-prometheus/` |
| | SensorsData | `connector-sensorsdata/` |
| | Typesense | `connector-typesense/` |
| | Hive | `connector-hive/` |
| | Fluss | `connector-fluss/` |
| | Aerospike | `connector-aerospike/` |
| **公共** | connector-common | `connector-common/` |

---

## Transform 算子模块

**路径**: `seatunnel-transforms-v2/src/main/java/org/apache/seatunnel/transform/`

| 算子包 | 核心类 | Factory类 | 能力 |
|--------|--------|-----------|------|
| `sql/` | `SQLTransform` | `SQLTransformFactory` | SQL表达式转换，ZetaSQL引擎 |
| `copy/` | `CopyFieldTransform` | `CopyFieldTransformFactory` | 字段复制 |
| `replace/` | `ReplaceTransform` | `ReplaceTransformFactory` | 正则字符串替换 |
| `filter/` | `FilterFieldTransform` | `FilterFieldTransformFactory` | 字段选择/过滤 |
| `filterrowkind/` | `FilterRowKindTransform` | `FilterRowKindTransformFactory` | 按RowKind过滤 |
| `split/` | `SplitTransform` | `SplitTransformFactory` | 字段拆分 |
| `fieldmapper/` | `FieldMapperTransform` | `FieldMapperTransformFactory` | 字段映射/重命名 |
| `regexextract/` | `RegexExtractTransform` | `RegexExtractTransformFactory` | 正则提取 |
| `jsonpath/` | `JsonPathTransform` | `JsonPathTransformFactory` | JSONPath提取 |
| `metadata/` | `MetadataTransform` | `MetadataTransformFactory` | 元数据字段注入 |
| `dynamiccompile/` | `DynamicCompileTransform` | `DynamicCompileTransformFactory` | Groovy/Java/Scala动态编译 |
| `nlpmodel/llm/` | `LLMTransform` | `LLMTransformFactory` | LLM大模型处理 |
| `nlpmodel/embedding/` | `EmbeddingTransform` | `EmbeddingTransformFactory` | 向量嵌入 |
| `rowkind/` | `RowKindExtractorTransform` | `RowKindExtractorTransformFactory` | RowKind提取为字段 |
| `rename/` | `FieldRenameTransform`/`TableRenameTransform` | `FieldRenameTransformFactory`/`TableRenameTransformFactory` | 字段/表重命名 |
| `table/` | `TableFilterTransform`/`TableMergeTransform` | `TableFilterTransformFactory`/`TableMergeTransformFactory` | 表级过滤/合并 |
| `validator/` | `DataValidatorTransform` | `DataValidatorTransformFactory` | 数据质量校验 |
| `adaptsink/` | `DefineSinkTypeTransform` | `DefineSinkTypeTransformFactory` | Sink类型适配 |
| `common/` | `AbstractSeaTunnelTransform`, `AbstractCatalogSupportMapTransform` | — | 公共基类 |

---

## 辅助模块

| 模块名称 | 完整源码路径 | 核心职责 | 说明 |
|----------|-------------|----------|------|
| **seatunnel-shade** | `seatunnel-shade/` | 依赖shade避免冲突 | hazelcast/jackson/guava/hikari/jetty等 |
| **seatunnel-dist** | `seatunnel-dist/` | 发行包组装 | Maven assembly 插件打包 |
| **seatunnel-e2e** | `seatunnel-e2e/` | E2E集成测试 | Testcontainers + Docker |
| **seatunnel-examples** | `seatunnel-examples/` | 运行示例 | LocalExample模式 |
| **seatunnel-ci-tools** | `seatunnel-ci-tools/` | CI工具 | 代码检查 |
