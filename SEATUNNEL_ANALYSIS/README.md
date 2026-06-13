# Apache SeaTunnel 2.3.13 源码全景分析 - 目录索引

> 基于 Apache SeaTunnel 2.3.13 官方版本源码，分支: 2.3.13-release
> 分析日期: 2026-06-13

---

## 文档目录

| 编号 | 文档 | 内容概要 |
|------|------|----------|
| 01 | [项目全景与基础认知](./01_project_overview.md) | 定位、技术栈、目录结构、引擎差异 |
| 02 | [整体分层架构](./02_architecture_layers.md) | API/Core/Engine/Connector/Transform/Config 分层拆解 |
| 03 | [全模块拆解手册](./03_module_catalog.md) | 所有核心模块路径、职责、入口类导航表 |
| 04 | [数据迁移核心主流程](./04_execution_pipeline.md) | 任务提交到完成的完整链路，含三引擎差异 |
| 05 | [核心扩展机制](./05_extension_mechanism.md) | Connector/Transform/Engine/SPI 扩展体系 |
| 06 | [代码优化与整改方向](./06_optimization_recommendations.md) | 性能、耦合、扩展性分析与整改建议 |
| 07 | [Zeta 引擎深度分析](./07_zeta_engine_deep_dive.md) | Master/Worker架构、Checkpoint、网络通信 |
| 08 | [JDBC Connector 深度分析](./08_connector_jdbc.md) | Source/Sink全链路、Dialect方言、分片策略、XA事务 |
| 08 | [Kafka Connector 深度分析](./08_connector_kafka.md) | Consumer/Producer、Offset管理、事务、消息格式 |
| 08 | [File Connector 深度分析](./08_connector_file.md) | 多文件系统、Split策略、格式适配、压缩支持 |
| 08 | [OLAP/搜索 Connector 分析](./08_connector_sink_analytics.md) | ES/ClickHouse/StarRocks/Doris 写入机制对比 |
| 09 | [CDC 同步机制深度分析](./09_cdc_mechanism.md) | 增量快照、Binlog/WAL解析、Schema变更传播 |
| 10 | [配置解析与 DAG 编排](./10_config_and_dag.md) | HOCON解析、多表合并、物理计划、三引擎适配 |

---

## 快速导航

### 按功能找代码
- **任务提交入口**: `seatunnel-core/seatunnel-starter/.../SeaTunnelClient.java`
- **Zeta引擎核心**: `seatunnel-engine/seatunnel-engine-server/`
- **连接器目录**: `seatunnel-connectors-v2/`
- **Transform算子**: `seatunnel-transforms-v2/src/main/java/`
- **配置解析**: `seatunnel-core/seatunnel-core-starter/.../ConfigBuilder.java`
- **插件加载**: `seatunnel-plugin-discovery/`

### Connector 快速索引
- **JDBC (30+ DB)**: `connector-jdbc/` → [详细分析](./08_connector_jdbc.md)
- **Kafka**: `connector-kafka/` → [详细分析](./08_connector_kafka.md)
- **File (S3/OSS/HDFS/本地)**: `connector-file/` → [详细分析](./08_connector_file.md)
- **Elasticsearch**: `connector-elasticsearch/` → [详细分析](./08_connector_sink_analytics.md)
- **ClickHouse**: `connector-clickhouse/` → [详细分析](./08_connector_sink_analytics.md)
- **StarRocks**: `connector-starrocks/` → [详细分析](./08_connector_sink_analytics.md)
- **Doris**: `connector-doris/` → [详细分析](./08_connector_sink_analytics.md)
- **CDC (MySQL/PG/Oracle/SQL Server/MongoDB)**: `connector-cdc/` → [详细分析](./09_cdc_mechanism.md)

### 按代码懂功能
- `SeaTunnelSource.java` → 数据源读取接口 (Split/Reader/Enumerator 模式)
- `SeaTunnelSink.java` → 数据写入接口 (Writer/Committer 模式)
- `SeaTunnelTransform.java` → 数据转换接口
- `AbstractPluginDiscovery.java` → SPI插件发现机制
- `ConfigBuilder.java` → HOCON配置文件解析
- `MultipleTableJobConfigParser.java` → 多表任务配置解析为DAG
- `JdbcDialect.java` → JDBC方言抽象层，30+数据库统一接口
- `IncrementalSource.java` → CDC增量数据源基类
- `JdbcExactlyOnceSinkWriter.java` → XA两阶段提交实现
