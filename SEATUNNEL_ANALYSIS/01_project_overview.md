# 一、项目全景与基础认知

## 1. 核心定位

Apache SeaTunnel 是一款**分布式异构数据集成引擎**，核心能力：
- 多源数据同步（批/流）
- ETL 数据转换处理
- 支持 Zeta/Spark/Flink 三种运行引擎
- 插件化架构，70+ 内置连接器

## 2. 技术栈

| 维度 | 版本/技术 |
|------|-----------|
| **Java** | 1.8 |
| **Scala** | 2.12.15 (binary: 2.12) |
| **构建工具** | Maven |
| **分布式协调** | Hazelcast (Zeta引擎核心) |
| **配置格式** | HOCON (Typesafe Config, shaded) |
| **序列化** | Jackson 2.13.3 |
| **日志** | SLF4J 1.7.36 + Log4j2 |
| **工具库** | Guava 27.0-jre (shaded) |
| **RPC框架** | Hazelcast IMDG (shaded) |
| **SQL引擎** | ZetaSQL (自研，基于Calcite思想) |

## 3. 精简项目根目录结构

```
seatunnel/
├── seatunnel-api/                    # [核心] API抽象层 - Source/Sink/Transform/Factory接口
├── seatunnel-common/                 # [核心] 公共工具类、异常体系、常量定义
├── seatunnel-config/                 # [核心] 配置体系 (seatunnel.yaml等)
├── seatunnel-core/                   # [核心] 执行内核
│   ├── seatunnel-core-starter/       #   通用启动框架 (SeaTunnel入口、Command模式)
│   ├── seatunnel-starter/            #   Zeta引擎启动器 (SeaTunnelClient/Server)
│   ├── seatunnel-flink-starter/      #   Flink引擎启动器
│   └── seatunnel-spark-starter/      #   Spark引擎启动器
├── seatunnel-engine/                 # [核心] Zeta原生引擎
│   ├── seatunnel-engine-core/        #   DAG模型、Checkpoint、Job协议
│   ├── seatunnel-engine-server/      #   Master/Worker执行、调度、容错
│   ├── seatunnel-engine-client/      #   客户端SDK
│   ├── seatunnel-engine-common/      #   引擎公共配置
│   ├── seatunnel-engine-serializer/  #   序列化
│   ├── seatunnel-engine-storage/     #   状态存储
│   └── seatunnel-engine-ui/          #   Web UI
├── seatunnel-connectors-v2/          # [插件] 70+ 连接器
├── seatunnel-transforms-v2/          # [插件] Transform算子 (SQL/Filter/Copy等)
├── seatunnel-translation/            # [适配] 引擎翻译层
│   ├── seatunnel-translation-base/   #   通用翻译抽象
│   ├── seatunnel-translation-flink/  #   Flink适配 (FlinkSource/Sink包装)
│   └── seatunnel-translation-spark/  #   Spark适配 (SparkSource/Sink包装)
├── seatunnel-plugin-discovery/       # [机制] SPI插件发现与加载
├── seatunnel-formats/                # [格式] 数据格式 (JSON/CSV/Avro/Protobuf/Text)
├── seatunnel-shade/                  # [工具] 依赖shade (避免版本冲突)
├── seatunnel-e2e/                    # [测试] 端到端集成测试
├── seatunnel-dist/                   # [发布] 发行包组装
├── seatunnel-examples/               # [示例] 运行示例
├── config/                           # [配置] 运行时配置文件
│   ├── hazelcast.yaml                #   Hazelcast集群配置
│   ├── seatunnel.yaml                #   SeaTunnel引擎配置
│   ├── plugin_config                 #   插件映射配置
│   └── v2.batch.config.template      #   批处理配置模板
├── bin/                              # [工具] 安装脚本
├── deploy/                           # [部署] 部署相关
└── tools/                            # [工具] CI/构建工具
```

## 4. 三引擎定位差异

| 维度 | Zeta (原生引擎) | Spark | Flink |
|------|-----------------|-------|-------|
| **定位** | SeaTunnel自研分布式执行引擎 | 对接Spark生态 | 对接Flink生态 |
| **底层** | Hazelcast IMDG | Spark DataSource V2 API | Flink Source/Sink API |
| **分布式** | Hazelcast集群自管理 | Spark Cluster/YARN/K8s | Flink Cluster/YARN/K8s |
| **Checkpoint** | 自研 (基于Hazelcast IMap) | Spark Checkpoint | Flink Checkpoint |
| **适用场景** | 轻量级独立部署、低延迟 | 大规模离线处理 | 流批一体、复杂CEP |
| **启动入口** | `SeaTunnelClient.main()` | `SeaTunnelSpark.main()` | `SeaTunnelFlink.main()` |
| **配置传递** | 直传SeaTunnelConfig | 转为SparkSession参数 | 转为StreamExecutionEnvironment |

## 5. 运行架构 (Zeta引擎)

```
                    ┌─────────────────┐
                    │  SeaTunnelClient│  (CLI/API 提交任务)
                    └────────┬────────┘
                             │ Hazelcast Client
                    ┌────────▼────────┐
                    │   Master Node   │  (CoordinatorService: Job调度)
                    │  JobMaster      │  (CheckpointCoordinator)
                    └────────┬────────┘
                             │ Hazelcast Operation
              ┌──────────────┼──────────────┐
     ┌────────▼──────┐              ┌───────▼────────┐
     │  Worker Node  │              │  Worker Node   │
     │ TaskGroupRunner│              │ TaskGroupRunner│
     │ (Source→Transform→Sink)      │ (Source→Transform→Sink)
     └───────────────┘              └────────────────┘
```
