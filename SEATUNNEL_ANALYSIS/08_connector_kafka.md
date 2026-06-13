# Apache SeaTunnel Kafka Connector 深度源码分析

> 代码位置：`seatunnel-connectors-v2/connector-kafka/`
> 分析版本：基于 2.3.13-release 分支

---

## 目录

1. [总体架构概览](#1-总体架构概览)
2. [核心类路径速查表](#2-核心类路径速查表)
3. [KafkaSource 详细分析](#3-kafkasource-详细分析)
4. [KafkaSink 详细分析](#4-kafkasink-详细分析)
5. [Consumer 消费与 Offset 管理机制](#5-consumer-消费与-offset-管理机制)
6. [Producer 发送与事务](#6-producer-发送与事务)
7. [消息格式化策略](#7-消息格式化策略)
8. [分区策略](#8-分区策略)
9. [配置类体系](#9-配置类体系)
10. [总结与设计模式](#10-总结与设计模式)

---

## 1. 总体架构概览

### 1.1 Kafka Connector 全链路处理流程

```
┌──────────────────────────────────────────────────┐
│                  KafkaSource                      │
│  implements SeaTunnelSource, SupportParallelism  │
│  Boundedness: BATCH→BOUNDED, STREAMING→UNBOUNDED │
└─────────────┬────────────────────────────────────┘
              │
    ┌─────────┴──────────┐
    │                    │
    ▼                    ▼
┌──────────────────────┐  ┌──────────────────────────────┐
│ KafkaSourceSplit     │  │   KafkaSourceSplitEnumerator  │
│ Enumerator (协调器)    │  │   使用 AdminClient 发现分区    │
│ 分区发现 + 分配        │  │   支持动态分区发现             │
│ StartMode 管理        │  │   支持 5 种启动模式            │
└───────┬──────────────┘  └──────────────┬───────────────┘
        │                               │
        ▼                               │
┌──────────────────────┐                │
│  KafkaSourceReader   │◄───────────────┘
│  继承 SingleThread-  │
│  MultiplexSource-    │
│  ReaderBase          │
└──────────┬───────────┘
           │
    ┌──────┴──────┐
    │             │
    ▼             ▼
┌──────────────────────┐  ┌──────────────────────────────┐
│ KafkaSourceFetcher   │  │     KafkaRecordEmitter        │
│ Manager              │  │  ConsumerRecord → SeaTunnelRow│
│ 管理 Fetch 任务       │  │  反序列化 + 容错跳过           │
└──────────┬───────────┘  └──────────────────────────────┘
           │
           ▼
┌──────────────────────────────────────────┐
│       KafkaPartitionSplitReader          │
│  原生 KafkaConsumer poll 数据            │
│  ByteArrayDeserializer (原始字节)        │
│  停止 Offset 管理 (BOUNDED 模式)          │
└──────────────────────────────────────────┘

┌──────────────────────────────────────────────────┐
│                   KafkaSink                       │
│  implements SeaTunnelSink<KafkaSinkState,         │
│            KafkaCommitInfo, KafkaAggregated...>   │
└─────────────┬────────────────────────────────────┘
              │
    ┌─────────┴──────────┐
    │                    │
    ▼                    ▼
┌──────────────────┐  ┌─────────────────────┐
│  KafkaSinkWriter  │  │  KafkaSinkCommitter  │
│  序列化 + 发送    │  │  事务提交 / 回滚      │
│  SeaTunnelRow →   │  │  KafkaInternalProducer│
│  ProducerRecord   │  │  resumeTransaction()  │
└────────┬─────────┘  └─────────────────────┘
         │
    ┌────┴─────────────────────┐
    │                          │
    ▼                          ▼
┌──────────────────┐  ┌──────────────────────────┐
│ KafkaTransaction │  │  KafkaNoTransactionSender │
│ Sender (EXACTLY_ │  │  (NON / AT_LEAST_ONCE)    │
│ ONCE)            │  │  普通 KafkaProducer 发送   │
│ 2PC 事务提交      │  │  flush on snapshot        │
└────────┬─────────┘  └──────────────────────────┘
         │
         ▼
┌──────────────────────────────────────────┐
│     DefaultSeaTunnelRowSerializer        │
│  SeaTunnelRow → ProducerRecord           │
│  topic/partition/key/value/headers       │
│  JSON / TEXT / Avro / Protobuf / Canal...│
└──────────────────────────────────────────┘
```

---

## 2. 核心类路径速查表

| 类名 | 路径 | 职责 |
|------|------|------|
| `KafkaSource` | `source/KafkaSource.java` | Source 入口，支持并行读取，动态切换 Boundedness |
| `KafkaSourceConfig` | `source/KafkaSourceConfig.java` | 配置解析，创建 DeserializationSchema 和 CatalogTable |
| `KafkaSourceSplit` | `source/KafkaSourceSplit.java` | 数据分片，封装 TopicPartition + startOffset + endOffset |
| `KafkaSourceSplitEnumerator` | `source/KafkaSourceSplitEnumerator.java` | 分区发现、StartMode 管理、Split 分配 |
| `KafkaSourceReader` | `source/KafkaSourceReader.java` | 单线程多路复用读取器，管理 Offset 提交 |
| `KafkaSourceSplitState` | `source/KafkaSourceSplitState.java` | 运行时 Split 状态，跟踪 currentOffset |
| `KafkaPartitionSplitReader` | `source/KafkaPartitionSplitReader.java` | 原生 KafkaConsumer 封装，管理分区分配和停止偏移 |
| `KafkaSourceFetcherManager` | `source/fetch/KafkaSourceFetcherManager.java` | 管理 Fetch 任务，代理 Offset 提交 |
| `KafkaRecordEmitter` | `source/KafkaRecordEmitter.java` | 将 ConsumerRecord 反序列化为 SeaTunnelRow |
| `KafkaEventTimeDeserializationSchema` | `source/KafkaEventTimeDeserializationSchema.java` | 装饰器，附加 Kafka 记录时间戳为 EventTime |
| `ConsumerMetadata` | `source/ConsumerMetadata.java` | 消费者元数据（topic / startMode / deserializationSchema） |
| `KafkaSink` | `sink/KafkaSink.java` | Sink 入口，创建 Writer 和 Committer |
| `KafkaSinkWriter` | `sink/KafkaSinkWriter.java` | 写入器，序列化行并发送到 Kafka |
| `KafkaSinkCommitter` | `sink/KafkaSinkCommitter.java` | 事务提交器，使用 KafkaInternalProducer 恢复事务 |
| `KafkaProduceSender` | `sink/KafkaProduceSender.java` | 发送器接口，定义 send/beginTransaction/prepareCommit/abort |
| `KafkaTransactionSender` | `sink/KafkaTransactionSender.java` | EXACTLY_ONCE 事务发送器，管理 Kafka 事务生命周期 |
| `KafkaNoTransactionSender` | `sink/KafkaNoTransactionSender.java` | 非事务发送器，NON / AT_LEAST_ONCE 语义 |
| `KafkaInternalProducer` | `sink/KafkaInternalProducer.java` | 扩展 KafkaProducer，支持事务恢复 (resumeTransaction) |
| `MessageContentPartitioner` | `sink/MessageContentPartitioner.java` | 基于消息内容的自定义分区器 |
| `DefaultSeaTunnelRowSerializer` | `serialize/DefaultSeaTunnelRowSerializer.java` | 序列化器，SeaTunnelRow → ProducerRecord |
| `SeaTunnelRowSerializer` | `serialize/SeaTunnelRowSerializer.java` | 序列化器接口 |
| `KafkaSourceState` | `state/KafkaSourceState.java` | Source 端 checkpoint 状态 |
| `KafkaSinkState` | `state/KafkaSinkState.java` | Sink 端 checkpoint 状态 |
| `KafkaCommitInfo` | `state/KafkaCommitInfo.java` | 事务提交元信息 |
| `KafkaAggregatedCommitInfo` | `state/KafkaAggregatedCommitInfo.java` | 聚合提交信息 |
| `KafkaSourceOptions` | `config/KafkaSourceOptions.java` | Source 配置项定义 |
| `KafkaSinkOptions` | `config/KafkaSinkOptions.java` | Sink 配置项定义 |
| `KafkaBaseOptions` | `config/KafkaBaseOptions.java` | Source/Sink 共享配置项 |
| `KafkaBaseConstants` | `config/KafkaBaseConstants.java` | 常量定义 (headers/key/value/offset/partition...) |
| `MessageFormat` | `config/MessageFormat.java` | 消息格式枚举 |
| `StartMode` | `config/StartMode.java` | 消费起始模式枚举 |
| `KafkaSemantics` | `config/KafkaSemantics.java` | 投递语义枚举 (NON/AT_LEAST_ONCE/EXACTLY_ONCE) |
| `KafkaSourceFactory` | `source/KafkaSourceFactory.java` | Source 工厂，SPI 注册 |
| `KafkaSinkFactory` | `sink/KafkaSinkFactory.java` | Sink 工厂，SPI 注册 |

---

## 3. KafkaSource 详细分析

### 3.1 KafkaSource — 入口类

```java
public class KafkaSource
        implements SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaSourceState>,
                SupportParallelism {
```

**关键设计点：**

1. **泛型参数**：`SeaTunnelSource<SeaTunnelRow, KafkaSourceSplit, KafkaSourceState>` — 声明输出类型、Split 类型和状态类型
2. **SupportParallelism**：通过将 TopicPartition 作为 Split 分发给多个 Reader 实现并行
3. **动态 Boundedness**：根据 `JobContext.getJobMode()` 动态切换有界/无界模式

**Boundedness 动态切换：**

```java
@Override
public Boundedness getBoundedness() {
    return JobMode.BATCH.equals(jobContext.getJobMode())
            ? Boundedness.BOUNDED
            : Boundedness.UNBOUNDED;
}
```

- **BATCH 模式** → `Boundedness.BOUNDED`：读取到 endOffset 后停止
- **STREAMING 模式** → `Boundedness.UNBOUNDED`：持续消费，无限流

**工厂方法：**

| 方法 | 返回值 | 职责 |
|------|--------|------|
| `createReader()` | `KafkaSourceReader` | 创建读取器，组装 FetcherManager + RecordEmitter |
| `createEnumerator()` | `KafkaSourceSplitEnumerator` | 首次创建分片枚举器 |
| `restoreEnumerator()` | `KafkaSourceSplitEnumerator` | 从 checkpoint 恢复分片枚举器 |

**createReader() 组装流程：**

```java
public SourceReader<SeaTunnelRow, KafkaSourceSplit> createReader(SourceReader.Context readerContext) {
    // 1. 创建有界阻塞队列
    BlockingQueue<RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>>> elementsQueue =
            new LinkedBlockingQueue<>(kafkaSourceConfig.getReaderCacheQueueSize());

    // 2. 创建 SplitReader 供应商
    Supplier<KafkaPartitionSplitReader> kafkaPartitionSplitReaderSupplier =
            () -> new KafkaPartitionSplitReader(kafkaSourceConfig, readerContext);

    // 3. 创建 FetcherManager
    KafkaSourceFetcherManager kafkaSourceFetcherManager =
            new KafkaSourceFetcherManager(elementsQueue, kafkaPartitionSplitReaderSupplier::get);

    // 4. 创建 RecordEmitter
    KafkaRecordEmitter kafkaRecordEmitter =
            new KafkaRecordEmitter(kafkaSourceConfig.getMapMetadata(), ...);

    // 5. 组装为 KafkaSourceReader
    return new KafkaSourceReader(elementsQueue, kafkaSourceFetcherManager,
            kafkaRecordEmitter, new SourceReaderOptions(readonlyConfig), ...);
}
```

### 3.2 KafkaSourceSplit — 分片数据结构

```java
public class KafkaSourceSplit implements SourceSplit {
    private TablePath tablePath;                    // 表路径
    private TopicPartition topicPartition;          // Kafka TopicPartition
    private long startOffset = -1L;                 // 起始 Offset
    private long endOffset = -1L;                   // 结束 Offset (-1 表示 LATEST)
    private transient volatile boolean finish;      // 是否已完成
}
```

**每个 KafkaSourceSplit 对应一个 Kafka TopicPartition**，这是 Kafka Source 并行读取的基本单位：

- **splitId()**：返回 `"topic-partition"` 格式的字符串
- **startOffset**：消费起始偏移量，由 StartMode 决定
- **endOffset**：消费结束偏移量
  - `BATCH` 模式：设置为 LATEST offset（有界消费）
  - `STREAMING` 模式：设置为 `Long.MAX_VALUE`（无界消费）

### 3.3 KafkaSourceSplitState — 运行时状态

```java
public class KafkaSourceSplitState extends KafkaSourceSplit {
    private long currentOffset;

    public KafkaSourceSplitState(KafkaSourceSplit sourceSplit) {
        // 继承 startOffset 和 endOffset
        this.currentOffset = sourceSplit.getStartOffset();
    }

    public KafkaSourceSplit toKafkaSourceSplit() {
        // 将 currentOffset 作为新的 startOffset 快照
        return new KafkaSourceSplit(tablePath, topicPartition, getCurrentOffset(), getEndOffset());
    }
}
```

**关键设计：** `currentOffset` 跟踪当前消费进度，每次 checkpoint 时 `toKafkaSourceSplit()` 将 currentOffset 转换为新的 startOffset，实现断点续传。

### 3.4 KafkaSourceSplitEnumerator — 分区发现与 Split 分配

**核心数据结构：**

```java
private final Map<TablePath, ConsumerMetadata> tablePathMetadataMap; // 每张表的消费者元数据
private final Map<TopicPartition, KafkaSourceSplit> pendingSplit;     // 待分配的 Split
private final Map<TopicPartition, KafkaSourceSplit> assignedSplit;    // 已分配的 Split
private final AdminClient adminClient;                                // Kafka 管理客户端
private ScheduledExecutorService executor;                            // 动态分区发现线程池
```

#### 3.4.1 run() 方法流程

```
run()
  ├── fetchPendingPartitionSplit()  // 发现所有分区
  │     └── getTopicInfo()
  │           ├── 通过 AdminClient.listTopics() 获取 Topic 列表
  │           ├── 支持正则表达式匹配 Topic
  │           ├── describeTopics() 获取所有分区信息
  │           ├── 支持 ignore_no_leader_partition 跳过无 Leader 分区
  │           └── 为每个 TopicPartition 创建 KafkaSourceSplit
  │
  ├── setPartitionStartOffset()     // 设置起始 Offset
  │     └── 根据 StartMode 确定每个分区的起始偏移量
  │
  └── assignSplit()                 // 分配 Split 给 Reader
        └── 按 TopicPartition 哈希轮询分配到各 Reader
```

#### 3.4.2 StartMode 五种启动模式

```
setPartitionStartOffset()
  ├── EARLIEST       → listOffsets(tp, OffsetSpec.earliest())    // 从最早消息开始
  ├── GROUP_OFFSETS  → listConsumerGroupOffsets(tp)              // 从 Consumer Group 提交的 Offset 开始
  ├── LATEST         → listOffsets(tp, OffsetSpec.latest())      // 从最新消息开始
  ├── TIMESTAMP      → listOffsets(tp, OffsetSpec.forTimestamp()) // 从指定时间戳开始
  └── SPECIFIC_OFFSETS → metadata.getSpecificStartOffsets()      // 从指定 Offset 开始
```

**恢复时强制重置：** 当 `isRestored = true`（从 checkpoint 恢复），所有 Topic 的 StartMode 强制改为 `GROUP_OFFSETS`，确保从 checkpoint 中保存的 Offset 继续消费。

#### 3.4.3 Split 分配算法

```java
private static int getSplitOwner(TopicPartition tp, int numReaders) {
    int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;
    return (startIndex + tp.partition()) % numReaders;
}
```

使用 topic 名称的 hashCode 作为基础，加上 partition 编号，确保同一 TopicPartition 总是分配给同一 Reader，对 checkpoint 恢复友好。

#### 3.4.4 动态分区发现

当 `discoveryIntervalMillis > 0` 时，启动后台线程定期扫描新分区：

```java
this.scheduledFuture = executor.scheduleWithFixedDelay(
    () -> { if (initialized) discoverySplits(); },
    discoveryIntervalMillis, discoveryIntervalMillis, TimeUnit.MILLISECONDS);
```

新发现的分区从 EARLIEST 开始消费，避免丢失数据。

#### 3.4.5 addSplitsBack() — Split 回退机制

当 Reader 处理 Split 失败时，Split 被回退到 Enumerator：

```java
public void addSplitsBack(List<KafkaSourceSplit> splits, int subtaskId) {
    // 从 assignedSplit 中移除，重新加入 pendingSplit
    // 重新计算 startOffset = endOffset + 1 (继续从未完成的位置开始)
    // BATCH 模式下重新获取 latest offset
}
```

---

## 4. KafkaSink 详细分析

### 4.1 KafkaSink — 入口类

```java
public class KafkaSink
        implements SeaTunnelSink<
                SeaTunnelRow, KafkaSinkState, KafkaCommitInfo, KafkaAggregatedCommitInfo> {
```

**泛型参数含义：**

| 参数 | 类型 | 含义 |
|------|------|------|
| `T` | `SeaTunnelRow` | 输入数据类型 |
| `StateT` | `KafkaSinkState` | Writer 状态类型 |
| `CommitT` | `KafkaCommitInfo` | 单次提交信息 |
| `AggCommitT` | `KafkaAggregatedCommitInfo` | 聚合提交信息 |

**工厂方法：**

| 方法 | 返回值 | 职责 |
|------|--------|------|
| `createWriter()` | `KafkaSinkWriter` | 创建写入器（无状态恢复） |
| `restoreWriter()` | `KafkaSinkWriter` | 从 checkpoint 恢复写入器 |
| `createCommitter()` | `KafkaSinkCommitter` | 创建事务提交器 |

### 4.2 KafkaSinkWriter — 写入器

**核心成员：**

```java
private final KafkaProduceSender<byte[], byte[]> kafkaProducerSender;  // 发送器
private final SeaTunnelRowSerializer<byte[], byte[]> seaTunnelRowSerializer; // 序列化器
private String transactionPrefix;  // 事务 ID 前缀
private long lastCheckpointId;     // 上次 checkpoint ID
```

**语义选择策略：**

```java
if (KafkaSemantics.EXACTLY_ONCE.equals(getKafkaSemantics(pluginConfig))) {
    // 使用 KafkaTransactionSender (2PC 事务)
    this.kafkaProducerSender = new KafkaTransactionSender<>(transactionPrefix, getKafkaProperties());
    // 恢复时 abort 所有大于当前 checkpoint 的事务
    if (!kafkaStates.isEmpty()) {
        this.kafkaProducerSender.abortTransaction(kafkaStates.get(0).getCheckpointId() + 1);
    }
    this.kafkaProducerSender.beginTransaction(generateTransactionId(transactionPrefix, lastCheckpointId + 1));
} else {
    // 使用 KafkaNoTransactionSender (NON / AT_LEAST_ONCE)
    this.kafkaProducerSender = new KafkaNoTransactionSender<>(getKafkaProperties());
}
```

**写路径：**

```
write(SeaTunnelRow)
  └── seaTunnelRowSerializer.serializeRow(element)
        └── DefaultSeaTunnelRowSerializer.serializeRow()
              ├── 提取 topic (支持 ${field_name} 动态提取)
              ├── 提取 partition (固定或 Key 哈希)
              ├── 提取 timestamp
              ├── 提取 key (指定字段或 null)
              ├── 提取 value (格式化后序列化)
              └── 提取 headers (Map → RecordHeaders)
  └── kafkaProducerSender.send(producerRecord)
```

**snapshotState() 流程：**

```java
public List<KafkaSinkState> snapshotState(long checkpointId) {
    List<KafkaSinkState> states = kafkaProducerSender.snapshotState(checkpointId);
    this.lastCheckpointId = checkpointId;
    // 立即开始下一个事务
    this.kafkaProducerSender.beginTransaction(
            generateTransactionId(this.transactionPrefix, this.lastCheckpointId + 1));
    return states;
}
```

### 4.3 KafkaTransactionSender — 事务发送器

**事务生命周期：**

```
beginTransaction(transactionId)
  └── 创建 KafkaInternalProducer
  └── producer.initTransactions()
  └── producer.beginTransaction()

send(producerRecord)
  └── producer.send(producerRecord)
  └── recordNumInTransaction++

prepareCommit() → KafkaCommitInfo
  └── 返回 transactionId + producerId + epoch + kafkaProperties

snapshotState(checkpointId)
  ├── 如果 recordNumInTransaction == 0 (空事务)
  │     └── producer.commitTransaction()  // 提交空事务以推进 Offset
  └── 返回 KafkaSinkState (transactionId, prefix, checkpointId)

abortTransaction(checkpointId)
  └── 循环 abort 从 checkpointId 开始的所有未完成事务
  └── 直到 epoch == 0 (事务已被清理)
```

**关键设计：** `snapshotState()` 时不提交事务，而是保存事务元数据。实际提交由 `KafkaSinkCommitter` 在 checkpoint 完成后执行。这保证了 Exactly-Once 语义。

### 4.4 KafkaNoTransactionSender — 非事务发送器

```java
send()        → kafkaProducer.send(producerRecord)
snapshotState() → kafkaProducer.flush(); return emptyList()
prepareCommit() → return Optional.empty()
// beginTransaction / abortTransaction 均为空操作
```

适用于 NON 和 AT_LEAST_ONCE 语义，checkpoint 时仅 flush 缓冲区。

### 4.5 KafkaSinkCommitter — 事务提交器

```java
public List<KafkaCommitInfo> commit(List<KafkaCommitInfo> commitInfos) {
    for (KafkaCommitInfo commitInfo : commitInfos) {
        KafkaProducer<?, ?> producer = getProducer(commitInfo);
        producer.commitTransaction();   // 提交事务
        producer.flush();
    }
    // 关闭 producer
}

public void abort(List<KafkaCommitInfo> commitInfos) {
    for (KafkaCommitInfo commitInfo : commitInfos) {
        KafkaProducer<?, ?> producer = getProducer(commitInfo);
        producer.abortTransaction();    // 回滚事务
    }
}
```

**事务恢复机制 (getProducer)：**

```java
private KafkaInternalProducer<?, ?> getProducer(KafkaCommitInfo commitInfo) {
    // 复用已有 producer 或创建新的
    kafkaProducer.setTransactionalId(commitInfo.getTransactionId());
    // 关键：恢复事务上下文
    kafkaProducer.resumeTransaction(commitInfo.getProducerId(),
            commitInfo.getEpoch(), commitInfo.isTxnStarted());
    return kafkaProducer;
}
```

### 4.6 KafkaInternalProducer — 事务恢复引擎

这是 Kafka Connector 最核心的创新之一，通过反射操作 Kafka 客户端内部状态实现事务恢复：

**核心能力：**

| 方法 | 功能 | 实现方式 |
|------|------|---------|
| `resumeTransaction(producerId, epoch, txnStarted)` | 恢复指定事务的上下文 | 反射设置 TransactionManager 状态 |
| `getProducerId()` | 获取 Producer ID | 反射读取 `producerIdAndEpoch` |
| `getEpoch()` | 获取当前 Epoch | 反射读取 `producerIdAndEpoch` |
| `setTransactionalId()` | 动态切换 TransactionalId | 反射修改 TransactionManager 的 transactionalId 和 currentState |

**resumeTransaction 内部流程：**

```
1. 获取 TransactionManager 对象
2. synchronized (transactionManager) {
3.   重置 txnPartitionMap
4.   设置 producerIdAndEpoch (反射创建 ProducerIdAndEpoch 实例)
5.   状态转换: INITIALIZING → READY → IN_TRANSACTION
6.   设置 transactionStarted = txnStarted
7. }
```

这种设计允许 Committer 在另一个 JVM 进程中恢复 Writer 创建的事务并完成提交，是实现 Exactly-Once 的关键。

---

## 5. Consumer 消费与 Offset 管理机制

### 5.1 KafkaPartitionSplitReader — 原生 KafkaConsumer 封装

**核心成员：**

```java
private final KafkaConsumer<byte[], byte[]> consumer;  // 原生 KafkaConsumer
private final Map<TopicPartition, Long> stoppingOffsets; // 停止偏移量
private final long pollTimeout;                         // 轮询超时
```

**初始化：**

```java
private KafkaConsumer<byte[], byte[]> initConsumer(KafkaSourceConfig config, int subtaskId) {
    Properties props = new Properties();
    props.setProperty(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.setProperty(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, ByteArrayDeserializer.class);
    props.setProperty(ConsumerConfig.ENABLE_AUTO_COMMIT_CONFIG, "false");  // 禁用自动提交
    props.setProperty(ConsumerConfig.ALLOW_AUTO_CREATE_TOPICS_CONFIG, "false");
    return new KafkaConsumer<>(props);
}
```

**关键设计：** 使用 `ByteArrayDeserializer` 获取原始字节，反序列化延迟到 `KafkaRecordEmitter` 中执行，实现格式无关的解耦。

**fetch() 核心逻辑：**

```
fetch()
  ├── consumer.poll(Duration.ofMillis(pollTimeout))
  ├── 遍历每个分区的记录
  │     ├── 检查最后一条记录的 offset 是否 >= stoppingOffset - 1
  │     │     └── 是 → 标记该分区为 finished
  │     └── 未达停止条件 → 继续消费
  ├── 标记空 Split 为 finished
  └── 取消分配已完成的 partitions
```

**handleSplitsChanges() 处理 Split 分配：**

```
handleSplitsChanges(splitsChange)
  ├── 解析每个 Split 的 startOffset 和 endOffset
  ├── consumer.assign(newPartitionAssignments)  // 分配新分区
  ├── consumer.seek() 到各分区的起始偏移量
  ├── 获取 LATEST 分区的实际停止偏移量
  └── removeEmptySplits()  // 移除起始偏移 >= 停止偏移的空分区
```

### 5.2 KafkaPartitionSplitRecords — 内部记录容器

```java
private static class KafkaPartitionSplitRecords
        implements RecordsWithSplitIds<ConsumerRecord<byte[], byte[]>> {
    private final Set<String> finishedSplits;
    private final Map<TopicPartition, Long> stoppingOffsets;
    // nextSplit() → 按分区遍历
    // nextRecordFromSplit() → 过滤 offset >= stoppingOffset 的记录
}
```

**关键设计：** `nextRecordFromSplit()` 中过滤掉 `offset >= currentSplitStoppingOffset` 的记录，确保 BATCH 模式下精确停止在 endOffset 之前。

### 5.3 KafkaSourceFetcherManager — 管理 Fetch 任务

```java
public class KafkaSourceFetcherManager
        extends SingleThreadFetcherManager<ConsumerRecord<byte[], byte[]>, KafkaSourceSplit> {
```

继承自 `SingleThreadFetcherManager`，扩展了 Offset 提交能力：

```java
public void commitOffsets(Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
        OffsetCommitCallback callback) {
    // 获取运行中的 SplitFetcher
    SplitFetcher splitFetcher = fetchers.get(0);
    // 通过 enqueueOffsetsCommitTask 提交 Offset 提交任务
    splitFetcher.addTask(() -> {
        KafkaPartitionSplitReader kafkaReader = (KafkaPartitionSplitReader) splitFetcher.getSplitReader();
        kafkaReader.notifyCheckpointComplete(offsetsToCommit, callback);
    });
}
```

### 5.4 KafkaSourceReader — Offset 管理

**checkpoint 流程：**

```
snapshotState(checkpointId)
  ├── super.snapshotState(checkpointId)  // 获取所有 Split 的快照
  ├── if (!commitOnCheckpoint) → 直接返回 splits
  ├── 构建 offsetAndMetadataMap:
  │     ├── 遍历活跃 Split: offset = startOffset
  │     └── 合并 offsetsOfFinishedSplits: offset = finished offset
  └── 存入 checkpointOffsetMap

notifyCheckpointComplete(checkpointId)
  ├── 从 checkpointOffsetMap 获取对应 checkpoint 的 Offset
  ├── 调用 KafkaSourceFetcherManager.commitOffsets()
  │     └── consumer.commitAsync(offsetsToCommit, callback)
  └── 清理已提交的 Offset 记录
```

**Offset 提交策略：**
- `commitOnCheckpoint = true`：checkpoint 完成后提交 Offset 到 Kafka
- `commitOnCheckpoint = false`：不提交 Offset（由用户自行管理）

---

## 6. Producer 发送与事务

### 6.1 发送器架构

```
KafkaProduceSender<K, V> (接口)
  ├── KafkaTransactionSender<K, V>     // EXACTLY_ONCE: 2PC 事务
  └── KafkaNoTransactionSender<K, V>   // NON / AT_LEAST_ONCE: 普通发送
```

### 6.2 三种投递语义对比

| 语义 | 发送器 | 行为 | 适用场景 |
|------|--------|------|---------|
| **NON** | `KafkaNoTransactionSender` | 直接发送，不保证可靠性 | 非关键数据 |
| **AT_LEAST_ONCE** | `KafkaNoTransactionSender` | checkpoint 时 flush，可能重复 | 可容忍重复的场景 |
| **EXACTLY_ONCE** | `KafkaTransactionSender` | 2PC 事务，checkpoint 后 Committer 提交 | 金融、对账等严格要求 |

### 6.3 Exactly-Once 两阶段提交流程

```
┌─ Writer 端 ─────────────────────────────────────────┐
│ 1. beginTransaction("SeaTunnel-{prefix}-{cpId+1}")   │
│ 2. send(record1), send(record2), ...                 │
│ 3. snapshotState(checkpointId)                       │
│    ├── 保存 KafkaSinkState (transactionId, prefix)   │
│    └── beginTransaction("SeaTunnel-{prefix}-{cpId+2}")│
│ 4. prepareCommit() → KafkaCommitInfo                 │
│    └── 包含 producerId, epoch, transactionId          │
└──────────────────────────────────────────────────────┘
                         │
                         ▼
┌─ Committer 端 ──────────────────────────────────────┐
│ 1. commit([KafkaCommitInfo])                         │
│    ├── 创建 KafkaInternalProducer                     │
│    ├── setTransactionalId(transactionId)              │
│    ├── resumeTransaction(producerId, epoch, txnStarted)│
│    └── producer.commitTransaction()                   │
│ 2. flush + close                                     │
└──────────────────────────────────────────────────────┘
```

**恢复时的隔离保证：** Writer 恢复时调用 `abortTransaction(lastCheckpointId + 1)` 终止所有未完成的事务，确保不会产生重复数据。

### 6.4 事务 ID 生成规则

```java
protected static String generateTransactionId(String transactionPrefix, long checkpointId) {
    return transactionPrefix + "-" + checkpointId;
}
```

- 默认前缀：`SeaTunnel{random 4 digits}`（如 `SeaTunnel0042`）
- 完整 ID：`SeaTunnel0042-3`（前缀 + checkpoint 序号）
- 用户可通过 `transaction_prefix` 配置自定义前缀

---

## 7. 消息格式化策略

### 7.1 支持的格式

`MessageFormat` 枚举定义了 11 种格式：

| 格式 | 枚举值 | 适用场景 |
|------|--------|---------|
| **JSON** | `JSON` | 通用 JSON 序列化 |
| **TEXT** | `TEXT` | 分隔符文本（如 CSV） |
| **CANAL_JSON** | `CANAL_JSON` | 阿里 Canal CDC 格式 |
| **DEBEZIUM_JSON** | `DEBEZIUM_JSON` | Debezium CDC 格式 |
| **COMPATIBLE_DEBEZIUM_JSON** | `COMPATIBLE_DEBEZIUM_JSON` | 兼容 Debezium 格式 |
| **COMPATIBLE_KAFKA_CONNECT_JSON** | `COMPATIBLE_KAFKA_CONNECT_JSON` | 兼容 Kafka Connect JSON |
| **OGG_JSON** | `OGG_JSON` | Oracle GoldenGate 格式 |
| **AVRO** | `AVRO` | Apache Avro 二进制格式 |
| **MAXWELL_JSON** | `MAXWELL_JSON` | Maxwell CDC 格式 |
| **PROTOBUF** | `PROTOBUF` | Protocol Buffers 格式 |
| **NATIVE** | `NATIVE` | 原生 Kafka 格式 (headers/key/value/offset/...) |

### 7.2 Source 端反序列化

**KafkaSourceConfig.createDeserializationSchema() 决策树：**

```
format == NATIVE ?
  ├── YES → NativeKafkaConnectDeserializationSchema
  │         (暴露 headers/key/value/offset/partition/timestamp)
  └── NO →
       schema 未配置 ?
         ├── YES → TextDeserializationSchema (无 schema 时按文本解析)
         └── NO → 按 format 分派:
              ├── JSON → JsonDeserializationSchema
              ├── TEXT → TextDeserializationSchema (指定 delimiter)
              ├── CANAL_JSON → CanalJsonDeserializationSchema
              ├── OGG_JSON → OggJsonDeserializationSchema
              ├── MAXWELL_JSON → MaxWellJsonDeserializationSchema
              ├── COMPATIBLE_KAFKA_CONNECT_JSON → CompatibleKafkaConnectDeserializationSchema
              ├── DEBEZIUM_JSON → DebeziumJsonDeserializationSchema (支持表过滤)
              ├── AVRO → AvroDeserializationSchema
              └── PROTOBUF → ProtobufDeserializationSchema
                              (支持 Schema Registry header 剥离)
```

**KafkaEventTimeDeserializationSchema 装饰器：**

除 Native 和 CompatibleKafkaConnect 外的所有反序列化 Schema 都被包装为 `KafkaEventTimeDeserializationSchema`：

```java
if (!(schema instanceof NativeKafkaConnectDeserializationSchema
        || schema instanceof CompatibleKafkaConnectDeserializationSchema)) {
    return new KafkaEventTimeDeserializationSchema(schema);
}
```

该装饰器在每条记录上附加 Kafka 记录的 `timestamp` 作为 `EventTime` 元数据，供下游 `Metadata` Transform 使用。

### 7.3 KafkaRecordEmitter — 反序列化调度器

```java
public void emitRecord(ConsumerRecord<byte[], byte[]> consumerRecord,
        Collector<SeaTunnelRow> collector, KafkaSourceSplitState splitState) {
    // 1. 根据 TablePath 获取对应的 DeserializationSchema
    DeserializationSchema<SeaTunnelRow> deserializationSchema =
            mapMetadata.get(splitState.getTablePath()).getDeserializationSchema();

    // 2. 设置 EventTime (如果有)
    if (deserializationSchema instanceof KafkaEventTimeDeserializationSchema) {
        ((KafkaEventTimeDeserializationSchema) deserializationSchema)
                .setCurrentRecordTimestamp(consumerRecord.timestamp());
    }

    // 3. 反序列化 (分类处理 Native/Compatible/普通)
    try {
        if (deserializationSchema instanceof CompatibleKafkaConnectDeserializationSchema) {
            ((CompatibleKafkaConnectDeserializationSchema) deserializationSchema)
                    .deserialize(consumerRecord, outputCollector);
        } else if (deserializationSchema instanceof NativeKafkaConnectDeserializationSchema) {
            ((NativeKafkaConnectDeserializationSchema) deserializationSchema)
                    .deserialize(consumerRecord, outputCollector);
        } else {
            deserializationSchema.deserialize(consumerRecord.value(), outputCollector);
        }
    } catch (Exception e) {
        // 支持 SKIP 模式：格式错误时跳过该消息
        if (this.messageFormatErrorHandleWay == MessageFormatErrorHandleWay.SKIP) {
            logger.warn("Deserialize message failed, skip this message");
        } else {
            throw e;  // FAIL 模式：抛出异常
        }
    }

    // 4. 更新 currentOffset = consumerRecord.offset() + 1
    //   (offset + 1 是提交到 Kafka 的 offset，也是下次消费的起始 offset)
    splitState.setCurrentOffset(consumerRecord.offset() + 1);
}
```

**错误处理策略：**
- `FAIL`（默认）：反序列化失败抛异常，任务失败
- `SKIP`：反序列化失败记录警告日志，跳过该消息继续处理

### 7.4 Sink 端序列化

**DefaultSeaTunnelRowSerializer** 使用函数式组合模式：

```java
public class DefaultSeaTunnelRowSerializer implements SeaTunnelRowSerializer {
    private final Function<SeaTunnelRow, String> topicExtractor;       // 提取 Topic
    private final Function<SeaTunnelRow, Integer> partitionExtractor;  // 提取 Partition
    private final Function<SeaTunnelRow, Long> timestampExtractor;     // 提取 Timestamp
    private final Function<SeaTunnelRow, byte[]> keyExtractor;         // 提取 Key
    private final Function<SeaTunnelRow, byte[]> valueExtractor;       // 提取 Value
    private final Function<SeaTunnelRow, Iterable<Header>> headersExtractor; // 提取 Headers

    public ProducerRecord serializeRow(SeaTunnelRow row) {
        return new ProducerRecord(
                topicExtractor.apply(row),
                partitionExtractor.apply(row),
                timestampExtractor.apply(row),
                keyExtractor.apply(row),
                valueExtractor.apply(row),
                headersExtractor.apply(row));
    }
}
```

**多个工厂方法覆盖不同场景：**

| 工厂方法 | 场景 |
|---------|------|
| `create(topic, format, rowType)` | NATIVE 模式 |
| `create(topic, partition, rowType, format, delimiter, config)` | 固定分区 |
| `create(topic, keyFields, rowType, format, delimiter, config)` | 指定 Key 字段 |
| `create(topic, rowType, format, delimiter, config)` | 默认（随机分区） |

**动态 Topic 提取：**

支持 `${field_name}` 语法从行数据中提取 Topic，例如：`topic = "user_${region}"` 会将 `region=cn` 的行发送到 `user_cn`。

---

## 8. 分区策略

### 8.1 Source 端分区策略

Kafka Source 的分区策略由 Kafka 自身决定，每个 TopicPartition 作为一个 Split。Split 分配算法为：

```java
private static int getSplitOwner(TopicPartition tp, int numReaders) {
    int startIndex = ((tp.topic().hashCode() * 31) & 0x7FFFFFFF) % numReaders;
    return (startIndex + tp.partition()) % numReaders;
}
```

### 8.2 Sink 端分区策略

KafkaSink 支持三种分区方式：

**方式一：固定分区 (`partition`)**

```java
// 所有消息发送到指定分区
if (pluginConfig.get(PARTITION) != null) {
    return DefaultSeaTunnelRowSerializer.create(topic, partition, rowType, format, delimiter, config);
}
```

**方式二：基于 Key 的分区 (`partition_key_fields`)**

```java
// 指定字段作为 Key，Kafka 根据 Key 的 hash 分配分区
if (pluginConfig.get(PARTITION_KEY_FIELDS) != null) {
    return DefaultSeaTunnelRowSerializer.create(topic, keyFields, rowType, format, delimiter, config);
}
```

**方式三：基于内容的分区 (`assign_partitions` + `MessageContentPartitioner`)**

```java
// 配置自定义分区器
if (pluginConfig.get(ASSIGN_PARTITIONS) != null) {
    kafkaProperties.put(ProducerConfig.PARTITIONER_CLASS_CONFIG,
        "org.apache.seatunnel.connectors.seatunnel.kafka.sink.MessageContentPartitioner");
}
```

`MessageContentPartitioner` 的算法：
```
1. 遍历 assignPartitions 列表
2. 如果消息内容包含列表中第 i 个字符串 → 发送到分区 i
3. 都不匹配 → 根据消息 hashCode 分配到剩余分区
```

**方式四：默认（随机分区）**

当不指定任何分区策略时，Kafka 默认随机分配分区。

**互斥检查：** `partition` 和 `partition_key_fields` 不能同时配置，否则抛出异常。

---

## 9. 配置类体系

### 9.1 配置类继承关系

```
ConnectorCommonOptions
  └── KafkaBaseOptions          // Source/Sink 共享配置
        ├── KafkaSourceOptions  // Source 专属配置
        └── KafkaSinkOptions    // Sink 专属配置
```

### 9.2 KafkaBaseOptions — 共享配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `topic` | String | 必填 | Kafka Topic 名称 |
| `bootstrap.servers` | String | 必填 | Kafka 集群地址 |
| `format` | MessageFormat | `JSON` | 消息格式 |
| `field_delimiter` | String | `,` | 字段分隔符（TEXT 格式） |
| `kafka.config` | Map | `null` | 原生 Kafka 客户端参数 |
| `protobuf_schema` | String | `null` | Protobuf Schema 定义 |
| `protobuf_message_name` | String | `null` | Protobuf 消息类名 |

### 9.3 KafkaSourceOptions — Source 专属配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `pattern` | Boolean | `false` | 是否使用正则匹配 Topic |
| `consumer.group` | String | `SeaTunnel-Consumer-Group` | 消费者组 ID |
| `commit_on_checkpoint` | Boolean | `true` | 是否在 checkpoint 时提交 Offset |
| `start_mode` | StartMode | `GROUP_OFFSETS` | 消费起始模式 |
| `start_mode.timestamp` | Long | — | TIMESTAMP 模式的起始时间戳 |
| `start_mode.offsets` | Map | — | SPECIFIC_OFFSETS 模式的偏移量 |
| `start_mode.end_timestamp` | Long | — | BATCH 模式的结束时间戳 |
| `partition-discovery.interval-millis` | Long | `-1` | 动态分区发现间隔 |
| `poll.timeout` | Long | `10000` | 轮询超时（毫秒） |
| `reader_cache_queue_size` | Integer | `1024` | Reader 队列大小 |
| `ignore_no_leader_partition` | Boolean | `false` | 是否跳过无 Leader 分区 |
| `debezium_record_include_schema` | Boolean | `true` | Debezium 记录是否包含 Schema |
| `debezium_record_table_filter` | TableIdentifierConfig | — | Debezium 表过滤 |
| `format_error_handle_way` | Enum | `FAIL` | 格式错误处理方式 (FAIL/SKIP) |
| `strip_schema_registry_header` | Boolean | `false` | 是否剥离 Schema Registry 头部 |

### 9.4 KafkaSinkOptions — Sink 专属配置

| 配置项 | 类型 | 默认值 | 说明 |
|--------|------|--------|------|
| `semantics` | KafkaSemantics | `NON` | 投递语义 (NON/AT_LEAST_ONCE/EXACTLY_ONCE) |
| `partition` | Integer | — | 固定分区编号 |
| `partition_key_fields` | List\<String\> | — | 分区 Key 字段 |
| `assign_partitions` | List\<String\> | — | 基于消息内容的分区分配 |
| `transaction_prefix` | String | 自动生成 | 事务 ID 前缀 |

### 9.5 配置条件约束

KafkaSourceFactory 中定义的条件约束：

```
• START_MODE = TIMESTAMP → 必须配置 START_MODE_TIMESTAMP
• START_MODE = SPECIFIC_OFFSETS → 必须配置 START_MODE_OFFSETS
• IGNORE_NO_LEADER_PARTITION = true → 必须配置 KEY_PARTITION_DISCOVERY_INTERVAL_MILLIS
• FORMAT = PROTOBUF → 可选 STRIP_SCHEMA_REGISTRY_HEADER
• TOPIC、TABLE_CONFIGS、TABLE_LIST 互斥
```

---

## 10. 总结与设计模式

### 10.1 设计模式

| 模式 | 体现 |
|------|------|
| **策略模式** | `KafkaProduceSender` — EXACTLY_ONCE vs NON 两种语义策略；`DeserializationSchema` — 多种格式反序列化策略；`StartMode` — 五种消费起始模式 |
| **工厂模式** | `KafkaSourceFactory` / `KafkaSinkFactory` — SPI 自动发现；`DefaultSeaTunnelRowSerializer` 的多个静态工厂方法 |
| **模板方法** | `SingleThreadMultiplexSourceReaderBase` — 定义 SourceReader 骨架，子类实现 `initializedState()` / `toSplitType()` / `onSplitFinished()` |
| **装饰器模式** | `KafkaEventTimeDeserializationSchema` — 在反序列化结果上附加 EventTime 元数据 |
| **观察者模式** | `SplitFetcherTask` — Offset 提交通过任务队列异步执行 |
| **生产者-消费者** | `BlockingQueue<RecordsWithSplitIds>` — FetcherManager 生产，SourceReader 消费 |
| **函数式组合** | `DefaultSeaTunnelRowSerializer` — 使用 `Function<SeaTunnelRow, ?>` 组合 topic/key/value/partition 提取逻辑 |

### 10.2 核心亮点

1. **动态 Boundedness**：同一个 `KafkaSource` 根据 `JobMode` 在 BATCH 和 STREAMING 之间无缝切换，无需改动代码
2. **Exactly-Once 事务恢复**：通过 `KafkaInternalProducer` 的反射机制，实现跨 JVM 进程的事务恢复，保证断点恢复后不丢不重
3. **分区级并行**：每个 Kafka TopicPartition 作为一个 Split，天然支持分区级并行读取
4. **动态分区发现**：支持运行时自动发现新增分区，且从 EARLIEST 开始消费，避免数据丢失
5. **多格式支持**：覆盖 JSON、TEXT、Avro、Protobuf、Canal、Debezium、Ogg、Maxwell 等 11+ 种格式
6. **容错跳过**：`format_error_handle_way = SKIP` 支持反序列化失败时跳过脏数据，保证任务不中断
7. **灵活的分区策略**：支持固定分区、Key 哈希分区、内容匹配分区、随机分区四种方式
8. **Offset 精确管理**：`currentOffset = consumerRecord.offset() + 1` 确保 Offset 提交和断点续传的准确性

### 10.3 可改进方向

1. **多线程 Fetcher**：当前使用 `SingleThreadFetcherManager`，高吞吐场景可考虑多线程 Fetcher
2. **事务超时处理**：长时间运行的事务可能触发 Kafka `transaction.timeout.ms`，需要更健壮的超时处理
3. **反射依赖**：`KafkaInternalProducer` 严重依赖 Kafka 客户端内部实现，跨版本兼容性风险较高
4. **Key 序列化**：当前 Key 始终使用相同的序列化方式，可考虑支持 Key 独立的格式配置
5. **消费者组管理**：`consumer.group` 默认值固定，多任务场景下建议动态生成以避免冲突