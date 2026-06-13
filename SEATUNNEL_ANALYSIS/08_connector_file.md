# Apache SeaTunnel File Connector 深度源码分析

> 代码位置：`seatunnel-connectors-v2/connector-file/`
> 分析版本：基于 3.0.0-SNAPSHOT 分支

---

## 目录

1. [总体架构概览](#1-总体架构概览)
2. [核心类路径速查表](#2-核心类路径速查表)
3. [BaseFileSource 详细分析](#3-basefilesource-详细分析)
4. [BaseFileSink 详细分析](#4-basefilesink-详细分析)
5. [文件 Split 策略](#5-文件-split-策略)
6. [文件格式适配](#6-文件格式适配)
7. [文件系统适配层](#7-文件系统适配层)
8. [文件压缩支持](#8-文件压缩支持)
9. [总结与关键设计模式](#9-总结与关键设计模式)

---

## 1. 总体架构概览

### 1.1 子模块关系

File Connector 采用 base 模块 + 平台适配模块的多模块架构，通过 Hadoop FileSystem API 统一不同存储后端的访问：

```
┌──────────────────────────────────────────────────────────────┐
│           connector-file-base（核心基础模块）                    │
│                                                              │
│  ┌─────────────────────┐  ┌─────────────────────┐            │
│  │   source/            │  │   sink/              │            │
│  │  ├─ BaseFileSource   │  │  ├─ BaseFileSink     │            │
│  │  ├─ BaseMultiple-    │  │  ├─ BaseMultiple-     │            │
│  │  │  TableFileSource  │  │  │  TableFileSink    │            │
│  │  ├─ reader/          │  │  ├─ writer/           │            │
│  │  │  ├─ ReadStrategy  │  │  │  ├─ WriteStrategy  │            │
│  │  │  ├─ CsvRead-      │  │  │  ├─ CsvWrite-      │            │
│  │  │  │  Strategy      │  │  │  │  Strategy      │            │
│  │  │  ├─ JsonRead-     │  │  │  ├─ JsonWrite-     │            │
│  │  │  │  Strategy      │  │  │  │  Strategy      │            │
│  │  │  ├─ ParquetRead-  │  │  │  ├─ ParquetWrite-  │            │
│  │  │  │  Strategy      │  │  │  │  Strategy      │            │
│  │  │  ├─ OrcRead-      │  │  │  ├─ OrcWrite-      │            │
│  │  │  │  Strategy      │  │  │  │  Strategy      │            │
│  │  │  ├─ TextRead-     │  │  │  ├─ TextWrite-     │            │
│  │  │  │  Strategy      │  │  │  │  Strategy      │            │
│  │  │  ├─ ExcelRead-    │  │  │  ├─ ExcelWrite-    │            │
│  │  │  │  Strategy      │  │  │  │  Strategy      │            │
│  │  │  ├─ XmlRead-      │  │  │  ├─ XmlWrite-      │            │
│  │  │  │  Strategy      │  │  │  │  Strategy      │            │
│  │  │  ├─ BinaryRead-   │  │  │  ├─ BinaryWrite-   │            │
│  │  │  │  Strategy      │  │  │  │  Strategy      │            │
│  │  │  └─ Markdown-     │  │  │  ├─ CanalJson-     │            │
│  │  │     ReadStrategy  │  │  │  │  WriteStrategy │            │
│  │  └─ split/           │  │  │  ├─ Debezium-      │            │
│  │     ├─ FileSplit-    │  │  │  │  JsonWrite-    │            │
│  │     │  Strategy      │  │  │  │  Strategy      │            │
│  │     ├─ DefaultFile-  │  │  │  └─ MaxWellJson-   │            │
│  │     │  SplitStrategy │  │  │     WriteStrategy │            │
│  │     ├─ AccordingTo-  │  │  └─ commit/           │            │
│  │     │  SplitSizeSplit│  │     └─ FileSink-      │            │
│  │     │  Strategy      │  │        Aggregated-    │            │
│  │     └─ ParquetFile-  │  │        Committer      │            │
│  │        SplitStrategy │  └─────────────────────┘            │
│  └─────────────────────┘                                      │
│  ┌─────────────────────┐                                      │
│  │ hadoop/              │                                      │
│  │ ├─ HadoopFileSystem- │                                      │
│  │ │  Proxy             │  ← 统一文件系统代理                   │
│  │ └─ HadoopLogin-      │                                      │
│  │    Factory           │                                      │
│  └─────────────────────┘                                      │
│  ┌─────────────────────┐                                      │
│  │ config/              │                                      │
│  │ ├─ FileFormat (enum) │  ← 格式到策略的桥接                   │
│  │ ├─ CompressFormat    │                                      │
│  │ ├─ FileSystemType    │                                      │
│  │ └─ HadoopConf        │                                      │
│  └─────────────────────┘                                      │
└──────────┬───────────┬───────────┬──────────┬──────────┐      │
           │           │           │          │          │
    ┌──────┴──┐  ┌────┴───┐ ┌────┴───┐ ┌───┴───┐ ┌───┴───┐
    │ local   │  │  s3    │ │  oss   │ │ hadoop│ │  ftp  │ ...
    └─────────┘  └────────┘ └────────┘ └───────┘ └───────┘
```

### 1.2 子模块依赖关系

| 子模块 | 描述 | 平台适配点 |
|--------|------|-----------|
| `connector-file-base` | 核心基类、Reader/Writer 策略、Split 策略 | 所有平台共享的逻辑 |
| `connector-file-base-hadoop` | Hadoop 依赖封装（供 base 模块编译） | 提供 org.apache.hadoop 依赖 |
| `connector-file-local` | 本地文件系统 | `HadoopConf` 子类 + `FileSystem` SPI |
| `connector-file-s3` | AWS S3 对象存储 | `S3HadoopConf` + `FileSystem` SPI |
| `connector-file-oss` | 阿里云 OSS 对象存储 | `OssHadoopConf` + `FileSystem` SPI |
| `connector-file-oss-jindo` | OSS Jindo SDK 适配 | `OssJindoHadoopConf` + `FileSystem` SPI |
| `connector-file-hadoop` | HDFS 分布式文件系统 | `HdfsFileHadoopConfig` + `FileSystem` SPI |
| `connector-file-ftp` | FTP 服务器 | `FtpHadoopConf` + `FileSystem` SPI |
| `connector-file-sftp` | SFTP 服务器 | `SftpHadoopConf` + `FileSystem` SPI |
| `connector-file-cos` | 腾讯云 COS | `CosHadoopConf` + `FileSystem` SPI |
| `connector-file-obs` | 华为云 OBS | `ObsHadoopConf` + `FileSystem` SPI |

---

## 2. 核心类路径速查表

### 2.1 Source 端核心类

| 类名 | 路径 | 职责 |
|------|------|------|
| `BaseFileSource` | `source/BaseFileSource.java` | Source 入口（单表，旧版） |
| `BaseMultipleTableFileSource` | `source/BaseMultipleTableFileSource.java` | Source 入口（多表，新版） |
| `BaseFileSourceReader` | `source/BaseFileSourceReader.java` | 逐 Split 读取文件数据 |
| `MultipleTableFileSourceReader` | `source/reader/MultipleTableFileSourceReader.java` | 多表读取器 |
| `FileSourceSplit` | `source/split/FileSourceSplit.java` | 数据分片，含 filePath + start + length |
| `FileSourceSplitEnumerator` | `source/split/FileSourceSplitEnumerator.java` | 单表分片枚举器 |
| `MultipleTableFileSourceSplitEnumerator` | `source/split/MultipleTableFileSourceSplitEnumerator.java` | 多表分片枚举器 |
| `ReadStrategy` | `source/reader/ReadStrategy.java` | 文件读取策略接口 |
| `AbstractReadStrategy` | `source/reader/AbstractReadStrategy.java` | 读取策略抽象基类 |
| `ReadStrategyFactory` | `source/reader/ReadStrategyFactory.java` | 根据 FileFormat 创建 ReadStrategy |

### 2.2 Sink 端核心类

| 类名 | 路径 | 职责 |
|------|------|------|
| `BaseFileSink` | `sink/BaseFileSink.java` | Sink 入口（单表，旧版） |
| `BaseMultipleTableFileSink` | `sink/BaseMultipleTableFileSink.java` | Sink 入口（多表，新版） |
| `BaseFileSinkWriter` | `sink/BaseFileSinkWriter.java` | 文件写入器，委托 WriteStrategy |
| `WriteStrategy` | `sink/writer/WriteStrategy.java` | 文件写入策略接口 |
| `AbstractWriteStrategy` | `sink/writer/AbstractWriteStrategy.java` | 写入策略抽象基类 |
| `WriteStrategyFactory` | `sink/writer/WriteStrategyFactory.java` | 根据 FileFormat 创建 WriteStrategy |
| `Transaction` | `sink/writer/Transaction.java` | 两阶段提交事务接口 |
| `FileSinkAggregatedCommitter` | `sink/commit/FileSinkAggregatedCommitter.java` | 聚合提交器（文件重命名） |
| `FileSourceState` | `source/state/FileSourceState.java` | Source 快照状态 |
| `FileSinkState` | `sink/state/FileSinkState.java` | Sink 快照状态 |

### 2.3 配置与基础设施

| 类名 | 路径 | 职责 |
|------|------|------|
| `FileFormat` | `config/FileFormat.java` | 文件格式枚举（CSV/JSON/Parquet/ORC 等） |
| `CompressFormat` | `config/CompressFormat.java` | 压缩格式枚举（GZIP/SNAPPY/LZ4/ZSTD 等） |
| `ArchiveCompressFormat` | `config/ArchiveCompressFormat.java` | 归档压缩格式（ZIP/TAR/TAR_GZ/GZ） |
| `FileSystemType` | `config/FileSystemType.java` | 文件系统类型枚举（HDFS/LOCAL/OSS/S3 等） |
| `HadoopConf` | `config/HadoopConf.java` | Hadoop 配置抽象（含 Kerberos 认证） |
| `HadoopFileSystemProxy` | `hadoop/HadoopFileSystemProxy.java` | 统一文件系统代理（Kerberos/RemoteUser） |
| `HadoopLoginFactory` | `hadoop/HadoopLoginFactory.java` | Kerberos 登录工厂 |
| `FileSplitStrategy` | `source/split/FileSplitStrategy.java` | 文件分割策略接口 |
| `FileSplitStrategyFactory` | `source/split/FileSplitStrategyFactory.java` | 分割策略工厂 |

---

## 3. BaseFileSource 详细分析

### 3.1 两类 Source 基类

File Connector 提供了两套 Source 基类，分别对应单表和多表场景：

```
SeaTunnelSource<SeaTunnelRow, FileSourceSplit, FileSourceState>
├── BaseFileSource (Legacy, 单表)
│   └── 直接持有 rowType / readStrategy / hadoopConf / filePaths
│
└── BaseMultipleTableFileSource (Modern, 多表)
    ├── 持有 BaseMultipleTableFileSourceConfig（包含多表配置）
    ├── 持有 FileSplitStrategy（路由到多表分割策略）
    └── 子类：LocalFileSource / S3FileSource / OssFileSource / HdfsFileSource ...
```

**BaseFileSource 关键字段：**

```java
public abstract class BaseFileSource
        implements SeaTunnelSource<SeaTunnelRow, FileSourceSplit, FileSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    protected SeaTunnelRowType rowType;
    protected ReadStrategy readStrategy;
    protected HadoopConf hadoopConf;
    protected List<String> filePaths;
}
```

**BaseMultipleTableFileSource 关键字段：**

```java
public abstract class BaseMultipleTableFileSource
        implements SeaTunnelSource<SeaTunnelRow, FileSourceSplit, FileSourceState>,
                SupportParallelism,
                SupportColumnProjection {
    private final BaseMultipleTableFileSourceConfig baseMultipleTableFileSourceConfig;
    private final FileSplitStrategy fileSplitStrategy;
}
```

**工厂方法对照：**

| 方法 | BaseFileSource | BaseMultipleTableFileSource |
|------|---------------|---------------------------|
| `createReader()` | `BaseFileSourceReader` | `MultipleTableFileSourceReader` |
| `createEnumerator()` | `FileSourceSplitEnumerator` | `MultipleTableFileSourceSplitEnumerator` |
| `Boundedness` | `BOUNDED` | `BOUNDED` |

### 3.2 BaseFileSourceReader 读取器

[`BaseFileSourceReader.java`](#)

核心的 `pollNext()` 实现：

```java
public void pollNext(Collector<SeaTunnelRow> output) throws Exception {
    synchronized (output.getCheckpointLock()) {
        FileSourceSplit split = sourceSplits.poll();
        if (null != split) {
            readStrategy.read(split.splitId(), "", output);
        } else if (noMoreSplit && sourceSplits.isEmpty()) {
            context.signalNoMoreElement();
        } else {
            Thread.sleep(1000L);  // 等待新 Split
        }
    }
}
```

**关键设计：**
- 使用 `synchronized (output.getCheckpointLock())` 保证 checkpoint 一致性
- 采用 `ConcurrentLinkedDeque` 接收 Split，线程安全
- `snapshotState()` 返回尚未处理的 Split 列表，用于 checkpoint

### 3.3 ReadStrategy 接口体系

[`ReadStrategy.java`](#)

```
ReadStrategy (Interface)
    extends Serializable, Closeable
    │
    ├── init(HadoopConf)
    ├── read(String path, String tableId, Collector<SeaTunnelRow>)
    ├── read(FileSourceSplit split, Collector<SeaTunnelRow>)  [default, 调用上面的 read]
    ├── getSeaTunnelRowTypeInfo(String path)
    ├── setCatalogTable(CatalogTable)
    ├── getFileNamesByPath(String path)
    ├── setPluginConfig(Config)
    └── getActualSeaTunnelRowTypeInfo()
```

**ReadStrategy 继承体系：**

```
ReadStrategy (接口)
    │
    └── AbstractReadStrategy (抽象基类)
            │  - hadoopConf / seaTunnelRowType / pluginConfig
            │  - hadoopFileSystemProxy (HadoopFileSystemProxy)
            │  - getFileNamesByPath(): 文件发现（递归遍历 + 模式过滤 + 时间过滤 + 分区过滤）
            │  - setPluginConfig(): 解析所有 Source 配置项
            │  - resolveArchiveCompressedInputStream(): 压缩文件解包
            │  - parsePartitionsByPath(): 从路径解析分区信息
            │  - mergePartitionTypes(): 合并分区列到 RowType
            │  - safeSlice(): 安全的流切片
            │  - shouldSyncFileInUpdateMode(): 增量同步模式文件比较
            │
            ├── CsvReadStrategy       (CSV 格式，支持编码/分隔符/BOM/表头)
            ├── TextReadStrategy      (纯文本，逐行读取)
            ├── JsonReadStrategy      (JSON 格式解析)
            ├── ParquetReadStrategy   (Parquet 列式存储)
            ├── OrcReadStrategy       (ORC 列式存储)
            ├── ExcelReadStrategy     (Excel .xls/.xlsx)
            ├── XmlReadStrategy       (XML 格式解析)
            ├── BinaryReadStrategy    (二进制文件复制)
            └── MarkdownReadStrategy  (Markdown 表格解析)
```

**AbstractReadStrategy 的核心能力（共 963 行代码）：**

| 功能 | 方法 | 说明 |
|------|------|------|
| 文件发现 | `getFileNamesByPath()` | 递归遍历目录，支持正则过滤、日期过滤、分区过滤、后缀过滤 |
| 归档解压 | `resolveArchiveCompressedInputStream()` | 支持 ZIP/TAR/TAR_GZ/GZ 四种归档格式的透明解压 |
| 分区解析 | `parsePartitionsByPath()` | 从 Hive 风格路径 `key=value` 解析分区信息 |
| 增量同步 | `shouldSyncFileInUpdateMode()` | 支持 LEN_MTIME/CHECKSUM 两种比较模式，DISTCP/STRICT 两种更新策略 |
| 流切片 | `safeSlice()` | 安全的跳过指定字节，返回 BoundedInputStream |
| 模式匹配 | `filterFileByPattern()` | 支持基于文件名或全路径的正则过滤 |

---

## 4. BaseFileSink 详细分析

### 4.1 两类 Sink 基类

与 Source 对称，Sink 也有两套基类：

```
SeaTunnelSink<SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo>
├── BaseFileSink (Legacy, 单表)
│   └── 通过 setTypeInfo() 接收 rowType
│
└── BaseMultipleTableFileSink (Modern, 多表)
    implements SupportMultiTableSink, SupportSaveMode
    └── 通过构造函数接收 CatalogTable
```

**BaseFileSink 关键设计点：**

```java
public abstract class BaseFileSink
        implements SeaTunnelSink<
                SeaTunnelRow, FileSinkState, FileCommitInfo, FileAggregatedCommitInfo> {
    protected SeaTunnelRowType seaTunnelRowType;
    protected Config pluginConfig;
    protected HadoopConf hadoopConf;
    protected FileSinkConfig fileSinkConfig;
    // ...
    protected WriteStrategy createWriteStrategy() {
        WriteStrategy writeStrategy =
                WriteStrategyFactory.of(fileSinkConfig.getFileFormat(), fileSinkConfig);
        writeStrategy.setCatalogTable(CatalogTableUtil.getCatalogTable(...));
        return writeStrategy;
    }
}
```

### 4.2 BaseFileSinkWriter 写入器

[`BaseFileSinkWriter.java`](#)

```java
public class BaseFileSinkWriter
        implements SinkWriter<SeaTunnelRow, FileCommitInfo, FileSinkState>,
                SupportMultiTableSinkWriter<WriteStrategy> {
    protected final WriteStrategy writeStrategy;
}
```

**write() 委托给 WriteStrategy：**

```java
public void write(SeaTunnelRow element) throws IOException {
    writeStrategy.write(element);
}
```

**两阶段提交流程（通过 WriteStrategy.Transaction 接口）：**

```
BaseFileSinkWriter
    │
    write(SeaTunnelRow) → writeStrategy.write(row)
    │
    prepareCommit() → writeStrategy.prepareCommit()
    │   └── finishAndCloseFile()      // 关闭当前文件
    │   └── 返回 FileCommitInfo(needMoveFiles, partitionDirAndValuesMap, transactionDir)
    │
    snapshotState(checkpointId) → writeStrategy.snapshotState(checkpointId)
    │   └── 保存当前事务状态
    │   └── beginTransaction(checkpointId + 1)  // 开始新事务
    │
    FileSinkAggregatedCommitter.commit()
        └── 将临时文件 rename 到最终路径
        └── 删除临时事务目录
```

### 4.3 Transaction 事务接口

[`Transaction.java`](#)

```java
public interface Transaction extends Serializable {
    Optional<FileCommitInfo> prepareCommit();    // 准备提交
    void abortPrepare();                         // 中止准备
    void abortPrepare(String transactionId);     // 中止指定事务
    List<FileSinkState> snapshotState(long checkpointId);  // 快照状态
    void beginTransaction(Long checkpointId);     // 开始新事务
}
```

### 4.4 WriteStrategy 接口体系

[`WriteStrategy.java`](#)

```
WriteStrategy<T> (Interface)
    extends Transaction, Serializable, Closeable
    │
    ├── init(HadoopConf, jobId, uuidPrefix, subTaskIndex)
    ├── getConfiguration(HadoopConf) → Configuration
    ├── write(SeaTunnelRow)
    ├── setCatalogTable(CatalogTable)
    ├── generatorPartitionDir(SeaTunnelRow) → LinkedHashMap
    ├── getOrCreateOutputStream(String path) → T
    ├── generateFileName(String transactionId) → String
    ├── finishAndCloseFile()
    ├── getCheckpointId() → long
    ├── getFileSinkConfig() → FileSinkConfig
    └── getHadoopFileSystemProxy() → HadoopFileSystemProxy
```

**WriteStrategy 继承体系：**

```
WriteStrategy<T> (接口)
    │
    └── AbstractWriteStrategy<T> (抽象基类)
            │  - fileSinkConfig / compressFormat / hadoopConf
            │  - hadoopFileSystemProxy (HadoopFileSystemProxy)
            │  - transactionId / transactionDirectory
            │  - needMoveFiles / beingWrittenFile
            │  - write(): 基类实现 batch 分片逻辑
            │  - generateFileName(): 变量替换（UUID/NOW/transactionId）
            │  - generatorPartitionDir(): 分区目录生成
            │  - prepareCommit(): 标准实现，调用 finishAndCloseFile
            │  - snapshotState(): 标准实现，保存状态并 beginTransaction
            │  - getOrCreateFilePathBeingWritten(): 获取或创建当前写入路径
            │  - getTargetLocation(): 临时路径 → 最终路径转换
            │
            ├── CsvWriteStrategy        (CSV 格式写入)
            ├── TextWriteStrategy       (纯文本写入)
            ├── JsonWriteStrategy       (JSON 格式写入)
            ├── ParquetWriteStrategy    (Parquet 列式写入)
            ├── OrcWriteStrategy        (ORC 列式写入)
            ├── ExcelWriteStrategy      (Excel 格式写入)
            ├── XmlWriteStrategy        (XML 格式写入)
            ├── BinaryWriteStrategy     (二进制文件复制)
            ├── CanalJsonWriteStrategy  (Canal CDC JSON 写入)
            ├── DebeziumJsonWriteStrategy (Debezium CDC JSON 写入)
            └── MaxWellJsonWriteStrategy  (Maxwell CDC JSON 写入)
```

**AbstractWriteStrategy 事务目录结构：**

```
{tmpPath}/seatunnel/{jobId}/{uuidPrefix}/
    └── T_{jobId}_{uuidPrefix}_{subTaskIndex}_{checkpointId}/
        ├── {partition_dir}/
        │   └── {fileName}.{format}{compress}
        └── {another_partition}/
            └── ...
```

提交后 rename 到最终路径：`{outputPath}/{partition_dir}/{fileName}.{format}{compress}`

**文件命名变量替换规则：**

```java
// 支持的变量
${uuid}          → UUID.randomUUID()
${now}           → 当前时间（格式化）
${yyyy-MM-dd}   → 当前时间戳
${transactionId} → 事务 ID

// 格式：baseName_partId.extension
// 单文件模式不加 _partId 后缀
```

---

## 5. 文件 Split 策略

### 5.1 FileSplitStrategy 接口

[`FileSplitStrategy.java`](#)

```java
public interface FileSplitStrategy extends Serializable {
    List<FileSourceSplit> split(String tableId, String filePath);
}
```

极简接口 — 输入文件路径，输出一个或多个 `FileSourceSplit`。

### 5.2 FileSourceSplit 数据结构

[`FileSourceSplit.java`](#)

```java
public class FileSourceSplit implements SourceSplit {
    @Getter private final String tableId;   // 表标识（多表场景）
    @Getter private final String filePath;  // 文件路径
    @Getter private long start = 0;         // 起始偏移（字节）
    @Getter private long length = -1;       // 长度（-1 表示读到末尾）

    public String splitId() {
        if (tableId == null) return filePath;                    // 兼容旧版
        if (start == 0L && length < 0L) return tableId + "_" + filePath;
        return tableId + "_" + filePath + "_" + start;
    }
}
```

**Split 演进兼容性：** `readObject()` 中处理旧 checkpoint 数据（start=0, length=0 → 修正为 length=-1）。

### 5.3 DefaultFileSplitStrategy

[`DefaultFileSplitStrategy.java`](#)

```java
public class DefaultFileSplitStrategy implements FileSplitStrategy {
    public List<FileSourceSplit> split(String tableId, String filePath) {
        return Collections.singletonList(new FileSourceSplit(tableId, filePath));
    }
}
```

最简单策略：**一个文件 = 一个 Split**。适用于不支持切分的格式（ORC/Excel/XML 等）。

### 5.4 AccordingToSplitSizeSplitStrategy — 按大小分割

[`AccordingToSplitSizeSplitStrategy.java`](#)

这是面向文本文件（CSV/JSON/Text）的核心分割策略。核心原则：**以 rowDelimiter 为最小不可分割单元，绝不切断行分隔符**。

**算法流程：**

```
1. 获取文件大小 fileSize
2. skipHeaderRowNumber 跳过表头（如果启用）
3. currentStart = 0
4. while currentStart < fileSize:
     a. tentativeEnd = currentStart + splitSize
     b. 如果 tentativeEnd >= fileSize:
          创建最后一个 Split [currentStart, fileSize - currentStart)，结束
     c. actualEnd = findNextDelimiterWithSeek(tentativeEnd)
        - 从 tentativeEnd 处开始向后搜索行分隔符
        - 找到后返回分隔符结束位置作为 split 边界
     d. 创建 Split [currentStart, actualEnd - currentStart)
     e. currentStart = actualEnd
```

**跳过表头算法（`skipLinesUsingBuffer`）：**
- 使用 64KB 缓冲区
- 逐字节匹配 delimiterBytes
- 跳过 `skipHeaderRowNumber` 个行分隔符后的 `pos` 作为起始偏移

**查找分隔符算法（`findNextDelimiterWithSeek`）：**
- 从 `startPos - (delimiterBytes.length - 1)` 处开始扫描（防止分隔符跨越边界）
- 使用 KMP 风格的逐字节匹配
- 返回第一个在 `startPos` 或之后的完整分隔符结束位置

**异常处理：** 将 HDFS 异常映射到 `FileConnectorErrorCode`（FILE_NOT_FOUND / FILE_ACCESS_DENIED / FILE_IO_TIMEOUT / FILE_READ_FAILED）。

### 5.5 ParquetFileSplitStrategy

[`ParquetFileSplitStrategy.java`](#)

**核心设计思想：** 以 Parquet 的 RowGroup 作为最小不可分割单元（而非字节），保证分割后的每个 Split 在 Parquet 层面完整可读。

**算法流程：**

```
1. 读取文件的所有 RowGroup 元信息（BlockMetaData）
2. 遍历 RowGroup：
     a. 当前 Split 为空时，开始新 Split
     b. 计算 currentStart = first_rg.startingPos
     c. 继续合并后续 RowGroup，直到 rgEnd - currentStart > splitSizeBytes
     d. 关闭当前 Split，开始下一个
     e. 最后一个 RowGroup 合并到最后的 Split
```

**认证支持：** 通过 `hadoopFileSystemProxy.doWithHadoopAuth()` 安全读取 Parquet 文件。

### 5.6 MultipleTableFileSplitStrategy

[`MultipleTableFileSplitStrategy.java`](#)

多表场景下的策略路由器，内部维护 `Map<String, FileSplitStrategy>`，根据 `tableId` 路由到对应的分割策略。

### 5.7 FileSplitStrategyFactory

[`FileSplitStrategyFactory.java`](#)

```java
public static FileSplitStrategy initFileSplitStrategy(
        ReadonlyConfig readonlyConfig, HadoopConf hadoopConf) {
    // 1. 未启用文件分割 → DefaultFileSplitStrategy
    if (!readonlyConfig.get(ENABLE_FILE_SPLIT)) return new DefaultFileSplitStrategy();
    // 2. 格式不支持分割 → DefaultFileSplitStrategy
    if (!fileFormat.supportFileSplit()) return new DefaultFileSplitStrategy();
    // 3. 压缩文件 → DefaultFileSplitStrategy
    if (compressFormat != NONE || archiveCompressFormat != NONE) return new DefaultFileSplitStrategy();
    // 4. Parquet 格式 → ParquetFileSplitStrategy
    if (fileFormat == PARQUET) return new ParquetFileSplitStrategy(...);
    // 5. 文本格式 (CSV/JSON/Text) → AccordingToSplitSizeSplitStrategy
    return new AccordingToSplitSizeSplitStrategy(hadoopConf, rowDelimiter, ...);
}
```

**FileFormat.supportFileSplit() 支持矩阵：**

| 格式 | CSV | TEXT | JSON | PARQUET | ORC | EXCEL | XML | 其他 |
|------|-----|------|------|---------|-----|-------|-----|------|
| 支持分割 | ✅ | ✅ | ✅ | ✅ | ❌ | ❌ | ❌ | ❌ |

### 5.8 FileSourceSplitEnumerator 分配策略

[`FileSourceSplitEnumerator.java`](#)

```java
private void assignSplit(int taskId) {
    if (context.currentParallelism() == 1) {
        currentTaskSplits.addAll(allSplit);  // 全部给单个 Reader
    } else {
        // Round-Robin: assignCount % parallelism == taskId
        for (FileSourceSplit split : allSplit) {
            if (getSplitOwner(assignCount.getAndIncrement(), context.currentParallelism()) == taskId) {
                currentTaskSplits.add(split);
            }
        }
    }
    context.assignSplit(taskId, currentTaskSplits);
    context.signalNoMoreSplits(taskId);
}

private static int getSplitOwner(int assignCount, int numReaders) {
    return assignCount % numReaders;  // 简单的取模轮询
}
```

---

## 6. 文件格式适配

### 6.1 FileFormat 枚举 — 格式与策略的桥接器

[`FileFormat.java`](#)

```java
public enum FileFormat implements Serializable {
    CSV("csv") {
        public WriteStrategy getWriteStrategy(FileSinkConfig cfg) { return new CsvWriteStrategy(cfg); }
        public ReadStrategy getReadStrategy() { return new CsvReadStrategy(); }
    },
    TEXT("txt"),
    PARQUET("parquet"),
    ORC("orc"),
    JSON("json"),
    EXCEL("xlsx", "xls"),
    XML("xml"),
    BINARY(""),
    CANAL_JSON("canal_json") {   // 只写不读
        public ReadStrategy getReadStrategy() { throw new UnsupportedOperationException(...); }
    },
    DEBEZIUM_JSON("debezium_json") { // 只写不读
        public ReadStrategy getReadStrategy() { throw new UnsupportedOperationException(...); }
    },
    MAXWELL_JSON("maxwell_json") {  // 只写不读
        public ReadStrategy getReadStrategy() { throw new UnsupportedOperationException(...); }
    },
    MARKDOWN("md", "markdown") {    // 只读不写
        public WriteStrategy getWriteStrategy(FileSinkConfig cfg) { throw new UnsupportedOperationException(...); }
    };
}
```

**完整支持矩阵：**

| FileFormat | 后缀 | ReadStrategy | WriteStrategy | 文件分割 |
|-----------|------|-------------|--------------|---------|
| CSV | .csv | `CsvReadStrategy` | `CsvWriteStrategy` | ✅ |
| TEXT | .txt | `TextReadStrategy` | `TextWriteStrategy` | ✅ |
| JSON | .json | `JsonReadStrategy` | `JsonWriteStrategy` | ✅ |
| PARQUET | .parquet | `ParquetReadStrategy` | `ParquetWriteStrategy` | ✅ |
| ORC | .orc | `OrcReadStrategy` | `OrcWriteStrategy` | ❌ |
| EXCEL | .xlsx, .xls | `ExcelReadStrategy` | `ExcelWriteStrategy` | ❌ |
| XML | .xml | `XmlReadStrategy` | `XmlWriteStrategy` | ❌ |
| BINARY | (无) | `BinaryReadStrategy` | `BinaryWriteStrategy` | ❌ |
| CANAL_JSON | .canal_json | ❌ 不支持 | `CanalJsonWriteStrategy` | ❌ |
| DEBEZIUM_JSON | .debezium_json | ❌ 不支持 | `DebeziumJsonWriteStrategy` | ❌ |
| MAXWELL_JSON | .maxwell_json | ❌ 不支持 | `MaxWellJsonWriteStrategy` | ❌ |
| MARKDOWN | .md, .markdown | `MarkdownReadStrategy` | ❌ 不支持 | ❌ |

### 6.2 ReadStrategy 实现亮点

#### CsvReadStrategy
- **BOM 自动检测**：通过 `BOMInputStream` 自动移除 UTF-8 BOM 标记
- **编码支持**：解析 BOM 后自动选择对应字符集，否则使用配置的 encoding
- **表头处理**：支持 firstLineAsHeader 模式；启用 split 时只有第一个 Split 读取表头
- **列投影**：支持 readColumns 参数，只读取指定列
- **LZO 压缩**：通过 `LzopCodec` 支持 LZO 解压

#### ParquetReadStrategy
- **列式读取优化**：利用 Parquet 的投影下推，只读取需要的列
- **时间戳兼容**：支持 INT96 和 INT64 两种时间戳格式
- **Schema 自动发现**：从 Parquet Schema 映射到 SeaTunnel 类型

#### ExcelReadStrategy
- **双引擎**：支持 `.xls` (HSSF) 和 `.xlsx` (XSSF) 两种格式
- **公式计算**：支持读取 Excel 公式的计算结果

### 6.3 WriteStrategy 实现亮点

#### AbstractWriteStrategy 的事务和分片管理

**批处理机制：**
```java
public void write(SeaTunnelRow seaTunnelRow) {
    if (currentBatchSize >= batchSize && !singleFileMode) {
        newFilePart();      // 切换新文件分片
        currentBatchSize = 0;
    }
    currentBatchSize++;
}
```

**文件路径生成链：**
```
getOrCreateFilePathBeingWritten(row)
  → generatorPartitionDir(row)         // 从行数据提取分区
  → getPathWithPartitionInfo(...)      // 拼接事务目录 + 分区 + 文件名
    → generateFileName(transactionId)  // 变量替换生成文件名
```

#### CDC 格式 WriteStrategy

`CanalJsonWriteStrategy`、`DebeziumJsonWriteStrategy`、`MaxWellJsonWriteStrategy` 继承自 `JsonWriteStrategy`，在写入 CDC 数据时保留了 `RowKind`（INSERT/UPDATE/DELETE）信息。

---

## 7. 文件系统适配层

### 7.1 HadoopFileSystemProxy — 统一文件系统代理

[`HadoopFileSystemProxy.java`](#)

这是 File Connector 最核心的基础设施类，封装了所有文件系统操作，并提供 Kerberos/RemoteUser 认证支持。

**核心设计：**

```java
public class HadoopFileSystemProxy implements Serializable, Closeable {
    private transient UserGroupInformation userGroupInformation;
    private transient FileSystem fileSystem;
    private transient Configuration configuration;
    private final HadoopConf hadoopConf;
    private boolean isAuthTypeKerberos;
}
```

**三种认证模式：**

```
initialize()
├── enableKerberos() → initializeWithKerberosLogin()
│   └── HadoopLoginFactory.loginWithKerberos(config, krb5Path, principal, keytab, ...)
│       └── UserGroupInformation.checkTGTAndReloginFromKeytab()  // 长期作业自动续期
│
├── enableRemoteUser() → initializeWithRemoteUserLogin()
│   └── HadoopLoginFactory.loginWithRemoteUser(config, remoteUser, ...)
│
└── 普通模式 → FileSystem.get(configuration)
    └── fileSystem.setWriteChecksum(false)  // 关闭写入校验和
```

**open() 方法中执行 `execute()` 的一体化安全处理：**

```java
private <T> T execute(PrivilegedExceptionAction<T> action) throws IOException {
    if (isAuthTypeKerberos) {
        maybeRelogin();  // Kerberos ticket 续期
        return userGroupInformation.doAs(action);  // 以代理身份执行
    } else {
        return action.run();  // 直接执行
    }
}
```

**抽象的文件操作集合：**

| 方法 | 作用 |
|------|------|
| `fileExist(path)` | 检查文件是否存在 |
| `isFile(path)` | 判断是否为文件 |
| `createFile(path)` | 创建文件 |
| `deleteFile(path)` | 删除文件/目录 |
| `renameFile(old, new, overwrite)` | 重命名/移动文件 |
| `createDir(path)` | 创建目录 |
| `listFile(path)` | 列出文件（`LocatedFileStatus`） |
| `getAllSubFiles(path)` | 列出所有子目录 |
| `listStatus(path)` | 列出 FileStatus 数组 |
| `getFileStatus(path)` | 获取 FileStatus |
| `getFileChecksum(path)` | 获取文件校验和 |
| `getOutputStream(path)` | 获取输出流（`FSDataOutputStream`） |
| `getInputStream(path)` | 获取输入流（`FSDataInputStream`） |
| `doWithHadoopAuth(func)` | 在 Hadoop 认证上下文中执行自定义逻辑 |

**Kerberos 长期作业支持：** `maybeRelogin()` 在每个 `execute()` 调用前检查 ticket 并自动续期，确保长时间运行的作业不会因 Kerberos ticket 过期而失败。

### 7.2 HadoopConf — Hadoop 配置抽象

[`HadoopConf.java`](#)

```java
@Data
public class HadoopConf implements Serializable {
    protected Map<String, String> extraOptions = new HashMap<>();
    protected String hdfsNameKey;          // fs.defaultFS
    protected String hdfsSitePath;         // hdfs-site.xml 路径
    protected String remoteUser;           // 简单认证用户
    protected String krb5Path;             // krb5.conf 路径
    protected String kerberosPrincipal;    // Kerberos 主体
    protected String kerberosKeytabPath;   // Keytab 路径
}
```

**`toConfiguration()` 返回标准 Hadoop Configuration：**
- 设置 Parquet 兼容性参数（`READ_INT96_AS_FIXED`/`ADD_LIST_ELEMENT_RECORDS`/`WRITE_OLD_LIST_STRUCTURE`）
- 禁用 `fs.{schema}.impl.disable.cache` 以避免跨作业缓存污染
- 支持 `viewfs://` 前缀

### 7.3 FileSystemType 枚举

[`FileSystemType.java`](#)

```java
public enum FileSystemType implements Serializable {
    HDFS("HdfsFile"),
    LOCAL("LocalFile"),
    OSS("OssFile"),
    OSS_JINDO("OssJindoFile"),
    COS("CosFile"),
    FTP("FtpFile"),
    SFTP("SftpFile"),
    S3("S3File"),
    OBS("ObsFile");
}
```

### 7.4 各平台适配的实现方式

每个子模块通过以下方式接入：

**1. 自定义 `HadoopConf` 子类：** 添加平台特定配置项（如 S3 的 endpoint/bucket/accessKey/secretKey）

**2. `META-INF/services/org.apache.hadoop.fs.FileSystem` SPI 注册：**
- `connector-file-s3`: `s3a` → `org.apache.hadoop.fs.s3a.S3AFileSystem`
- `connector-file-oss`: `oss` → `com.aliyun.oss.hadoop.OSSFileSystem`
- `connector-file-ftp`: `ftp` → 自定义 FTP FileSystem

**3. 继承 `BaseMultipleTableFileSource` / `BaseMultipleTableFileSink`：**

```java
// Local 文件源
public class LocalFileSource extends BaseMultipleTableFileSource {
    public LocalFileSource(ReadonlyConfig readonlyConfig) {
        super(new MultipleTableLocalFileSourceConfig(readonlyConfig),
              initFileSplitStrategy(sourceConfig));
    }
    public String getPluginName() {
        return FileSystemType.LOCAL.getFileSystemPluginName();  // "LocalFile"
    }
}

// S3 文件源
public class S3FileSource extends BaseMultipleTableFileSource {
    public S3FileSource(ReadonlyConfig readonlyConfig) {
        super(new MultipleTableS3FileSourceConfig(readonlyConfig));
    }
    public String getPluginName() {
        return FileSystemType.S3.getFileSystemPluginName();  // "S3File"
    }
}
```

**4. 对应的 Factory 类：**
- `LocalFileSourceFactory` / `LocalFileSinkFactory`
- `S3FileSourceFactory` / `S3FileSinkFactory`
- `HdfsFileSourceFactory` / `HdfsFileSinkFactory`
- 等等

---

## 8. 文件压缩支持

### 8.1 CompressFormat 枚举

[`CompressFormat.java`](#)

```java
public enum CompressFormat implements Serializable {
    LZO(".lzo",     CompressionKind.LZO,    CompressionCodecName.LZO),           // 通用
    NONE("",        CompressionKind.NONE,   CompressionCodecName.UNCOMPRESSED),   // ORC + Parquet
    SNAPPY(".snappy", CompressionKind.SNAPPY, CompressionCodecName.SNAPPY),       // ORC + Parquet
    LZ4(".lz4",     CompressionKind.LZ4,    CompressionCodecName.LZ4),            // ORC + Parquet
    ZLIB(".zlib",   CompressionKind.ZLIB,   CompressionCodecName.UNCOMPRESSED),   // 仅 ORC
    GZIP(".gz",     CompressionKind.NONE,   CompressionCodecName.GZIP),           // 仅 Parquet
    BROTLI(".br",   CompressionKind.NONE,   CompressionCodecName.BROTLI),         // 仅 Parquet
    ZSTD(".zstd",   CompressionKind.NONE,   CompressionCodecName.ZSTD);           // 仅 Parquet
}
```

**关键设计：** CompressFormat 维护了双重视图 — 同时映射到 ORC 的 `CompressionKind` 和 Parquet 的 `CompressionCodecName`，因为这两个库使用不同的压缩枚举体系。

### 8.2 压缩格式支持矩阵

| 压缩格式 | 后缀 | TEXT/JSON | CSV | ORC | Parquet |
|---------|------|-----------|-----|-----|---------|
| NONE | (无) | ✅ | ✅ | ✅ | ✅ |
| LZO | .lzo | ✅ | ✅ | ✅ | ✅ |
| SNAPPY | .snappy | ❌ | ❌ | ✅ | ✅ |
| LZ4 | .lz4 | ❌ | ❌ | ✅ | ✅ |
| ZLIB | .zlib | ❌ | ❌ | ✅ | ❌ |
| GZIP | .gz | ❌ | ❌ | ❌ | ✅ |
| BROTLI | .br | ❌ | ❌ | ❌ | ✅ |
| ZSTD | .zstd | ❌ | ❌ | ❌ | ✅ |

### 8.3 ArchiveCompressFormat — 归档压缩

[`ArchiveCompressFormat.java`](#)

支持 ZIP、TAR、TAR_GZ（tar.gz）、GZ 四种归档格式。在 `AbstractReadStrategy.resolveArchiveCompressedInputStream()` 中透明解压，提取其中的文件后逐个读取。

---

## 9. 总结与关键设计模式

### 9.1 设计模式

| 模式 | 体现 |
|------|------|
| **策略模式** | `ReadStrategy` / `WriteStrategy` — 不同文件格式的读写策略；`FileSplitStrategy` — 不同文件的分割策略 |
| **工厂模式** | `ReadStrategyFactory` / `WriteStrategyFactory` / `FileSplitStrategyFactory` — 根据配置动态创建对应策略 |
| **桥接模式** | `FileFormat` 枚举 — 连接格式标识与其对应的 ReadStrategy 和 WriteStrategy |
| **代理模式** | `HadoopFileSystemProxy` — 对 Hadoop `FileSystem` 做一层代理，统一处理 Kerberos/RemoteUser 认证 |
| **模板方法** | `AbstractReadStrategy` / `AbstractWriteStrategy` — 提供初始化和生命周期管理，子类只需实现核心读写方法 |
| **两阶段提交** | `Transaction` / `FileSinkAggregatedCommitter` — 先写临时文件，checkpoint 完成后 rename 到最终位置 |
| **组合策略** | `MultipleTableFileSplitStrategy` — 内部持有 `Map<String, FileSplitStrategy>`，根据 tableId 路由到具体策略 |

### 9.2 核心亮点

1. **多平台统一抽象**：基于 Hadoop FileSystem API，一套代码支持 Local/HDFS/S3/OSS/FTP/SFTP/COS/OBS 等 9 种存储后端，新增平台只需实现 `HadoopConf` 子类 + SPI 注册
2. **格式无关的读写策略**：`FileFormat` 枚举作为桥接器，通过 `ReadStrategyFactory` / `WriteStrategyFactory` 自动创建对应的读写策略，新增格式只需添加枚举值和对应的 Strategy 实现
3. **智能文件分割**：`AccordingToSplitSizeSplitStrategy` 和 `ParquetFileSplitStrategy` 分别面向文本格式和列式格式，以行分隔符/RowGroup 作为最小不可分割单元，保证分割后的数据完整性
4. **Exactly-Once 语义**：通过两阶段提交（写临时目录 → checkpoint 后 rename）实现端到端的精确一次语义，支持失败恢复
5. **增量同步模式**：`AbstractReadStrategy` 支持 `sync_mode=update` 的增量文件同步，可使用 len+mtime（DISTCP 策略）或 checksum（STRICT 策略）比较文件，避免重复同步
6. **Kerberos 长期作业支持**：`HadoopFileSystemProxy` 的 `maybeRelogin()` 在每次文件操作前自动续期 Kerberos ticket
7. **归档文件透明读取**：支持 ZIP/TAR/TAR_GZ/GZ 四种归档格式的透明解压，对上层 Reader 完全无感

### 9.3 架构演进

File Connector 经历了从 `BaseFileSource`/`BaseFileSink`（单表）到 `BaseMultipleTableFileSource`/`BaseMultipleTableFileSink`（多表）的架构演进。新版的 `BaseMultipleTableFileSource` 通过 `BaseMultipleTableFileSourceConfig` 支持多表配置，每张表可以有不同的文件格式、路径和分割策略，通过 `MultipleTableFileSourceSplitEnumerator` 统一分片。

### 9.4 可改进方向

1. **ReadStrategy 与 Split 策略的耦合**：分割策略（`FileSplitStrategy`）返回 Split 后，Reader 需要理解 Split 含义才能正确读取。当前 `CsvReadStrategy` 和 `ParquetReadStrategy` 各自处理 Split 的 offset/length，可考虑定义更明确的 `SplittableReadStrategy` 子接口
2. **WriteStrategy 写入性能**：`AbstractWriteStrategy.write()` 中 synchronized 的 `newFilePart()` 在高并发场景可能是瓶颈
3. **压缩格式对文本文件的支持**：当前 CompressFormat 中只有 LZO 对 text/json/csv 可用，其他压缩（snappy/lz4/gzip/zstd）不能用于文本文件