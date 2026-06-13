# 五、核心扩展机制 (二次开发/整改核心)

## 1. Connector V2 API 扩展体系

### 1.1 核心接口体系

**接口路径**: `seatunnel-api/src/main/java/org/apache/seatunnel/api/`

```
Factory (SPI入口)
├── TableSourceFactory → createSource() → SeaTunnelSource
│   └── SeaTunnelSource<T, SplitT, StateT>
│       ├── SourceSplitEnumerator<SplitT, StateT>  // 分片枚举
│       ├── SourceReader<T, SplitT>                 // 数据读取
│       └── Serializer<SplitT> / Serializer<StateT> // 序列化
├── TableSinkFactory → createSink() → SeaTunnelSink
│   └── SeaTunnelSink<IN, StateT, CommitInfoT, AggregatedCommitInfoT>
│       ├── SinkWriter<IN, CommitInfoT, StateT>     // 数据写入
│       ├── SinkCommitter<CommitInfoT>              // 单节点提交
│       └── SinkAggregatedCommitter<...>            // 聚合提交
└── TableTransformFactory → createTransform() → SeaTunnelTransform
    └── SeaTunnelTransform<T>
        ├── SeaTunnelMapTransform<T>                // 1:1 转换
        └── SeaTunnelFlatMapTransform<T>            // 1:N 转换
```

### 1.2 新增自定义数据源完整步骤

**示例**: 创建 `connector-mydb` 数据源

#### Step 1: 创建 Maven 模块

```
seatunnel-connectors-v2/connector-mydb/
├── pom.xml
└── src/main/java/org/apache/seatunnel/connectors/seatunnel/mydb/
    ├── source/
    │   ├── MyDbSourceFactory.java      // TableSourceFactory 实现
    │   ├── MyDbSource.java             // SeaTunnelSource 实现
    │   ├── MyDbSourceReader.java       // SourceReader 实现
    │   ├── MyDbSourceSplit.java        // SourceSplit 实现
    │   └── MyDbSourceSplitEnumerator.java // SourceSplitEnumerator 实现
    ├── sink/
    │   ├── MyDbSinkFactory.java        // TableSinkFactory 实现
    │   ├── MyDbSink.java              // SeaTunnelSink 实现
    │   └── MyDbSinkWriter.java        // SinkWriter 实现
    └── catalog/
        └── MyDbCatalogFactory.java    // CatalogFactory (可选)
```

#### Step 2: 实现 Factory (SPI 注册入口)

```java
// MyDbSourceFactory.java
@AutoService(Factory.class)  // Google AutoService 自动生成 SPI 文件
public class MyDbSourceFactory implements TableSourceFactory {

    @Override
    public String factoryIdentifier() {
        return "MyDb";  // 配置文件中使用此名称
    }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(MY_DB_URL, USERNAME, PASSWORD)  // 必填参数
                .optional(BATCH_SIZE, TABLE_NAME)         // 可选参数
                .build();
    }

    @Override
    public TableSource createSource(TableSourceFactoryContext context) {
        return () -> new MyDbSource(context.getOptions());
    }

    @Override
    public Class<? extends SeaTunnelSource> getSourceClass() {
        return MyDbSource.class;
    }
}
```

#### Step 3: 实现 SeaTunnelSource

```java
public class MyDbSource implements SeaTunnelSource<SeaTunnelRow, MyDbSourceSplit, MyDbSourceState> {

    @Override
    public String getPluginName() { return "MyDb"; }

    @Override
    public Boundedness getBoundedness() { return Boundedness.BOUNDED; }

    @Override
    public List<CatalogTable> getProducedCatalogTables() { /* 返回表结构 */ }

    @Override
    public SourceReader<SeaTunnelRow, MyDbSourceSplit> createReader(Context ctx) {
        return new MyDbSourceReader(ctx);
    }

    @Override
    public SourceSplitEnumerator<MyDbSourceSplit, MyDbSourceState> createEnumerator(Context ctx) {
        return new MyDbSourceSplitEnumerator(ctx);
    }

    @Override
    public SourceSplitEnumerator<MyDbSourceSplit, MyDbSourceState> restoreEnumerator(Context ctx, MyDbSourceState state) {
        return new MyDbSourceSplitEnumerator(ctx, state);
    }
}
```

#### Step 4: SPI 注册

在 `src/main/resources/META-INF/services/org.apache.seatunnel.api.table.factory.Factory` 中写入:
```
org.apache.seatunnel.connectors.seatunnel.mydb.source.MyDbSourceFactory
org.apache.seatunnel.connectors.seatunnel.mydb.sink.MyDbSinkFactory
```

或使用 `@AutoService(Factory.class)` 注解自动生成。

#### Step 5: 注册到 plugin-mapping.properties

在 `plugin-mapping.properties` 的 `--connectors-v2--` 段添加:
```
connector-mydb
```

#### Step 6: 配置使用

```hocon
source {
  MyDb {
    url = "jdbc:mydb://localhost:3306/test"
    username = "root"
    password = "123"
  }
}
```

### 1.3 关键接口规范

| 接口 | 路径 | 核心方法 | 约束 |
|------|------|----------|------|
| `Factory` | `api/table/factory/Factory.java` | `factoryIdentifier()`, `optionRule()` | 必须唯一标识符 |
| `TableSourceFactory` | `api/table/factory/TableSourceFactory.java` | `createSource()`, `getSourceClass()` | 返回TableSource |
| `TableSinkFactory` | `api/table/factory/TableSinkFactory.java` | `createSink()` | 返回TableSink |
| `SeaTunnelSource` | `api/source/SeaTunnelSource.java` | `getBoundedness()`, `createReader()`, `createEnumerator()`, `restoreEnumerator()` | 必须实现Split模式 |
| `SourceReader` | `api/source/SourceReader.java` | `pollNext()`, `addSplits()`, `handleNoMoreSplitsEvent()` | 非阻塞式读取 |
| `SourceSplitEnumerator` | `api/source/SourceSplitEnumerator.java` | `run()`, `addSplitsBack()`, `registerReader()` | 协调分配Split |
| `SeaTunnelSink` | `api/sink/SeaTunnelSink.java` | `createWriter()`, `createCommitter()`, `createAggregatedCommitter()` | Writer+两阶段提交 |
| `SinkWriter` | `api/sink/SinkWriter.java` | `write()`, `prepareCommit()`, `snapshotState()` | 写入+状态快照 |
| `CatalogTable` | `api/table/catalog/CatalogTable.java` | `getTablePath()`, `getTableSchema()` | 元数据描述 |

---

## 2. Transform 自定义算子开发

### 2.1 开发规范

**路径**: `seatunnel-transforms-v2/src/main/java/org/apache/seatunnel/transform/`

**基类**:
- `AbstractCatalogSupportMapTransform` — 1:1 转换 (Map模式)
- `AbstractCatalogSupportFlatMapTransform` — 1:N 转换 (FlatMap模式)
- `SingleFieldOutputTransform` — 单字段输出
- `MultipleFieldOutputTransform` — 多字段输出

### 2.2 新增 Transform 步骤

#### Step 1: 创建 Transform 类

```java
// MyTransform.java
public class MyTransform extends AbstractCatalogSupportMapTransform {

    @Override
    public String getPluginName() { return "MyTransform"; }

    @Override
    protected SeaTunnelRow transformRow(SeaTunnelRow inputRow) {
        // 转换逻辑
        Object[] values = inputRow.getFields().clone();
        values[outputIndex] = processData(values[inputIndex]);
        return new SeaTunnelRow(values);
    }

    @Override
    protected SeaTunnelRowType transformRowType(SeaTunnelRowType inputRowType) {
        // 输出类型推导
    }
}
```

#### Step 2: 创建 Factory

```java
@AutoService(Factory.class)
public class MyTransformFactory implements TableTransformFactory {
    @Override
    public String factoryIdentifier() { return "MyTransform"; }

    @Override
    public OptionRule optionRule() {
        return OptionRule.builder()
                .required(INPUT_FIELD, OUTPUT_FIELD)
                .build();
    }

    @Override
    public TableTransform createTransform(TableTransformFactoryContext context) {
        return () -> new MyTransform(context.getOptions(), context.getCatalogTable());
    }
}
```

#### Step 3: SPI 注册

文件: `META-INF/services/org.apache.seatunnel.api.table.factory.Factory`
```
org.apache.seatunnel.transform.my.MyTransformFactory
```

---

## 3. 引擎层扩展点

### 3.1 自定义引擎适配方式

**现有适配层结构**:
```
seatunnel-translation/
├── seatunnel-translation-base/     ← 定义通用抽象
│   ├── ParallelSource              ← 并行Source执行模型
│   ├── CoordinatedSource           ← 协调Source执行模型
│   ├── SinkConverter               ← Sink转换接口
│   └── RowConverter                ← 行数据转换
├── seatunnel-translation-flink/    ← Flink适配
└── seatunnel-translation-spark/    ← Spark适配
```

**适配新引擎的步骤**:
1. 创建 `seatunnel-translation-<engine>/` 模块
2. 实现 `SinkConverter` → 将 SeaTunnelSink 转为引擎原生 Sink
3. 实现 `SinkWriterConverter` → 将 SinkWriter 转为引擎原生 Writer
4. 实现 Source 适配: 将 `ParallelSource`/`CoordinatedSource` 适配为引擎 Source
5. 创建引擎 Starter: `seatunnel-core/<engine>-starter/`
6. 实现 `RuntimeEnvironment` + `PluginExecuteProcessor` (Source/Sink/Transform)

### 3.2 引擎层面关键扩展点

| 扩展点 | 位置 | 说明 |
|--------|------|------|
| Source 执行模型 | `translation-base/.../source/` | ParallelSource vs CoordinatedSource |
| Sink 转换 | `translation-base/.../sink/SinkConverter.java` | 引擎特定的 Sink 包装 |
| 数据序列化 | `translation-base/.../serialization/RowConverter.java` | 引擎特定的 Row 转换 |
| 运行时环境 | `core-starter/.../execution/RuntimeEnvironment.java` | 引擎初始化 |
| 插件执行 | `core-starter/.../execution/PluginExecuteProcessor.java` | 引擎特定的 DAG 构建 |

---

## 4. 插件加载与 SPI 机制实现

### 4.1 SPI 机制详解

**核心实现**: `seatunnel-plugin-discovery/.../AbstractPluginDiscovery.java`

**加载流程**:
```
1. 扫描 pluginDir (SEATUNNEL_HOME/connectors/) 下所有 JAR
2. 读取 plugin-mapping.properties 获取 pluginName → artifactId 映射
3. 根据 PluginIdentifier 查找对应 JAR
4. URLClassLoader 加载 JAR
5. ServiceLoader.load(Factory.class, classLoader) 发现所有 Factory
6. 匹配 factoryIdentifier() 找到目标 Factory
7. 调用 Factory.createSource/Sink/Transform 创建实例
```

### 4.2 ClassLoader 策略

| 场景 | ClassLoader | 说明 |
|------|------------|------|
| 开发模式 | Thread ContextClassLoader | 插件已在 classpath |
| 生产模式 | URLClassLoader | 从 pluginDir 动态加载 |
| Zeta 引擎 | SeaTunnelChildFirstClassLoader | 子优先加载，避免冲突 |
| 上传模式 | 客户端上传 JAR → Master 分配 | connectorJarStorage 模式 |

---

## 5. 模块稳定性分类

### 框架核心稳定模块 (禁止/谨慎修改)
| 模块 | 原因 |
|------|------|
| `seatunnel-api/` | API 兼容性，所有插件依赖此接口 |
| `seatunnel-common/` | 公共工具，全局影响 |
| `seatunnel-translation-base/` | 引擎适配基础，三引擎共用 |
| `seatunnel-engine-core/` (DAG模型) | Zeta 引擎核心数据结构 |

### 可优化的中间层
| 模块 | 可优化方向 |
|------|-----------|
| `seatunnel-plugin-discovery/` | 插件加载性能、缓存优化 |
| `seatunnel-core-starter/` | ConfigBuilder 解析效率 |
| `seatunnel-engine-server/` | 调度策略、资源分配算法 |
| `seatunnel-translation-flink/` | Flink 适配效率 |
| `seatunnel-translation-spark/` | Spark 适配效率 |

### 插件化扩展层 (可自由扩展)
| 模块 | 说明 |
|------|------|
| `seatunnel-connectors-v2/` | 新增 Connector 完全独立 |
| `seatunnel-transforms-v2/` | 新增 Transform 完全独立 |
| `seatunnel-formats/` | 新增数据格式 |
