# Apache SeaTunnel 配置解析与 DAG 编排机制深度源码分析

## 目录

1. [总体架构概览](#1-总体架构概览)
2. [HOCON 配置解析完整流程 - ConfigBuilder](#2-hocon-配置解析完整流程---configbuilder)
3. [变量替换与占位符机制](#3-变量替换与占位符机制)
4. [多表配置合并 - MultipleTableJobConfigParser](#4-多表配置合并---multipletablejobconfigparser)
5. [DAG 构建 - Source/Sink/Transform 拓扑](#5-dag-构建---sourcetransformsink-拓扑)
6. [物理计划生成 - 逻辑计划到物理计划转换](#6-物理计划生成---逻辑计划到物理计划转换)
7. [三引擎适配差异](#7-三引擎适配差异)
8. [总结与关键设计模式](#8-总结与关键设计模式)

---

## 1. 总体架构概览

### 1.1 配置与 DAG 全链路处理流程

```
用户配置文件(.conf/.yaml/.json)
        │
        ▼
┌─────────────────────────────┐
│  ConfigBuilder              │  ← HOCP 解析 + ConfigAdapter SPI
│  (seatunnel-core-starter)   │
└─────────────┬───────────────┘
              │ Config (Typesafe Config对象)
              ▼
┌─────────────────────────────┐
│  MultipleTableJobConfigPars │  ← 多表合并 + Action 生成
│  (seatunnel-engine-core)    │
└─────────────┬───────────────┘
              │ List<Action>
              ▼
┌─────────────────────────────┐
│  LogicalDagGenerator        │  ← 逻辑 DAG 构建
│  (seatunnel-engine-core)    │
└─────────────┬───────────────┘
              │ LogicalDag
              ▼
┌─────────────────────────────┐
│  ExecutionPlanGenerator     │  ← Transform 链合并 + Pipeline 拆分
│  (seatunnel-engine-server)  │
└─────────────┬───────────────┘
              │ ExecutionPlan
              ▼
┌─────────────────────────────┐
│  PhysicalPlanGenerator      │  ← 物理任务生成 (TaskGroup)
│  (seatunnel-engine-server)  │
└─────────────┬───────────────┘
              │ PhysicalPlan
              ▼
        任务调度执行
```

### 1.2 关键源文件索引

| 模块 | 文件路径 | 职责 |
|------|---------|------|
| 核心配置 | `seatunnel-core/seatunnel-core-starter/.../utils/ConfigBuilder.java` | HOCON 解析入口 |
| 配置适配 | `seatunnel-core/seatunnel-core-starter/.../utils/ConfigAdapterUtils.java` | 多格式 SPI 适配 |
| 配置加密 | `seatunnel-core/seatunnel-core-starter/.../utils/ConfigShadeUtils.java` | 敏感字段加解密 |
| 多表解析 | `seatunnel-engine/seatunnel-engine-core/.../parse/MultipleTableJobConfigParser.java` | 多表 Action 生成 |
| 图校验 | `seatunnel-engine/seatunnel-engine-core/.../parse/ConfigParserUtil.java` | DAG 合法性校验 |
| Action 命名 | `seatunnel-engine/seatunnel-engine-core/.../parse/JobConfigParser.java` | Action 名称生成 |
| 逻辑 DAG | `seatunnel-engine/seatunnel-engine-core/.../dag/logical/LogicalDagGenerator.java` | Action → LogicalDag |
| 逻辑 DAG 模型 | `seatunnel-engine/seatunnel-engine-core/.../dag/logical/LogicalDag.java` | 逻辑 DAG 数据结构 |
| 执行计划生成 | `seatunnel-engine/seatunnel-engine-server/.../dag/execution/ExecutionPlanGenerator.java` | Transform 链 + Pipeline |
| Pipeline 拆分 | `seatunnel-engine/seatunnel-engine-server/.../dag/execution/PipelineGenerator.java` | Pipeline 拆分逻辑 |
| 物理计划生成 | `seatunnel-engine/seatunnel-engine-server/.../dag/physical/PhysicalPlanGenerator.java` | TaskGroup 生成 |
| 物理计划入口 | `seatunnel-engine/seatunnel-engine-server/.../dag/physical/PlanUtils.java` | LogicalDag → PhysicalPlan |
| Flink 引擎 | `seatunnel-core/seatunnel-flink-starter/.../flink/execution/FlinkExecution.java` | Flink 执行 |
| Spark 引擎 | `seatunnel-core/seatunnel-spark-starter/.../spark/execution/SparkExecution.java` | Spark 执行 |
| 客户端环境 | `seatunnel-engine/seatunnel-engine-client/.../job/ClientJobExecutionEnvironment.java` | SeaTunnel Engine 客户端 |

---

## 2. HOCON 配置解析完整流程 - ConfigBuilder

### 2.1 入口方法签名

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-core/seatunnel-core-starter/src/main/java/org/apache/seatunnel/core/starter/utils/ConfigBuilder.java`

ConfigBuilder 是一个纯工具类（第54行），通过静态方法提供配置解析能力。核心入口有以下几种形式：

```java
// 第82-84行：无变量替换入口
public static Config of(@NonNull Path filePath) {
    return of(filePath, null);
}

// 第86-100行：带变量替换入口（核心）
public static Config of(@NonNull Path filePath, List<String> variables) {
    log.info("Loading config file from path: {}", filePath);
    Optional<ConfigAdapter> adapterSupplier = ConfigAdapterUtils.selectAdapter(filePath);
    Config config =
            adapterSupplier
                    .map(adapter -> of(adapter, filePath, variables))
                    .orElseGet(() -> ofInner(filePath, variables));
    // ... 日志输出（含脱敏处理）
    return config;
}

// 第106-124行：Map 对象入口（REST API 使用）
public static Config of(@NonNull Map<String, Object> objectMap, boolean isEncrypt) {
    Config config =
            ConfigFactory.parseMap(objectMap)
                    .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true))
                    .resolveWith(ConfigFactory.systemProperties(),
                            ConfigResolveOptions.defaults().setAllowUnresolved(true));
    if (!isEncrypt) {
        config = ConfigShadeUtils.decryptConfig(config);
    }
    return config;
}
```

### 2.2 两种解析路径

#### 路径一：ConfigAdapter SPI 适配（第176-191行）

```java
public static Config of(@NonNull ConfigAdapter configAdapter, @NonNull Path filePath, List<String> variables) {
    log.info("With config adapter spi {}", configAdapter.getClass().getName());
    try {
        Map<String, Object> flattenedMap = configAdapter.loadConfig(filePath);
        Config config = ConfigFactory.parseMap(flattenedMap);
        return ConfigShadeUtils.decryptConfig(backfillUserVariables(config, variables));
    } catch (ParserException | IllegalArgumentException e) {
        throw e;
    } catch (Exception warn) {
        log.warn("Loading config failed with spi {}, fallback to HOCON loader.", ...);
        return ofInner(filePath, variables);
    }
}
```

**流程**：
1. 通过 `ConfigAdapterUtils.selectAdapter()` 根据文件扩展名选择适配器（SPI机制）
2. 适配器将配置文件转为 `Map<String, Object>` 扁平化Map
3. `ConfigFactory.parseMap()` 转为 Typesafe Config 对象
4. 进入变量替换和解密流程
5. 如果适配器解析失败，自动回退到 HOCON 解析

#### 路径二：HOCON 原生解析（第65-70行）

```java
private static Config ofInner(@NonNull Path filePath, List<String> variables) {
    Config config =
            ConfigFactory.parseFile(filePath.toFile())
                    .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
    return ConfigShadeUtils.decryptConfig(backfillUserVariables(config, variables));
}
```

**流程**：
1. `ConfigFactory.parseFile()` 直接解析 HOCON 格式文件
2. `resolve()` 配置解析，支持 `${}` 引用（设置 `setAllowUnresolved(true)` 容忍未解析变量）
3. 进入变量替换和解密流程

### 2.3 ConfigAdapter SPI 机制

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-core/seatunnel-core-starter/src/main/java/org/apache/seatunnel/core/starter/utils/ConfigAdapterUtils.java`

```java
// 第39-43行：静态初始化加载所有 ConfigAdapter
static {
    ServiceLoader<ConfigAdapter> serviceLoader = ServiceLoader.load(ConfigAdapter.class);
    Iterator<ConfigAdapter> it = serviceLoader.iterator();
    it.forEachRemaining(CONFIG_ADAPTERS::add);
}

// 第45-56行：根据文件扩展名选择适配器
public static Optional<ConfigAdapter> selectAdapter(@NonNull String filePath) {
    for (ConfigAdapter configAdapter : CONFIG_ADAPTERS) {
        String extension = FileUtils.getFileExtension(filePath);
        for (String extensionIdentifier :
                ArrayUtils.nullToEmpty(configAdapter.extensionIdentifiers())) {
            if (StringUtils.equalsIgnoreCase(extension, extensionIdentifier)) {
                return Optional.of(configAdapter);
            }
        }
    }
    return Optional.empty();
}
```

通过 Java SPI 机制加载 `ConfigAdapter` 实现，支持用户自定义配置格式（如 YAML、JSON、TOML 等）。如果文件扩展名没有匹配的适配器，返回 `Optional.empty()`，回退到 HOCON 解析。

### 2.4 解析完整调用链

```
ConfigBuilder.of(path, variables)
  ├── ConfigAdapterUtils.selectAdapter(path)     // 选择 ConfigAdapter
  ├── [有适配器] ConfigBuilder.of(adapter, path, variables)
  │     ├── adapter.loadConfig(path)             // 加载为 Map
  │     ├── ConfigFactory.parseMap(map)          // 转为 Config
  │     ├── backfillUserVariables(config, vars)  // 变量替换
  │     └── ConfigShadeUtils.decryptConfig()     // 解密
  └── [无适配器] ConfigBuilder.ofInner(path, variables)
        ├── ConfigFactory.parseFile(file)        // HOCON 原生解析
        ├── .resolve(...)                        // 引用解析
        ├── backfillUserVariables(config, vars)  // 变量替换
        └── ConfigShadeUtils.decryptConfig()     // 解密
```

---

## 3. 变量替换与占位符机制

SeaTunnel 有两层占位符体系：**用户变量占位符**和**系统表级占位符**。

### 3.1 用户变量占位符（`${variable_name}`）

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-core/seatunnel-core-starter/src/main/java/org/apache/seatunnel/core/starter/utils/ConfigBuilder.java`

#### 3.1.1 占位符正则（第59行）

```java
private static final String PLACEHOLDER_REGEX = "\\$\\{([^:{}]+)(?::[^}]*)?\\}";
```

匹配模式：`${variable_name}` 或 `${variable_name:default_value}`

- `[^:{}]+`：变量名不包含 `:`、`{`、`}`
- `(?::[^}]*)?`：可选的默认值部分，以 `:` 分隔

#### 3.1.2 变量回填流程（第193-238行）

```java
private static Config backfillUserVariables(Config config, List<String> variables) {
    if (variables != null) {
        // Step 1: 将变量写入 System Properties
        variables.stream()
                .filter(Objects::nonNull)
                .map(variable -> variable.split("=", 2))
                .filter(pair -> pair.length == 2)
                .peek(pair -> {
                    if (TablePlaceholder.isSystemPlaceholder(pair[0])) {
                        throw new ConfigCheckException("System placeholders cannot be used...");
                    }
                })
                .forEach(pair -> System.setProperty(pair[0], pair[1]));

        // Step 2: 用 System Properties 解析 Config 中的引用
        Config systemConfig = Parseable.newProperties(System.getProperties(), ...).parse().toConfig();
        Config resolvedConfig = config.resolveWith(systemConfig,
                ConfigResolveOptions.defaults().setAllowUnresolved(true));

        // Step 3: 递归处理嵌套 Map 中的占位符
        Map<String, Object> configMap = resolvedConfig.root().unwrapped();
        configMap.forEach((key, value) -> {
            if (value instanceof Map) {
                processVariablesMap((Map<String, Object>) value);
            } else if (value instanceof List) {
                ((List<Map<String, Object>>) value).forEach(map -> processVariablesMap(map));
            }
        });

        // Step 4: 序列化为 JSON 重新解析
        return ConfigFactory.parseString(
                JsonUtils.toJsonString(configMap),
                ConfigParseOptions.defaults().setSyntax(ConfigSyntax.JSON))
                .resolve(ConfigResolveOptions.defaults().setAllowUnresolved(true));
    }
    return config;
}
```

**关键安全检查**（第199-205行）：用户变量不能使用系统保留的表级占位符名称（如 `table_name`、`primary_key` 等），通过 `TablePlaceholder.isSystemPlaceholder()` 检查。

#### 3.1.3 嵌套值处理（第240-298行）

`processVariablesMap()` 和 `processVariable()` 递归遍历配置树，对每个字符串值执行占位符替换：

```java
// 第300-310行：提取所有占位符名
public static List<String> extractPlaceholder(String input) {
    Pattern pattern = Pattern.compile(PLACEHOLDER_REGEX);
    Matcher matcher = pattern.matcher(input);
    List<String> placeholders = new ArrayList<>();
    while (matcher.find()) {
        placeholders.add(matcher.group(1));
    }
    return placeholders;
}

// 第280-298行：替换每个占位符
private static void processVariable(String variableKey, Object variableValue, Map<String, Object> parentMap) {
    String variableString = variableValue.toString();
    List<String> placeholders = extractPlaceholder(variableString);
    for (String placeholder : placeholders) {
        String replacedValue = replacePlaceholders(variableString, placeholder,
                System.getProperty(placeholder), null);
        variableString = replacedValue;
    }
    if (!placeholders.isEmpty()) {
        parentMap.put(variableKey, variableString);
    }
}
```

### 3.2 系统表级占位符（`${table_name}` 等）

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-api/src/main/java/org/apache/seatunnel/api/sink/TablePlaceholder.java`

```java
public enum TablePlaceholder {
    REPLACE_DATABASE_NAME_KEY("database_name"),
    REPLACE_SCHEMA_NAME_KEY("schema_name"),
    REPLACE_SCHEMA_FULL_NAME_KEY("schema_full_name"),
    REPLACE_TABLE_NAME_KEY("table_name"),
    REPLACE_TABLE_FULL_NAME_KEY("table_full_name"),
    REPLACE_PRIMARY_KEY("primary_key"),
    REPLACE_UNIQUE_KEY("unique_key"),
    REPLACE_FIELD_NAMES_KEY("field_names"),
    REPLACE_PARTITION_KEYS_KEY("partition_keys");
    // ...
    public static boolean isSystemPlaceholder(String str) {
        return PLACEHOLDER_KEYS.contains(str);
    }
}
```

### 3.3 PlaceholderUtils 占位符替换引擎

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-common/src/main/java/org/apache/seatunnel/common/utils/PlaceholderUtils.java`

```java
// 第31-52行：核心替换方法
public static String replacePlaceholders(String input, String placeholderName, String value, String defaultValue) {
    String placeholderRegex = "\\$\\{" + Pattern.quote(placeholderName) + "(:[^}]*)?\\}";
    Pattern pattern = Pattern.compile(placeholderRegex);
    Matcher matcher = pattern.matcher(input);
    StringBuffer result = new StringBuffer();
    while (matcher.find()) {
        String replacement = value != null && !value.isEmpty()
                ? value
                : (matcher.group(1) != null ? matcher.group(1).substring(1).trim() : defaultValue);
        if (replacement == null) { continue; }
        matcher.appendReplacement(result, Matcher.quoteReplacement(replacement));
    }
    matcher.appendTail(result);
    return result.toString();
}
```

**替换逻辑**：优先使用传入的 `value`；若为空则使用 `${name:default_value}` 中的默认值；否则保留原样。

### 3.4 TablePlaceholderProcessor 表级占位符处理

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-api/src/main/java/org/apache/seatunnel/api/sink/TablePlaceholderProcessor.java`

在 Sink 创建时，根据 CatalogTable 元数据替换占位符（第158-221行）：

```java
public static ReadonlyConfig replaceTablePlaceholder(ReadonlyConfig config, CatalogTable table, Collection<String> excludeKeys) {
    Map<String, Object> copyOnWriteData = ObjectUtils.clone(config.getSourceMap());
    for (String key : copyOnWriteData.keySet()) {
        if (excludeKeys.contains(key)) continue;
        Object value = copyOnWriteData.get(key);
        if (value instanceof String) {
            String strValue = (String) value;
            strValue = replaceTableIdentifier(strValue, table.getTableId());
            strValue = replaceTablePrimaryKey(strValue, table.getTableSchema().getPrimaryKey());
            strValue = replaceTableUniqueKey(strValue, table.getTableSchema().getConstraintKeys());
            strValue = replaceTableFieldNames(strValue, table.getTableSchema());
            strValue = replaceTablePartitionKeys(strValue, table.getPartitionKeys());
            copyOnWriteData.put(key, strValue);
        }
        // ... List 类型处理（primary_key/unique_key/field_names 特殊展开为列表）
    }
    return ReadonlyConfig.fromMap(copyOnWriteData);
}
```

**支持的表级占位符**：

| 占位符 | 含义 | 示例值 |
|--------|------|--------|
| `${database_name}` | 数据库名 | `mydb` |
| `${schema_name}` | Schema 名 | `myschema` |
| `${schema_full_name}` | Schema 完整路径 | `mydb.myschema` |
| `${table_name}` | 表名 | `users` |
| `${table_full_name}` | 表完整路径 | `mydb.myschema.users` |
| `${primary_key}` | 主键字段 | `id` |
| `${unique_key}` | 唯一键字段 | `id,email` |
| `${field_names}` | 所有字段名 | `id,name,email` |
| `${partition_keys}` | 分区键 | `dt,hr` |

### 3.5 配置加密/解密（ConfigShade）

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-core/seatunnel-core-starter/src/main/java/org/apache/seatunnel/core/starter/utils/ConfigShadeUtils.java`

```java
// 第100-116行：配置解密入口
public static Config decryptConfig(Config config) {
    String identifier = TypesafeConfigUtils.getConfig(
            config.hasPath(Constants.ENV) ? config.getConfig(Constants.ENV) : ConfigFactory.empty(),
            SHADE_IDENTIFIER_OPTION, DEFAULT_SHADE.getIdentifier());
    Map<String, Object> props = TypesafeConfigUtils.getConfig(
            config.hasPath(Constants.ENV) ? config.getConfig(Constants.ENV) : ConfigFactory.empty(),
            SHADE_PROPS_OPTION, new HashMap<>());
    return decryptConfig(identifier, config, props);
}
```

通过 SPI 加载 `ConfigShade` 实现（第62-68行），内置 `Base64ConfigShade`（第224-246行）。解密范围仅覆盖 `source`、`sink`、`transform` 配置中的敏感字段（第187-204行）。

默认敏感字段（第54-55行）：
```java
public static final String[] DEFAULT_SENSITIVE_KEYWORDS =
        new String[] {"password", "username", "auth", "token", "access_key", "secret_key"};
```

---

## 4. 多表配置合并 - MultipleTableJobConfigParser

### 4.1 类概述

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/parse/MultipleTableJobConfigParser.java`

这是 SeaTunnel Engine 中最核心的配置解析器，负责将 HOCON 配置转换为 Action 列表（即 DAG 的节点）。

### 4.2 构造函数

```java
// 第164-182行：完整参数构造器
public MultipleTableJobConfigParser(
        String jobDefineFilePath,
        List<String> variables,
        IdGenerator idGenerator,
        JobConfig jobConfig,
        List<URL> commonPluginJars,
        boolean isStartWithSavePoint,
        List<JobPipelineCheckpointData> pipelineCheckpoints) {
    this.idGenerator = idGenerator;
    this.jobConfig = jobConfig;
    this.commonPluginJars = commonPluginJars;
    this.isStartWithSavePoint = isStartWithSavePoint;
    this.seaTunnelJobConfig =
            MetalakeConfigUtils.getMetalakeConfig(
                    ConfigBuilder.of(Paths.get(jobDefineFilePath), variables));
    this.envOptions = ReadonlyConfig.fromConfig(seaTunnelJobConfig.getConfig("env"));
    this.pipelineCheckpoints = pipelineCheckpoints;
    ConfigValidator.of(this.envOptions).validate(new EnvOptionRule().optionRule());
}
```

**初始化流程**：
1. 调用 `ConfigBuilder.of()` 解析配置文件
2. 通过 `MetalakeConfigUtils.getMetalakeConfig()` 处理 Metalake 配置合并
3. 提取 `env` 环境配置
4. 使用 `ConfigValidator` 校验环境配置规则

### 4.3 核心解析方法 `parse()`（第201-273行）

```java
public ImmutablePair<List<Action>, Set<URL>> parse(ClassLoaderService classLoaderService) {
    // Step 1: 填充 JobConfig 和公共 JAR
    this.fillJobConfigAndCommonJars();

    // Step 2: 提取 source/transform/sink 配置列表
    List<? extends Config> sourceConfigs =
            TypesafeConfigUtils.getConfigList(seaTunnelJobConfig, "source", Collections.emptyList());
    List<? extends Config> transformConfigs =
            TypesafeConfigUtils.getConfigList(seaTunnelJobConfig, "transform", Collections.emptyList());
    List<? extends Config> sinkConfigs =
            TypesafeConfigUtils.getConfigList(seaTunnelJobConfig, "sink", Collections.emptyList());

    // Step 3: 加载插件 JAR 并创建类加载器
    List<URL> sourceJars = Stream.of(sourceConnectorJars, transformConnectorJars)
            .flatMap(Collection::stream).distinct().collect(Collectors.toList());
    ClassLoader sourceAndTransformClassLoader = getClassLoader(classLoaderService, parentClassLoader, sourceJars);
    ClassLoader sinkClassLoader = getClassLoader(classLoaderService, parentClassLoader, sinkConnectorJars);

    // Step 4: 校验 DAG 合法性
    ConfigParserUtil.checkGraph(sourceConfigs, transformConfigs, sinkConfigs);

    // Step 5: 解析 Source → Transform → Sink，构建 tableWithActionMap
    LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap = new LinkedHashMap<>();

    // 5a: 解析所有 Source
    for (int configIndex = 0; configIndex < sourceConfigs.size(); configIndex++) {
        Tuple2<String, List<Tuple2<CatalogTable, Action>>> tuple2 =
                parseSource(configIndex, sourceConfigs.get(configIndex), sourceAndTransformClassLoader);
        tableWithActionMap.put(tuple2._1(), tuple2._2());
    }

    // 5b: 解析所有 Transform
    parseTransforms(transformConfigs, sourceAndTransformClassLoader, tableWithActionMap);

    // 5c: 解析所有 Sink
    List<Action> sinkActions = new ArrayList<>();
    for (int configIndex = 0; configIndex < sinkConfigs.size(); configIndex++) {
        sinkActions.addAll(parseSink(configIndex, sinkConfigs.get(configIndex), sinkClassLoader, tableWithActionMap));
    }

    return new ImmutablePair<>(sinkActions, factoryUrls);
}
```

### 4.4 tableWithActionMap 核心数据结构

`tableWithActionMap` 是整个配置解析的核心数据结构：

```
类型: LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>>
  ├── key:   表ID（plugin_output 或默认 "default"）
  └── value: 该表对应的所有 (CatalogTable, Action) 对
```

**数据流向**：
1. `parseSource()` 为每个 source 创建 Action，以 `plugin_output` 为 key 存入
2. `parseTransform()` 从 `plugin_input` 查找上游数据，处理后以新的 `plugin_output` 为 key 存入
3. `parseSink()` 从 `plugin_input` 查找上游数据，创建 Sink Action

### 4.5 Source 解析（第367-417行）

```java
public Tuple2<String, List<Tuple2<CatalogTable, Action>>> parseSource(
        int configIndex, Config sourceConfig, ClassLoader classLoader) {
    final ReadonlyConfig readonlyConfig = ReadonlyConfig.fromConfig(sourceConfig);
    final String factoryId = getFactoryId(readonlyConfig);           // 插件名
    final String tableId = readonlyConfig.getOptional(ConnectorCommonOptions.PLUGIN_OUTPUT)
            .orElse(DEFAULT_ID);                                     // 输出表ID
    final int parallelism = getParallelism(readonlyConfig);          // 并行度

    // 创建 Source 实例
    Tuple2<SeaTunnelSource, List<CatalogTable>> tuple2;
    if (isStartWithSavePoint && ...) {
        tuple2 = FactoryUtil.restoreAndPrepareSource(...);           // 从 SavePoint 恢复
    } else {
        tuple2 = FactoryUtil.createAndPrepareSource(...);            // 新建 Source
    }

    // 包装为 SourceAction
    SourceAction action = new SourceAction(id, actionName, source, factoryUrls, new HashSet<>());
    action.setParallelism(parallelism);

    // 为每个 CatalogTable 生成一条记录
    List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
    for (CatalogTable catalogTable : tuple2._2()) {
        actions.add(new Tuple2<>(catalogTable, action));
    }
    return new Tuple2<>(tableId, actions);
}
```

### 4.6 Transform 解析（第419-505行）

```java
public void parseTransforms(List<? extends Config> transformConfigs, ClassLoader classLoader,
        LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
    Queue<Config> configList = new LinkedList<>(transformConfigs);
    int index = 0;
    while (!configList.isEmpty()) {
        parseTransform(index++, configList, classLoader, tableWithActionMap);
    }
}
```

**Transform 解析的关键逻辑**：

```java
private void parseTransform(int index, Queue<Config> transforms, ClassLoader classLoader,
        LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
    Config config = transforms.poll();
    final List<String> inputIds = getInputIds(readonlyConfig);

    // 查找上游 Action
    List<Tuple2<CatalogTable, Action>> inputs =
            inputIds.stream().map(tableWithActionMap::get).filter(Objects::nonNull)
                    .flatMap(Collection::stream).collect(Collectors.toList());

    if (inputs.isEmpty()) {
        if (transforms.isEmpty()) {
            inputs = findLast(tableWithActionMap);   // 兼容简单图：自动匹配最后一个
        } else {
            transforms.offer(config);                // 上游 Transform 还未创建，重新入队
            return;
        }
    }

    // 创建 Transform 实例
    SeaTunnelTransform<?> transform =
            FactoryUtil.createAndPrepareMultiTableTransform(catalogTables, readonlyConfig, classLoader, factoryId);

    // 包装为 TransformAction
    TransformAction transformAction = new TransformAction(id, actionName, inputActions, transform, jarUrls, new HashSet<>());

    // 输出写入 tableWithActionMap
    List<Tuple2<CatalogTable, Action>> actions = new ArrayList<>();
    for (CatalogTable catalogTable : producedCatalogTables) {
        actions.add(new Tuple2<>(catalogTable, transformAction));
    }
    tableWithActionMap.put(tableId, actions);
}
```

**Transform 解析的特殊机制**：
- **重新入队**（第458行）：如果上游 Transform 尚未创建，将当前 Transform 重新放入队列，等待上游创建后重试
- **自动匹配**（第455行）：对于简单图（无 `plugin_input` 配置），自动使用 `findLast()` 匹配最后一个上游
- **类型一致性校验**（第475行）：`checkProducedTypeEquals()` 确保所有输入 Action 的产出类型相同

### 4.7 Sink 解析（第553-631行）

```java
public List<SinkAction<?, ?, ?, ?>> parseSink(int configIndex, Config sinkConfig,
        ClassLoader classLoader, LinkedHashMap<String, List<Tuple2<CatalogTable, Action>>> tableWithActionMap) {
    List<String> inputIds = getInputIds(readonlyConfig);
    List<List<Tuple2<CatalogTable, Action>>> inputVertices =
            inputIds.stream().map(tableWithActionMap::get).filter(Objects::nonNull).collect(Collectors.toList());

    // Union 模式：多个 Source/Transform 同时写入一个 Sink
    if (inputVertices.size() > 1) {
        // ... 创建 Union SinkAction
    }

    // 模板模式：单个输入的多表 Sink
    for (Tuple2<CatalogTable, Action> tuple : inputVertices.get(0)) {
        SinkAction sinkAction = createSinkAction(...);
        sinkActions.add(sinkAction);
    }

    // 尝试合并为 MultiTableSink
    Optional<SinkAction<?, ?, ?, ?>> multiTableSink =
            tryGenerateMultiTableSink(sinkActions, readonlyConfig, classLoader, factoryId, configIndex);
    return multiTableSink.<List<SinkAction<?, ?, ?, ?>>>map(Collections::singletonList).orElse(sinkActions);
}
```

**MultiTableSink 合并逻辑**（第633-669行）：
- 检查所有 Sink 是否实现 `SupportMultiTableSink` 接口
- 如果支持，通过 `FactoryUtil.createMultiTableSink()` 合并为单个 `MultiTableSink`
- 否则退回到 Sink 模板模式（每个表单独一个 Sink Action）

### 4.8 SaveMode 处理（第721-759行）

```java
public void handleSaveMode(SeaTunnelSink<?, ?, ?, ?> sink) {
    if (SupportSaveMode.class.isAssignableFrom(sink.getClass())) {
        SupportSaveMode saveModeSink = (SupportSaveMode) sink;
        if (envOptions.get(EnvCommonOptions.SAVEMODE_EXECUTE_LOCATION).equals(SaveModeExecuteLocation.CLIENT)) {
            Optional<SaveModeHandler> saveModeHandler = saveModeSink.getSaveModeHandler();
            if (saveModeHandler.isPresent()) {
                try (SaveModeHandler handler = saveModeHandler.get()) {
                    handler.open();
                    new SaveModeExecuteWrapper(handler).execute();
                }
            }
        }
    }
}
```

---

## 5. DAG 构建 - Source/Sink/Transform 拓扑

### 5.1 Action 接口体系

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/dag/actions/Action.java`

```java
public interface Action extends Serializable {
    @NonNull String getName();
    @NonNull List<Action> getUpstream();     // 上游 Action 列表（反向边）
    int getParallelism();
    long getId();
    Set<URL> getJarUrls();
    Config getConfig();
}
```

**继承体系**：

```
Action (接口)
  └── AbstractAction (抽象类)
        ├── SourceAction<T, SplitT, StateT>     — 包装 SeaTunnelSource
        ├── TransformAction                      — 包装 SeaTunnelTransform
        ├── TransformChainAction<T>              — 包装多个 Transform（链式合并）
        └── SinkAction<IN, StateT, CommitInfoT, AggregatedCommitInfoT> — 包装 SeaTunnelSink
```

**关键设计**：每个 Action 的 `upstreams` 列表记录了上游 Action，形成隐式反向边。

### 5.2 LogicalDagGenerator - Action 到逻辑 DAG

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/dag/logical/LogicalDagGenerator.java`

```java
// 第72-80行：生成逻辑 DAG
public LogicalDag generate() {
    // Step 1: 为每个 Action 创建 LogicalVertex
    actions.forEach(this::createLogicalVertex);
    // Step 2: 根据 Action 的 upstream 关系创建 LogicalEdge
    Set<LogicalEdge> logicalEdges = createLogicalEdges();
    // Step 3: 组装 LogicalDag
    LogicalDag logicalDag = new LogicalDag(jobConfig, idGenerator);
    logicalDag.getEdges().addAll(logicalEdges);
    logicalDag.getLogicalVertexMap().putAll(logicalVertexMap);
    logicalDag.setStartWithSavePoint(isStartWithSavePoint);
    return logicalDag;
}

// 第82-101行：递归创建 LogicalVertex
private void createLogicalVertex(Action action) {
    final Long logicalVertexId = action.getId();
    if (logicalVertexMap.containsKey(logicalVertexId)) {
        return;  // 幂等：已创建则跳过
    }
    // 递归处理上游 Action
    action.getUpstream().forEach(inputAction -> {
        createLogicalVertex(inputAction);                      // 先创建上游
        inputVerticesMap.computeIfAbsent(inputAction.getId(), id -> new LinkedHashSet<>())
                .add(logicalVertexId);                          // 记录边关系
    });
    // 创建当前 Vertex
    final LogicalVertex logicalVertex = new LogicalVertex(logicalVertexId, action, action.getParallelism());
    logicalVertexMap.put(logicalVertexId, logicalVertex);
}

// 第103-112行：从 inputVerticesMap 创建边
private Set<LogicalEdge> createLogicalEdges() {
    return inputVerticesMap.entrySet().stream()
            .map(entry -> entry.getValue().stream()
                    .map(targetId -> new LogicalEdge(entry.getKey(), targetId))
                    .collect(Collectors.toList()))
            .flatMap(Collection::stream)
            .collect(Collectors.toCollection(LinkedHashSet::new));
}
```

**核心数据结构**：
- `logicalVertexMap`: `Map<Long, LogicalVertex>` — vertexId → LogicalVertex
- `inputVerticesMap`: `Map<Long, Set<Long>>` — inputVertexId → targetVertexIds（即邻接表）

### 5.3 LogicalDag 数据模型

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-core/src/main/java/org/apache/seatunnel/engine/core/dag/logical/LogicalDag.java`

```java
public class LogicalDag implements IdentifiedDataSerializable {
    private JobConfig jobConfig;
    private final Set<LogicalEdge> edges = new LinkedHashSet<>();
    private final LinkedHashMap<Long, LogicalVertex> logicalVertexMap = new LinkedHashMap<>();
    private IdGenerator idGenerator;
    private boolean isStartWithSavePoint = false;
}
```

**LogicalVertex**（第34行）：
```java
public class LogicalVertex implements IdentifiedDataSerializable {
    private Long vertexId;
    private Action action;
    private int parallelism;
}
```

**LogicalEdge**（第30行）：
```java
public class LogicalEdge implements IdentifiedDataSerializable {
    private Long inputVertexId;    // 上游 vertex ID
    private Long targetVertexId;   // 下游 vertex ID
}
```

### 5.4 DAG 可视化

LogicalDag 提供 JSON 序列化方法（第96-135行）：

```java
@NonNull public JsonObject getLogicalDagAsJson() {
    JsonObject logicalDag = new JsonObject();
    JsonArray vertices = new JsonArray();
    logicalVertexMap.values().forEach(v -> {
        JsonObject vertex = new JsonObject();
        vertex.add("id", v.getVertexId());
        vertex.add("name", v.getAction().getName() + "(id=" + v.getVertexId() + ")");
        vertex.add("parallelism", v.getParallelism());
        vertices.add(vertex);
    });
    logicalDag.add("vertices", vertices);
    // ... edges 类似处理
    return logicalDag;
}
```

---

## 6. 物理计划生成 - 逻辑计划到物理计划转换

### 6.1 整体转换链

```
LogicalDag
    │
    ├──→ ExecutionPlanGenerator.generate()
    │       ├── Phase 1: generateExecutionEdges()     — 创建 ExecutionVertex/Edge
    │       ├── Phase 2: generateTransformChainEdges() — 合并 Transform 链
    │       └── Phase 3: generatePipelines()           — 拆分为 Pipeline
    │
    │       结果: ExecutionPlan (List<Pipeline>)
    │
    └──→ PhysicalPlanGenerator.generate()
            ├── getEnumeratorTask()   — SourceSplitEnumerator 协调器任务
            ├── getCommitterTask()    — SinkAggregatedCommitter 提交器任务
            └── getSourceTask()       — Source→Transform→Sink 数据流任务
            │
            结果: PhysicalPlan (List<SubPlan>)
```

**入口文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/physical/PlanUtils.java`

```java
public static Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> fromLogicalDAG(...) {
    return new PhysicalPlanGenerator(
            new ExecutionPlanGenerator(logicalDag, jobImmutableInformation, engineConfig).generate(),
            nodeEngine, jobImmutableInformation, initializationTimestamp, executorService,
            classLoaderService, flakeIdGenerator, runningJobStateIMap, runningJobStateTimestampsIMap,
            queueType)
            .generate();
}
```

### 6.2 ExecutionPlanGenerator - 执行计划生成

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/execution/ExecutionPlanGenerator.java`

#### Phase 1: 生成 ExecutionEdge（第130-189行）

将 LogicalEdge 转换为 ExecutionEdge，同时为每个 LogicalVertex 创建对应的 ExecutionVertex（重新分配 ID）：

```java
private Set<ExecutionEdge> generateExecutionEdges(Set<LogicalEdge> logicalEdges) {
    Set<ExecutionEdge> executionEdges = new LinkedHashSet<>();
    Map<Long, ExecutionVertex> logicalVertexIdToExecutionVertexMap = new HashMap();

    for (LogicalEdge logicalEdge : sortedLogicalEdges) {
        ExecutionVertex executionInputVertex = logicalVertexIdToExecutionVertexMap.computeIfAbsent(
                logicalInputVertex.getVertexId(),
                vertexId -> {
                    long newId = idGenerator.getNextId();
                    Action newLogicalInputAction = recreateAction(logicalInputVertex.getAction(), newId, ...);
                    return new ExecutionVertex(newId, newLogicalInputAction, parallelism);
                });
        // ... targetVertex 类似处理
        executionEdges.add(new ExecutionEdge(executionInputVertex, executionTargetVertex));
    }
    return executionEdges;
}
```

#### Phase 2: Transform 链合并（第191-345行）

**核心优化**：将线性连接的 Transform 合并为 `TransformChainAction`，减少运行时的序列化/反序列化开销。

```java
private Set<ExecutionEdge> generateTransformChainEdges(Set<ExecutionEdge> executionEdges) {
    // 从所有 Source 出发，收集线性 Transform 链
    for (ExecutionVertex sourceVertex : sourceExecutionVertices) {
        // BFS 遍历
        fillChainedTransformExecutionVertex(sourceVertex, chainedTransformVerticesMapping,
                transformChainVertexMap, executionEdges, inputVerticesMap, targetVerticesMap);
    }
    // 重建边：将被合并的 Transform 替换为 TransformChain
}
```

**TransformChain 合并条件**（第320-344行 `collectChainedVertices()`）：
- Transform 只有单一输入（`inputVerticesMap.get(id).size() == 1`）
- Transform 只有单一输出（`targetVerticesMap.get(id).size() == 1`）
- 只有 TransformAction 可以被链化（不包括 SourceAction 和 SinkAction）

```java
private void collectChainedVertices(...) {
    Action action = currentVertex.getAction();
    if (action instanceof TransformAction) {
        if (chainedVertices.size() == 0) {
            chainedVertices.add(currentVertex);                    // 第一个 Transform
        } else if (inputVerticesMap.get(currentVertex.getVertexId()).size() == 1) {
            executionEdges.remove(new ExecutionEdge(...));         // 移除已合并的边
            chainedVertices.add(currentVertex);
        } else {
            return;                                                // 多输入不可链化
        }
    } else {
        return;                                                    // 非 Transform 不可链化
    }
    // 递归处理下一个
    if (targetVerticesMap.get(currentVertex.getVertexId()).size() == 1) {
        collectChainedVertices(targetVerticesMap.get(...).get(0), ...);
    }
}
```

**TransformChainAction 创建**（第276-309行）：
```java
TransformChainAction transformChainAction =
        new TransformChainAction(newVertexId, transformChainActionName, jars, identifiers, transforms);
```

#### Phase 3: Pipeline 生成（第347-376行）

```java
private List<Pipeline> generatePipelines(Set<ExecutionEdge> executionEdges) {
    Set<ExecutionVertex> executionVertices = new LinkedHashSet<>();
    for (ExecutionEdge edge : executionEdges) {
        executionVertices.add(edge.getLeftVertex());
        executionVertices.add(edge.getRightVertex());
    }
    PipelineGenerator pipelineGenerator = new PipelineGenerator(executionVertices, new ArrayList<>(executionEdges));
    List<Pipeline> pipelines = pipelineGenerator.generatePipelines();
    // ... Action 名称去重检查
    return pipelines;
}
```

### 6.3 PipelineGenerator - Pipeline 拆分

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/execution/PipelineGenerator.java`

```java
public List<Pipeline> generatePipelines() {
    // Step 1: 按并行度展开边（TODO: 当前未展开）
    List<ExecutionEdge> executionEdges = expandEdgeByParallelism(edges);

    // Step 2: 拆分不相关的子图
    List<List<ExecutionEdge>> edgesList = splitUnrelatedEdges(executionEdges);

    // Step 3: 拆分 Union 边（有多个输入的顶点需拆为独立 Pipeline）
    edgesList = edgesList.stream()
            .flatMap(e -> this.splitUnionEdge(e).stream())
            .collect(Collectors.toList());

    // Step 4: 转换为 Pipeline 对象
    return edgesList.stream().map(e -> new Pipeline(...)).collect(Collectors.toList());
}
```

**splitUnrelatedEdges**（第211-252行）：通过图的连通性分析，将无关联的子图拆分为独立 Pipeline。每个 Pipeline 可以独立调度执行。

**splitUnionEdge**（第122-133行）：当一个顶点有多个输入（Union）时，需要拆分为多个 Pipeline，每个 Pipeline 对应一个 Source 分支：

```java
private boolean checkCanSplit(List<ExecutionEdge> edges) {
    return edges.stream().anyMatch(e -> inputVerticesMap.get(e.getRightVertexId()).size() > 1);
}
```

### 6.4 PhysicalPlanGenerator - 物理计划生成

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/physical/PhysicalPlanGenerator.java`

#### 核心生成流程（第155-236行）

```java
public Tuple2<PhysicalPlan, Map<Integer, CheckpointPlan>> generate() {
    // 过滤已完成的 Pipeline
    List<Pipeline> unclosedPipelines = pipelines.stream()
            .filter(p -> !PipelineStatus.FINISHED.equals(runningJobStateIMap.get(pipelineLocation)))
            .collect(Collectors.toList());

    Stream<SubPlan> subPlanStream = unclosedPipelines.stream().map(pipeline -> {
        // 1. 获取协调器任务（SourceSplitEnumerator + SinkAggregatedCommitter）
        List<PhysicalVertex> coordinatorVertexList = getEnumeratorTask(sources, pipelineId, totalPipelineNum);
        coordinatorVertexList.addAll(getCommitterTask(edges, pipelineId, totalPipelineNum));

        // 2. 获取数据处理任务（Source + Transform + Sink 链）
        List<PhysicalVertex> physicalVertexList = getSourceTask(edges, sources, pipelineId, totalPipelineNum);

        // 3. 构建 CheckpointPlan
        checkpointPlans.put(pipelineId, CheckpointPlan.builder()...build());

        return new SubPlan(pipelineId, totalPipelineNum, initializationTimestamp,
                physicalVertexList, coordinatorVertexList, ...);
    });

    PhysicalPlan physicalPlan = new PhysicalPlan(subPlanStream.collect(Collectors.toList()), ...);
    return Tuple2.tuple2(physicalPlan, checkpointPlans);
}
```

#### Source 任务创建（第378-511行）

```java
private List<PhysicalVertex> getSourceTask(List<ExecutionEdge> edges, List<SourceAction<?, ?, ?>> sources,
        int pipelineIndex, int totalPipelineNum) {
    return sources.stream()
            .map(s -> new PhysicalExecutionFlow(s, getNextWrapper(edges, s)))  // 构建执行流
            .flatMap(flow -> {
                List<Flow> flows = new ArrayList<>(Collections.singletonList(flow));
                if (sourceWithSink(flow)) {
                    flows.addAll(splitSinkFromFlow(flow));  // 分离 Sink 到独立 TaskGroup
                }
                for (int i = 0; i < flow.getAction().getParallelism(); i++) {
                    // 为每个并行度创建 TaskGroup
                    List<SeaTunnelTask> taskList = flows.stream().map(f -> {
                        if (f instanceof PhysicalExecutionFlow) {
                            return new SourceSeaTunnelTask<>(...);
                        } else {
                            return new TransformSeaTunnelTask(...);
                        }
                    }).collect(Collectors.toList());

                    // 根据是否包含 Transform 选择 TaskGroup 类型
                    if (taskList.stream().anyMatch(TransformSeaTunnelTask.class::isInstance)) {
                        if (queueType.equals(BLOCKINGQUEUE)) {
                            taskGroup = new TaskGroupWithIntermediateBlockingQueue(...);
                        } else {
                            taskGroup = new TaskGroupWithIntermediateDisruptor(...);
                        }
                    } else {
                        taskGroup = new TaskGroupDefaultImpl(...);
                    }
                }
            });
}
```

### 6.5 SubPlan 状态机

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-server/src/main/java/org/apache/seatunnel/engine/server/dag/physical/SubPlan.java`

```
CREATED → SCHEDULED → DEPLOYING → RUNNING → FINISHED
                                       │
                                       ├──→ FAILING → FAILED → [恢复? → CREATED]
                                       └──→ CANCELING → CANCELED → [恢复? → CREATED]
```

**状态转换流程**（第616-717行 `stateProcess()`）：

| 状态 | 触发动作 |
|------|---------|
| CREATED | → SCHEDULED |
| SCHEDULED | 申请资源 → DEPLOYING |
| DEPLOYING | 启动所有 Coordinator 和 PhysicalVertex → RUNNING |
| RUNNING | 等待任务完成 |
| FAILING/CANCELING | 取消所有 Coordinator 和 PhysicalVertex |
| FAILED/CANCELED | 检查是否需要恢复；如需恢复则重新调度；否则通知完成 |
| FINISHED | 清理资源，通知完成 |

**Pipeline 恢复机制**（第291-293行）：
```java
private boolean checkNeedRestore(PipelineStatus pipelineStatus) {
    return canRestorePipeline() && !PipelineStatus.FINISHED.equals(pipelineStatus);
}

public boolean canRestorePipeline() {
    return jobMaster.isNeedRestore() && getPipelineRestoreNum() < pipelineMaxRestoreNum;
}
```

---

## 7. 三引擎适配差异

### 7.1 架构差异总览

| 维度 | Flink 引擎 | Spark 引擎 | SeaTunnel Engine |
|------|-----------|-----------|-----------------|
| **配置解析** | `ConfigBuilder.of()` | `ConfigBuilder.of()` | `ConfigBuilder.of()` → `MultipleTableJobConfigParser` |
| **DAG 构建** | 链式 `PluginExecuteProcessor` | 链式 `PluginExecuteProcessor` | `LogicalDagGenerator` → 多层计划转换 |
| **执行模型** | Flink DataStream API | Spark Dataset API | 自研 TaskGroup + Pipeline |
| **物理计划** | 无（Flink 内部优化） | 无（Spark 内部优化） | `PhysicalPlanGenerator` |
| **故障恢复** | Flink Checkpoint | Spark Checkpoint | `SubPlan.restorePipeline()` |
| **并行度控制** | Flink 算子级 | Spark Partition | `PhysicalVertex` 按并行度分片 |

### 7.2 Flink 引擎

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-core/seatunnel-flink-starter/seatunnel-flink-starter-common/src/main/java/org/apache/seatunnel/core/starter/flink/command/FlinkTaskExecuteCommand.java`

```java
// 第49-68行：Flink 入口
public void execute() throws CommandExecuteException {
    Path configFile = FileUtils.getConfigPath(flinkCommandArgs);
    checkConfigExist(configFile);
    Config config = MetalakeConfigUtils.getMetalakeConfig(
            ConfigBuilder.of(configFile, flinkCommandArgs.getVariables()));
    // 命令行 JobName 覆盖配置文件中的 JobName
    if (!flinkCommandArgs.getJobName().equals(Constants.LOGO)) {
        config = config.withValue(ConfigUtil.joinPath("env", "job.name"),
                ConfigValueFactory.fromAnyRef(flinkCommandArgs.getJobName()));
    }
    FlinkExecution seaTunnelTaskExecution = new FlinkExecution(config);
    seaTunnelTaskExecution.execute();
}
```

**FlinkExecution**（第86-170行）：

```java
public FlinkExecution(Config config) {
    Config envConfig = config.getConfig("env");
    JobContext jobContext = new JobContext();
    jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));

    // 创建 Source/Transform/Sink 的 PluginExecuteProcessor
    this.sourcePluginExecuteProcessor = new SourceExecuteProcessor(jarPaths, envConfig,
            config.getConfigList(Constants.SOURCE), jobContext);
    this.transformPluginExecuteProcessor = new TransformExecuteProcessor(jarPaths, envConfig,
            TypesafeConfigUtils.getConfigList(config, Constants.TRANSFORM, Collections.emptyList()), jobContext);
    this.sinkPluginExecuteProcessor = new SinkExecuteProcessor(jarPaths, envConfig,
            config.getConfigList(Constants.SINK), jobContext);
}

@Override
public void execute() throws TaskExecuteException {
    List<DataStreamTableInfo> dataStreams = new ArrayList<>();
    dataStreams = sourcePluginExecuteProcessor.execute(dataStreams);    // Source → DataStream
    dataStreams = transformPluginExecuteProcessor.execute(dataStreams); // Transform → DataStream
    sinkPluginExecuteProcessor.execute(dataStreams);                   // Sink → 写入
    // 由 Flink 引擎执行 Execution Plan
    flinkRuntimeEnvironment.getStreamExecutionEnvironment().execute(jobName);
}
```

**Flink 引擎特点**：
- 使用 `DataStreamTableInfo` 作为中间数据结构
- DAG 由 Flink 内部的 `StreamExecutionEnvironment` 管理
- 链式处理：Source → Transform → Sink 按序执行，数据在 Flink DataStream 中流转
- Checkpoint 由 Flink 框架提供
- 支持 `RuntimeExecutionMode.BATCH` 和流式模式

### 7.3 Spark 引擎

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-core/seatunnel-spark-starter/seatunnel-spark-starter-common/src/main/java/org/apache/seatunnel/core/starter/spark/command/SparkTaskExecuteCommand.java`

```java
// 第49-67行：Spark 入口（与 Flink 几乎一致）
public void execute() throws CommandExecuteException {
    Path configFile = FileUtils.getConfigPath(sparkCommandArgs);
    checkConfigExist(configFile);
    Config config = MetalakeConfigUtils.getMetalakeConfig(
            ConfigBuilder.of(configFile, sparkCommandArgs.getVariables()));
    if (!sparkCommandArgs.getJobName().equals(Constants.LOGO)) {
        config = config.withValue(ConfigUtil.joinPath("env", "job.name"),
                ConfigValueFactory.fromAnyRef(sparkCommandArgs.getJobName()));
    }
    SparkExecution seaTunnelTaskExecution = new SparkExecution(config);
    seaTunnelTaskExecution.execute();
}
```

**SparkExecution**（第39-95行）：

```java
public SparkExecution(Config config) {
    this.sparkRuntimeEnvironment = SparkRuntimeEnvironment.getInstance(config);
    JobContext jobContext = new JobContext();
    jobContext.setJobMode(RuntimeEnvironment.getJobMode(config));

    this.sourcePluginExecuteProcessor = new SourceExecuteProcessor(sparkRuntimeEnvironment, jobContext,
            config.getConfigList(Constants.SOURCE));
    this.transformPluginExecuteProcessor = new TransformExecuteProcessor(sparkRuntimeEnvironment, jobContext,
            TypesafeConfigUtils.getConfigList(config, Constants.TRANSFORM, Collections.emptyList()));
    this.sinkPluginExecuteProcessor = new SinkExecuteProcessor(sparkRuntimeEnvironment, jobContext,
            config.getConfigList(Constants.SINK));
}

@Override
public void execute() throws TaskExecuteException {
    List<DatasetTableInfo> datasets = new ArrayList<>();
    datasets = sourcePluginExecuteProcessor.execute(datasets);
    datasets = transformPluginExecuteProcessor.execute(datasets);
    sinkPluginExecuteProcessor.execute(datasets);
}
```

**Spark 引擎特点**：
- 使用 `DatasetTableInfo` 作为中间数据结构
- 同样使用链式 `PluginExecuteProcessor` 模式
- 无显式 `execute()` 调用（Spark 的惰性求值）
- 由 Spark 引擎内部管理 DAG 优化和调度

### 7.4 SeaTunnel Engine（自研引擎）

**文件**: `/Users/wangzhijun/sourceCode/seatunnel/seatunnel-engine/seatunnel-engine-client/src/main/java/org/apache/seatunnel/engine/client/job/ClientJobExecutionEnvironment.java`

```java
// 第119-157行：获取逻辑 DAG
public LogicalDag getLogicalDag() {
    ImmutablePair<List<Action>, Set<URL>> immutablePair = getJobConfigParser().parse(null);
    actions.addAll(immutablePair.getLeft());

    // 可选：上传 Connector JAR 到 Engine Server
    if (enableUploadConnectorJarPackage) {
        uploadActionPluginJar(actions, pluginJarIdentifiers);
    }

    return getLogicalDagGenerator().generate();  // LogicalDagGenerator.generate()
}

// 第189-202行：执行
public ClientJobProxy execute() throws ExecutionException, InterruptedException {
    LogicalDag logicalDag = getLogicalDag();
    JobImmutableInformation jobImmutableInformation = new JobImmutableInformation(
            jobId, jobConfig.getName(), isStartWithSavePoint,
            serializationService, logicalDag, jarUrls, connectorJarIdentifiers);
    return jobClient.createJobProxy(jobImmutableInformation);
}
```

**SeaTunnel Engine 引擎特点**：
- 完整的 LogicalDag → ExecutionPlan → PhysicalPlan 转换链
- `MultipleTableJobConfigParser` 支持多表合并（MultiTableSink）
- Transform 链合并优化（`TransformChainAction`）
- Pipeline 拆分支持并行执行
- 自研 Checkpoint 机制（`CheckpointPlan`）
- Pipeline 级故障恢复（`SubPlan.restorePipeline()`）
- 类加载器隔离（Source/Transform 和 Sink 使用不同 ClassLoader）
- 支持 SavePoint 恢复

### 7.5 引擎选择决策路径

```
启动入口: SeaTunnel.run() / seacluster.sh / seatunnel.sh
    │
    ├── MasterType.FLINK → FlinkTaskExecuteCommand → FlinkExecution
    │     └── 使用 Flink DataStream API，配置 → 链式处理 → Flink 调度执行
    │
    ├── MasterType.SPARK → SparkTaskExecuteCommand → SparkExecution
    │     └── 使用 Spark Dataset API，配置 → 链式处理 → Spark 调度执行
    │
    └── MasterType.SEATUNNEL (默认) → SeaTunnel Engine
          └── ConfigBuilder → MultipleTableJobConfigParser → LogicalDagGenerator
              → ExecutionPlanGenerator → PhysicalPlanGenerator → 自研调度执行
```

---

## 8. 总结与关键设计模式

### 8.1 配置解析设计模式

| 模式 | 应用 | 文件 |
|------|------|------|
| **SPI 服务发现** | ConfigAdapter、ConfigShade、ConfigAdapter | `ConfigAdapterUtils.java`、`ConfigShadeUtils.java` |
| **适配器模式** | 多格式配置文件支持 | `ConfigAdapter` SPI |
| **模板方法** | 多种配置入口的统一流程 | `ConfigBuilder.of()` 系列方法 |
| **安全脱敏** | 日志输出时自动掩码敏感字段 | `configDesensitization()` |

### 8.2 DAG 编排设计模式

| 模式 | 应用 | 文件 |
|------|------|------|
| **图抽象** | Action 为节点，upstream 为隐式边 | `Action.java` |
| **递归构建** | LogicalDagGenerator 深度优先遍历 Action 树 | `LogicalDagGenerator.java` |
| **链合并优化** | TransformAction 合并为 TransformChainAction | `ExecutionPlanGenerator.java` |
| **连通分量拆分** | 不相关的子图拆分为独立 Pipeline | `PipelineGenerator.splitUnrelatedEdges()` |
| **状态机** | SubPlan 和 PhysicalPlan 的生命周期管理 | `SubPlan.java`、`PhysicalPlan.java` |

### 8.3 关键配置路径总结

| 路径 | 用途 |
|------|------|
| `env` | 全局环境配置（job.name、parallelism、checkpoint 等） |
| `source[n]` | Source 插件配置列表 |
| `transform[n]` | Transform 插件配置列表 |
| `sink[n]` | Sink 插件配置列表 |
| `env.shade.identifier` | 配置加密标识 |
| `env.shade.options` | 额外敏感字段列表 |
| `env.jars` | 公共依赖 JAR |

### 8.4 数据流总结

```
配置文件 → ConfigBuilder.of() → Typesafe Config
    → MultipleTableJobConfigParser.parse() → List<Action>
        → LogicalDagGenerator.generate() → LogicalDag
            → ExecutionPlanGenerator.generate() → ExecutionPlan (List<Pipeline>)
                → PhysicalPlanGenerator.generate() → PhysicalPlan (List<SubPlan>)
                    → JobMaster 调度执行
```

### 8.5 线程安全与类加载器设计

**类加载器隔离**（`MultipleTableJobConfigParser.java` 第217-231行）：

```java
// Source 和 Transform 共享类加载器
ClassLoader sourceAndTransformClassLoader =
        getClassLoader(classLoaderService, parentClassLoader, sourceJars);
// Sink 使用独立类加载器
ClassLoader sinkClassLoader =
        getClassLoader(classLoaderService, parentClassLoader, sinkConnectorJars);

Thread.currentThread().setContextClassLoader(sourceAndTransformClassLoader);
// ... 解析 Source 和 Transform
Thread.currentThread().setContextClassLoader(sinkClassLoader);
// ... 解析 Sink
```

这种设计避免了 Source 插件和 Sink 插件的类冲突（如不同版本的 Guava、Jackson 等）。
