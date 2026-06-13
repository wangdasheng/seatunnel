# SQL Lookup Transform Plugin 使用说明

## 插件概述

这是一个 SeaTunnel SQL Lookup Transform 插件，用于在数据转换过程中通过 SQL 查询外部数据库来 enrich 数据。

## 已完成的工作

1. ✅ 修复了字段索引解析问题 - 现在可以根据字段名称自动查找索引
2. ✅ 添加了 MySQL JDBC 驱动依赖
3. ✅ 配置了插件映射 (plugin-mapping.properties)
4. ✅ 创建了测试配置文件 (test_sql_lookup.conf)
5. ✅ 编译并部署插件到 connectors 目录

## 插件位置

- **插件源码**: `/Users/wangzhijun/IdeaProjects/szwego-etldevops/szwego-seatunnel-plugin-sql-lookup`
- **编译后的jar**: `/Users/wangzhijun/sourceCode/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-4.0.0-SNAPSHOT.jar`

## 配置示例

```hocon
transform {
  SqlLookup_0 {
    plugin = "SqlLookup"
    source_table_name = "your_source_table"
    result_table_name = "your_result_table"
    
    # 数据库连接配置
    lookup_url = "jdbc:mysql://host:port/database?params"
    lookup_driver = "com.mysql.cj.jdbc.Driver"
    lookup_user = "username"
    lookup_password = "password"
    
    # SQL 查询（使用 ? 作为占位符）
    lookup_sql = "select target_column from table where condition_column = ?"
    
    # 缓存配置
    lookup_cache_id = "unique_cache_id"
    lookup_cache_max_size = 10000
    lookup_cache_ttl_seconds = 1800
    
    # 字段映射
    source_field = "source_column_name"  # 源表中的字段名
    target_field = "result_column_name"  # 结果中添加的字段名
  }
}
```

## 工作原理

1. 插件从源数据行中读取 `source_field` 指定的字段值
2. 使用该值作为参数执行 SQL 查询
3. 查询结果会被缓存（基于 Guava Cache）
4. 将查询结果作为新字段添加到输出行中，字段名为 `target_field`

## 测试方法

### 方法 1: 使用 IntelliJ IDEA（推荐）

1. 在 IntelliJ IDEA 中打开 SeaTunnel 项目
2. 确保项目已正确导入所有模块
3. 找到 `SeaTunnelEngineLocalExample` 类
   - 路径: `seatunnel-examples/seatunnel-engine-examples/src/main/java/org/apache/seatunnel/example/engine/SeaTunnelEngineLocalExample.java`
4. 右键点击 -> Run 'SeaTunnelEngineLocalExample.main()'
5. 查看控制台输出，应该能看到经过 SQL Lookup 转换后的数据

### 方法 2: 使用 Maven 运行

如果项目可以成功编译，可以使用以下命令：

```bash
cd /Users/wangzhijun/sourceCode/seatunnel
mvn exec:java -pl seatunnel-examples/seatunnel-engine-examples \
  -Dexec.mainClass="org.apache.seatunnel.example.engine.SeaTunnelEngineLocalExample"
```

### 方法 3: 重新编译整个项目后运行

首先需要修复项目的编译问题（主要是 seatunnel-config-shade 模块），然后：

```bash
cd /Users/wangzhijun/sourceCode/seatunnel
mvn clean install -DskipTests
cd seatunnel-examples/seatunnel-engine-examples
mvn exec:java -Dexec.mainClass="org.apache.seatunnel.example.engine.SeaTunnelEngineLocalExample"
```

## 当前状态

- ✅ 插件代码已修复并编译成功
- ✅ MySQL 驱动已包含在 jar 包中
- ✅ 插件已部署到 SeaTunnel connectors 目录
- ⚠️ SeaTunnel 主项目有编译问题（shade 相关），需要修复后才能完整测试

## 已知问题

1. **SeaTunnel 项目编译问题**: `seatunnel-config-shade` 模块有类型找不到错误，这可能是 shade 插件配置问题
2. **Lombok 注解处理**: 部分类可能缺少 Lombok 生成的代码

## 下一步

1. 修复 SeaTunnel 项目的编译问题
2. 运行测试验证插件功能
3. 根据测试结果调整配置或代码

## 联系信息

如有问题，请检查：
- 数据库连接是否正确
- SQL 查询是否返回预期结果
- 日志输出中的字段解析信息
