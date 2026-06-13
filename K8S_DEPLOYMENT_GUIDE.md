# SQL Lookup Plugin K8s 部署指南

## 🎯 问题根因

之前插件无法加载的原因是：**缺少 SPI 配置文件**

虽然 jar 包已经部署到 NFS，但 SeaTunnel 无法通过 SPI 机制发现 `SqlLookupTransformFactory`。

## ✅ 已修复

已在插件项目中添加了 SPI 配置文件：
```
src/main/resources/META-INF/services/org.apache.seatunnel.api.table.factory.TableTransformFactory
```

内容：
```
com.szwego.seatunnel.plugin.sqlookup.SqlLookupTransformFactory
```

## 📦 部署步骤

### 步骤 1: 确认新 jar 包已编译

```bash
cd /Users/wangzhijun/IdeaProjects/szwego-etldevops/szwego-seatunnel-plugin-sql-lookup
mvn clean package -DskipTests

# 验证 SPI 文件在 jar 中
jar tf target/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar | grep TableTransformFactory
# 应该输出: META-INF/services/org.apache.seatunnel.api.table.factory.TableTransformFactory
```

✅ 已完成！SPI 文件已确认在 jar 包中。

### 步骤 2: 更新 NFS 上的 jar 包

由于 NFS 服务器 (10.1.12.11) 无法直接从你的 Mac SSH 访问，你需要：

**方式 A: 通过跳板机/堡垒机**
```bash
# 先复制到跳板机
scp szwego-seatunnel-plugin-sql-lookup-1.0.0.jar jump-server:/tmp/

# 再从跳板机复制到 NFS 服务器
ssh jump-server "scp /tmp/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar root@10.1.12.11:/wangzhijun/seatunnel/connectors/"
```

**方式 B: 在 Kubernetes 节点上操作**
```bash
# SSH 到任意一个 K8s 节点（因为 NFS 已挂载）
ssh k8s-node-1

# 复制新 jar 到 NFS
cp /path/to/new/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar \
   /wangzhijun/seatunnel/connectors/

# 验证
ls -lh /wangzhijun/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar
```

**方式 C: 使用运维工具**
如果你有 Ansible、SaltStack 等运维工具，可以通过它们推送文件到 NFS 服务器。

### 步骤 3: 验证 NFS 上的文件

```bash
# 在可以访问 NFS 的机器上执行
ls -lh /wangzhijun/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar

# 应该看到文件大小约 7.5M，时间是最新的
```

### 步骤 4: 重启 SeaTunnel Pods

```bash
# 重启 Master 和 Worker
kubectl rollout restart deployment/seatunnel-master -n seatunnel
kubectl rollout restart deployment/seatunnel-worker -n seatunnel

# 等待 Pods 就绪
kubectl wait --for=condition=ready pod -l app=seatunnel-master -n seatunnel --timeout=120s
kubectl wait --for=condition=ready pod -l app=seatunnel-worker -n seatunnel --timeout=120s
```

### 步骤 5: 验证插件加载

```bash
# 获取新的 Pod 名称
MASTER_POD=$(kubectl get pods -n seatunnel -l app=seatunnel-master -o jsonpath='{.items[0].metadata.name}')

# 查看日志，应该能看到插件被发现
kubectl logs -n seatunnel $MASTER_POD | grep -i "SqlLookup"

# 应该看到类似这样的日志：
# Discovery plugin jar for: PluginIdentifier{..., pluginName='SqlLookup'} at: [file:/opt/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar]
```

### 步骤 6: 提交任务测试

使用你之前的配置提交任务，现在应该能成功了！

## 🔍 验证清单

在提交任务前，确保以下内容都已完成：

- [ ] ✅ SPI 配置文件已添加到 jar 包中
- [ ] ⏳ 新 jar 包已复制到 NFS `/wangzhijun/seatunnel/connectors/`
- [ ] ⏳ SeaTunnel Pods 已重启
- [ ] ⏳ 日志中可以看到 SqlLookup 插件被加载
- [ ] ⏳ plugin-mapping.properties 中包含映射配置

## 📝 plugin-mapping.properties 配置

确保 NFS 上的 `/wangzhijun/seatunnel/connectors/plugin-mapping.properties` 包含：

```properties
seatunnel.transform.SqlLookup = szwego-seatunnel-plugin-sql-lookup-1.0.0
```

这个配置之前已经添加，应该不需要再次修改。

## 🐛 故障排查

如果仍然报错，检查：

1. **jar 包版本是否正确**
   ```bash
   # 在 Pod 中执行
   kubectl exec -n seatunnel <pod-name> -- ls -lh /opt/seatunnel/connectors/szwego-seatunnel-plugin-sql-lookup-1.0.0.jar
   ```

2. **SPI 文件是否在 jar 中**
   ```bash
   # 本地验证
   jar tf szwego-seatunnel-plugin-sql-lookup-1.0.0.jar | grep TableTransformFactory
   ```

3. **plugin-mapping.properties 是否正确**
   ```bash
   kubectl exec -n seatunnel <pod-name> -- cat /opt/seatunnel/connectors/plugin-mapping.properties | grep SqlLookup
   ```

4. **查看完整错误日志**
   ```bash
   kubectl logs -n seatunnel <pod-name> --tail=200 > seatunnel.log
   # 搜索错误信息
   grep -i "error\|exception\|sqllookup" seatunnel.log
   ```

## 🚀 快速部署脚本（需要在可访问 NFS 的机器上执行）

创建文件 `deploy_plugin.sh`:

```bash
#!/bin/bash
set -e

NFS_PATH="/wangzhijun/seatunnel/connectors"
PLUGIN_JAR="szwego-seatunnel-plugin-sql-lookup-1.0.0.jar"

echo "Copying plugin to NFS..."
cp -f "$PLUGIN_JAR" "$NFS_PATH/"

echo "Verifying..."
ls -lh "$NFS_PATH/$PLUGIN_JAR"

echo "Restarting SeaTunnel pods..."
kubectl rollout restart deployment/seatunnel-master deployment/seatunnel-worker -n seatunnel

echo "Waiting for pods to be ready..."
kubectl wait --for=condition=ready pod -l app=seatunnel-master -n seatunnel --timeout=120s
kubectl wait --for=condition=ready pod -l app=seatunnel-worker -n seatunnel --timeout=120s

echo "Done! You can now submit your job."
```

执行：
```bash
chmod +x deploy_plugin.sh
./deploy_plugin.sh
```

## ✨ 总结

**核心问题**：缺少 SPI 配置文件 `META-INF/services/org.apache.seatunnel.api.table.factory.TableTransformFactory`

**解决方案**：
1. ✅ 已添加 SPI 配置文件
2. ✅ 已重新编译插件（包含 SPI 配置）
3. ⏳ 需要将新 jar 包部署到 NFS
4. ⏳ 需要重启 SeaTunnel Pods

完成上述步骤后，你的 SQL Lookup Transform 插件就能正常工作了！
