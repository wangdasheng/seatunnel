/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.seatunnel.cdc.sync.demo;

import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.cdc.sync.api.BusinessTransformPlugin;
import org.apache.seatunnel.cdc.sync.api.SourceRow;
import org.apache.seatunnel.cdc.sync.api.TargetRow;
import org.apache.seatunnel.cdc.sync.api.TransformContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <b>Pattern 2: Horizontal sharding</b> — {@code test.cdc_test} → {@code wangzhijun.cdc_test_0},
 * {@code wangzhijun.cdc_test_1}, ...
 *
 * <p>One source table maps to N shard tables with <b>identical schemas</b>. Routing is determined
 * by {@code hash(id) % shard_count} at runtime — each row goes to exactly one shard.
 *
 * <p>Configuration:
 *
 * <pre>{@code
 * plugin_config {
 *     target_database = "wangzhijun"
 *     shard_count = 2                    # number of shards (default: 2)
 *     filter_empty_name = true
 *     log_every_row = false
 * }
 * }</pre>
 *
 * <p>Example with shard_count=2:
 *
 * <ul>
 *   <li>Row id=1 (hash=1) → {@code wangzhijun.cdc_test_1}
 *   <li>Row id=2 (hash=0) → {@code wangzhijun.cdc_test_0}
 *   <li>Row id=3 (hash=0) → {@code wangzhijun.cdc_test_0}
 * </ul>
 *
 * @see CdcTestMigrationPlugin Pattern 1 — single table
 * @see CdcSplitTableMigrationPlugin Pattern 3 — field splitting
 */
public class CdcShardingMigrationPlugin implements BusinessTransformPlugin {

    private static final Logger log = LoggerFactory.getLogger(CdcShardingMigrationPlugin.class);

    private static final String CFG_TARGET_DATABASE = "target_database";
    private static final String CFG_SHARD_COUNT = "shard_count";
    private static final String CFG_FILTER_EMPTY_NAME = "filter_empty_name";
    private static final String CFG_LOG_EVERY_ROW = "log_every_row";

    private static final String STATE_INSERT = "stats.insert";
    private static final String STATE_UPDATE = "stats.update";
    private static final String STATE_DELETE = "stats.delete";
    private static final String STATE_FILTERED = "stats.filtered";

    private String sourceTable = "test.cdc_test";
    private String targetDatabase = "wangzhijun";
    private int shardCount = 2;
    private boolean filterEmptyName = true;
    private boolean logEveryRow;

    /** Precomputed shard table names for fast lookup at runtime. */
    private String[] shardTableNames;

    @Override
    public void init(TransformContext context) {
        targetDatabase = context.getConfig(CFG_TARGET_DATABASE, "wangzhijun");
        shardCount = context.getConfig(CFG_SHARD_COUNT, 2);
        filterEmptyName = context.getConfig(CFG_FILTER_EMPTY_NAME, true);
        logEveryRow = context.getConfig(CFG_LOG_EVERY_ROW, false);

        if (shardCount < 1) {
            shardCount = 1;
        }

        shardTableNames = new String[shardCount];
        for (int i = 0; i < shardCount; i++) {
            shardTableNames[i] = targetDatabase + ".cdc_test_" + i;
        }

        context.putState(STATE_INSERT, 0L);
        context.putState(STATE_UPDATE, 0L);
        context.putState(STATE_DELETE, 0L);
        context.putState(STATE_FILTERED, 0L);

        log.info(
                "CdcShardingMigrationPlugin initialized. Source: {}, Target DB: {}, Shards: {}, "
                        + "Shard names: {}, Filter empty name: {}",
                sourceTable,
                targetDatabase,
                shardCount,
                shardTableNames,
                filterEmptyName);
    }

    @Override
    public List<TargetRow> transform(SourceRow sourceRow, TransformContext context) {
        if (!sourceTable.equals(sourceRow.getTableId())) {
            return Collections.emptyList();
        }

        String name = sourceRow.getField("name", String.class);

        if (filterEmptyName && (name == null || name.trim().isEmpty())) {
            incrementCounter(context, STATE_FILTERED);
            if (logEveryRow) {
                log.debug(
                        "Filtered row: table={}, id={}, op={}, reason=empty_name",
                        sourceRow.getTableId(),
                        sourceRow.getField("id"),
                        sourceRow.getOpType());
            }
            return Collections.emptyList();
        }

        // ---- Sharding: hash(id) % shard_count ----
        Object idObj = sourceRow.getField("id");
        int shardIndex = 0;
        if (idObj != null) {
            shardIndex = Math.abs(idObj.hashCode()) % shardCount;
        }
        String targetTable = shardTableNames[shardIndex];

        String opType = sourceRow.getOpType().toUpperCase();
        switch (opType) {
            case "INSERT":
                incrementCounter(context, STATE_INSERT);
                break;
            case "UPDATE":
                incrementCounter(context, STATE_UPDATE);
                break;
            case "DELETE":
                incrementCounter(context, STATE_DELETE);
                break;
            default:
                break;
        }

        if (logEveryRow) {
            log.debug(
                    "Shard route: {} → {} (shard {}), op={}, id={}, name={}, age={}, status={}",
                    sourceRow.getTableId(),
                    targetTable,
                    shardIndex,
                    opType,
                    idObj,
                    name,
                    sourceRow.getField("age"),
                    sourceRow.getField("status"));
        }

        TargetRow targetRow = new TargetRow(targetTable, sourceRow.getFields(), opType);
        return Collections.singletonList(targetRow);
    }

    @Override
    public void close() {
        log.info("CdcShardingMigrationPlugin closed.");
    }

    @Override
    public String getPluginName() {
        return "CdcShardingMigration";
    }

    @Override
    public List<CatalogTable> getTargetTables(
            CatalogTable sourceTable, Map<String, Object> pluginConfig) {
        TableIdentifier sourceId = sourceTable.getTableId();
        int count = 2;
        Object rawCount = pluginConfig.get(CFG_SHARD_COUNT);
        if (rawCount != null) {
            count = Integer.parseInt(String.valueOf(rawCount));
        }
        // Use target database so writer key matches TargetRow.getTargetTable()
        String targetDb =
                String.valueOf(pluginConfig.getOrDefault(CFG_TARGET_DATABASE, targetDatabase));

        List<CatalogTable> shards = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            TableIdentifier targetId =
                    TableIdentifier.of(
                            sourceId.getCatalogName(),
                            targetDb,
                            sourceId.getSchemaName(),
                            "cdc_test_" + i);
            shards.add(CatalogTable.of(targetId, sourceTable));
        }
        return shards;
    }

    @SuppressWarnings("unchecked")
    private void incrementCounter(TransformContext context, String key) {
        Long current = (Long) context.getState(key);
        if (current == null) {
            current = 0L;
        }
        context.putState(key, current + 1);
    }
}
