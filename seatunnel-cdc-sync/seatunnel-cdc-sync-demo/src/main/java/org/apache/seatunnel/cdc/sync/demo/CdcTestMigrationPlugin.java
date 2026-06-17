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

import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * <b>Pattern 1: Single-table migration</b> — {@code test.cdc_test} → {@code wangzhijun.cdc_test}.
 *
 * <p>One source table maps to one target table with full-column passthrough.
 *
 * <p>Demonstrates:
 *
 * <ul>
 *   <li><b>Table routing</b>: redirects rows from source to target table
 *   <li><b>Field access by name</b>: uses {@link SourceRow#getField(String)}
 *   <li><b>Filtering</b>: drops rows with empty/null {@code name}
 *   <li><b>OpType passthrough</b>: preserves INSERT/UPDATE/DELETE
 *   <li><b>State counters</b>: tracks insert/update/delete/filtered counts
 * </ul>
 *
 * <p>Configuration:
 *
 * <pre>{@code
 * plugin_config {
 *     target_database = "wangzhijun"   # target database name
 *     filter_empty_name = true          # drop rows where name is blank
 *     log_every_row = false             # set true for verbose DEBUG
 * }
 * }</pre>
 *
 * @see CdcShardingMigrationPlugin Pattern 2 — horizontal sharding
 * @see CdcSplitTableMigrationPlugin Pattern 3 — field-level splitting
 */
public class CdcTestMigrationPlugin implements BusinessTransformPlugin {

    private static final Logger log = LoggerFactory.getLogger(CdcTestMigrationPlugin.class);

    // ---- Config keys ----
    private static final String CFG_TARGET_DATABASE = "target_database";
    private static final String CFG_FILTER_EMPTY_NAME = "filter_empty_name";
    private static final String CFG_LOG_EVERY_ROW = "log_every_row";

    // ---- State keys ----
    private static final String STATE_INSERT_COUNT = "stats.insert";
    private static final String STATE_UPDATE_COUNT = "stats.update";
    private static final String STATE_DELETE_COUNT = "stats.delete";
    private static final String STATE_FILTERED_COUNT = "stats.filtered";

    // ---- Plugin instance state ----
    private String sourceTable = "test.cdc_test";
    private String targetTable = "wangzhijun.cdc_test";
    private String targetDatabase = "wangzhijun";
    private boolean filterEmptyName = true;
    private boolean logEveryRow;

    @Override
    public void init(TransformContext context) {
        // Read business config from plugin_config block
        targetDatabase = context.getConfig(CFG_TARGET_DATABASE, "wangzhijun");
        filterEmptyName = context.getConfig(CFG_FILTER_EMPTY_NAME, true);
        logEveryRow = context.getConfig(CFG_LOG_EVERY_ROW, false);

        // Source table is determined by CDC stream; target table = database.table_name
        // Here we keep the same table name, just different database
        this.targetTable = targetDatabase + ".cdc_test";

        // Initialize counters in state
        context.putState(STATE_INSERT_COUNT, 0L);
        context.putState(STATE_UPDATE_COUNT, 0L);
        context.putState(STATE_DELETE_COUNT, 0L);
        context.putState(STATE_FILTERED_COUNT, 0L);

        log.info(
                "CdcTestMigrationPlugin initialized. Source table: {}, Target table: {}, "
                        + "Filter empty name: {}, Log every row: {}",
                sourceTable,
                targetTable,
                filterEmptyName,
                logEveryRow);
    }

    @Override
    public List<TargetRow> transform(SourceRow sourceRow, TransformContext context) {
        // ---- 1. Filter by table (in case multi-table CDC outputs other tables) ----
        if (!sourceTable.equals(sourceRow.getTableId())) {
            return Collections.emptyList();
        }

        // ---- 2. Access fields by name (demonstrating SourceRow field API) ----
        String name = sourceRow.getField("name", String.class);

        // ---- 3. Filter: drop rows where name is empty/null ----
        if (filterEmptyName && (name == null || name.trim().isEmpty())) {
            incrementCounter(context, STATE_FILTERED_COUNT);
            if (logEveryRow) {
                log.debug(
                        "Filtered row: table={}, id={}, op={}, reason=empty_name",
                        sourceRow.getTableId(),
                        sourceRow.getField("id"),
                        sourceRow.getOpType());
            }
            return Collections.emptyList();
        }

        // ---- 4. Route: map source table → target table ----
        // Keep same field values, same order; just change the target table
        String opType = sourceRow.getOpType().toUpperCase();

        // ---- 5. Update stats counters ----
        switch (opType) {
            case "INSERT":
                incrementCounter(context, STATE_INSERT_COUNT);
                break;
            case "UPDATE":
                incrementCounter(context, STATE_UPDATE_COUNT);
                break;
            case "DELETE":
                incrementCounter(context, STATE_DELETE_COUNT);
                break;
            default:
                break;
        }

        // ---- 6. DEBUG logging ----
        if (logEveryRow) {
            log.debug(
                    "Migrated row: {} → {}, op={}, id={}, name={}, age={}, status={}",
                    sourceRow.getTableId(),
                    targetTable,
                    opType,
                    sourceRow.getField("id"),
                    name,
                    sourceRow.getField("age"),
                    sourceRow.getField("status"));
        }

        // ---- 7. Build target row: same fields, different table, same opType ----
        TargetRow targetRow = new TargetRow(targetTable, sourceRow.getFields(), opType);
        return Collections.singletonList(targetRow);
    }

    @Override
    public void close() {
        // Stats are in per-instance state (not shared), so we can't read them here
        // In practice, the framework logs stats via TransformRunner's close
        log.info("CdcTestMigrationPlugin closed.");
    }

    @Override
    public String getPluginName() {
        return "CdcTestMigration";
    }

    /**
     * Single-table routing: clones the source CatalogTable with target database name.
     *
     * <p>Must use the target database so the {@code TableIdentifier} matches the {@code
     * TargetRow.getTargetTable()} value at runtime.
     */
    @Override
    public List<CatalogTable> getTargetTables(
            CatalogTable sourceTable, Map<String, Object> pluginConfig) {
        String targetDb =
                String.valueOf(pluginConfig.getOrDefault(CFG_TARGET_DATABASE, targetDatabase));
        TableIdentifier sourceId = sourceTable.getTableId();
        TableIdentifier targetId =
                TableIdentifier.of(
                        sourceId.getCatalogName(),
                        targetDb,
                        sourceId.getSchemaName(),
                        sourceId.getTableName());
        return Collections.singletonList(CatalogTable.of(targetId, sourceTable));
    }

    // ---- Helper: increment a Long counter in state ----
    @SuppressWarnings("unchecked")
    private void incrementCounter(TransformContext context, String key) {
        Long current = (Long) context.getState(key);
        if (current == null) {
            current = 0L;
        }
        context.putState(key, current + 1);
    }
}
