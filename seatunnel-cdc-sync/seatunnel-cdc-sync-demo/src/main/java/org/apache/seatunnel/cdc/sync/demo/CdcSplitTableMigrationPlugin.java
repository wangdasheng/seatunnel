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
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.PhysicalColumn;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.cdc.sync.api.BusinessTransformPlugin;
import org.apache.seatunnel.cdc.sync.api.SourceRow;
import org.apache.seatunnel.cdc.sync.api.TargetRow;
import org.apache.seatunnel.cdc.sync.api.TransformContext;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * <b>Pattern 3: Field-level splitting with optional renaming</b>.
 *
 * <p>One wide source table maps to several narrow target tables, each with a <b>subset of
 * columns</b>. Source→target column name mapping is supported via the optional {@code rename}
 * block.
 *
 * <p>Configuration:
 *
 * <pre>{@code
 * plugin_config {
 *     target_database = "wangzhijun"
 *     filter_empty_name = true
 *     log_every_row = false
 *     split_mapping = [
 *       {
 *         suffix = "_basic"
 *         columns = ["id", "name", "email", "status"]
 *         # Optional: rename source→target columns
 *         rename = {email = "contact_email"}
 *       }
 *       {
 *         suffix = "_info"
 *         columns = ["id", "age", "score", "created_at"]
 *       }
 *       {
 *         suffix = "_detail"
 *         columns = ["id", "email", "created_at", "updated_at"]
 *       }
 *     ]
 * }
 * }</pre>
 *
 * <p>How it works:
 *
 * <ol>
 *   <li>{@code columns} = source column names to extract from CDC row
 *   <li>{@code rename} = optional map of source→target column names
 *   <li>{@code buildSplitTable()} creates a {@link CatalogTable} with <b>target</b> column names so
 *       the JDBC sink generates correct INSERT SQL
 *   <li>{@code projectFields()} extracts values by <b>source</b> column names and returns them in
 *       target column order
 * </ol>
 *
 * @see CdcTestMigrationPlugin Pattern 1 — single table
 * @see CdcShardingMigrationPlugin Pattern 2 — horizontal sharding
 */
public class CdcSplitTableMigrationPlugin implements BusinessTransformPlugin {

    private static final Logger log = LoggerFactory.getLogger(CdcSplitTableMigrationPlugin.class);

    private static final String CFG_TARGET_DATABASE = "target_database";
    private static final String CFG_FILTER_EMPTY_NAME = "filter_empty_name";
    private static final String CFG_LOG_EVERY_ROW = "log_every_row";
    private static final String CFG_SPLIT_MAPPING = "split_mapping";

    private static final String STATE_INSERT = "stats.insert";
    private static final String STATE_UPDATE = "stats.update";
    private static final String STATE_DELETE = "stats.delete";
    private static final String STATE_FILTERED = "stats.filtered";

    private String sourceTable = "test.cdc_test";
    private String targetDatabase = "wangzhijun";
    private boolean filterEmptyName = true;
    private boolean logEveryRow;

    /** Ordered list of split definitions, key = full target table name. */
    private final LinkedHashMap<String, SplitDef> splitDefs = new LinkedHashMap<>();

    /** Default split definitions when no {@code split_mapping} is configured. */
    private static final List<Map<String, Object>> DEFAULT_SPLITS =
            Arrays.asList(
                    createSplit("_basic", "id", "name", "status"),
                    createSplit("_info", "id", "age", "amount", "create_time"),
                    createSplit("_detail", "id", "description", "extra_json"));

    @Override
    @SuppressWarnings("unchecked")
    public void init(TransformContext context) {
        targetDatabase = context.getConfig(CFG_TARGET_DATABASE, "wangzhijun");
        filterEmptyName = context.getConfig(CFG_FILTER_EMPTY_NAME, true);
        logEveryRow = context.getConfig(CFG_LOG_EVERY_ROW, false);

        List<Map<String, Object>> splits = context.getConfig(CFG_SPLIT_MAPPING);
        if (splits == null || splits.isEmpty()) {
            splits = DEFAULT_SPLITS;
        }

        for (Map<String, Object> split : splits) {
            String suffix = String.valueOf(split.get("suffix"));
            String fullName = targetDatabase + ".cdc_test_split" + suffix;

            @SuppressWarnings("unchecked")
            List<String> srcColumns = (List<String>) split.get("columns");
            String[] sourceColumnNames = srcColumns.toArray(new String[0]);

            // Parse rename map (source → target)
            Map<String, String> renameMap = parseRename(split.get("rename"));

            String[] targetColumnNames = buildTargetNames(sourceColumnNames, renameMap);

            splitDefs.put(fullName, new SplitDef(sourceColumnNames, targetColumnNames));
        }

        context.putState(STATE_INSERT, 0L);
        context.putState(STATE_UPDATE, 0L);
        context.putState(STATE_DELETE, 0L);
        context.putState(STATE_FILTERED, 0L);

        log.info(
                "CdcSplitTableMigrationPlugin initialized. Source: {}, Target DB: {}, "
                        + "Split tables: {}, Filter empty name: {}",
                sourceTable,
                targetDatabase,
                splitDefs.keySet(),
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

        // Produce one target row per split table
        List<TargetRow> results = new ArrayList<>(splitDefs.size());
        for (Map.Entry<String, SplitDef> entry : splitDefs.entrySet()) {
            String targetTable = entry.getKey();
            SplitDef def = entry.getValue();
            // Extract values by SOURCE column names, output in target column order
            Object[] projectedFields = projectFields(sourceRow, def.sourceColumns);

            if (logEveryRow) {
                log.debug(
                        "Split route: {} → {}, op={}, id={}, srcCols={}, targetCols={}",
                        sourceRow.getTableId(),
                        targetTable,
                        opType,
                        sourceRow.getField("id"),
                        Arrays.toString(def.sourceColumns),
                        Arrays.toString(def.targetColumns));
            }

            results.add(new TargetRow(targetTable, projectedFields, opType));
        }
        return results;
    }

    @Override
    public void close() {
        log.info("CdcSplitTableMigrationPlugin closed.");
    }

    @Override
    public String getPluginName() {
        return "CdcSplitTableMigration";
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<CatalogTable> getTargetTables(
            CatalogTable sourceTable, Map<String, Object> pluginConfig) {
        TableIdentifier sourceId = sourceTable.getTableId();
        // Use target database so writer key matches TargetRow.getTargetTable()
        String targetDb =
                String.valueOf(pluginConfig.getOrDefault(CFG_TARGET_DATABASE, targetDatabase));

        List<Map<String, Object>> splits =
                (List<Map<String, Object>>) pluginConfig.get(CFG_SPLIT_MAPPING);
        if (splits == null || splits.isEmpty()) {
            splits = DEFAULT_SPLITS;
        }

        List<CatalogTable> tables = new ArrayList<>(splits.size());
        for (Map<String, Object> split : splits) {
            String suffix = String.valueOf(split.get("suffix"));

            @SuppressWarnings("unchecked")
            List<String> srcColumns = (List<String>) split.get("columns");
            Map<String, String> renameMap = parseRename(split.get("rename"));

            TableIdentifier targetId =
                    TableIdentifier.of(
                            sourceId.getCatalogName(),
                            targetDb,
                            sourceId.getSchemaName(),
                            "cdc_test_split" + suffix);

            tables.add(buildSplitTable(sourceTable, sourceId, targetId, srcColumns, renameMap));
        }
        return tables;
    }

    // ---- Helpers ----

    /** Holds source→target column mapping for one split table. */
    private static class SplitDef {
        final String[] sourceColumns;
        final String[] targetColumns;

        SplitDef(String[] sourceColumns, String[] targetColumns) {
            this.sourceColumns = sourceColumns;
            this.targetColumns = targetColumns;
        }
    }

    /**
     * Build a CatalogTable for a split target table.
     *
     * <p>Columns are taken from the source schema by {@code sourceColumnNames}. If a column has a
     * rename entry, a new {@link PhysicalColumn} is created with the target column name so the JDBC
     * sink generates correct INSERT SQL.
     */
    private static CatalogTable buildSplitTable(
            CatalogTable sourceTable,
            TableIdentifier sourceId,
            TableIdentifier targetId,
            List<String> sourceColumnNames,
            Map<String, String> renameMap) {

        TableSchema sourceSchema = sourceTable.getTableSchema();
        TableSchema.Builder schemaBuilder = TableSchema.builder();

        for (String srcName : sourceColumnNames) {
            Column srcCol =
                    sourceSchema.getColumns().stream()
                            .filter(c -> c.getName().equals(srcName))
                            .findFirst()
                            .orElseThrow(
                                    () ->
                                            new IllegalArgumentException(
                                                    "Column '"
                                                            + srcName
                                                            + "' not found in source table schema"));

            String targetName = renameMap.getOrDefault(srcName, srcName);

            if (!targetName.equals(srcName)) {
                // Create renamed column — target table has a different column name
                srcCol =
                        PhysicalColumn.of(
                                targetName,
                                srcCol.getDataType(),
                                srcCol.getColumnLength(),
                                srcCol.getScale(),
                                srcCol.isNullable(),
                                srcCol.getDefaultValue(),
                                srcCol.getComment(),
                                srcCol.getSourceType(),
                                srcCol.getOptions());
            }
            schemaBuilder.column(srcCol);
        }

        return CatalogTable.of(
                targetId,
                schemaBuilder.build(),
                sourceTable.getOptions(),
                sourceTable.getPartitionKeys(),
                sourceTable.getComment(),
                sourceTable.getCatalogName(),
                sourceTable.getMetadataSchema());
    }

    /**
     * Extract field values by <b>source</b> column names, returning them in the same order as
     * {@code sourceColumnNames} (which matches the target column order).
     */
    private Object[] projectFields(SourceRow sourceRow, String[] sourceColumnNames) {
        Object[] projected = new Object[sourceColumnNames.length];
        for (int i = 0; i < sourceColumnNames.length; i++) {
            projected[i] = sourceRow.getField(sourceColumnNames[i]);
        }
        return projected;
    }

    @SuppressWarnings("unchecked")
    private void incrementCounter(TransformContext context, String key) {
        Long current = (Long) context.getState(key);
        if (current == null) {
            current = 0L;
        }
        context.putState(key, current + 1);
    }

    /** Parse the rename map from config. Accepts Map<String, String>. */
    @SuppressWarnings("unchecked")
    private static Map<String, String> parseRename(Object renameObj) {
        Map<String, String> result = new LinkedHashMap<>();
        if (renameObj instanceof Map) {
            Map<?, ?> raw = (Map<?, ?>) renameObj;
            for (Map.Entry<?, ?> e : raw.entrySet()) {
                result.put(String.valueOf(e.getKey()), String.valueOf(e.getValue()));
            }
        }
        return result;
    }

    /** Build target column names: start with source names, apply rename overrides. */
    private static String[] buildTargetNames(
            String[] sourceColumns, Map<String, String> renameMap) {
        String[] target = new String[sourceColumns.length];
        for (int i = 0; i < sourceColumns.length; i++) {
            target[i] = renameMap.getOrDefault(sourceColumns[i], sourceColumns[i]);
        }
        return target;
    }

    private static Map<String, Object> createSplit(String suffix, String... columns) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("suffix", suffix);
        m.put("columns", Arrays.asList(columns));
        return m;
    }
}
