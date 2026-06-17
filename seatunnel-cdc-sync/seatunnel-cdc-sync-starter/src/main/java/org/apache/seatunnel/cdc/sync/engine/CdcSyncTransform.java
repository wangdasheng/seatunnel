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

package org.apache.seatunnel.cdc.sync.engine;

import org.apache.seatunnel.api.configuration.ReadonlyConfig;
import org.apache.seatunnel.api.table.catalog.CatalogTable;
import org.apache.seatunnel.api.table.catalog.Column;
import org.apache.seatunnel.api.table.catalog.TableIdentifier;
import org.apache.seatunnel.api.table.catalog.TableSchema;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.cdc.sync.api.BusinessTransformFactory;
import org.apache.seatunnel.cdc.sync.api.BusinessTransformPlugin;
import org.apache.seatunnel.transform.common.AbstractCatalogSupportFlatMapTransform;

import lombok.NonNull;
import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.Map;

/**
 * SeaTunnel Transform adapter for the CDC Sync framework.
 *
 * <p>Registers as a standard SeaTunnel Transform plugin named {@value #PLUGIN_NAME}. Internally:
 *
 * <ol>
 *   <li>Loads the business {@link BusinessTransformPlugin} from config
 *   <li>Creates a {@link TransformRunner} to bridge {@code SeaTunnelRow} ↔ business API
 *   <li>For each incoming CDC row, calls {@link TransformRunner#processRow(SeaTunnelRow)}
 *   <li>Returns rows with {@code tableId} set for downstream {@code MultiTableSink} routing
 * </ol>
 *
 * <p><b>Configuration example:</b>
 *
 * <pre>{@code
 * transform {
 *   CdcSync {
 *     source_table_name = "cdc_raw"
 *     result_table_name = "routed_to_sink"
 *     plugin_class = "com.company.migration.OrderMigrationPlugin"
 *     plugin_config {
 *       join_mode = "left"
 *       order_table = "test.tb_sale_order"
 *       extend_table = "test.tb_sale_order_extend"
 *     }
 *   }
 * }
 * }</pre>
 */
@Slf4j
public class CdcSyncTransform extends AbstractCatalogSupportFlatMapTransform {

    public static final String PLUGIN_NAME = "CdcSync";

    private final ReadonlyConfig config;
    private TransformRunner runner;

    /** Cached output tables for {@code MultiTableSink} writer pre-creation. */
    private volatile List<CatalogTable> outputCatalogTables;

    public CdcSyncTransform(@NonNull ReadonlyConfig config, @NonNull CatalogTable catalogTable) {
        super(catalogTable);
        this.config = config;
    }

    @Override
    public String getPluginName() {
        return PLUGIN_NAME;
    }

    @Override
    public void open() {
        // Extract field names from the input catalog table
        String[] fieldNames =
                inputCatalogTable.getTableSchema().getColumns().stream()
                        .map(Column::getName)
                        .toArray(String[]::new);

        // Load business plugin via reflection
        BusinessTransformPlugin plugin = BusinessTransformFactory.loadPlugin(config);

        // Build context with business config
        Map<String, Object> pluginConfig = BusinessTransformFactory.extractPluginConfig(config);
        TransformContextImpl context = new TransformContextImpl(pluginConfig);

        // Initialize plugin
        plugin.init(context);

        // Create runner
        this.runner = new TransformRunner(plugin, context, fieldNames);

        log.info(
                "CdcSyncTransform initialized. Plugin: {}, Input table: {}, Fields: {}",
                plugin.getPluginName(),
                inputCatalogTable.getTableId().toTablePath(),
                fieldNames.length);
    }

    /**
     * Declares output tables so {@code MultiTableSink} can pre-create writers.
     *
     * <p>Supports horizontal sharding (one source → table_0 .. table_N) and field splitting.
     */
    @Override
    public List<CatalogTable> getProducedCatalogTables() {
        if (outputCatalogTables == null) {
            synchronized (this) {
                if (outputCatalogTables == null) {
                    Map<String, Object> pluginConfig =
                            BusinessTransformFactory.extractPluginConfig(config);
                    BusinessTransformPlugin plugin = BusinessTransformFactory.loadPlugin(config);
                    outputCatalogTables = plugin.getTargetTables(inputCatalogTable, pluginConfig);
                    log.info(
                            "Plugin '{}' declared {} output CatalogTable(s)",
                            plugin.getPluginName(),
                            outputCatalogTables.size());
                }
            }
        }
        return outputCatalogTables;
    }

    @Override
    protected List<SeaTunnelRow> transformRow(SeaTunnelRow inputRow) {
        if (runner == null) {
            throw new IllegalStateException(
                    "CdcSyncTransform runner is null — open() failed to initialize. "
                            + "Check that the plugin_class JAR is on the classpath and class "
                            + "implements BusinessTransformPlugin.");
        }
        return runner.processRow(inputRow);
    }

    @Override
    protected TableSchema transformTableSchema() {
        // Schema stays the same; per-row routing is handled by transformRow
        return inputCatalogTable.getTableSchema();
    }

    @Override
    protected TableIdentifier transformTableIdentifier() {
        // Table identifier is set per-row in transformRow
        return inputCatalogTable.getTableId();
    }

    @Override
    public void close() {
        if (runner != null) {
            log.info(
                    "CdcSyncTransform closing. Final state size: {}",
                    runner.getContext().getStateSize());
        }
    }
}
