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

package org.apache.seatunnel.cdc.sync.api;

import org.apache.seatunnel.api.table.catalog.CatalogTable;

import java.io.Serializable;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * Core interface for business teams to define CDC sync logic.
 *
 * <p>Each row from CDC source (either full-snapshot chunk or incremental binlog event) is passed
 * through {@link #transform(SourceRow, TransformContext)}. The business plugin decides:
 *
 * <ul>
 *   <li><b>Routing</b>: which target table(s) the row should go to
 *   <li><b>Field mapping</b>: which columns from the source map to which target columns
 *   <li><b>Fan-out</b>: one source row can produce 0~N target rows (e.g. 1 join result → 5 target
 *       tables)
 *   <li><b>Filtering</b>: return empty list to drop the row
 *   <li><b>Stateful JOIN</b>: use {@link TransformContext#getState(String)} / {@link
 *       TransformContext#putState(String, Object)} to cache rows for cross-table matching
 * </ul>
 *
 * <p><b>Execution model:</b> {@code transform()} is called synchronously on the Source Reader
 * thread. <b>Do NOT perform blocking I/O</b> (no HTTP calls, no DB queries). Use {@link
 * TransformContext#getState(String)} for local state access.
 *
 * <p><b>Lifecycle:</b>
 *
 * <ol>
 *   <li>Framework calls {@code init()} once when the job starts
 *   <li>{@code transform()} is called for every row, including full-snapshot and incremental phases
 *   <li>Framework calls {@code close()} when the job ends
 * </ol>
 *
 * <p><b>Example — Order migration plugin:</b>
 *
 * <pre>{@code
 * public class OrderMigrationPlugin implements BusinessTransformPlugin {
 *     public List<TargetRow> transform(SourceRow row, TransformContext ctx) {
 *         if ("tb_sale_order".equals(row.getTableId())) {
 *             ctx.putState("order_" + row.getField("c_sale_order_id"), row);
 *             List<TargetRow> results = new ArrayList<>();
 *             results.add(project(row, "sale_order", "c_sale_order_id", "c_order_no"));
 *             results.add(project(row, "settlement", "c_settle_id", "c_settle_amount"));
 *             return results;
 *         } else {
 *             SourceRow cached = ctx.getState("order_" + row.getField("c_sale_order_id"));
 *             if (cached != null) {
 *                 ctx.removeState("order_" + row.getField("c_sale_order_id"));
 *                 return Arrays.asList(projectWithExtend(cached, row, "sale_order"));
 *             }
 *             return Collections.emptyList();
 *         }
 *     }
 * }
 * }</pre>
 */
public interface BusinessTransformPlugin extends Serializable {

    /**
     * Called once when the transform is initialized. Use this to parse custom config and prepare
     * resources.
     *
     * @param context the transform context with config and state access
     */
    default void init(TransformContext context) {}

    /**
     * Transform a single CDC source row into 0~N target rows.
     *
     * @param sourceRow the incoming CDC row with table metadata, field values, and operation type
     * @param context context with config and state API
     * @return list of target rows; return empty list to filter out this row
     */
    List<TargetRow> transform(SourceRow sourceRow, TransformContext context);

    /** Called when the job ends. Clean up resources. */
    default void close() {}

    /** Human-readable name for logging and monitoring purposes. */
    default String getPluginName() {
        return getClass().getSimpleName();
    }

    /**
     * Declares all target {@link CatalogTable}s this plugin may produce at runtime.
     *
     * <p>Called during <b>job planning</b> (before {@link #init(TransformContext)}) so that {@code
     * MultiTableSink} can pre-create writers for every possible target table. The default
     * implementation returns the source table as-is (one-to-one mapping).
     *
     * <p><b>Override this for:</b>
     *
     * <ul>
     *   <li><b>Horizontal sharding</b>: one source table → N shard tables (table_0 .. table_N), all
     *       sharing the same schema
     *   <li><b>Field-level splitting</b>: one wide source table → multiple narrow target tables,
     *       each with a different column subset
     *   <li><b>Fan-out routing</b>: one source table → many target tables with identical or
     *       transformed schemas
     * </ul>
     *
     * @param sourceTable the input CatalogTable from the CDC source
     * @param pluginConfig the business plugin's {@code plugin_config} block as a flat map
     * @return list of all possible output CatalogTables
     */
    default List<CatalogTable> getTargetTables(
            CatalogTable sourceTable, Map<String, Object> pluginConfig) {
        return Collections.singletonList(sourceTable);
    }
}
