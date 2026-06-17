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

import org.apache.seatunnel.api.table.type.RowKind;
import org.apache.seatunnel.api.table.type.SeaTunnelRow;
import org.apache.seatunnel.cdc.sync.api.BusinessTransformPlugin;
import org.apache.seatunnel.cdc.sync.api.SourceRow;
import org.apache.seatunnel.cdc.sync.api.TargetRow;
import org.apache.seatunnel.cdc.sync.api.TransformContext;

import lombok.extern.slf4j.Slf4j;

import java.util.ArrayList;
import java.util.List;

/**
 * Bridge between SeaTunnel's {@code SeaTunnelRow} and the business plugin's {@link
 * BusinessTransformPlugin#transform(SourceRow, TransformContext)}.
 *
 * <p>Responsibilities:
 *
 * <ul>
 *   <li>Convert {@code SeaTunnelRow} → {@link SourceRow} (framework-internal → business API)
 *   <li>Invoke {@link BusinessTransformPlugin#transform(SourceRow, TransformContext)}
 *   <li>Convert {@link List<TargetRow>} → {@link List<SeaTunnelRow>} with correct {@code tableId}
 *       and {@code rowKind} for downstream {@code MultiTableSink} routing
 * </ul>
 *
 * <p>This is the <b>hot path</b> of the CDC sync pipeline. Every CDC row passes through here.
 */
@Slf4j
public class TransformRunner {

    private final BusinessTransformPlugin plugin;
    private final TransformContextImpl context;

    /** Field names from the source CDC stream, used to build SourceRow. */
    private final String[] sourceFieldNames;

    /** Whether we're in the full-snapshot phase. Used for logging and state management hints. */
    private volatile boolean snapshotPhase = true;

    public TransformRunner(
            BusinessTransformPlugin plugin,
            TransformContextImpl context,
            String[] sourceFieldNames) {
        this.plugin = plugin;
        this.context = context;
        this.sourceFieldNames = sourceFieldNames;
    }

    /**
     * Process a single CDC row through the business plugin.
     *
     * @param rawRow the raw SeaTunnelRow from CDC source
     * @return list of SeaTunnelRows ready for MultiTableSink routing
     */
    public List<SeaTunnelRow> processRow(SeaTunnelRow rawRow) {
        // 1. Convert SeaTunnelRow → SourceRow
        SourceRow sourceRow = toSourceRow(rawRow);

        // 2. Call business plugin
        List<TargetRow> targetRows;
        try {
            targetRows = plugin.transform(sourceRow, context);
        } catch (Exception e) {
            log.error(
                    "Business plugin '{}' threw exception on row: {}",
                    plugin.getPluginName(),
                    sourceRow,
                    e);
            throw new RuntimeException(
                    "Business plugin '" + plugin.getPluginName() + "' failed on row: " + sourceRow,
                    e);
        }

        if (targetRows == null || targetRows.isEmpty()) {
            return java.util.Collections.emptyList();
        }

        // 3. Convert TargetRow → SeaTunnelRow
        List<SeaTunnelRow> results = new ArrayList<>(targetRows.size());
        for (TargetRow tr : targetRows) {
            results.add(toSeaTunnelRow(tr));
        }
        return results;
    }

    /**
     * Called by framework when the full-snapshot phase finishes and incremental phase begins.
     *
     * <p>Clears state to free memory, as full-snapshot JOIN state is no longer needed.
     */
    public void onSnapshotPhaseComplete() {
        snapshotPhase = false;
        context.clearState();
        log.info(
                "Snapshot phase complete for plugin '{}'. State cleared. State size before clear: {}",
                plugin.getPluginName(),
                context.getStateSize());
    }

    public TransformContextImpl getContext() {
        return context;
    }

    // ---- Internal conversion methods ----

    /** Convert SeaTunnelRow (CDC raw data) → SourceRow (business API). */
    SourceRow toSourceRow(SeaTunnelRow row) {
        String tableId = row.getTableId();
        String opType = toOpTypeString(row.getRowKind());
        Object[] fields = row.getFields();
        long timestamp = System.currentTimeMillis(); // CDC timestamp not directly available

        return new SourceRow(tableId, opType, fields, sourceFieldNames, timestamp);
    }

    /** Convert TargetRow (business API) → SeaTunnelRow (for MultiTableSink). */
    SeaTunnelRow toSeaTunnelRow(TargetRow targetRow) {
        SeaTunnelRow row = new SeaTunnelRow(targetRow.getFields());
        row.setTableId(targetRow.getTargetTable());
        row.setRowKind(toRowKind(targetRow.getOpType()));
        return row;
    }

    // ---- Utility ----

    private static String toOpTypeString(RowKind rowKind) {
        switch (rowKind) {
            case INSERT:
                return "INSERT";
            case UPDATE_AFTER:
                return "UPDATE";
            case DELETE:
                return "DELETE";
            case UPDATE_BEFORE:
                return "UPDATE_BEFORE";
            default:
                return "UNKNOWN";
        }
    }

    private static RowKind toRowKind(String opType) {
        switch (opType.toUpperCase()) {
            case "INSERT":
                return RowKind.INSERT;
            case "UPDATE":
                return RowKind.UPDATE_AFTER;
            case "DELETE":
                return RowKind.DELETE;
            default:
                return RowKind.INSERT;
        }
    }
}
