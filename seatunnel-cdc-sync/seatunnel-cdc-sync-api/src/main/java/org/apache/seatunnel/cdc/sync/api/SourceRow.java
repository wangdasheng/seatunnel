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

import java.io.Serializable;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

/**
 * Represents a single CDC source row with metadata.
 *
 * <p>Wraps the internal {@code SeaTunnelRow} to provide a clean, framework-agnostic API for
 * business plugins. Contains:
 *
 * <ul>
 *   <li><b>tableId</b> — the source table name (e.g. "tb_sale_order")
 *   <li><b>opType</b> — the DML operation type: "INSERT", "UPDATE", "DELETE"
 *   <li><b>fields</b> — the field values array
 *   <li><b>fieldNames</b> — the field name array, allows {@link #getField(String)} by name
 *   <li><b>timestamp</b> — binlog event timestamp (0 during full snapshot phase)
 * </ul>
 *
 * <p>This class is intentionally separated from {@code SeaTunnelRow} so business teams never need
 * to import SeaTunnel internals.
 */
public class SourceRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String tableId;
    private final String opType;
    private final Object[] fields;
    private final String[] fieldNames;
    private final long timestamp;

    /** Lazily-built index for O(1) field lookup by name. */
    private transient Map<String, Integer> nameToIndex;

    public SourceRow(
            String tableId, String opType, Object[] fields, String[] fieldNames, long timestamp) {
        this.tableId = tableId;
        this.opType = opType;
        this.fields = fields;
        this.fieldNames = fieldNames;
        this.timestamp = timestamp;
    }

    // ---- Getters ----

    /** Source table name, e.g. "test.tb_sale_order". */
    public String getTableId() {
        return tableId;
    }

    /** DML operation: "INSERT", "UPDATE", or "DELETE". */
    public String getOpType() {
        return opType;
    }

    /** All field values in source column order. */
    public Object[] getFields() {
        return fields;
    }

    /** All field names in source column order. */
    public String[] getFieldNames() {
        return fieldNames;
    }

    /** Binlog event timestamp in milliseconds; 0 during full-snapshot phase. */
    public long getTimestamp() {
        return timestamp;
    }

    /** Number of fields. */
    public int getArity() {
        return fields.length;
    }

    // ---- Field access ----

    /** Get field value by index. */
    public Object getField(int index) {
        return fields[index];
    }

    /** Get field value by name (first match). Returns null if field not found. */
    public Object getField(String name) {
        Integer index = getIndex(name);
        return index != null ? fields[index] : null;
    }

    /** Get field value by name with type cast. */
    @SuppressWarnings("unchecked")
    public <T> T getField(String name, Class<T> type) {
        return (T) getField(name);
    }

    /** Get the column index for a field name, or null if not found. */
    public Integer getIndex(String name) {
        if (nameToIndex == null) {
            nameToIndex = new HashMap<>();
            for (int i = 0; i < fieldNames.length; i++) {
                nameToIndex.put(fieldNames[i], i);
            }
        }
        return nameToIndex.get(name);
    }

    /** Returns true if the field at the given index is null. */
    public boolean isNullAt(int index) {
        return fields[index] == null;
    }

    /** Returns true if the named field is null or the field does not exist. */
    public boolean isNullAt(String name) {
        Integer idx = getIndex(name);
        return idx == null || fields[idx] == null;
    }

    // ---- Convenience ----

    /** True if this is an INSERT operation. */
    public boolean isInsert() {
        return "INSERT".equals(opType);
    }

    /** True if this is an UPDATE operation. */
    public boolean isUpdate() {
        return "UPDATE".equals(opType);
    }

    /** True if this is a DELETE operation. */
    public boolean isDelete() {
        return "DELETE".equals(opType);
    }

    @Override
    public String toString() {
        return "SourceRow{"
                + "tableId='"
                + tableId
                + '\''
                + ", opType='"
                + opType
                + '\''
                + ", fields="
                + Arrays.toString(fields)
                + '}';
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SourceRow)) {
            return false;
        }
        SourceRow that = (SourceRow) o;
        return timestamp == that.timestamp
                && Objects.equals(tableId, that.tableId)
                && Objects.equals(opType, that.opType)
                && Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(tableId, opType, timestamp);
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }
}
