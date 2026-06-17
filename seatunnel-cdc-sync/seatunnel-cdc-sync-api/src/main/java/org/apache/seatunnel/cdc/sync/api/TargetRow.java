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
import java.util.Objects;

/**
 * Represents a target row to be written by the framework's MultiTableSink.
 *
 * <p>Each {@code TargetRow} specifies:
 *
 * <ul>
 *   <li><b>targetTable</b> — the destination table name, used by MultiTableSink to route the row to
 *       the correct JDBC writer
 *   <li><b>fields</b> — the projected field values, in the same order as the target table's columns
 *   <li><b>opType</b> — the DML operation type: "INSERT", "UPDATE", or "DELETE"
 * </ul>
 *
 * <p><b>Fan-out example:</b> one source JOIN result → 5 target rows:
 *
 * <pre>{@code
 * SourceRow joined = ...;
 * return Arrays.asList(
 *     new TargetRow("sale_order",    project1, "INSERT"),
 *     new TargetRow("sale_order_ext", project2, "INSERT"),
 *     new TargetRow("settlement",    project3, "INSERT"),
 *     new TargetRow("receive_info",  project4, "INSERT"),
 *     new TargetRow("ship_info",     project5, "INSERT")
 * );
 * }</pre>
 */
public class TargetRow implements Serializable {

    private static final long serialVersionUID = 1L;

    private final String targetTable;
    private final Object[] fields;
    private final String opType;

    public TargetRow(String targetTable, Object[] fields, String opType) {
        this.targetTable = targetTable;
        this.fields = fields;
        this.opType = opType;
    }

    /** Convenience constructor, defaults to INSERT. */
    public TargetRow(String targetTable, Object[] fields) {
        this(targetTable, fields, "INSERT");
    }

    // ---- Getters ----

    /** Target table name for MultiTableSink routing. */
    public String getTargetTable() {
        return targetTable;
    }

    /** Field values in target column order. */
    public Object[] getFields() {
        return fields;
    }

    /** DML operation: "INSERT", "UPDATE", or "DELETE". */
    public String getOpType() {
        return opType;
    }

    // ---- Convenience ----

    public boolean isInsert() {
        return "INSERT".equals(opType);
    }

    public boolean isUpdate() {
        return "UPDATE".equals(opType);
    }

    public boolean isDelete() {
        return "DELETE".equals(opType);
    }

    @Override
    public String toString() {
        return "TargetRow{"
                + "targetTable='"
                + targetTable
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
        if (!(o instanceof TargetRow)) {
            return false;
        }
        TargetRow that = (TargetRow) o;
        return Objects.equals(targetTable, that.targetTable)
                && Objects.equals(opType, that.opType)
                && Arrays.equals(fields, that.fields);
    }

    @Override
    public int hashCode() {
        int result = Objects.hash(targetTable, opType);
        result = 31 * result + Arrays.hashCode(fields);
        return result;
    }
}
