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
import java.util.Map;

/**
 * Framework context passed to {@link BusinessTransformPlugin#transform(SourceRow,
 * TransformContext)}.
 *
 * <p>Provides two categories of capabilities:
 *
 * <h3>1. Read-only configuration</h3>
 *
 * <pre>{@code
 * // Access business-defined config from the job .conf file:
 * String mode = ctx.getConfig("join_mode", "left");
 * int batchSize = ctx.getConfig("batch_size", 5000);
 * }</pre>
 *
 * <h3>2. Stateful operations</h3>
 *
 * <p>For scenarios like streaming JOIN where you need to cache rows for cross-table matching:
 *
 * <pre>{@code
 * // Cache order row, waiting for extend row
 * ctx.putState("order_" + orderId, row);
 *
 * // Later, when extend row arrives:
 * SourceRow order = ctx.removeState("order_" + orderId);
 * if (order != null) { ... emit joined row ... }
 * }</pre>
 *
 * <p><b>Important:</b> State is <b>in-memory</b> in this version. During the full-snapshot phase, a
 * large amount of state may accumulate. Business plugins should:
 *
 * <ul>
 *   <li>Manage their own eviction policies (e.g. remove old entries after matching)
 *   <li>Keep FULL-snapshot state minimal (e.g. use a separate JOIN strategy for batch phase)
 * </ul>
 */
public interface TransformContext extends Serializable {

    /**
     * Get a typed configuration value. Falls back to defaultValue if the key is not present.
     *
     * @param key config key
     * @param defaultValue fallback value
     * @param <T> expected type
     * @return config value
     */
    <T> T getConfig(String key, T defaultValue);

    /** Get a typed configuration value, or null if not present. */
    <T> T getConfig(String key);

    /** Returns all raw configuration key-value pairs. */
    Map<String, Object> getConfigMap();

    // ---- State API ----

    /**
     * Store a value in the in-memory state map.
     *
     * @param key state key
     * @param value state value
     */
    void putState(String key, Object value);

    /**
     * Retrieve a value from the in-memory state map.
     *
     * @param key state key
     * @param <T> expected type
     * @return the value, or null if not present
     */
    @SuppressWarnings("unchecked")
    <T> T getState(String key);

    /**
     * Remove and return a value from the state map.
     *
     * @param key state key
     * @param <T> expected type
     * @return the removed value, or null if not present
     */
    @SuppressWarnings("unchecked")
    <T> T removeState(String key);

    /** Check if the state map contains a key. */
    boolean containsState(String key);

    /** Current size of the state map (for monitoring). */
    int getStateSize();

    /** Clear all state entries (called after full-snapshot phase completes). */
    void clearState();
}
