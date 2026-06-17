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

import org.apache.seatunnel.cdc.sync.api.TransformContext;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Default implementation of {@link TransformContext}.
 *
 * <p>Provides in-memory state storage backed by {@link ConcurrentHashMap} and configuration from
 * the job config's {@code plugin_config} block.
 *
 * <p><b>State lifecycle:</b>
 *
 * <ul>
 *   <li>State is ephemeral — it does not survive job restarts in this version
 *   <li>{@link #clearState()} is called by the framework when the full-snapshot phase completes
 *   <li>Business plugins should implement their own eviction strategies for long-running state
 * </ul>
 */
public class TransformContextImpl implements TransformContext {

    private static final long serialVersionUID = 1L;

    private final Map<String, Object> configMap;
    private final ConcurrentHashMap<String, Object> stateMap;

    public TransformContextImpl(Map<String, Object> configMap) {
        this.configMap = new HashMap<>(configMap);
        this.stateMap = new ConcurrentHashMap<>();
    }

    // ---- Config access ----

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getConfig(String key, T defaultValue) {
        Object value = configMap.get(key);
        if (value == null) {
            return defaultValue;
        }
        try {
            return (T) value;
        } catch (ClassCastException e) {
            return defaultValue;
        }
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getConfig(String key) {
        return (T) configMap.get(key);
    }

    @Override
    public Map<String, Object> getConfigMap() {
        return Collections.unmodifiableMap(configMap);
    }

    // ---- State API ----

    @Override
    public void putState(String key, Object value) {
        stateMap.put(key, value);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T getState(String key) {
        return (T) stateMap.get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public <T> T removeState(String key) {
        return (T) stateMap.remove(key);
    }

    @Override
    public boolean containsState(String key) {
        return stateMap.containsKey(key);
    }

    @Override
    public int getStateSize() {
        return stateMap.size();
    }

    @Override
    public void clearState() {
        stateMap.clear();
    }

    // ---- Framework-internal ----

    /** For snapshot/restore if checkpoint support is added later. */
    public Map<String, Object> snapshotStateSnapshot() {
        return new HashMap<>(stateMap);
    }

    /** Restore state from a previous snapshot. */
    public void restoreState(Map<String, Object> savedState) {
        stateMap.clear();
        if (savedState != null) {
            stateMap.putAll(savedState);
        }
    }
}
