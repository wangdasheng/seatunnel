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

import org.apache.seatunnel.api.configuration.ReadonlyConfig;

import lombok.extern.slf4j.Slf4j;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Factory for loading business {@link BusinessTransformPlugin} implementations.
 *
 * <p>Used internally by the framework. Business teams should NOT implement this interface — they
 * only need to implement {@link BusinessTransformPlugin} and specify the fully qualified class name
 * in the job config.
 *
 * <p><b>Configuration:</b>
 *
 * <pre>{@code
 * transform {
 *   CdcSync {
 *     plugin_class = "com.company.migration.OrderMigrationPlugin"
 *     plugin_config {
 *       join_mode = "left"
 *       filter_deleted = true
 *     }
 *   }
 * }
 * }</pre>
 *
 * <p>The framework reads {@code plugin_class}, instantiates it via reflection, and passes {@code
 * plugin_config} to {@link BusinessTransformPlugin#init(TransformContext)}.
 */
@Slf4j
public final class BusinessTransformFactory {

    private static final String PLUGIN_CLASS_KEY = "plugin_class";
    private static final String PLUGIN_CONFIG_KEY = "plugin_config";

    /** Cache loaded plugin instances by class name to avoid repeated reflection. */
    private static final Map<String, BusinessTransformPlugin> PLUGIN_CACHE =
            new ConcurrentHashMap<>();

    private BusinessTransformFactory() {}

    /**
     * Load and instantiate the business plugin from config.
     *
     * @param config the SeaTunnel transform config block
     * @return the instantiated plugin
     * @throws IllegalArgumentException if plugin_class is missing or the class cannot be loaded
     */
    public static BusinessTransformPlugin loadPlugin(ReadonlyConfig config) {
        String className = (String) config.toMap().get(PLUGIN_CLASS_KEY);
        if (className == null || className.isEmpty()) {
            throw new IllegalArgumentException(
                    "Missing required config '"
                            + PLUGIN_CLASS_KEY
                            + "'. Please specify the fully qualified class name of your "
                            + BusinessTransformPlugin.class.getSimpleName()
                            + " implementation.");
        }

        BusinessTransformPlugin cached = PLUGIN_CACHE.get(className);
        if (cached != null) {
            log.info("Reusing cached plugin instance: {}", className);
            return cached;
        }

        try {
            Class<?> clazz = Class.forName(className);
            if (!BusinessTransformPlugin.class.isAssignableFrom(clazz)) {
                throw new IllegalArgumentException(
                        "Class '"
                                + className
                                + "' does not implement "
                                + BusinessTransformPlugin.class.getName());
            }
            BusinessTransformPlugin plugin =
                    (BusinessTransformPlugin) clazz.getDeclaredConstructor().newInstance();
            PLUGIN_CACHE.put(className, plugin);
            log.info("Successfully loaded business plugin: {}", className);
            return plugin;
        } catch (ClassNotFoundException e) {
            throw new IllegalArgumentException(
                    "Business plugin class not found: "
                            + className
                            + ". Make sure the JAR is on the SeaTunnel classpath.",
                    e);
        } catch (Exception e) {
            throw new IllegalArgumentException(
                    "Failed to instantiate business plugin: " + className, e);
        }
    }

    /**
     * Extract the plugin-specific configuration from the transform config.
     *
     * <p>The {@code plugin_config} block from the config is returned as a flat map.
     */
    @SuppressWarnings("unchecked")
    public static Map<String, Object> extractPluginConfig(ReadonlyConfig config) {
        Map<String, Object> result = new java.util.HashMap<>();
        // Flatten plugin_config if present; otherwise pass all non-framework keys
        Object pluginConfig = config.getSourceMap().get(PLUGIN_CONFIG_KEY);
        if (pluginConfig instanceof Map) {
            result.putAll((Map<String, Object>) pluginConfig);
        }
        // Also put the class name for reference
        String className = (String) config.getSourceMap().get(PLUGIN_CLASS_KEY);
        if (className != null) {
            result.put(PLUGIN_CLASS_KEY, className);
        }
        return result;
    }
}
