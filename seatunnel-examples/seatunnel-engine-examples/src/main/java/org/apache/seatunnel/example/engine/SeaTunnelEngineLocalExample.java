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

package org.apache.seatunnel.example.engine;

import org.apache.seatunnel.core.starter.SeaTunnel;
import org.apache.seatunnel.core.starter.enums.MasterType;
import org.apache.seatunnel.core.starter.exception.CommandException;
import org.apache.seatunnel.core.starter.seatunnel.args.ClientCommandArgs;

import java.io.FileNotFoundException;
import java.net.URISyntaxException;
import java.net.URL;
import java.nio.file.Paths;

public class SeaTunnelEngineLocalExample {

    static {
        // https://logging.apache.org/log4j/2.x/manual/simple-logger.html#isThreadContextMapInheritable
        System.setProperty("log4j2.isThreadContextMapInheritable", "true");

        // === DEBUG: 延长 Hazelcast 超时，避免断点调试时超时断开 ===
        // 客户端调用超时 (默认 120s，改为 1 小时)
        System.setProperty("hazelcast.client.invocation.timeout.seconds", "3600");
        // 服务端调用超时
        System.setProperty("hazelcast.invocation.timeout.seconds", "3600");
        // 操作调用超时
        System.setProperty("hazelcast.operation.call.timeout.millis", "3600000");
        // 心跳间隔延长，减少心跳频率
        System.setProperty("hazelcast.heartbeat.interval.seconds", "60");
        // 允许无心跳的最大时间 (默认 180s → 1 小时)
        System.setProperty("hazelcast.max.no.heartbeat.seconds", "3600");
        // 客户端心跳
        System.setProperty("hazelcast.client.heartbeat.timeout", "3600000");
        System.setProperty("hazelcast.client.heartbeat.interval", "60000");
        // 使用 deadline 检测器代替 phi-accrual (更简单，不会误判)
        System.setProperty("hazelcast.heartbeat.failuredetector.type", "deadline");
    }

    public static void main(String[] args)
            throws FileNotFoundException, URISyntaxException, CommandException {
        String configurePath = args.length > 0 ? args[0] : "/examples/cdc_split_table.conf";
        String configFile = getTestConfigFile(configurePath);
        ClientCommandArgs clientCommandArgs = new ClientCommandArgs();
        clientCommandArgs.setConfigFile(configFile);
        clientCommandArgs.setCheckConfig(false);
        clientCommandArgs.setJobName(Paths.get(configFile).getFileName().toString());
        // Change Execution Mode to CLUSTER to use client mode, before do this, you should start
        // SeaTunnelEngineClusterServerExample
        clientCommandArgs.setMasterType(MasterType.LOCAL);
        SeaTunnel.run(clientCommandArgs.buildCommand());
    }

    public static String getTestConfigFile(String configFile)
            throws FileNotFoundException, URISyntaxException {
        URL resource = SeaTunnelEngineLocalExample.class.getResource(configFile);
        if (resource == null) {
            throw new FileNotFoundException("Can't find config file: " + configFile);
        }
        return Paths.get(resource.toURI()).toString();
    }
}
