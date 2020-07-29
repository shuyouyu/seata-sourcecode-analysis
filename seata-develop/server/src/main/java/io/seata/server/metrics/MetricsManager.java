/*
 *  Copyright 1999-2019 Seata.io Group.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */
package io.seata.server.metrics;

import java.util.List;

import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.metrics.exporter.Exporter;
import io.seata.metrics.exporter.ExporterFactory;
import io.seata.metrics.registry.Registry;
import io.seata.metrics.registry.RegistryFactory;
import io.seata.server.event.EventBusManager;

/**
 * Metrics manager for init
 *
 * @author zhengyangyong
 */
public class MetricsManager {
    private static class SingletonHolder {
        private static MetricsManager INSTANCE = new MetricsManager();
    }

    public static final MetricsManager get() {
        return MetricsManager.SingletonHolder.INSTANCE;
    }

    private Registry registry;

    public Registry getRegistry() {
        return registry;
    }

    /**
     * 初始化指标
     */
    public void init() {
        // 指标是否已启动
        boolean enabled = ConfigurationFactory.getInstance().getBoolean(
            ConfigurationKeys.METRICS_PREFIX + ConfigurationKeys.METRICS_ENABLED, false);
        if (enabled) {
            // 获取注册中心
            registry = RegistryFactory.getInstance();
            if (registry != null) {
                List<Exporter> exporters = ExporterFactory.getInstanceList();
                // only at least one metrics exporter implement had imported in pom then need register MetricsSubscriber
                // 只有至少一个度量标准导出器工具已在pom中导入，然后需要注册MetricsSubscriber
                // 把度量数据同步给对应的监控系统，如：Prometheus
                if (exporters.size() != 0) {
                    exporters.forEach(exporter -> exporter.setRegistry(registry));
                    EventBusManager.get().register(new MetricsSubscriber(registry));
                }
            }
        }
    }
}
