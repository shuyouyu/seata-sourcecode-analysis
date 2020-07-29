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
package io.seata.metrics.exporter;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import io.seata.common.loader.EnhancedServiceLoader;
import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Exporter Factory for load all configured exporters
 *
 * @author zhengyangyong
 */
public class ExporterFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(ExporterFactory.class);

    public static List<Exporter> getInstanceList() {
        List<Exporter> exporters = new ArrayList<>();

        // 从配置中心获取要初始化哪些exporter
        String exporterTypeNameList = ConfigurationFactory.getInstance().getConfig(
            ConfigurationKeys.METRICS_PREFIX + ConfigurationKeys.METRICS_EXPORTER_LIST, null);
        if (!StringUtils.isNullOrEmpty(exporterTypeNameList)) {

            //配置中多个exporter肯定是用英文逗号隔开的
            String[] exporterTypeNames = exporterTypeNameList.split(",");
            for (String exporterTypeName : exporterTypeNames) {
                ExporterType exporterType;
                try {
                    // 得到exporterType
                    exporterType = ExporterType.getType(exporterTypeName);
                    // 通过扩展点加载exporter，并保存在exporters中
                    exporters.add(
                        EnhancedServiceLoader.load(Exporter.class, Objects.requireNonNull(exporterType).getName()));
                } catch (Exception exx) {
                    LOGGER.error("not support metrics exporter type: {}",exporterTypeName, exx);
                }
            }
        }
        return exporters;
    }
}
