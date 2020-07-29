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
package io.seata.rm.datasource;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.SQLException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import io.seata.common.thread.NamedThreadFactory;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.constants.DBType;
import io.seata.core.model.BranchType;
import io.seata.core.model.Resource;
import io.seata.rm.DefaultResourceManager;
import io.seata.rm.datasource.sql.struct.TableMetaCacheFactory;
import io.seata.rm.datasource.util.JdbcUtils;

import static io.seata.core.constants.DefaultValues.DEFAULT_CLIENT_TABLE_META_CHECK_ENABLE;

/**
 * 数据源的代理（适配器模式）
 * The type Data source proxy.
 *
 * @author sharajava
 */
public class DataSourceProxy extends AbstractDataSourceProxy implements Resource {

    private String resourceGroupId;

    private static final String DEFAULT_RESOURCE_GROUP_ID = "DEFAULT";

    private String jdbcUrl;

    private String dbType;

    /**
     * Enable the table meta checker
     */
    private static boolean ENABLE_TABLE_META_CHECKER_ENABLE = ConfigurationFactory.getInstance().getBoolean(
        ConfigurationKeys.CLIENT_TABLE_META_CHECK_ENABLE, DEFAULT_CLIENT_TABLE_META_CHECK_ENABLE);

    /**
     * Table meta checker interval
     */
    private static final long TABLE_META_CHECKER_INTERVAL = 60000L;

    private final ScheduledExecutorService tableMetaExcutor = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("tableMetaChecker", 1, true));

    /**
     * 初始化数据源
     * Instantiates a new Data source proxy.
     *
     * @param targetDataSource the target data source
     */
    public DataSourceProxy(DataSource targetDataSource) {
        this(targetDataSource, DEFAULT_RESOURCE_GROUP_ID);
    }

    /**
     * 初始化数据源
     * Instantiates a new Data source proxy.
     *
     * @param targetDataSource the target data source
     * @param resourceGroupId  the resource group id
     */
    public DataSourceProxy(DataSource targetDataSource, String resourceGroupId) {
        super(targetDataSource);
        /** 初始化数据源 */
        init(targetDataSource, resourceGroupId);
    }

    /**
     * 初始化数据源（增强数据源的功能）
     *
     *      1、为每个数据源标识了资源组ID
     *      2、如果配置打开，会有一个定时线程池定时更新表的元数据信息并缓存到本地
     *      3、生成代理连接ConnectionProxy
     *
     * @param dataSource
     * @param resourceGroupId
     */
    private void init(DataSource dataSource, String resourceGroupId) {
        // 设置资源组id（默认值为default）
        this.resourceGroupId = resourceGroupId;
        // 获取连接
        try (Connection connection = dataSource.getConnection()) {
            // 获取数据库url
            jdbcUrl = connection.getMetaData().getURL();
            // 获取数据库类型
            dbType = JdbcUtils.getDbType(jdbcUrl);
        } catch (SQLException e) {
            throw new IllegalStateException("can not init dataSource", e);
        }
        /** 注册资源 */
        DefaultResourceManager.get().registerResource(this);
        if (ENABLE_TABLE_META_CHECKER_ENABLE) {
            //如果配置开关打开，会定时线程池不断更新表的元数据信息
            tableMetaExcutor.scheduleAtFixedRate(() -> {
                /** 获取数据库连接 */
                try (Connection connection = dataSource.getConnection()) {
                    TableMetaCacheFactory.getTableMetaCache(DataSourceProxy.this.getDbType())
                        .refresh(connection, DataSourceProxy.this.getResourceId());
                } catch (Exception ignore) {
                }
            }, 0, TABLE_META_CHECKER_INTERVAL, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * Gets plain connection.
     *
     * @return the plain connection
     * @throws SQLException the sql exception
     */
    public Connection getPlainConnection() throws SQLException {
        return targetDataSource.getConnection();
    }

    /**
     * Gets db type.
     *
     * @return the db type
     */
    public String getDbType() {
        return dbType;
    }

    /**
     * 获取数据库连接
     * @return
     * @throws SQLException
     */
    @Override
    public ConnectionProxy getConnection() throws SQLException {
        Connection targetConnection = targetDataSource.getConnection();
        /** 构建一个数据库连接代理 */
        return new ConnectionProxy(this, targetConnection);
    }

    @Override
    public ConnectionProxy getConnection(String username, String password) throws SQLException {
        Connection targetConnection = targetDataSource.getConnection(username, password);
        return new ConnectionProxy(this, targetConnection);
    }

    @Override
    public String getResourceGroupId() {
        return resourceGroupId;
    }

    @Override
    public String getResourceId() {
        if (DBType.POSTGRESQL.name().equalsIgnoreCase(getDbType())) {
            return getPGResourceId();
        } else {
            return getDefaultResourceId();
        }
    }

    /**
     * get the default resource id
     * @return resource id
     */
    private String getDefaultResourceId() {
        if (jdbcUrl.contains("?")) {
            return jdbcUrl.substring(0, jdbcUrl.indexOf('?'));
        } else {
            return jdbcUrl;
        }
    }

    /**
     * prevent pg sql url like
     * jdbc:postgresql://127.0.0.1:5432/seata?currentSchema=public
     * jdbc:postgresql://127.0.0.1:5432/seata?currentSchema=seata
     * cause the duplicated resourceId
     * it will cause the problem like
     * 1.get file lock fail
     * 2.error table meta cache
     * @return resourceId
     */
    private String getPGResourceId() {
        if (jdbcUrl.contains("?")) {
            StringBuilder jdbcUrlBuilder = new StringBuilder();
            jdbcUrlBuilder.append(jdbcUrl.substring(0, jdbcUrl.indexOf('?')));
            StringBuilder paramsBuilder = new StringBuilder();
            String paramUrl = jdbcUrl.substring(jdbcUrl.indexOf('?') + 1, jdbcUrl.length());
            String[] urlParams = paramUrl.split("&");
            for (String urlParam : urlParams) {
                if (urlParam.contains("currentSchema")) {
                    paramsBuilder.append(urlParam);
                    break;
                }
            }

            if (paramsBuilder.length() > 0) {
                jdbcUrlBuilder.append("?");
                jdbcUrlBuilder.append(paramsBuilder);
            }
            return jdbcUrlBuilder.toString();
        } else {
            return jdbcUrl;
        }
    }

    @Override
    public BranchType getBranchType() {
        return BranchType.AT;
    }
}
