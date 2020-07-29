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
package io.seata.rm.datasource.exec;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.Savepoint;
import java.sql.Statement;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import io.seata.common.util.StringUtils;
import io.seata.core.context.RootContext;
import io.seata.rm.datasource.StatementProxy;
import io.seata.sqlparser.SQLRecognizer;
import io.seata.sqlparser.SQLSelectRecognizer;
import io.seata.rm.datasource.sql.struct.TableRecords;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Select for update executor.
 *
 * @param <S> the type parameter
 * @author sharajava
 */
public class SelectForUpdateExecutor<T, S extends Statement> extends BaseTransactionalExecutor<T, S> {

    private static final Logger LOGGER = LoggerFactory.getLogger(SelectForUpdateExecutor.class);

    /**
     * Instantiates a new Select for update executor.
     *
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param sqlRecognizer     the sql recognizer
     */
    public SelectForUpdateExecutor(StatementProxy<S> statementProxy, StatementCallback<T, S> statementCallback,
                                   SQLRecognizer sqlRecognizer) {
        super(statementProxy, statementCallback, sqlRecognizer);
    }

    /**
     * 执行sql（如果应用在特定场景下，必需要求全局的 读已提交 ，目前 Seata 的方式是通过 SELECT FOR UPDATE 语句的代理）
     * @param args the args
     * @return
     * @throws Throwable
     */
    @Override
    public T doExecute(Object... args) throws Throwable {

        // 获取连接
        Connection conn = statementProxy.getConnection();
        // 获取元数据
        DatabaseMetaData dbmd = conn.getMetaData();
        T rs;
        Savepoint sp = null;
        // 构建一个锁重试控制器
        LockRetryController lockRetryController = new LockRetryController();
        // 连接获取自动提交
        boolean originalAutoCommit = conn.getAutoCommit();
        // 创建参数集合
        ArrayList<List<Object>> paramAppenderList = new ArrayList<>();
        /** 2、构建select for update语句，获得行锁 */
        String selectPKSQL = buildSelectSQL(paramAppenderList);

        try {
            // 如果时自动提交的，修改为手动提交
            if (originalAutoCommit) {
                /*
                 * In order to hold the local db lock during global lock checking
                 * set auto commit value to false first if original auto commit was true
                 */
                conn.setAutoCommit(false);
            // 检索此数据库是否支持保存点
            } else if (dbmd.supportsSavepoints()) {
                /*
                 * In order to release the local db lock when global lock conflict
                 * create a save point if original auto commit was false, then use the save point here to release db
                 * lock during global lock checking if necessary
                 */
                sp = conn.setSavepoint();
            } else {
                throw new SQLException("not support savepoint. please check your db version");
            }

            while (true) {
                try {
                    // #870
                    // execute return Boolean
                    // executeQuery return ResultSet
                    /** 3、执行sql */
                    rs = statementCallback.execute(statementProxy.getTargetStatement(), args);

                    /** 尝试获取选定行的全局锁定 并且查询前镜像 */
                    TableRecords selectPKRows = buildTableRecords(getTableMeta(), selectPKSQL, paramAppenderList);
                    /**
                     * 构建localLock（很重要），拿不到本地锁，就跳出循环等在本地锁释放，事务的隔离性
                     */
                    String lockKeys = buildLockKey(selectPKRows);
                    if (StringUtils.isNullOrEmpty(lockKeys)) {
                        break;
                    }

                    /** 判断当前应用的运行时是否处于全局事务的上下文中 */
                    if (RootContext.inGlobalTransaction()) {
                        // 检查锁
                        statementProxy.getConnectionProxy().checkLock(lockKeys);
                    // 需要全局锁定检查
                    } else if (RootContext.requireGlobalLock()) {
                        // 像DML一样在提交之前检查锁键，以避免可重入的锁问题（没有xid因此无法重入）
                        statementProxy.getConnectionProxy().appendLockKey(lockKeys);
                    } else {
                        throw new RuntimeException("Unknown situation!");
                    }
                    break;
                } catch (LockConflictException lce) {
                    if (sp != null) {
                        conn.rollback(sp);
                    } else {
                        conn.rollback();
                    }
                    lockRetryController.sleep(lce);
                }
            }
        } finally {
            if (sp != null) {
                try {
                    // 连接释放保存点
                    conn.releaseSavepoint(sp);
                } catch (SQLException e) {
                    if (LOGGER.isWarnEnabled()) {
                        LOGGER.warn("{} does not support release save point, but this is not a error.", getDbType());
                    }
                }
            }
            if (originalAutoCommit) {
                // 设置自动提交
                conn.setAutoCommit(true);
            }
        }
        return rs;
    }

    /**
     * 构建select for update语句，获得行锁
     * @param paramAppenderList
     * @return
     */
    private String buildSelectSQL(ArrayList<List<Object>> paramAppenderList) {
        SQLSelectRecognizer recognizer = (SQLSelectRecognizer)sqlRecognizer;
        StringBuilder selectSQLAppender = new StringBuilder("SELECT ");
        selectSQLAppender.append(getColumnNameInSQL(getTableMeta().getPkName()));
        selectSQLAppender.append(" FROM ").append(getFromTableInSQL());
        String whereCondition = buildWhereCondition(recognizer, paramAppenderList);
        if (StringUtils.isNotBlank(whereCondition)) {
            selectSQLAppender.append(" WHERE ").append(whereCondition);
        }
        selectSQLAppender.append(" FOR UPDATE");
        return selectSQLAppender.toString();
    }
}
