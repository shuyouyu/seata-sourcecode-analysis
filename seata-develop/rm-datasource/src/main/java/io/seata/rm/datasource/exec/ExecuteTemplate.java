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

import io.seata.common.util.StringUtils;
import io.seata.common.util.CollectionUtils;
import io.seata.core.context.RootContext;
import io.seata.core.model.BranchType;
import io.seata.rm.datasource.StatementProxy;
import io.seata.rm.datasource.sql.SQLVisitorFactory;
import io.seata.sqlparser.SQLRecognizer;

import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;

/**
 * sql执行模板
 * The type Execute template.
 *
 * @author sharajava
 */
public class ExecuteTemplate {

    /**
     * 执行sql（分支事务）
     * Execute t.
     *
     * @param <T>               the type parameter
     * @param <S>               the type parameter
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param args              the args
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {
        return execute(null, statementProxy, statementCallback, args);
    }

    /**
     * 执行sql（分支事务）
     * Execute t.
     *
     * @param <T>               the type parameter
     * @param <S>               the type parameter
     * @param sqlRecognizers     the sql recognizer list
     * @param statementProxy    the statement proxy
     * @param statementCallback the statement callback
     * @param args              the args
     * @return the t
     * @throws SQLException the sql exception
     */
    public static <T, S extends Statement> T execute(List<SQLRecognizer> sqlRecognizers,
                                                     StatementProxy<S> statementProxy,
                                                     StatementCallback<T, S> statementCallback,
                                                     Object... args) throws SQLException {

        // 判断是否时AT模式
        if (!shouldExecuteInATMode()) {
            /** 非AT模式 */
            return statementCallback.execute(statementProxy.getTargetStatement(), args);
        }

        /** 构建sql解析器 */
        if (sqlRecognizers == null) {
            sqlRecognizers = SQLVisitorFactory.get(
                    statementProxy.getTargetSQL(),
                    statementProxy.getConnectionProxy().getDbType());
        }
        Executor<T> executor;
        if (CollectionUtils.isEmpty(sqlRecognizers)) {
            // executor是PlainExecutor（普通执行器）
            executor = new PlainExecutor<>(statementProxy, statementCallback);
        } else {
            if (sqlRecognizers.size() == 1) {
                SQLRecognizer sqlRecognizer = sqlRecognizers.get(0);
                // 根据不同的sql类型创建不同的执行器
                switch (sqlRecognizer.getSQLType()) {
                    case INSERT:
                        /** 这里时构建一个insert执行器 */
                        executor = new InsertExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    case UPDATE:
                        executor = new UpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    case DELETE:
                        executor = new DeleteExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    case SELECT_FOR_UPDATE:
                        executor = new SelectForUpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                        break;
                    default:
                        executor = new PlainExecutor<>(statementProxy, statementCallback);
                        break;
                }
            } else {
                // 如果存在多个sql解析器就创建多个sql执行器
                executor = new MultiExecutor<>(statementProxy, statementCallback, sqlRecognizers);
            }
        }
        T rs;
        try {
            /** sql执行，并返回结果 executor是BaseTransactionalExecutor */
            rs = executor.execute(args);
        } catch (Throwable ex) {
            if (!(ex instanceof SQLException)) {
                // Turn other exception into SQLException
                ex = new SQLException(ex);
            }
            throw (SQLException) ex;
        }
        return rs;
    }

    private static boolean shouldExecuteInATMode() {
        if (!RootContext.inGlobalTransaction() && !RootContext.requireGlobalLock()) {
            return false;
        }
        if (RootContext.inGlobalTransaction()) {
            String branchType = RootContext.getBranchType();
            if (StringUtils.equals(BranchType.TCC.name(), branchType) || StringUtils.equals(BranchType.SAGA.name(), branchType)) {
                return false;
            }
        }
        return true;
    }
}
