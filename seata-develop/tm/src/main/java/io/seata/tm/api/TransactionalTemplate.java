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
package io.seata.tm.api;


import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.util.StringUtils;
import io.seata.core.context.RootContext;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;
import io.seata.tm.api.transaction.Propagation;
import io.seata.tm.api.transaction.SuspendedResourcesHolder;
import io.seata.tm.api.transaction.TransactionHook;
import io.seata.tm.api.transaction.TransactionHookManager;
import io.seata.tm.api.transaction.TransactionInfo;
import java.util.List;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Template of executing business logic with a global transaction.
 *
 * 事务化模板：通过 GlobalTransaction 和 GlobalTransactionContext API 把一个业务服务的调用包装成带有分布式事务支持的服务。
 *
 * @author sharajava
 */
public class TransactionalTemplate {

    private static final Logger LOGGER = LoggerFactory.getLogger(TransactionalTemplate.class);


    /**
     * 事务化模板
     * Execute object.
     *
     * @param business the business
     * @return the object
     * @throws TransactionalExecutor.ExecutionException the execution exception
     */
    public Object execute(TransactionalExecutor business) throws Throwable {
        // 获取事务信息（当前全局事务的超时时间、名称等等）
        TransactionInfo txInfo = business.getTransactionInfo();
        if (txInfo == null) {
            throw new ShouldNeverHappenException("transactionInfo does not exist");
        }
        /** 1. 获取当前全局事务实例或创建新的实例（可以跟进去看看）如果是第一次则创建全局事务，那么他的事务规则是：开始当前的全局事务（Launcher） */
        GlobalTransaction tx = GlobalTransactionContext.getCurrentOrCreate();

        //获取事务传播等级
        Propagation propagation = txInfo.getPropagation();
        SuspendedResourcesHolder suspendedResourcesHolder = null;
        try {
            switch (propagation) {
                case NOT_SUPPORTED:
                    suspendedResourcesHolder = tx.suspend(true);
                    return business.execute();
                case REQUIRES_NEW:
                    suspendedResourcesHolder = tx.suspend(true);
                    break;
                case SUPPORTS:
                    if (!existingTransaction()) {
                        return business.execute();
                    }
                    break;
                case REQUIRED:
                    break;
                case NEVER:
                    if (existingTransaction()) {
                        throw new TransactionException(
                                String.format("Existing transaction found for transaction marked with propagation 'never',xid = %s"
                                        ,RootContext.getXID()));
                    } else {
                        return business.execute();
                    }
                case MANDATORY:
                    if (!existingTransaction()) {
                        throw new TransactionException("No existing transaction found for transaction marked with propagation 'mandatory'");
                    }
                    break;
                default:
                    throw new TransactionException("Not Supported Propagation:" + propagation);
            }


            try {

                /** 2. 开启全局事务（由步骤1所得，当前tx为DefaultGlobalTransaction） */
                beginTransaction(txInfo, tx);

                Object rs = null;
                try {

                    // 调用业务服务
                    rs = business.execute();

                } catch (Throwable ex) {

                    /** 3. 业务调用本身的异常（回滚） */
                    completeTransactionAfterThrowing(txInfo, tx, ex);
                    throw ex;
                }

                /** 4. 全局提交 */
                commitTransaction(tx);

                return rs;
            } finally {
                //5. clear
                triggerAfterCompletion();
                cleanUp();
            }
        } finally {
            tx.resume(suspendedResourcesHolder);
        }

    }

    public boolean existingTransaction() {
        return StringUtils.isNotEmpty(RootContext.getXID());

    }

    /**
     * 业务调用本身的异常
     * @param txInfo
     * @param tx
     * @param ex
     * @throws TransactionalExecutor.ExecutionException
     */
    private void completeTransactionAfterThrowing(TransactionInfo txInfo, GlobalTransaction tx, Throwable ex) throws TransactionalExecutor.ExecutionException {
        // 回滚
        if (txInfo != null && txInfo.rollbackOn(ex)) {
            try {
                /** 事务回滚 */
                rollbackTransaction(tx, ex);
            } catch (TransactionException txe) {
                // Failed to rollback
                throw new TransactionalExecutor.ExecutionException(tx, txe,
                        TransactionalExecutor.Code.RollbackFailure, ex);
            }
        } else {
            // 这个异常不需要回滚，所以继续提交
            commitTransaction(tx);
        }
    }

    /**
     * 全局提交
     * @param tx
     * @throws TransactionalExecutor.ExecutionException
     */
    private void commitTransaction(GlobalTransaction tx) throws TransactionalExecutor.ExecutionException {
        try {
            // 全局事务提交之前的钩子
            triggerBeforeCommit();
            /** 全局事务提交 */
            tx.commit();
            // 全局事务提交之后的钩子
            triggerAfterCommit();
        } catch (TransactionException txe) {
            // 4.1 Failed to commit
            throw new TransactionalExecutor.ExecutionException(tx, txe,
                TransactionalExecutor.Code.CommitFailure);
        }
    }

    /**
     * 事务回滚
     * @param tx
     * @param ex
     * @throws TransactionException
     * @throws TransactionalExecutor.ExecutionException
     */
    private void rollbackTransaction(GlobalTransaction tx, Throwable ex) throws TransactionException, TransactionalExecutor.ExecutionException {
        // 钩子
        triggerBeforeRollback();
        /** 事务回滚 */
        tx.rollback();
        // 钩子
        triggerAfterRollback();
        // 3.1 Successfully rolled back
        throw new TransactionalExecutor.ExecutionException(tx, GlobalStatus.RollbackRetrying.equals(tx.getLocalStatus())
            ? TransactionalExecutor.Code.RollbackRetrying : TransactionalExecutor.Code.RollbackDone, ex);
    }

    /**
     * 开启全局事务
     * @param txInfo
     * @param tx
     * @throws TransactionalExecutor.ExecutionException
     */
    private void beginTransaction(TransactionInfo txInfo, GlobalTransaction tx) throws TransactionalExecutor.ExecutionException {
        try {
            // 触发事务开启之前的钩子
            triggerBeforeBegin();
            /** 全局事务开启 这里调用的 DefaultGlobalTransaction */
            tx.begin(/** @GlobalTransactional 注解配的超时时间和名称 */txInfo.getTimeOut(), txInfo.getName());
            // 触发事务开启之后的钩子
            triggerAfterBegin();
        } catch (TransactionException txe) {
            throw new TransactionalExecutor.ExecutionException(tx, txe,
                TransactionalExecutor.Code.BeginFailure);

        }
    }

    private void triggerBeforeBegin() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.beforeBegin();
            } catch (Exception e) {
                LOGGER.error("Failed execute beforeBegin in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerAfterBegin() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterBegin();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterBegin in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerBeforeRollback() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.beforeRollback();
            } catch (Exception e) {
                LOGGER.error("Failed execute beforeRollback in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerAfterRollback() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterRollback();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterRollback in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerBeforeCommit() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.beforeCommit();
            } catch (Exception e) {
                LOGGER.error("Failed execute beforeCommit in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerAfterCommit() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterCommit();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterCommit in hook {}",e.getMessage(),e);
            }
        }
    }

    private void triggerAfterCompletion() {
        for (TransactionHook hook : getCurrentHooks()) {
            try {
                hook.afterCompletion();
            } catch (Exception e) {
                LOGGER.error("Failed execute afterCompletion in hook {}",e.getMessage(),e);
            }
        }
    }

    private void cleanUp() {
        TransactionHookManager.clear();
    }

    private List<TransactionHook> getCurrentHooks() {
        return TransactionHookManager.getHooks();
    }

}
