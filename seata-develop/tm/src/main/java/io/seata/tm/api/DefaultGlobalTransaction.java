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

import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.context.RootContext;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;
import io.seata.core.model.TransactionManager;
import io.seata.tm.TransactionManagerHolder;
import io.seata.tm.api.transaction.SuspendedResourcesHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static io.seata.core.constants.DefaultValues.DEFAULT_TM_COMMIT_RETRY_COUNT;
import static io.seata.core.constants.DefaultValues.DEFAULT_TM_ROLLBACK_RETRY_COUNT;

/**
 * The type Default global transaction.
 *
 * @author sharajava
 */
public class DefaultGlobalTransaction implements GlobalTransaction {

    private static final Logger LOGGER = LoggerFactory.getLogger(DefaultGlobalTransaction.class);

    private static final int DEFAULT_GLOBAL_TX_TIMEOUT = 60000;

    private static final String DEFAULT_GLOBAL_TX_NAME = "default";

    private TransactionManager transactionManager;

    private String xid;

    private GlobalStatus status;

    private GlobalTransactionRole role;

    private static final int COMMIT_RETRY_COUNT = ConfigurationFactory.getInstance().getInt(
        ConfigurationKeys.CLIENT_TM_COMMIT_RETRY_COUNT, DEFAULT_TM_COMMIT_RETRY_COUNT);

    private static final int ROLLBACK_RETRY_COUNT = ConfigurationFactory.getInstance().getInt(
        ConfigurationKeys.CLIENT_TM_ROLLBACK_RETRY_COUNT, DEFAULT_TM_ROLLBACK_RETRY_COUNT);

    /**
     * Instantiates a new Default global transaction.
     */
    DefaultGlobalTransaction() {
        this(null, GlobalStatus.UnKnown, GlobalTransactionRole.Launcher);
    }

    /**
     * Instantiates a new Default global transaction.
     *
     * @param xid    the xid
     * @param status the status
     * @param role   the role
     */
    DefaultGlobalTransaction(String xid, GlobalStatus status, GlobalTransactionRole role) {
        this.transactionManager = TransactionManagerHolder.get();
        this.xid = xid;
        this.status = status;
        this.role = role;
    }

    @Override
    public void begin() throws TransactionException {
        begin(DEFAULT_GLOBAL_TX_TIMEOUT);
    }

    @Override
    public void begin(int timeout) throws TransactionException {
        begin(timeout, DEFAULT_GLOBAL_TX_NAME);
    }

    /**
     * 全局事务开启
     * @param timeout Given timeout in MILLISECONDS.
     * @param name    Given name.
     * @throws TransactionException
     */
    @Override
    public void begin(int timeout, String name) throws TransactionException {
        // 判断事务规则是否是Launcher（开始当前的全局事务）
        if (role != GlobalTransactionRole.Launcher) {
            // 由于在GlobalTransactionContext.getCurrentOrCreate()的时候，
            // 该方法里判断了如果xid存在，则为Participant；不存在，则创建默认的Launcher。故这里断言xid不为null
            assertXIDNotNull();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignore Begin(): just involved in global transaction [{}]", xid);
            }
            return;
        }
        // 同上if里的逻辑判断
        assertXIDNull();
        if (RootContext.getXID() != null) {
            throw new IllegalStateException();
        }
        /** 构建xid DefaultTransactionManager */
        xid = transactionManager.begin(null, null, name, timeout);
        // 修改全局事务状态为开始
        status = GlobalStatus.Begin;
        /** 将全局事务 xid 绑定到当前应用的运行时中 */
        RootContext.bind(xid);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("Begin new global transaction [{}]", xid);
        }

    }

    /**
     * 全局事务提交
     * @throws TransactionException
     */
    @Override
    public void commit() throws TransactionException {
        // 判断是不是新的全局事务
        if (role == GlobalTransactionRole.Participant) {
            // Participant has no responsibility of committing
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignore Commit(): just involved in global transaction [{}]", xid);
            }
            return;
        }
        assertXIDNotNull();
        // 重试值
        int retry = COMMIT_RETRY_COUNT;
        try {
            while (retry > 0) {
                try {
                    /** 事务提交 */
                    status = transactionManager.commit(xid);
                    break;
                } catch (Throwable ex) {
                    // 如果在提交过程中出现了异常，则会重试，知道重试次数为0
                    LOGGER.error("Failed to report global commit [{}],Retry Countdown: {}, reason: {}", this.getXid(), retry, ex.getMessage());
                    retry--;
                    if (retry == 0) {
                        throw new TransactionException("Failed to report global commit", ex);
                    }
                }
            }
        } finally {
            // 最后将xid解绑
            if (RootContext.getXID() != null && xid.equals(RootContext.getXID())) {
                suspend(true);
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[{}] commit status: {}", xid, status);
        }

    }

    /**
     * 事务回滚
     * @throws TransactionException
     */
    @Override
    public void rollback() throws TransactionException {
        // 判断是不是新的全局事务
        if (role == GlobalTransactionRole.Participant) {
            // Participant has no responsibility of rollback
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Ignore Rollback(): just involved in global transaction [{}]", xid);
            }
            return;
        }
        assertXIDNotNull();

        // 重试值
        int retry = ROLLBACK_RETRY_COUNT;
        try {
            while (retry > 0) {
                try {
                    /** 事务回滚 */
                    status = transactionManager.rollback(xid);
                    break;
                } catch (Throwable ex) {
                    // 如果在回滚过程中出现了异常，则会重试，知道重试次数为0
                    LOGGER.error("Failed to report global rollback [{}],Retry Countdown: {}, reason: {}", this.getXid(), retry, ex.getMessage());
                    retry--;
                    if (retry == 0) {
                        throw new TransactionException("Failed to report global rollback", ex);
                    }
                }
            }
        } finally {
            // 最后将xid解绑
            if (RootContext.getXID() != null && xid.equals(RootContext.getXID())) {
                suspend(true);
            }
        }
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[{}] rollback status: {}", xid, status);
        }
    }

    @Override
    public SuspendedResourcesHolder suspend(boolean unbindXid) throws TransactionException {
        String xid = RootContext.getXID();
        if (StringUtils.isNotEmpty(xid) && unbindXid) {
            RootContext.unbind();
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Suspending current transaction,xid = {}",xid);
            }
        } else {
            xid = null;
        }
        return new SuspendedResourcesHolder(xid);
    }

    @Override
    public void resume(SuspendedResourcesHolder suspendedResourcesHolder) throws TransactionException {
        if (suspendedResourcesHolder == null) {
            return;
        }
        String xid = suspendedResourcesHolder.getXid();
        if (StringUtils.isNotEmpty(xid)) {
            RootContext.bind(xid);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Resumimg the transaction,xid = {}", xid);
            }
        }
    }

    @Override
    public GlobalStatus getStatus() throws TransactionException {
        if (xid == null) {
            return GlobalStatus.UnKnown;
        }
        status = transactionManager.getStatus(xid);
        return status;
    }

    @Override
    public String getXid() {
        return xid;
    }

    @Override
    public void globalReport(GlobalStatus globalStatus) throws TransactionException {
        assertXIDNotNull();

        if (globalStatus == null) {
            throw new IllegalStateException();
        }

        status = transactionManager.globalReport(xid, globalStatus);
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("[{}] report status: {}", xid, status);
        }

        if (RootContext.getXID() != null && xid.equals(RootContext.getXID())) {
            suspend(true);
        }
    }

    @Override
    public GlobalStatus getLocalStatus() {
        return status;
    }

    private void assertXIDNotNull() {
        if (xid == null) {
            throw new IllegalStateException();
        }
    }

    private void assertXIDNull() {
        if (xid != null) {
            throw new IllegalStateException();
        }
    }

}
