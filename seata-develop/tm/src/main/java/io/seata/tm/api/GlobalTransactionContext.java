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

import io.seata.core.context.RootContext;
import io.seata.core.exception.TransactionException;
import io.seata.core.model.GlobalStatus;

/**
 * GlobalTransaction API
 *
 * @author sharajava
 */
public class GlobalTransactionContext {

    private GlobalTransactionContext() {
    }

    /**
     * Try to create a new GlobalTransaction.
     *
     * @return
     */
    private static GlobalTransaction createNew() {
        return new DefaultGlobalTransaction();
    }

    /**
     * Get GlobalTransaction instance bind on current thread.
     *
     * @return null if no transaction context there.
     */
    private static GlobalTransaction getCurrent() {
        String xid = RootContext.getXID();
        if (xid == null) {
            return null;
        }
        return new DefaultGlobalTransaction(xid, GlobalStatus.Begin, GlobalTransactionRole.Participant);
    }

    /**
     * Get GlobalTransaction instance bind on current thread. Create a new on if no existing there.
     *
     * 获取当前的全局事务实例，如果没有则创建一个新的实例。
     *
     * @return new context if no existing there.
     */
    public static GlobalTransaction getCurrentOrCreate() {
        GlobalTransaction tx = getCurrent();
        if (tx == null) {
            return createNew();
        }
        return tx;
    }

    /**
     * Reload GlobalTransaction instance according to the given XID
     *
     * 重新载入给定 XID 的全局事务实例，这个实例不允许执行开启事务的操作。
     * 这个 API 通常用于失败的事务的后续集中处理。
     * 比如：全局提交超时，后续集中处理通过重新载入该实例，通过实例方法获取事务当前状态，并根据状态判断是否需要重试全局提交操作。
     *
     * @param xid the xid
     * @return reloaded transaction instance.
     * @throws TransactionException the transaction exception
     */
    public static GlobalTransaction reload(String xid) throws TransactionException {
        return new DefaultGlobalTransaction(xid, GlobalStatus.UnKnown, GlobalTransactionRole.Launcher) {
            @Override
            public void begin(int timeout, String name) throws TransactionException {
                throw new IllegalStateException("Never BEGIN on a RELOADED GlobalTransaction. ");
            }
        };
    }
}
