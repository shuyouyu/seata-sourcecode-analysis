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
package io.seata.rm;

import java.util.concurrent.Callable;

import io.seata.core.context.RootContext;

/**
 * Template of executing business logic in a local transaction with Global lock.
 *
 * @param <T>
 * @author deyou
 */
public class GlobalLockTemplate<T> {

    /**
     * 全局锁开启的处理
     * Execute object.
     *
     * @param business the business
     * @return the object
     * @throws Exception
     */
    public Object execute(Callable<T> business) throws Exception {

        Object rs;
        try {
            // 添加全局锁定义
            RootContext.bindGlobalLockFlag();

            // 业务处理
            rs = business.call();
        } finally {
            // 清除全局锁定义
            RootContext.unbindGlobalLockFlag();
        }

        return rs;
    }

}
