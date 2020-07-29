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

import io.seata.core.rpc.netty.RmMessageListener;
import io.seata.core.rpc.netty.RmRpcClient;

/**
 * The Rm client Initiator.
 *
 * @author slievrly
 */
public class RMClient {

    /**
     * 初始化RM
     * Init.
     *
     * @param applicationId           the application id
     * @param transactionServiceGroup the transaction service group
     */
    public static void init(String applicationId, String transactionServiceGroup) {
        /** 单例模式（双重检测锁），获取RM客户端实例 */
        RmRpcClient rmRpcClient = RmRpcClient.getInstance(applicationId, transactionServiceGroup);
        // 初始化设置RM资源管理器（DefaultResourceManager）
        rmRpcClient.setResourceManager(DefaultResourceManager.get());
        /** 设置RM客户端消息监听器（rmHandler用于接收fescar-server在二阶段发出的提交或者回滚请求） (DefaultRMHandler, RmRpcClient) */
        rmRpcClient.setClientMessageListener(new RmMessageListener(DefaultRMHandler.get(), rmRpcClient));
        /** 初始化 RM Client */
        rmRpcClient.init();
    }

}
