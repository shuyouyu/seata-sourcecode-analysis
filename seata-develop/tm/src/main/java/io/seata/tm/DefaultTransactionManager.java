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
package io.seata.tm;

import java.util.concurrent.TimeoutException;

import io.seata.core.exception.TmTransactionException;
import io.seata.core.exception.TransactionException;
import io.seata.core.exception.TransactionExceptionCode;
import io.seata.core.model.GlobalStatus;
import io.seata.core.model.TransactionManager;
import io.seata.core.protocol.ResultCode;
import io.seata.core.protocol.transaction.AbstractTransactionRequest;
import io.seata.core.protocol.transaction.AbstractTransactionResponse;
import io.seata.core.protocol.transaction.GlobalBeginRequest;
import io.seata.core.protocol.transaction.GlobalBeginResponse;
import io.seata.core.protocol.transaction.GlobalCommitRequest;
import io.seata.core.protocol.transaction.GlobalCommitResponse;
import io.seata.core.protocol.transaction.GlobalReportRequest;
import io.seata.core.protocol.transaction.GlobalReportResponse;
import io.seata.core.protocol.transaction.GlobalRollbackRequest;
import io.seata.core.protocol.transaction.GlobalRollbackResponse;
import io.seata.core.protocol.transaction.GlobalStatusRequest;
import io.seata.core.protocol.transaction.GlobalStatusResponse;
import io.seata.core.rpc.netty.TmRpcClient;

/**
 * The type Default transaction manager.
 *
 * @author sharajava
 */
public class DefaultTransactionManager implements TransactionManager {

    /**
     * 构建xid
     * @param applicationId           ID of the application who begins this transaction.
     * @param transactionServiceGroup ID of the transaction service group.
     * @param name                    Give a name to the global transaction.
     * @param timeout                 Timeout of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public String begin(String applicationId, String transactionServiceGroup, String name, int timeout)
        throws TransactionException {
        // 构建全局事务开始的请求体
        GlobalBeginRequest request = new GlobalBeginRequest();
        // 设置全局事务名称
        request.setTransactionName(name);
        // 设置全局事务超时时间
        request.setTimeout(timeout);
        /** 异步请求事务协调器，获得一个响应 */
        GlobalBeginResponse response = (GlobalBeginResponse)syncCall(request);
        // 判断响应状态码
        if (response.getResultCode() == ResultCode.Failed) {
            throw new TmTransactionException(TransactionExceptionCode.BeginFailed, response.getMsg());
        }
        // 获取到xid返回
        return response.getXid();
    }

    /**
     * 事务提交
     * @param xid XID of the global transaction.
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus commit(String xid) throws TransactionException {
        // 构建一个全局提交请求（里面包含里MessageType）
        GlobalCommitRequest globalCommit = new GlobalCommitRequest();
        // 设置事务xid
        globalCommit.setXid(xid);
        /** 发送事务提交请求 */
        GlobalCommitResponse response = (GlobalCommitResponse)syncCall(globalCommit);
        // 返回响应码
        return response.getGlobalStatus();
    }

    /**
     * 事务回滚
     * @param xid XID of the global transaction
     * @return
     * @throws TransactionException
     */
    @Override
    public GlobalStatus rollback(String xid) throws TransactionException {
        // 构建一个全局回滚请求（里面包含里MessageType）
        GlobalRollbackRequest globalRollback = new GlobalRollbackRequest();
        // 设置事务xid
        globalRollback.setXid(xid);
        /** 发送事务回滚请求 */
        GlobalRollbackResponse response = (GlobalRollbackResponse)syncCall(globalRollback);
        // 返回响应码
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus getStatus(String xid) throws TransactionException {
        GlobalStatusRequest queryGlobalStatus = new GlobalStatusRequest();
        queryGlobalStatus.setXid(xid);
        GlobalStatusResponse response = (GlobalStatusResponse)syncCall(queryGlobalStatus);
        return response.getGlobalStatus();
    }

    @Override
    public GlobalStatus globalReport(String xid, GlobalStatus globalStatus) throws TransactionException {
        GlobalReportRequest globalReport = new GlobalReportRequest();
        globalReport.setXid(xid);
        globalReport.setGlobalStatus(globalStatus);
        GlobalReportResponse response = (GlobalReportResponse) syncCall(globalReport);
        return response.getGlobalStatus();
    }

    /**
     * 异步请求事务协调器，获得一个响应
     * @param request
     * @return
     * @throws TransactionException
     */
    private AbstractTransactionResponse syncCall(AbstractTransactionRequest request) throws TransactionException {
        try {
            /** 客户端发送消息并获得响应 */
            return (AbstractTransactionResponse)TmRpcClient.getInstance().sendMsgWithResponse(request);
        } catch (TimeoutException toe) {
            throw new TmTransactionException(TransactionExceptionCode.IO, "RPC timeout", toe);
        }
    }
}
