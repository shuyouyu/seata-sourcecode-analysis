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
package io.seata.core.rpc.netty;

import io.seata.common.exception.FrameworkErrorCode;
import io.seata.core.protocol.ResultCode;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.protocol.transaction.BranchCommitRequest;
import io.seata.core.protocol.transaction.BranchCommitResponse;
import io.seata.core.protocol.transaction.BranchRollbackRequest;
import io.seata.core.protocol.transaction.BranchRollbackResponse;
import io.seata.core.protocol.transaction.UndoLogDeleteRequest;
import io.seata.core.rpc.ClientMessageListener;
import io.seata.core.rpc.ClientMessageSender;
import io.seata.core.rpc.TransactionMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * RM 消息监听器，用于监听TC发给RM的提交或者回滚请求
 * The type Rm message listener.
 *
 * @author slievrly
 */
public class RmMessageListener implements ClientMessageListener {

    private static final Logger LOGGER = LoggerFactory.getLogger(RmMessageListener.class);

    private TransactionMessageHandler handler;

    private ClientMessageSender sender;

    /**
     * 初始化RmMessageListener
     * Instantiates a new Rm message listener.
     *
     * @param handler the handler
     */
    public RmMessageListener(TransactionMessageHandler handler, ClientMessageSender sender) {
        this.handler = handler;
        this.sender = sender;
    }

    public void setSender(ClientMessageSender sender) {
        this.sender = sender;
    }

    public ClientMessageSender getSender() {
        if (sender == null) {
            throw new IllegalArgumentException("clientMessageSender must not be null");
        }
        return sender;
    }

    /**
     * 这里放的是 DefaultRMHandler
     * Sets handler.
     *
     * @param handler the handler
     */
    public void setHandler(TransactionMessageHandler handler) {
        this.handler = handler;
    }

    /**
     * 客户端处理消息
     * @param request       the msg id
     * @param serverAddress the server address
     */
    @Override
    public void onMessage(RpcMessage request, String serverAddress) {
        Object msg = request.getBody();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("onMessage:" + msg);
        }
        // 如果是分支事务提交
        if (msg instanceof BranchCommitRequest) {
            /** 处理分支事务提交 */
            handleBranchCommit(request, serverAddress, (BranchCommitRequest)msg);
        // 如果是分支事务回滚
        } else if (msg instanceof BranchRollbackRequest) {
            /** 处理分支事务回滚 */
            handleBranchRollback(request, serverAddress, (BranchRollbackRequest)msg);
        // 如果是删除undolog
        } else if (msg instanceof UndoLogDeleteRequest) {
            /** 处理undolog删除 */
            handleUndoLogDelete((UndoLogDeleteRequest) msg);
        }
    }

    /**
     * 处理分支事务回滚
     * @param request
     * @param serverAddress
     * @param branchRollbackRequest
     */
    private void handleBranchRollback(RpcMessage request, String serverAddress,
                                      BranchRollbackRequest branchRollbackRequest) {
        BranchRollbackResponse resultMessage = null;
        resultMessage = (BranchRollbackResponse)handler.onRequest(branchRollbackRequest, null);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("branch rollback result:" + resultMessage);
        }
        try {
            sender.sendResponse(request, serverAddress, resultMessage);
        } catch (Throwable throwable) {
            LOGGER.error("send response error: {}", throwable.getMessage(), throwable);
        }
    }

    /**
     * 处理分支事务提交
     * @param request
     * @param serverAddress
     * @param branchCommitRequest
     */
    private void handleBranchCommit(RpcMessage request, String serverAddress, BranchCommitRequest branchCommitRequest) {

        BranchCommitResponse resultMessage = null;
        try {
            /** 处理分支事务提交 */
            resultMessage = (BranchCommitResponse)handler.onRequest(branchCommitRequest, null);
            getSender().sendResponse(request, serverAddress, resultMessage);
        } catch (Exception e) {
            LOGGER.error(FrameworkErrorCode.NetOnMessage.getErrCode(), e.getMessage(), e);
            if (resultMessage == null) {
                resultMessage = new BranchCommitResponse();
            }
            resultMessage.setResultCode(ResultCode.Failed);
            resultMessage.setMsg(e.getMessage());
            getSender().sendResponse(request, serverAddress, resultMessage);
        }
    }

    /**
     * 处理undolog删除
     * @param undoLogDeleteRequest
     */
    private void handleUndoLogDelete(UndoLogDeleteRequest undoLogDeleteRequest) {
        try {
            handler.onRequest(undoLogDeleteRequest, null);
        } catch (Exception e) {
            LOGGER.error("Failed to delete undo log by undoLogDeleteRequest on" + undoLogDeleteRequest.getResourceId());
        }
    }
}
