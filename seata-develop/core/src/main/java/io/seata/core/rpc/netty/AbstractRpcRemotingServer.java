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

import io.netty.channel.Channel;
import io.netty.channel.ChannelHandler;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.timeout.IdleState;
import io.netty.handler.timeout.IdleStateEvent;
import io.seata.common.util.NetUtil;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.RegisterRMRequest;
import io.seata.core.protocol.RegisterTMRequest;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.ChannelManager;
import io.seata.core.rpc.RpcContext;
import io.seata.core.rpc.ServerMessageListener;
import io.seata.core.rpc.ServerMessageSender;
import io.seata.core.rpc.TransactionMessageHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * The type Rpc remoting server.
 *
 * @author slievrly
 * @author xingfudeshi@gmail.com
 */
public abstract class AbstractRpcRemotingServer extends AbstractRpcRemoting implements ServerMessageSender {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemotingServer.class);

    private final RpcServerBootstrap serverBootstrap;

    /**
     * The Server message listener.
     */
    private ServerMessageListener serverMessageListener;

    private TransactionMessageHandler transactionMessageHandler;

    private RegisterCheckAuthHandler checkAuthHandler;

    /**
     * 构建rpc服务端
     * Instantiates a new Rpc remoting server.
     *
     * @param messageExecutor   the message executor
     * @param nettyServerConfig the netty server config
     */
    public AbstractRpcRemotingServer(final ThreadPoolExecutor messageExecutor, NettyServerConfig nettyServerConfig) {
        super(messageExecutor);
        /** 构建rpc服务端 */
        serverBootstrap = new RpcServerBootstrap(nettyServerConfig);
    }

    /**
     * Sets transactionMessageHandler.
     *
     * @param transactionMessageHandler the transactionMessageHandler
     */
    public void setHandler(TransactionMessageHandler transactionMessageHandler) {
        setHandler(transactionMessageHandler, null);
    }

    private void setHandler(TransactionMessageHandler transactionMessageHandler, RegisterCheckAuthHandler checkAuthHandler) {
        this.transactionMessageHandler = transactionMessageHandler;
        this.checkAuthHandler = checkAuthHandler;
    }

    public TransactionMessageHandler getTransactionMessageHandler() {
        return transactionMessageHandler;
    }

    public RegisterCheckAuthHandler getCheckAuthHandler() {
        return checkAuthHandler;
    }

    /**
     * Sets server message listener.
     *
     * @param serverMessageListener the server message listener
     */
    public void setServerMessageListener(ServerMessageListener serverMessageListener) {
        this.serverMessageListener = serverMessageListener;
    }

    /**
     * Gets server message listener.
     *
     * @return the server message listener
     */
    public ServerMessageListener getServerMessageListener() {
        return serverMessageListener;
    }

    /**
     * Sets channel handlers.
     *
     * @param handlers the handlers
     */
    public void setChannelHandlers(ChannelHandler... handlers) {
        serverBootstrap.setChannelHandlers(handlers);
    }

    /**
     * Sets listen port.
     *
     * @param listenPort the listen port
     */
    public void setListenPort(int listenPort) {
        serverBootstrap.setListenPort(listenPort);
    }

    /**
     * Gets listen port.
     *
     * @return the listen port
     */
    public int getListenPort() {
        return serverBootstrap.getListenPort();
    }

    /**
     * 初始化服务端
     */
    @Override
    public void init() {
        /** 调用父类（AbstractRpcRemoting）的构造函数 清除超时的future */
        super.init();
        /** 启动TC服务 */
        serverBootstrap.start();
    }

    @Override
    public void destroy() {
        serverBootstrap.shutdown();
        super.destroy();
    }

    /**
     * Debug log.
     *
     * @param info the info
     */
    public void debugLog(String info) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(info);
        }
    }

    private void closeChannelHandlerContext(ChannelHandlerContext ctx) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("closeChannelHandlerContext channel:" + ctx.channel());
        }
        ctx.disconnect();
        ctx.close();
    }

    @Override
    public void destroyChannel(String serverAddress, Channel channel) {
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("will destroy channel:{},address:{}", channel, serverAddress);
        }
        channel.disconnect();
        channel.close();
    }

    /**
     * 服务端处理器
     * The type ServerHandler.
     */
    @ChannelHandler.Sharable
    class ServerHandler extends AbstractHandler {

        /**
         * Dispatch.
         *
         * @param request the request
         * @param ctx     the ctx
         */
        @Override
        public void dispatch(RpcMessage request, ChannelHandlerContext ctx) {
            Object msg = request.getBody();
            // 如果是RM请求，则交给serverMessageListener处理RM消息
            if (msg instanceof RegisterRMRequest) {

                /**
                 * serverMessageListener是super.setServerMessageListener(defaultServerMessageListenerImpl);
                 * 而defaultServerMessageListenerImpl里面初始化了new DefaultCoordinator(rpcServer)
                 * 而DefaultCoordinator里面又有RpcServer
                 *
                 * 层级关系如下：ServerMessageListener -> DefaultServerMessageListenerImpl -> DefaultCoordinator -> RpcServer
                 *
                 */
                serverMessageListener.onRegRmMessage(request, ctx, checkAuthHandler);
            } else {
                // 判断通道是否已注册
                if (ChannelManager.isRegistered(ctx.channel())) {
                    /** 处理事务消息 */
                    serverMessageListener.onTrxMessage(request, ctx);
                } else {

                    // 处理连接关闭的逻辑
                    try {
                        closeChannelHandlerContext(ctx);
                    } catch (Exception exx) {
                        LOGGER.error(exx.getMessage());
                    }
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info(String.format("close a unhandled connection! [%s]", ctx.channel().toString()));
                    }
                }
            }
        }

        /**
         * Channel read.
         *
         * @param ctx the ctx
         * @param msg the msg
         * @throws Exception the exception
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof RpcMessage) {
                RpcMessage rpcMessage = (RpcMessage) msg;
                debugLog("read:" + rpcMessage.getBody());
                // 如果是TM消息
                if (rpcMessage.getBody() instanceof RegisterTMRequest) {
                    serverMessageListener.onRegTmMessage(rpcMessage, ctx, checkAuthHandler);
                    return;
                }
                // 如果是心跳消息
                if (rpcMessage.getBody() == HeartbeatMessage.PING) {
                    serverMessageListener.onCheckMessage(rpcMessage, ctx);
                    return;
                }
            }
            /** 调用父类的通道处理方法 */
            super.channelRead(ctx, msg);
        }

        /**
         * Channel inactive.
         *
         * @param ctx the ctx
         * @throws Exception the exception
         */
        @Override
        public void channelInactive(ChannelHandlerContext ctx) throws Exception {
            debugLog("inactive:" + ctx);
            if (messageExecutor.isShutdown()) {
                return;
            }
            handleDisconnect(ctx);
            super.channelInactive(ctx);
        }

        private void handleDisconnect(ChannelHandlerContext ctx) {
            final String ipAndPort = NetUtil.toStringAddress(ctx.channel().remoteAddress());
            RpcContext rpcContext = ChannelManager.getContextFromIdentified(ctx.channel());
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(ipAndPort + " to server channel inactive.");
            }
            if (null != rpcContext && null != rpcContext.getClientRole()) {
                rpcContext.release();
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("remove channel:" + ctx.channel() + "context:" + rpcContext);
                }
            } else {
                if (LOGGER.isInfoEnabled()) {
                    LOGGER.info("remove unused channel:" + ctx.channel());
                }
            }
        }

        /**
         * Exception caught.
         *
         * @param ctx   the ctx
         * @param cause the cause
         * @throws Exception the exception
         */
        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("channel exx:" + cause.getMessage() + ",channel:" + ctx.channel());
            }
            ChannelManager.releaseRpcContext(ctx.channel());
            super.exceptionCaught(ctx, cause);
        }

        /**
         * User event triggered.
         *
         * @param ctx the ctx
         * @param evt the evt
         * @throws Exception the exception
         */
        @Override
        public void userEventTriggered(ChannelHandlerContext ctx, Object evt) {
            if (evt instanceof IdleStateEvent) {
                debugLog("idle:" + evt);
                IdleStateEvent idleStateEvent = (IdleStateEvent) evt;
                if (idleStateEvent.state() == IdleState.READER_IDLE) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("channel:" + ctx.channel() + " read idle.");
                    }
                    handleDisconnect(ctx);
                    try {
                        closeChannelHandlerContext(ctx);
                    } catch (Exception e) {
                        LOGGER.error(e.getMessage());
                    }
                }
            }
        }

    }

}
