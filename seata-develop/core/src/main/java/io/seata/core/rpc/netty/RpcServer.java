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

import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeoutException;

import io.netty.channel.Channel;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.ChannelManager;
import io.seata.core.rpc.DefaultServerMessageListenerImpl;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The type Abstract rpc server.
 *
 * @author slievrly
 */
public class RpcServer extends AbstractRpcRemotingServer {

    private static final Logger LOGGER = LoggerFactory.getLogger(RpcServer.class);


    /**
     * 构建rpc服务端
     * Instantiates a new Abstract rpc server.
     *
     * @param messageExecutor the message executor
     */
    public RpcServer(ThreadPoolExecutor messageExecutor) {
        super(messageExecutor, new NettyServerConfig());
    }

    /**
     * 初始化服务端
     * Init.
     */
    @Override
    public void init() {
        // 构建一个DefaultServerMessageListenerImp
        DefaultServerMessageListenerImpl defaultServerMessageListenerImpl =
            new DefaultServerMessageListenerImpl(getTransactionMessageHandler()); // getTransactionMessageHandler()就是获取在server的main() 里面的DefaultCoordinator
        /** 消息监听器初始化 */
        defaultServerMessageListenerImpl.init();
        defaultServerMessageListenerImpl.setServerMessageSender(this);
        super.setServerMessageListener(defaultServerMessageListenerImpl);
        /** 设置通道处理器，这里的ServerHandler就是在 RpcServerBootstrap#start 方法中启动nettyServer的时候放进pipeline的channelHandlers */
        super.setChannelHandlers(new ServerHandler());
        /** 调用父类（AbstractRpcRemotingServer）的构造函数 初始化服务端 */
        super.init();
    }

    /**
     * 这里主要调用父类关闭线程池、在注册中心中下线自己、优雅退出Netty
     * Destroy.
     */
    @Override
    public void destroy() {
        super.destroy();
        if (LOGGER.isInfoEnabled()) {
            LOGGER.info("destroyed rpcServer");
        }
    }



    /**
     * 根据Server Channel在ChannelManager找到对应的Client Channel并返回结果
     * Send response.
     * rm reg,rpc reg,inner response
     *
     * @param request the request
     * @param channel the channel
     * @param msg     the msg
     */
    @Override
    public void sendResponse(RpcMessage request, Channel channel, Object msg) {
        Channel clientChannel = channel;
        if (!(msg instanceof HeartbeatMessage)) {
            clientChannel = ChannelManager.getSameClientChannel(channel);
        }
        if (clientChannel != null) {
            super.defaultSendResponse(request, clientChannel, msg);
        } else {
            throw new RuntimeException("channel is error. channel:" + clientChannel);
        }
    }

    /**
     * 直接调用了父类的异步发送，异步发送会自动合并请求，下面的AbstractRpcRemoting类中会讲解
     * Send request with response object.
     * send syn request for rm
     *
     * @param resourceId the db key
     * @param clientId   the client ip
     * @param message    the message
     * @param timeout    the timeout
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(String resourceId, String clientId, Object message,
        long timeout) throws TimeoutException {
        Channel clientChannel = ChannelManager.getChannel(resourceId, clientId);
        if (clientChannel == null) {
            throw new RuntimeException("rm client is not connected. dbkey:" + resourceId
                + ",clientId:" + clientId);

        }
        /** 发送请求 */
        return sendAsyncRequestWithResponse(null, clientChannel, message, timeout);
    }

    /**
     * 同步调用，timeout参数必定要大于0，最终调用父类的sendAsyncRequestWithResponse实现
     * Send request with response object.
     * send syn request for rm
     *
     * @param clientChannel the client channel
     * @param message       the message
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(Channel clientChannel, Object message) throws TimeoutException {
        return sendSyncRequest(clientChannel, message, NettyServerConfig.getRpcRequestTimeout());
    }

    /**
     * Send request with response object.
     * send syn request for rm
     *
     * @param clientChannel the client channel
     * @param message       the message
     * @param timeout       the timeout
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(Channel clientChannel, Object message, long timeout) throws TimeoutException {
        if (clientChannel == null) {
            throw new RuntimeException("rm client is not connected");

        }
        return sendAsyncRequestWithResponse(null, clientChannel, message, timeout);
    }

    /**
     * 发送请求
     * Send request with response object.
     *
     * @param resourceId the db key
     * @param clientId   the client ip
     * @param message    the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendSyncRequest(String resourceId, String clientId, Object message)
        throws TimeoutException {
        /** 发送请求 */
        return sendSyncRequest(resourceId, clientId, message, NettyServerConfig.getRpcRequestTimeout());
    }

    /**
     * Send request with response object.
     *
     * @param channel   the channel
     * @param message    the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    @Override
    public Object sendASyncRequest(Channel channel, Object message) throws TimeoutException {
        return sendAsyncRequestWithoutResponse(channel, message);
    }
}