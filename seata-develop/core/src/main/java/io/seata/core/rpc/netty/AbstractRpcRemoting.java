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
import io.netty.channel.ChannelDuplexHandler;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelPromise;
import io.seata.common.exception.FrameworkErrorCode;
import io.seata.common.exception.FrameworkException;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.thread.PositiveAtomicCounter;
import io.seata.core.protocol.HeartbeatMessage;
import io.seata.core.protocol.MergeMessage;
import io.seata.core.protocol.MessageFuture;
import io.seata.core.protocol.ProtocolConstants;
import io.seata.core.protocol.RpcMessage;
import io.seata.core.rpc.Disposable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.net.SocketAddress;
import java.util.Map;
import java.util.Random;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

/**
 * The type Abstract rpc remoting.
 *
 * @author slievrly
 */
public abstract class AbstractRpcRemoting implements Disposable {

    private static final Logger LOGGER = LoggerFactory.getLogger(AbstractRpcRemoting.class);
    /**
     * The Timer executor.
     */
    protected final ScheduledExecutorService timerExecutor = new ScheduledThreadPoolExecutor(1,
        new NamedThreadFactory("timeoutChecker", 1, true));
    /**
     * The Message executor.
     */
    protected final ThreadPoolExecutor messageExecutor;

    /**
     * Id generator of this remoting
     */
    protected final PositiveAtomicCounter idGenerator = new PositiveAtomicCounter();

    /**
     * The Futures.
     */
    protected final ConcurrentHashMap<Integer, MessageFuture> futures = new ConcurrentHashMap<>();
    /**
     * The Basket map.
     */
    protected final ConcurrentHashMap<String, BlockingQueue<RpcMessage>> basketMap = new ConcurrentHashMap<>();

    private static final long NOT_WRITEABLE_CHECK_MILLS = 10L;
    /**
     * The Merge lock.
     */
    protected final Object mergeLock = new Object();
    /**
     * The Now mills.
     */
    protected volatile long nowMills = 0;
    private static final int TIMEOUT_CHECK_INTERNAL = 3000;
    private final Object lock = new Object();
    /**
     * The Is sending.
     */
    protected volatile boolean isSending = false;
    private String group = "DEFAULT";
    /**
     * The Merge msg map.
     */
    protected final Map<Integer, MergeMessage> mergeMsgMap = new ConcurrentHashMap<>();

    /**
     * Instantiates a new Abstract rpc remoting.
     *
     * @param messageExecutor the message executor
     */
    public AbstractRpcRemoting(ThreadPoolExecutor messageExecutor) {
        this.messageExecutor = messageExecutor;
    }

    /**
     * Gets next message id.
     *
     * @return the next message id
     */
    public int getNextMessageId() {
        return idGenerator.incrementAndGet();
    }

    /**
     * 清除超时的future
     * Init.
     */
    public void init() {
        // 启动一个定时线程，清除Netty中超时的future
        timerExecutor.scheduleAtFixedRate(new Runnable() {
            @Override
            public void run() {
                // 清除超时的future
                for (Map.Entry<Integer, MessageFuture> entry : futures.entrySet()) {
                    if (entry.getValue().isTimeout()) {
                        futures.remove(entry.getKey());
                        entry.getValue().setResultMessage(null);
                        if (LOGGER.isDebugEnabled()) {
                            LOGGER.debug("timeout clear future: {}", entry.getValue().getRequestMessage().getBody());
                        }
                    }
                }

                nowMills = System.currentTimeMillis();
            }
        }, TIMEOUT_CHECK_INTERNAL, TIMEOUT_CHECK_INTERNAL, TimeUnit.MILLISECONDS);
    }

    /**
     * Destroy.
     */
    @Override
    public void destroy() {
        timerExecutor.shutdown();
        messageExecutor.shutdown();
    }

    /**
     * Send async request with response object.
     *
     * @param channel the channel
     * @param msg     the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    protected Object sendAsyncRequestWithResponse(Channel channel, Object msg) throws TimeoutException {
        return sendAsyncRequestWithResponse(null, channel, msg, NettyClientConfig.getRpcRequestTimeout());
    }

    /**
     * 客户端发送请求给TC，并获取响应
     * Send async request with response object.
     *
     * @param address the address
     * @param channel the channel
     * @param msg     the msg
     * @param timeout the timeout
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    protected Object sendAsyncRequestWithResponse(String address, Channel channel, Object msg, long timeout) throws
        TimeoutException {
        if (timeout <= 0) {
            throw new FrameworkException("timeout should more than 0ms");
        }
        /** 客户端发送请求给TC，并获取响应 */
        return sendAsyncRequest(address, channel, msg, timeout);
    }

    /**
     * Send async request without response object.
     *
     * @param channel the channel
     * @param msg     the msg
     * @return the object
     * @throws TimeoutException the timeout exception
     */
    protected Object sendAsyncRequestWithoutResponse(Channel channel, Object msg) throws
        TimeoutException {
        return sendAsyncRequest(null, channel, msg, 0);
    }

    /**
     * 客户端发送请求给TC，并获取响应
     * @param address TC地址
     * @param channel TM连接TC的通道
     * @param msg 消息
     * @param timeout 超时时间
     * @return
     * @throws TimeoutException
     */
    private Object sendAsyncRequest(String address, Channel channel, Object msg, long timeout)
        throws TimeoutException {
        if (channel == null) {
            LOGGER.warn("sendAsyncRequestWithResponse nothing, caused by null channel.");
            return null;
        }
        // 构建一个rpc消息
        final RpcMessage rpcMessage = new RpcMessage();
        // 设置消息id，从0开始递增
        rpcMessage.setId(getNextMessageId());
        // 设置消息类型为request which no need response
        rpcMessage.setMessageType(ProtocolConstants.MSGTYPE_RESQUEST_ONEWAY);
        // 设置消息编码器（序列化）
        rpcMessage.setCodec(ProtocolConstants.CONFIGURED_CODEC);
        // 设置消息压缩器
        rpcMessage.setCompressor(ProtocolConstants.CONFIGURED_COMPRESSOR);
        rpcMessage.setBody(msg);

        // 构建一个异步消息
        final MessageFuture messageFuture = new MessageFuture();
        messageFuture.setRequestMessage(rpcMessage);
        messageFuture.setTimeout(timeout);
        // 放入集合中
        futures.put(rpcMessage.getId(), messageFuture);

        if (address != null) {
            /*
            The batch send.
            Object From big to small: RpcMessage -> MergedWarpMessage -> AbstractMessage
            @see AbstractRpcRemotingClient.MergedSendRunnable
            */
            /**
             * 判断是否开启消息批量请求（默认开启）与 AbstractRpcRemotingClient里面的init()方法里面首尾呼应
             *      而这里将rpc消息放入了阻塞队列，就由AbstractRpcRemotingClient里面的init()的new MergedSendRunnable()线程来处理发送
             */
            if (NettyClientConfig.isEnableClientBatchSendRequest()) {
                // 每个远程请求的地址，都会有一个BlockingQueue，消息先存在队列，然后会被批量发送
                ConcurrentHashMap</** 服务端地址 */String, /** 存放RpcMessage的阻塞队列 */ BlockingQueue<RpcMessage>> map = basketMap;
                BlockingQueue<RpcMessage> basket = map.get(address);
                if (basket == null) {
                    // 创建一个阻塞队列放入集合中
                    map.putIfAbsent(address, new LinkedBlockingQueue<>());
                    basket = map.get(address);
                }
                // 存消息到阻塞队列中
                basket.offer(rpcMessage);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("offer message: {}", rpcMessage.getBody());
                }

                // 唤醒Client的发送线程做发送，结合AbstractRpcRemotingClient.MergedSendRunnable
                // 实现最多等待1ms或有消息进入批量发送队列时，开始发送
                if (!isSending) {
                    synchronized (mergeLock) {
                        mergeLock.notifyAll();
                    }
                }
            } else {
                /** 单个消息发送 */
                sendSingleRequest(channel, msg, rpcMessage);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("send this msg[{}] by single send.", msg);
                }
            }
        } else {
            /** 单个消息发送 */
            sendSingleRequest(channel, msg, rpcMessage);
        }
        if (timeout > 0) {
            try {
                // 获取响应并返回
                return messageFuture.get(timeout, TimeUnit.MILLISECONDS);
            } catch (Exception exx) {
                LOGGER.error("wait response error:{},ip:{},request:{}", exx.getMessage(), address, msg);
                if (exx instanceof TimeoutException) {
                    throw (TimeoutException) exx;
                } else {
                    throw new RuntimeException(exx);
                }
            }
        } else {
            return null;
        }
    }

    /**
     * 单个消息发送
     * @param channel
     * @param msg
     * @param rpcMessage
     */
    private void sendSingleRequest(Channel channel, Object msg, RpcMessage rpcMessage) {
        ChannelFuture future;
        /** 检查水位，看是否能写 */
        channelWritableCheck(channel, msg);
        // 写出并刷新
        future = channel.writeAndFlush(rpcMessage);
        // 添加监听器
        future.addListener(new ChannelFutureListener() {
            @Override
            public void operationComplete(ChannelFuture future) {
                // 返回不成功，则从futures中移除对应的future，并且关闭通道
                if (!future.isSuccess()) {
                    MessageFuture messageFuture = futures.remove(rpcMessage.getId());
                    if (messageFuture != null) {
                        messageFuture.setResultMessage(future.cause());
                    }
                    destroyChannel(future.channel());
                }
            }
        });
    }

    /**
     * 客户端发消息给TC
     * Default Send request.
     *
     * @param channel the channel
     * @param msg     the msg
     */
    protected void defaultSendRequest(Channel channel, Object msg) {
        RpcMessage rpcMessage = new RpcMessage();
        // 设置消息类型
        rpcMessage.setMessageType(msg instanceof HeartbeatMessage ?
            ProtocolConstants.MSGTYPE_HEARTBEAT_REQUEST
            : ProtocolConstants.MSGTYPE_RESQUEST);
        // 设置编码器
        rpcMessage.setCodec(ProtocolConstants.CONFIGURED_CODEC);
        // 设置压缩器
        rpcMessage.setCompressor(ProtocolConstants.CONFIGURED_COMPRESSOR);
        rpcMessage.setBody(msg);
        rpcMessage.setId(getNextMessageId());
        if (msg instanceof MergeMessage) {
            mergeMsgMap.put(rpcMessage.getId(), (MergeMessage) msg);
        }
        // 检查水位线
        channelWritableCheck(channel, msg);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("write message:" + rpcMessage.getBody() + ", channel:" + channel + ",active?"
                + channel.isActive() + ",writable?" + channel.isWritable() + ",isopen?" + channel.isOpen());
        }
        // 将消息发出去
        channel.writeAndFlush(rpcMessage);
    }

    /**
     * Default Send response.
     *
     * @param request the msg id
     * @param channel the channel
     * @param msg     the msg
     */
    protected void defaultSendResponse(RpcMessage request, Channel channel, Object msg) {
        RpcMessage rpcMessage = new RpcMessage();
        rpcMessage.setMessageType(msg instanceof HeartbeatMessage ?
            ProtocolConstants.MSGTYPE_HEARTBEAT_RESPONSE :
            ProtocolConstants.MSGTYPE_RESPONSE);
        rpcMessage.setCodec(request.getCodec()); // same with request
        rpcMessage.setCompressor(request.getCompressor());
        rpcMessage.setBody(msg);
        rpcMessage.setId(request.getId());
        channelWritableCheck(channel, msg);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("send response:" + rpcMessage.getBody() + ",channel:" + channel);
        }
        channel.writeAndFlush(rpcMessage);
    }

    /**
     * 检查水位，看是否能写
     * @param channel
     * @param msg
     */
    private void channelWritableCheck(Channel channel, Object msg) {
        int tryTimes = 0;
        synchronized (lock) {
            while (!channel.isWritable()) {
                try {
                    tryTimes++;
                    if (tryTimes > NettyClientConfig.getMaxNotWriteableRetry()) {
                        destroyChannel(channel);
                        throw new FrameworkException("msg:" + ((msg == null) ? "null" : msg.toString()),
                            FrameworkErrorCode.ChannelIsNotWritable);
                    }
                    lock.wait(NOT_WRITEABLE_CHECK_MILLS);
                } catch (InterruptedException exx) {
                    LOGGER.error(exx.getMessage());
                }
            }
        }
    }

    /**
     * Gets group.
     *
     * @return the group
     */
    public String getGroup() {
        return group;
    }

    /**
     * Sets group.
     *
     * @param group the group
     */
    public void setGroup(String group) {
        this.group = group;
    }

    /**
     * Destroy channel.
     *
     * @param channel the channel
     */
    public void destroyChannel(Channel channel) {
        destroyChannel(getAddressFromChannel(channel), channel);
    }

    /**
     * Destroy channel.
     *
     * @param serverAddress the server address
     * @param channel       the channel
     */
    public abstract void destroyChannel(String serverAddress, Channel channel);

    /**
     * Gets address from context.
     *
     * @param ctx the ctx
     * @return the address from context
     */
    protected String getAddressFromContext(ChannelHandlerContext ctx) {
        return getAddressFromChannel(ctx.channel());
    }

    /**
     * Gets address from channel.
     *
     * @param channel the channel
     * @return the address from channel
     */
    protected String getAddressFromChannel(Channel channel) {
        SocketAddress socketAddress = channel.remoteAddress();
        String address = socketAddress.toString();
        if (socketAddress.toString().indexOf(NettyClientConfig.getSocketAddressStartChar()) == 0) {
            address = socketAddress.toString().substring(NettyClientConfig.getSocketAddressStartChar().length());
        }
        return address;
    }

    /**
     * For testing. When the thread pool is full, you can change this variable and share the stack
     */
    boolean allowDumpStack = false;

    /**
     * The type AbstractHandler.
     */
    abstract class AbstractHandler extends ChannelDuplexHandler {

        /**
         * Dispatch.
         *
         * @param request the request
         * @param ctx     the ctx
         */
        public abstract void dispatch(RpcMessage request, ChannelHandlerContext ctx);

        @Override
        public void channelWritabilityChanged(ChannelHandlerContext ctx) {
            synchronized (lock) {
                if (ctx.channel().isWritable()) {
                    lock.notifyAll();
                }
            }

            ctx.fireChannelWritabilityChanged();
        }

        /**
         * 客户端处理消息
         * @param ctx
         * @param msg
         * @throws Exception
         */
        @Override
        public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof RpcMessage) {
                final RpcMessage rpcMessage = (RpcMessage) msg;

                // 通过这个判断可以知道，Request进来这里。通常是Server端使用
                if (rpcMessage.getMessageType() == ProtocolConstants.MSGTYPE_RESQUEST
                    || rpcMessage.getMessageType() == ProtocolConstants.MSGTYPE_RESQUEST_ONEWAY) {
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String.format("%s msgId:%s, body:%s", this, rpcMessage.getId(), rpcMessage.getBody()));
                    }
                    try {
                        messageExecutor.execute(new Runnable() {
                            @Override
                            public void run() {
                                try {
                                    /** dispatch是抽象类，由具体的子类实现 */
                                    dispatch(rpcMessage, ctx);
                                } catch (Throwable th) {
                                    LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                                }
                            }
                        });
                    } catch (RejectedExecutionException e) {
                        LOGGER.error(FrameworkErrorCode.ThreadPoolFull.getErrCode(),
                            "thread pool is full, current max pool size is " + messageExecutor.getActiveCount());
                        // 如果配置了Dump堆栈，会在出现异常的时候jstack堆栈，但是貌似只能Windows并且有D盘，可以改进
                        if (allowDumpStack) {
                            String name = ManagementFactory.getRuntimeMXBean().getName();
                            String pid = name.split("@")[0];
                            int idx = new Random().nextInt(100);
                            try {
                                Runtime.getRuntime().exec("jstack " + pid + " >d:/" + idx + ".log");
                            } catch (IOException exx) {
                                LOGGER.error(exx.getMessage());
                            }
                            allowDumpStack = false;
                        }
                    }
                } else {
                    // 这里是Response进来，Client端使用。
                    // 根据id，移除并获得ConcurrentHasMap中的future，如果有返回future对象，则说明请求还没超时
                    // 如果返回的对象为空，则说明请求已经超时了，被线程池移除了。
                    MessageFuture messageFuture = futures.remove(rpcMessage.getId());
                    if (LOGGER.isDebugEnabled()) {
                        LOGGER.debug(String
                            .format("%s msgId:%s, future :%s, body:%s", this, rpcMessage.getId(), messageFuture,
                                rpcMessage.getBody()));
                    }

                    // 如果有返回结果，则说明请求还没有超时。
                    // MessageFuture其实是包装了CompletableFuture，这是1.8中引入的可以实现异步回调，并不需要使用Future的方式。
                    if (messageFuture != null) {
                        messageFuture.setResultMessage(rpcMessage.getBody());
                    } else {
                        // future为空，说明请求已经超时。此时做异步处理。dispatch也需要由于具体的子类实现。
                        try {
                            messageExecutor.execute(new Runnable() {
                                @Override
                                public void run() {
                                    try {
                                        dispatch(rpcMessage, ctx);
                                    } catch (Throwable th) {
                                        LOGGER.error(FrameworkErrorCode.NetDispatch.getErrCode(), th.getMessage(), th);
                                    }
                                }
                            });
                        } catch (RejectedExecutionException e) {
                            LOGGER.error(FrameworkErrorCode.ThreadPoolFull.getErrCode(),
                                "thread pool is full, current max pool size is " + messageExecutor.getActiveCount());
                        }
                    }
                }
            }
        }

        @Override
        public void exceptionCaught(ChannelHandlerContext ctx, Throwable cause) throws Exception {
            LOGGER.error(FrameworkErrorCode.ExceptionCaught.getErrCode(),
                ctx.channel() + " connect exception. " + cause.getMessage(),
                cause);
            try {
                destroyChannel(ctx.channel());
            } catch (Exception e) {
                LOGGER.error("failed to close channel {}: {}", ctx.channel(), e.getMessage(), e);
            }
        }

        @Override
        public void close(ChannelHandlerContext ctx, ChannelPromise future) throws Exception {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info(ctx + " will closed");
            }
            super.close(ctx, future);
        }

    }
}
