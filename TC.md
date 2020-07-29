#### TM
> TM即为事务管理器，用于聚合事务服务层，以注解的方式存在@GlobalTransactional，在spring容器启动的时候初始化；在程序运行过程中，以拦截器的形成对其增强做全局事务的处理。

> 总的来说，TM到底干了什么？和TC建立通信，AOP的拦截器（GlobalTransactionalInterceptor）使用模板方法的设计模式（TransactionalTemplate）对系统业务增强，在调用业务之前开启全局事务，对业务做异常回滚处理，最后提交全局事务。

---
TM作为事务管理器，是整个seata中AT模式下最最重要的一个模块，分布式事务的开始和结束都是他发起的，其中核心就是TransactionalTemplate模板方法，也是AOP的一种体现。其主要流程分为：
1. 获取当前全局事务实例或创建新的实例
    - 构建默认的事务全局事务
2. 开启全局事务（由步骤1所得，当前tx为DefaultGlobalTransaction）
    - 这里就会发送全局事务开启请求给TC，TC生成xid返回保存到当前应用上下文中
3. 调用业务服务
    - 由于这里的业务方法操作数据库时，数据源、连接器、执行器都是使用的seata包装后的，故这里的会有以下操作步骤：
        - 解析sql
        - 获取前镜像
        - 执行真正的业务sql
        - 获取后镜像
        - 将前后镜像插入到undoLog中
        - 注册本地事务给TC
        - 本地事务提交
        - 分支状态上报给TC
4. 业务调用本身的异常（回滚）
    - 业务出现了异常，就得回滚，TM发送回滚请求给TC，TC拿到全局回滚时，会发送回滚请求给当前全局事务下面的分支事务RM
5. 全局提交
    - 分支事务正确执行完，则TM发起全局事务提交请求给TC，TC拿到请求后，会发送UndoLogDeleteRequest请求给RM，然后RM会删掉对应的undoLog
6. 释放（这个不分析）


###### 源码分析
> 1）初始化TMClient

```
/**
 * The type Global transaction scanner.
 * 全局事务扫描器，说白了这里实现了 InitializingBean ，会调用afterPropertiesSet()方法
 *
 * @author slievrly
 */
public class GlobalTransactionScanner extends AbstractAutoProxyCreator
    implements InitializingBean, ApplicationContextAware,
    DisposableBean {

    /**
     * Instantiates a new Global transaction scanner.
     *
     * @param applicationId      the application id
     * @param txServiceGroup     the tx service group
     * @param mode               the mode
     * @param failureHandlerHook the failure handler hook
     */
    public GlobalTransactionScanner(String applicationId, String txServiceGroup, int mode,
                                    FailureHandler failureHandlerHook) {
        setOrder(ORDER_NUM);
        setProxyTargetClass(true);
        this.applicationId = applicationId;
        this.txServiceGroup = txServiceGroup;
        this.mode = mode;
        this.failureHandlerHook = failureHandlerHook;
    }

    /**
     * postProcessAfterInitialization初始化之后进行代理
     *      AbstractAutoProxyCreator中的postProcessAfterInitialization方法进行处理
     *      首先查看是否在earlyProxyReferences里存在，也就是已经处理过了，不存在就考虑是否要包装，也就是代理
     *      为了解决单例的循环依赖，在实例化bean之后，需要把一个工厂方法加到singletonFactories集合里
     * @param bean
     * @param beanName
     * @param cacheKey
     * @return
     */
    @Override
    protected Object wrapIfNecessary(Object bean, String beanName, Object cacheKey) {
        // 判断是否有开启全局事务
        if (disableGlobalTransaction) {
            return bean;
        }
        try {
            synchronized (PROXYED_SET) {
                if (PROXYED_SET.contains(beanName)) {
                    return bean;
                }
                interceptor = null;
                //check TCC proxy
                if (TCCBeanParserUtils.isTccAutoProxy(bean, beanName, applicationContext)) {
                    //TCC interceptor, proxy bean of sofa:reference/dubbo:reference, and LocalTCC
                    interceptor = new TccActionInterceptor(TCCBeanParserUtils.getRemotingDesc(beanName));
                } else {
                    Class<?> serviceInterface = SpringProxyUtils.findTargetClass(bean);
                    Class<?>[] interfacesIfJdk = SpringProxyUtils.findInterfaces(bean);
                    // 判断 bean 中是否有 GlobalTransactional 和 GlobalLock 注解
                    if (!existsAnnotation(new Class[]{serviceInterface})
                        && !existsAnnotation(interfacesIfJdk)) {
                        return bean;
                    }

                    if (interceptor == null) {
                        // 创建代理类
                        interceptor = new GlobalTransactionalInterceptor(failureHandlerHook);
                        ConfigurationFactory.getInstance().addConfigListener(ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION, (ConfigurationChangeListener) interceptor);
                    }
                }

                LOGGER.info("Bean[{}] with name [{}] would use interceptor [{}]", bean.getClass().getName(), beanName, interceptor.getClass().getName());
                if (!AopUtils.isAopProxy(bean)) {
                    bean = super.wrapIfNecessary(bean, beanName, cacheKey);
                } else {
                    AdvisedSupport advised = SpringProxyUtils.getAdvisedSupport(bean);
                    // 执行包装目标对象到代理对象
                    Advisor[] advisor = buildAdvisors(beanName, getAdvicesAndAdvisorsForBean(null, null, null));
                    for (Advisor avr : advisor) {
                        advised.addAdvisor(0, avr);
                    }
                }
                PROXYED_SET.add(beanName);
                return bean;
            }
        } catch (Exception exx) {
            throw new RuntimeException(exx);
        }
    }

    @Override
    public void afterPropertiesSet() {
        // 判断是否有开启全局事务
        if (disableGlobalTransaction) {
            if (LOGGER.isInfoEnabled()) {
                LOGGER.info("Global transaction is disabled.");
            }
            return;
        }
        /** 初始化客户端 */
        initClient();
    }

}

/**
 * 初始化客户端
 */
private void initClient() {
    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Initializing Global Transaction Clients ... ");
    }
    // check
    if (StringUtils.isNullOrEmpty(applicationId) || StringUtils.isNullOrEmpty(txServiceGroup)) {
        throw new IllegalArgumentException(String.format("applicationId: %s, txServiceGroup: %s", applicationId, txServiceGroup));
    }
    /** 初始化TM */
    TMClient.init(applicationId, txServiceGroup);
    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Transaction Manager Client is initialized. applicationId[{}] txServiceGroup[{}]", applicationId, txServiceGroup);
    }
    /** 初始化RM */
    RMClient.init(applicationId, txServiceGroup);
    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Resource Manager is initialized. applicationId[{}] txServiceGroup[{}]", applicationId, txServiceGroup);
    }

    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Global Transaction Clients are initialized. ");
    }
    registerSpringShutdownHook();

}

/**
 * 初始化TM客户端
 * Init.
 *
 * @param applicationId           the application id
 * @param transactionServiceGroup the transaction service group
 */
public static void init(String applicationId, String transactionServiceGroup) {
    /** 单例模式（双重检测锁），获取TM客户端实例 */
    TmRpcClient tmRpcClient = TmRpcClient.getInstance(applicationId, transactionServiceGroup);
    /** 初始化 TM Client */
    tmRpcClient.init();
}
/**
 * 获取TM客户端实例
 * Gets instance.
 *
 * @param applicationId           the application id
 * @param transactionServiceGroup the transaction service group
 * @return the instance
 */
public static TmRpcClient getInstance(String applicationId, String transactionServiceGroup) {
    /** 单例模式（双重检测锁），获取TM客户端实例 */
    TmRpcClient tmRpcClient = getInstance();
    tmRpcClient.setApplicationId(applicationId);
    tmRpcClient.setTransactionServiceGroup(transactionServiceGroup);
    return tmRpcClient;
}
/**
 * Gets instance.
 * 单例模式（双重检测锁），获取TM客户端实例
 *
 * @return the instance
 */
public static TmRpcClient getInstance() {
    if (null == instance) {
        synchronized (TmRpcClient.class) {
            if (null == instance) {
                NettyClientConfig nettyClientConfig = new NettyClientConfig();
                // 构建netty客户端线程池
                final ThreadPoolExecutor messageExecutor = new ThreadPoolExecutor(
                    nettyClientConfig.getClientWorkerThreads(), nettyClientConfig.getClientWorkerThreads(),
                    KEEP_ALIVE_TIME, TimeUnit.SECONDS,
                    new LinkedBlockingQueue<>(MAX_QUEUE_SIZE),
                    new NamedThreadFactory(nettyClientConfig.getTmDispatchThreadPrefix(),
                        nettyClientConfig.getClientWorkerThreads()),
                    RejectedPolicies.runsOldestTaskPolicy());
                /** 创建TM客户端 */
                instance = new TmRpcClient(nettyClientConfig, null, messageExecutor);
            }
        }
    }
    return instance;
}
/**
 * 创建TM客户端
 * @param nettyClientConfig
 * @param eventExecutorGroup
 * @param messageExecutor
 */
private TmRpcClient(NettyClientConfig nettyClientConfig,
                    EventExecutorGroup eventExecutorGroup,
                    ThreadPoolExecutor messageExecutor) {
    /** 调用父类（AbstractRpcRemotingClient）构造函数 创建TM客户端 */
    super(nettyClientConfig, eventExecutorGroup, messageExecutor, NettyPoolKey.TransactionRole.TMROLE);
}
/**
 * 创建rpc客户端
 * @param nettyClientConfig
 * @param eventExecutorGroup
 * @param messageExecutor
 * @param transactionRole
 */
public AbstractRpcRemotingClient(NettyClientConfig nettyClientConfig, EventExecutorGroup eventExecutorGroup,
                                 ThreadPoolExecutor messageExecutor, NettyPoolKey.TransactionRole transactionRole) {
    // 设置netty客户端线程池
    super(messageExecutor);
    // 设置事务规则
    this.transactionRole = transactionRole;
    // 构建rpc客户端
    clientBootstrap = new RpcClientBootstrap(nettyClientConfig, eventExecutorGroup, transactionRole);
    // 构建rpc客户端池管理
    clientChannelManager = new NettyClientChannelManager(
        new NettyPoolableFactory(this, clientBootstrap), getPoolKeyFunction(), nettyClientConfig);
}

/**
 * 初始化 TM Client
 */
@Override
public void init() {
    // 这里加上乐观锁，避免多线程并发
    if (initialized.compareAndSet(false, true)) {
        // 从配置文件中读取  是否启用降级，系统默认值是false
        enableDegrade = CONFIG.getBoolean(ConfigurationKeys.SERVICE_PREFIX + ConfigurationKeys.ENABLE_DEGRADE_POSTFIX);
        /** 调用父类（AbstractRpcRemotingClient）初始化方法 初始化 TM Client */
        super.init();
    }
}
/**
 * 初始化rpc客户端
 */
@Override
public void init() {
    /** 创建客户端处理器 */
    clientBootstrap.setChannelHandlers(new ClientHandler());
    // 开启客户端服务
    clientBootstrap.start();
    /** 定时尝试连接服务端 */
    timerExecutor.scheduleAtFixedRate(new Runnable() {
        @Override
        public void run() {
            clientChannelManager.reconnect(getTransactionServiceGroup());
        }
    }, SCHEDULE_DELAY_MILLS, SCHEDULE_INTERVAL_MILLS, TimeUnit.MILLISECONDS);

    /**
     * 判断是否开启消息批量请求（默认开启）与 AbstractRpcRemoting里面的sendAsyncRequest()方法里面首尾呼应
     *          如果开启了批量，就创建一个MergedSendRunnable线程来处理 AbstractRpcRemoting里面的sendAsyncRequest()方法里面
     *          开启了批量，存到阻塞队列的rpc消息
     *
     *          new MergedSendRunnable()，就看该类的run()
     */
    if (NettyClientConfig.isEnableClientBatchSendRequest()) {
        mergeSendExecutorService = new ThreadPoolExecutor(MAX_MERGE_SEND_THREAD,
            MAX_MERGE_SEND_THREAD,
            KEEP_ALIVE_TIME, TimeUnit.MILLISECONDS,
            new LinkedBlockingQueue<>(),
            new NamedThreadFactory(getThreadPrefix(), MAX_MERGE_SEND_THREAD));
        mergeSendExecutorService.submit(new MergedSendRunnable());
    }
    /** 调用父类（AbstractRpcRemoting）初始化方法 初始化 rpc客户端 */
    super.init();
}
/**
 * 创建客户端处理器
 * The type ClientHandler.
 */
@Sharable
class ClientHandler extends AbstractHandler {

    @Override
    public void dispatch(RpcMessage request, ChannelHandlerContext ctx) {
        /**
         * 注意：
         *      clientMessageListener只有RM客户端才有，见RMClient#init方法里面的
         *
         *      设置RM客户端消息监听器（rmHandler用于接收fescar-server在二阶段发出的提交或者回滚请求
         *      rmRpcClient.setClientMessageListener(new RmMessageListener(DefaultRMHandler.get(), rmRpcClient));
         */
        if (clientMessageListener != null) {
            String remoteAddress = NetUtil.toStringAddress(ctx.channel().remoteAddress());
            /** 客户端处理消息 */
            clientMessageListener.onMessage(request, remoteAddress);
        }
    }

    /**
     * 客户端处理消息
     * @param ctx
     * @param msg
     * @throws Exception
     */
    @Override
    public void channelRead(final ChannelHandlerContext ctx, Object msg) throws Exception {
        if (!(msg instanceof RpcMessage)) {
            return;
        }
        RpcMessage rpcMessage = (RpcMessage) msg;
        // 判断是否是心跳消息，打印心跳消息
        if (rpcMessage.getBody() == HeartbeatMessage.PONG) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("received PONG from {}", ctx.channel().remoteAddress());
            }
            return;
        }
        // 如果消息类型是合并结果的消息
        if (rpcMessage.getBody() instanceof MergeResultMessage) {
            // 获取消息
            MergeResultMessage results = (MergeResultMessage) rpcMessage.getBody();
            MergedWarpMessage mergeMessage = (MergedWarpMessage) mergeMsgMap.remove(rpcMessage.getId());
            for (int i = 0; i < mergeMessage.msgs.size(); i++) {
                int msgId = mergeMessage.msgIds.get(i);
                MessageFuture future = futures.remove(msgId);
                if (future == null) {
                    if (LOGGER.isInfoEnabled()) {
                        LOGGER.info("msg: {} is not found in futures.", msgId);
                    }
                } else {
                    future.setResultMessage(results.getMsgs()[i]);
                }
            }
            return;
        }
        /** 调用抽象父类（AbstractHandler）处理消息 */
        super.channelRead(ctx, msg);
    }
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
/**
 * 定时尝试连接服务端
 * Reconnect to remote server of current transaction service group.
 *
 * @param transactionServiceGroup transaction service group
 */
void reconnect(String transactionServiceGroup) {
    List<String> availList = null;
    try {
        /** 获取可用的服务列表 */
        availList = getAvailServerList(transactionServiceGroup);
    } catch (Exception e) {
        LOGGER.error("Failed to get available servers: {}", e.getMessage(), e);
        return;
    }
    if (CollectionUtils.isEmpty(availList)) {
        String serviceGroup = RegistryFactory.getInstance()
                                             .getServiceGroup(transactionServiceGroup);
        LOGGER.error("no available service '{}' found, please make sure registry config correct", serviceGroup);
        return;
    }
    for (String serverAddress : availList) {
        try {
            /** 连接服务端获取channel */
            acquireChannel(serverAddress);
        } catch (Exception e) {
            LOGGER.error("{} can not connect to {} cause:{}",FrameworkErrorCode.NetConnect.getErrCode(), serverAddress, e.getMessage(), e);
        }
    }
}
/**
 * 获取可用的服务列表
 * @param transactionServiceGroup
 * @return
 * @throws Exception
 */
private List<String> getAvailServerList(String transactionServiceGroup) throws Exception {
    // 从适配的注册中心中获取可用的服务端（ip + port）列表
    List<InetSocketAddress> availInetSocketAddressList = RegistryFactory.getInstance()
                                                                        .lookup(transactionServiceGroup);
    if (CollectionUtils.isEmpty(availInetSocketAddressList)) {
        return Collections.emptyList();
    }

    return availInetSocketAddressList.stream()
                                     .map(NetUtil::toStringAddress)
                                     .collect(Collectors.toList());
}
/**
 * 获取客户端连接到服务端后的channel
 * Acquire netty client channel connected to remote server.
 *
 * @param serverAddress server address
 * @return netty channel
 */
Channel acquireChannel(String serverAddress) {
    // 从channels集合中根据上面的服务列表遍历的地址获取相应的channel
    Channel channelToServer = channels.get(serverAddress);
    if (channelToServer != null) {
        // 如果当前channel是活跃的，就直接返回
        channelToServer = getExistAliveChannel(channelToServer, serverAddress);
        if (null != channelToServer) {
            return channelToServer;
        }
    }
    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("will connect to " + serverAddress);
    }
    channelLocks.putIfAbsent(serverAddress, new Object());
    /** 加锁重新连接服务端 */
    synchronized (channelLocks.get(serverAddress)) {
        return doConnect(serverAddress);
    }
}
/**
 * 连接服务端获取channel
 * @param serverAddress
 * @return
 */
private Channel doConnect(String serverAddress) {
    Channel channelToServer = channels.get(serverAddress);
    if (channelToServer != null && channelToServer.isActive()) {
        return channelToServer;
    }
    Channel channelFromPool;
    try {
        NettyPoolKey currentPoolKey = poolKeyFunction.apply(serverAddress);
        NettyPoolKey previousPoolKey = poolKeyMap.putIfAbsent(serverAddress, currentPoolKey);
        if (null != previousPoolKey && previousPoolKey.getMessage() instanceof RegisterRMRequest) {
            RegisterRMRequest registerRMRequest = (RegisterRMRequest) currentPoolKey.getMessage();
            ((RegisterRMRequest) previousPoolKey.getMessage()).setResourceIds(registerRMRequest.getResourceIds());
        }
        // 通过netty客户端线程池获取channel
        channelFromPool = nettyClientKeyPool.borrowObject(poolKeyMap.get(serverAddress));
        channels.put(serverAddress, channelFromPool);
    } catch (Exception exx) {
        LOGGER.error("{} register RM failed.",FrameworkErrorCode.RegisterRM.getErrCode(), exx);
        throw new FrameworkException("can not register RM,err:" + exx.getMessage());
    }
    return channelFromPool;
}
/**
 * 处理发送批量消息的线程
 * The type Merged send runnable.
 */
private class MergedSendRunnable implements Runnable {

    @Override
    public void run() {
        while (true) {
            // 加锁等待，由AbstractRpcRemoting#sendAsyncRequest的mergeLock.notifyAll();唤醒
            synchronized (mergeLock) {
                try {
                    mergeLock.wait(MAX_MERGE_SEND_MILLS);
                } catch (InterruptedException e) {
                }
            }
            isSending = true;
            // 这里就是在循环遍历存放阻塞队列的集合
            for (String address : basketMap.keySet()) {

                // 根据地址获取存放rpc消息的阻塞队列
                BlockingQueue<RpcMessage> basket = basketMap.get(address);
                if (basket.isEmpty()) {
                    continue;
                }

                // 构建消息装饰器
                MergedWarpMessage mergeMessage = new MergedWarpMessage();
                while (!basket.isEmpty()) {
                    // 取rpc消息
                    RpcMessage msg = basket.poll();
                    mergeMessage.msgs.add((AbstractMessage) msg.getBody());
                    mergeMessage.msgIds.add(msg.getId());
                }
                if (mergeMessage.msgIds.size() > 1) {
                    // 打日志
                    printMergeMessageLog(mergeMessage);
                }
                Channel sendChannel = null;
                try {
                    // 获取客户端连接到服务端后的channel
                    sendChannel = clientChannelManager.acquireChannel(address);
                    /** 客户端发消息给TC */
                    AbstractRpcRemotingClient.super.defaultSendRequest(sendChannel, mergeMessage);
                } catch (FrameworkException e) {
                    if (e.getErrcode() == FrameworkErrorCode.ChannelIsNotWritable && sendChannel != null) {
                        destroyChannel(address, sendChannel);
                    }
                    // fast fail
                    for (Integer msgId : mergeMessage.msgIds) {
                        MessageFuture messageFuture = futures.remove(msgId);
                        if (messageFuture != null) {
                            messageFuture.setResultMessage(null);
                        }
                    }
                    LOGGER.error("client merge call failed: {}", e.getMessage(), e);
                }
            }
            isSending = false;
        }
    }

    /**
     * 打日志
     * @param mergeMessage
     */
    private void printMergeMessageLog(MergedWarpMessage mergeMessage) {
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("merge msg size:{}", mergeMessage.msgIds.size());
            for (AbstractMessage cm : mergeMessage.msgs) {
                LOGGER.debug(cm.toString());
            }
            StringBuilder sb = new StringBuilder();
            for (long l : mergeMessage.msgIds) {
                sb.append(MSG_ID_PREFIX).append(l).append(SINGLE_LOG_POSTFIX);
            }
            sb.append("\n");
            for (long l : futures.keySet()) {
                sb.append(FUTURES_PREFIX).append(l).append(SINGLE_LOG_POSTFIX);
            }
            LOGGER.debug(sb.toString());
        }
    }
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
 * 调用父类（AbstractRpcRemoting）初始化方法 初始化 rpc客户端
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
```
至此，TM客户端的初始化已经完成，下面我们开始分析TM是怎样工作的。

> 2）AT全局事务@GlobalTransactional的处理流程，而TCC的话，需要分析TccActionInterceptor

```
/**
 * @GlobalTransactionl注解的拦截器
 *
 * 在需要加全局事务的方法中，会加上@GlobalTransactional注解，注解往往对应着拦截器，
 * Seata中拦截全局事务的拦截器是GlobalTransactionalInterceptor
 *
 * MethodInterceptor是spring中aop的接口，所以当前拦截器的入口在invoke方法
 *
 * The type Global transactional interceptor.
 *
 * @author slievrly
 */
public class GlobalTransactionalInterceptor implements ConfigurationChangeListener, MethodInterceptor {

    /**
     * 实例化一个新的全局事务拦截器
     * Instantiates a new Global transactional interceptor.
     *
     * @param failureHandler the failure handler
     */
    public GlobalTransactionalInterceptor(FailureHandler failureHandler) {
        this.failureHandler = failureHandler == null ? DEFAULT_FAIL_HANDLER : failureHandler;
        // 全局事务是否开启的开关
        this.disable = ConfigurationFactory.getInstance().getBoolean(ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION,
            DEFAULT_DISABLE_GLOBAL_TRANSACTION);
    }

    @Override
    public Object invoke(final MethodInvocation methodInvocation) throws Throwable {
        // 获取目标类
        Class<?> targetClass = methodInvocation.getThis() != null ? AopUtils.getTargetClass(methodInvocation.getThis())
            : null;
        Method specificMethod = ClassUtils.getMostSpecificMethod(methodInvocation.getMethod(), targetClass);
        final Method method = BridgeMethodResolver.findBridgedMethod(specificMethod);

        final GlobalTransactional globalTransactionalAnnotation =
            getAnnotation(method, targetClass, GlobalTransactional.class);
        final GlobalLock globalLockAnnotation = getAnnotation(method, targetClass, GlobalLock.class);

        if (!disable && globalTransactionalAnnotation != null) { // 全局事务注解
            /** 全局事务开启的处理 */
            return handleGlobalTransaction(methodInvocation, globalTransactionalAnnotation);
        } else if (!disable && globalLockAnnotation != null) {  // 全局锁注解
            /** 全局锁开启的处理 */
            return handleGlobalLock(methodInvocation);
        } else {
            // 普通消息
            return methodInvocation.proceed();
        }
    }
}

/**
 * 处理全局事务
 * @param methodInvocation
 * @param globalTrxAnno
 * @return
 * @throws Throwable
 */
private Object handleGlobalTransaction(final MethodInvocation methodInvocation,
                                       final GlobalTransactional globalTrxAnno) throws Throwable {
    try {
        /** 事务化模板：通过 GlobalTransaction 和 GlobalTransactionContext API 把一个业务服务的调用包装成带有分布式事务支持的服务 */
        return transactionalTemplate.execute(new TransactionalExecutor() {
            @Override
            public Object execute() throws Throwable {
                return methodInvocation.proceed();
            }

            public String name() {
                String name = globalTrxAnno.name();
                if (!StringUtils.isNullOrEmpty(name)) {
                    return name;
                }
                return formatMethod(methodInvocation.getMethod());
            }

            /**
             * 获取当前全局事务的信息（例如：超时时间，名称等等）
             * @return
             */
            @Override
            public TransactionInfo getTransactionInfo() {
                TransactionInfo transactionInfo = new TransactionInfo();
                transactionInfo.setTimeOut(globalTrxAnno.timeoutMills());
                transactionInfo.setName(name());
                // 从@GlobalTransactional注解上获取全局事务的传播性（默认值是REQUIRED——需要）
                transactionInfo.setPropagation(globalTrxAnno.propagation());
                Set<RollbackRule> rollbackRules = new LinkedHashSet<>();
                for (Class<?> rbRule : globalTrxAnno.rollbackFor()) {
                    rollbackRules.add(new RollbackRule(rbRule));
                }
                for (String rbRule : globalTrxAnno.rollbackForClassName()) {
                    rollbackRules.add(new RollbackRule(rbRule));
                }
                for (Class<?> rbRule : globalTrxAnno.noRollbackFor()) {
                    rollbackRules.add(new NoRollbackRule(rbRule));
                }
                for (String rbRule : globalTrxAnno.noRollbackForClassName()) {
                    rollbackRules.add(new NoRollbackRule(rbRule));
                }
                transactionInfo.setRollbackRules(rollbackRules);
                return transactionInfo;
            }
        });
    } catch (TransactionalExecutor.ExecutionException e) {
        // 出现异常了，拿到状态码，根据不同的状态码处理不同的失败逻辑
        TransactionalExecutor.Code code = e.getCode();
        switch (code) {
            case RollbackDone:
                throw e.getOriginalException();
            case BeginFailure:
                failureHandler.onBeginFailure(e.getTransaction(), e.getCause());
                throw e.getCause();
            case CommitFailure:
                failureHandler.onCommitFailure(e.getTransaction(), e.getCause());
                throw e.getCause();
            case RollbackFailure:
                failureHandler.onRollbackFailure(e.getTransaction(), e.getCause());
                throw e.getCause();
            case RollbackRetrying:
                failureHandler.onRollbackRetrying(e.getTransaction(), e.getCause());
                throw e.getCause();
            default:
                throw new ShouldNeverHappenException(String.format("Unknown TransactionalExecutor.Code: %s", code));

        }
    }
}

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
    /** 1. 获取当前全局事务实例或创建新的实例（可以跟进去看看）如果是第一次创建全局事务，那么他的事务规则是：开始当前的全局事务（Launcher） */
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
    // 将全局事务 xid 绑定到当前应用的运行时中
    RootContext.bind(xid);
    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("Begin new global transaction [{}]", xid);
    }

}

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
 * 异步请求事务协调器，获得一个响应
 * @param request
 * @return
 * @throws TransactionException
 */
private AbstractTransactionResponse syncCall(AbstractTransactionRequest request) throws TransactionException {
    try {
        /** TM客户端发送消息并获得响应 */
        return (AbstractTransactionResponse)TmRpcClient.getInstance().sendMsgWithResponse(request);
    } catch (TimeoutException toe) {
        throw new TmTransactionException(TransactionExceptionCode.IO, "RPC timeout", toe);
    }
}

/**
 * 客户端发送消息并获得响应
 * @param msg the msg
 * @return
 * @throws TimeoutException
 */
@Override
public Object sendMsgWithResponse(Object msg) throws TimeoutException {
    // msg：超时时间（系统默认值60s），名称
    // 超时时间：30s
    return sendMsgWithResponse(msg, NettyClientConfig.getRpcRequestTimeout());
}

/**
 * 客户端发送消息并获得响应
 * @param msg     the msg
 * @param timeout the timeout
 * @return
 * @throws TimeoutException
 */
@Override
public Object sendMsgWithResponse(Object msg, long timeout) throws TimeoutException {
    /** 从服务组里面选择一个 */
    String validAddress = loadBalance(getTransactionServiceGroup());
    /** 获取客户端连接到服务端后的channel */
    Channel channel = clientChannelManager.acquireChannel(validAddress);
    /** 客户端异步发送请求给TC */
    Object result = super.sendAsyncRequestWithResponse(validAddress, channel, msg, timeout);
    return result;
}
/**
 * 负载均衡选择一个地址
 * @param transactionServiceGroup
 * @return
 */
private String loadBalance(String transactionServiceGroup) {
    InetSocketAddress address = null;
    try {
        /** 从注册中心通过serviceGroup拿到所有的TC服务的地址 */
        List<InetSocketAddress> inetSocketAddressList = RegistryFactory.getInstance().lookup(transactionServiceGroup);
        /** 负载均衡选择一个地址  LoadBalanceFactory.getInstance()，获取负载均衡策略，轮询和随机两种 */
        address = LoadBalanceFactory.getInstance().select(inetSocketAddressList);
    } catch (Exception ex) {
        LOGGER.error(ex.getMessage());
    }
    if (address == null) {
        throw new FrameworkException(NoAvailableService);
    }
    // 将InetSocketAddress转成ip:prot的字符串形式，然后返回
    return NetUtil.toStringAddress(address);
}
/**
 * 获取客户端连接到服务端后的channel
 * Acquire netty client channel connected to remote server.
 *
 * @param serverAddress server address
 * @return netty channel
 */
Channel acquireChannel(String serverAddress) {
    // 从channels集合中根据上面的服务列表遍历的地址获取相应的channel
    Channel channelToServer = channels.get(serverAddress);
    if (channelToServer != null) {
        // 如果当前channel是活跃的，就直接返回
        channelToServer = getExistAliveChannel(channelToServer, serverAddress);
        if (null != channelToServer) {
            return channelToServer;
        }
    }
    if (LOGGER.isInfoEnabled()) {
        LOGGER.info("will connect to " + serverAddress);
    }
    channelLocks.putIfAbsent(serverAddress, new Object());
    /** 加锁重新连接服务端 */
    synchronized (channelLocks.get(serverAddress)) {
        return doConnect(serverAddress);
    }
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
 * Bind.
 *
 * 将全局事务 XID 绑定到当前应用的运行时中
 *
 * @param xid the xid
 */
public static void bind(String xid) {
    if (LOGGER.isDebugEnabled()) {
        LOGGER.debug("bind {}", xid);
    }
    /** 将全局事务 xid 绑定到当前应用的运行时中 */
    CONTEXT_HOLDER.put(KEY_XID, xid);
}
/**
 * 将全局事务 xid 绑定到当前应用的运行时中
 * @param key   the key
 * @param value the value
 * @return
 */
@Override
public String put(String key, String value) {
    return threadLocal.get().put(key, value);
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
```
<客户端发送请求给TC，并获取响应> 这是最重要和新手搞不懂的地方，当然，单个消息发送，就是直接发送消息给服务器了。那么批量是怎么处理的呢？还记得在TM的初始化时，我们看到也有这个批量的判断，同时，在里面创建了一个线程池，并提交了任务（new MergedSendRunnable()），故这里的消息，都是由MergedSendRunnable这个类的run()把消息发出去的。
