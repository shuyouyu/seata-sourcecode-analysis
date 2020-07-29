#### RM
> RM即为资源管理器

---
RM作为资源管理器，其原理就是代理，数据源代理、连接器代理、执行器代理，最后使用ExecuteTemplate模板类执行业务

###### 源码分析
> 1）初始化RMClient

```
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
    /** 设置RM客户端消息监听器（rmHandler用于接收fescar-server在二阶段发出的提交或者回滚请求）  (DefaultRMHandler, RmRpcClient) */
    rmRpcClient.setClientMessageListener(new RmMessageListener(DefaultRMHandler.get(), rmRpcClient));
    /** 初始化 RM Client */
    rmRpcClient.init();
}

/**
 * 获取RM客户端实例
 * Gets instance.
 *
 * @param applicationId           the application id
 * @param transactionServiceGroup the transaction service group
 * @return the instance
 */
public static RmRpcClient getInstance(String applicationId, String transactionServiceGroup) {
    /** 获取RM客户端实例 */
    RmRpcClient rmRpcClient = getInstance();
    rmRpcClient.setApplicationId(applicationId);
    rmRpcClient.setTransactionServiceGroup(transactionServiceGroup);
    return rmRpcClient;
}

/**
 * 获取RM客户端实例
 * Gets instance.
 *
 * @return the instance
 */
public static RmRpcClient getInstance() {
    if (null == instance) {
        synchronized (RmRpcClient.class) {
            if (null == instance) {
                // 构建netty客户端线程池
                NettyClientConfig nettyClientConfig = new NettyClientConfig();
                final ThreadPoolExecutor messageExecutor = new ThreadPoolExecutor(
                    nettyClientConfig.getClientWorkerThreads(), nettyClientConfig.getClientWorkerThreads(),
                    KEEP_ALIVE_TIME, TimeUnit.SECONDS, new LinkedBlockingQueue<>(MAX_QUEUE_SIZE),
                    new NamedThreadFactory(nettyClientConfig.getRmDispatchThreadPrefix(),
                        nettyClientConfig.getClientWorkerThreads()), new ThreadPoolExecutor.CallerRunsPolicy());
                /** 创建RM客户端 */
                instance = new RmRpcClient(nettyClientConfig, null, messageExecutor);
            }
        }
    }
    return instance;
}

/**
 * 创建RM客户端
 * @param nettyClientConfig
 * @param eventExecutorGroup
 * @param messageExecutor
 */
private RmRpcClient(NettyClientConfig nettyClientConfig, EventExecutorGroup eventExecutorGroup,
                    ThreadPoolExecutor messageExecutor) {
    /** 调用父类（AbstractRpcRemotingClient）的构造方法 创建RM客户端 */
    super(nettyClientConfig, eventExecutorGroup, messageExecutor, TransactionRole.RMROLE);
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
 * 初始化 RM Client
 */
@Override
public void init() {
    // 这里加上乐观锁，避免多线程并发
    if (initialized.compareAndSet(false, true)) {
        /** 初始化 RM Client */
        super.init();
    }
}

/**
 * 初始化rpc客户端（AbstractRpcRemotingClient到这里，后续的操作和TM的初始化一模一样）
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
```

这个是RM中很重的请求事件处理的实现
```
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
```


> 2）TM的处理流程，包括分支注册、分支报告、分支提交、分支回滚等等（比较重要的类DataSourceProxy、ConnectionProxy、PreparedStatementProxy、ExecuteTemplate，大致调用关系为DataSourceProxy获取连接ConnectionProxy，ConnectionProxy获取预编译PreparedStatementProxy，PreparedStatementProxy获取SQL执行器ExecuteTemplate）

```
// 这里我们就从ExecuteTemplate开始分析
/**
 * 执行sql（分支事务）
 * Execute t.
 *
 * @param <T>               the type parameter
 * @param <S>               the type parameter
 * @param sqlRecognizers     the sql recognizer list
 * @param statementProxy    the statement proxy
 * @param statementCallback the statement callback
 * @param args              the args
 * @return the t
 * @throws SQLException the sql exception
 */
public static <T, S extends Statement> T execute(List<SQLRecognizer> sqlRecognizers,
                                                 StatementProxy<S> statementProxy,
                                                 StatementCallback<T, S> statementCallback,
                                                 Object... args) throws SQLException {

    // 判断是否时AT模式
    if (!shouldExecuteInATMode()) {
        /** 非AT模式 */
        return statementCallback.execute(statementProxy.getTargetStatement(), args);
    }

    /** 构建sql解析器 */
    if (sqlRecognizers == null) {
        sqlRecognizers = SQLVisitorFactory.get(
                statementProxy.getTargetSQL(),
                statementProxy.getConnectionProxy().getDbType());
    }
    Executor<T> executor;
    if (CollectionUtils.isEmpty(sqlRecognizers)) {
        // executor是PlainExecutor（普通执行器）
        executor = new PlainExecutor<>(statementProxy, statementCallback);
    } else {
        if (sqlRecognizers.size() == 1) {
            SQLRecognizer sqlRecognizer = sqlRecognizers.get(0);
            // 根据不同的sql类型创建不同的执行器
            switch (sqlRecognizer.getSQLType()) {
                case INSERT:
                    /** 这里时构建一个insert执行器 */
                    executor = new InsertExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                    break;
                case UPDATE:
                    executor = new UpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                    break;
                case DELETE:
                    executor = new DeleteExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                    break;
                case SELECT_FOR_UPDATE:
                    executor = new SelectForUpdateExecutor<>(statementProxy, statementCallback, sqlRecognizer);
                    break;
                default:
                    executor = new PlainExecutor<>(statementProxy, statementCallback);
                    break;
            }
        } else {
            // 如果存在多个sql解析器就创建多个sql执行器
            executor = new MultiExecutor<>(statementProxy, statementCallback, sqlRecognizers);
        }
    }
    T rs;
    try {
        /** sql执行，并返回结果 executor是BaseTransactionalExecutor */
        rs = executor.execute(args);
    } catch (Throwable ex) {
        if (!(ex instanceof SQLException)) {
            // Turn other exception into SQLException
            ex = new SQLException(ex);
        }
        throw (SQLException) ex;
    }
    return rs;
}
/**
 * sql执行，并返回结果
 * @param args the args
 * @return
 * @throws Throwable
 */
@Override
public T execute(Object... args) throws Throwable {
    /** 判断当前应用的运行时是否处于全局事务的上下文中 */
    if (RootContext.inGlobalTransaction()) {
        String xid = RootContext.getXID();
        /** 连接器绑定全局事务xid */
        statementProxy.getConnectionProxy().bind(xid);
    }

    /** 根据当前应用运行时的全局事务锁来设置此连接是否需要全局锁定 */
    statementProxy.getConnectionProxy().setGlobalLockRequire(RootContext.requireGlobalLock());
    /** 执行sql AbstractDMLBaseExecutor */
    return doExecute(args);
}
/**
 * 执行sql
 * @param args the args
 * @return
 * @throws Throwable
 */
@Override
public T doExecute(Object... args) throws Throwable {
    AbstractConnectionProxy connectionProxy = statementProxy.getConnectionProxy();
    // 判断是否是自动提交
    if (connectionProxy.getAutoCommit()) {
        return executeAutoCommitTrue(args);
    } else {
        /** 非自动提交并执行sql */
        return executeAutoCommitFalse(args);
    }
}
/**
 * 非自动提交并执行sql
 * Execute auto commit false t.
 *
 * @param args the args
 * @return the t
 * @throws Exception the exception
 */
protected T executeAutoCommitFalse(Object[] args) throws Exception {
    /** 获取前镜像 */
    TableRecords beforeImage = beforeImage();
    // 执行sql并获得返回结构
    T result = statementCallback.execute(statementProxy.getTargetStatement(), args);
    /** 获取后镜像 */
    TableRecords afterImage = afterImage(beforeImage);
    /** 将前后镜像写到undoLog中 */
    prepareUndoLog(beforeImage, afterImage);
    return result;
}
```
未完待续
