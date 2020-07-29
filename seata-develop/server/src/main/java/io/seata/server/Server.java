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
package io.seata.server;

import io.seata.common.XID;
import io.seata.common.thread.NamedThreadFactory;
import io.seata.common.util.NetUtil;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.core.rpc.netty.RpcServer;
import io.seata.core.rpc.netty.ShutdownHook;
import io.seata.server.coordinator.DefaultCoordinator;
import io.seata.server.metrics.MetricsManager;
import io.seata.server.session.SessionHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * TC服务端
 * The type Server.
 *
 * @author slievrly
 */
public class Server {

    private static final Logger LOGGER = LoggerFactory.getLogger(Server.class);

    private static final int MIN_SERVER_POOL_SIZE = 50;
    private static final int MAX_SERVER_POOL_SIZE = 500;
    // 最大的任务队列容量
    private static final int MAX_TASK_QUEUE_SIZE = 20000;
    // 500s
    private static final int KEEP_ALIVE_TIME = 500;
    // 工作线程
    private static final ThreadPoolExecutor WORKING_THREADS = new ThreadPoolExecutor(MIN_SERVER_POOL_SIZE,
        MAX_SERVER_POOL_SIZE, KEEP_ALIVE_TIME, TimeUnit.SECONDS,
        new LinkedBlockingQueue<>(MAX_TASK_QUEUE_SIZE),
        new NamedThreadFactory("ServerHandlerThread", MAX_SERVER_POOL_SIZE), new ThreadPoolExecutor.CallerRunsPolicy());

    /**
     * TC 应用程序的入口点。
     * The entry point of application.
     *
     * @param args the input arguments
     * @throws IOException the io exception
     */
    public static void main(String[] args) throws IOException {
        // 初始化参数解析器
        // 请注意，参数解析器应始终是要执行的第一行。
        // 因为，这里我们需要解析启动所需的参数。
        ParameterParser parameterParser = new ParameterParser(args);

        /** 初始化指标 */
        MetricsManager.get().init();

        // 从启动参数中获取存储模式放到系统环境变量
        System.setProperty(ConfigurationKeys.STORE_MODE, parameterParser.getStoreMode());

        /** 构建rpc服务端 */
        RpcServer rpcServer = new RpcServer(WORKING_THREADS);
        // 设置服务端监听端口
        rpcServer.setListenPort(parameterParser.getPort());
        // 初始化UUID生成器
        UUIDGenerator.init(parameterParser.getServerNode());
        //log store mode : file, db
        // 设置资源存储模式
        SessionHolder.init(parameterParser.getStoreMode());

        /** 核心事务协调器创建 初始化了DefaultCore */
        DefaultCoordinator coordinator = new DefaultCoordinator(rpcServer);
        /** 初始化核心事务协调器 */
        coordinator.init();

        // 协调者作为handler设置到netty server中
        rpcServer.setHandler(coordinator);
        // register ShutdownHook
        ShutdownHook.getInstance().addDisposable(coordinator);
        ShutdownHook.getInstance().addDisposable(rpcServer);

        //127.0.0.1 and 0.0.0.0 are not valid here.
        // 设置XID的address和port
        if (NetUtil.isValidIp(parameterParser.getHost(), false)) {
            XID.setIpAddress(parameterParser.getHost());
        } else {
            XID.setIpAddress(NetUtil.getLocalIp());
        }
        XID.setPort(rpcServer.getListenPort());

        try {
            /** rpc服务端初始化 */
            rpcServer.init();
        } catch (Throwable e) {
            LOGGER.error("rpcServer init error:{}", e.getMessage(), e);
            System.exit(-1);
        }

        System.exit(0);
    }
}
