# seata-sourcecode-analysis
seata源码分析，从TM、TC、RM三个方面来分析

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
---
###### [TM源码分析](TM.md)
1. 初始化TMClient（入口在GlobalTransactionScanner）
2. AT全局事务@GlobalTransactional的处理流程，而TCC的话，需要分析TccActionInterceptor（入口在GlobalTransactionalInterceptor）
---
###### [TC源码分析](TC.md)
1. 程序入口，构建RpcServer并初始化（入口在Server#main）
2. rpc服务端初始化（入口在RpcServer）
---
###### [RM源码分析](RM.md)
1. 初始化客户端（入口在GlobalTransactionScanner）
2. TM的处理流程，包括分支注册、分支报告、分支提交、分支回滚等等（比较重要的类DataSourceProxy、ConnectionProxy、PreparedStatementProxy、ExecuteTemplate，大致调用关系为DataSourceProxy获取连接ConnectionProxy，ConnectionProxy获取预编译PreparedStatementProxy，PreparedStatementProxy获取SQL执行器ExecuteTemplate）（入口在ExecuteTemplate）
