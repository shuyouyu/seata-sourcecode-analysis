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
package io.seata.spring.annotation;

import java.lang.annotation.Annotation;
import java.lang.reflect.Method;
import java.util.LinkedHashSet;
import java.util.Set;

import io.seata.common.exception.ShouldNeverHappenException;
import io.seata.common.util.StringUtils;
import io.seata.config.ConfigurationChangeEvent;
import io.seata.config.ConfigurationChangeListener;
import io.seata.config.ConfigurationFactory;
import io.seata.core.constants.ConfigurationKeys;
import io.seata.rm.GlobalLockTemplate;
import io.seata.tm.api.DefaultFailureHandlerImpl;
import io.seata.tm.api.FailureHandler;
import io.seata.tm.api.TransactionalExecutor;
import io.seata.tm.api.TransactionalTemplate;
import io.seata.tm.api.transaction.NoRollbackRule;
import io.seata.tm.api.transaction.RollbackRule;
import io.seata.tm.api.transaction.TransactionInfo;
import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.core.BridgeMethodResolver;
import org.springframework.util.ClassUtils;

import static io.seata.core.constants.DefaultValues.DEFAULT_DISABLE_GLOBAL_TRANSACTION;

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

    private static final Logger LOGGER = LoggerFactory.getLogger(GlobalTransactionalInterceptor.class);
    private static final FailureHandler DEFAULT_FAIL_HANDLER = new DefaultFailureHandlerImpl();

    private final TransactionalTemplate transactionalTemplate = new TransactionalTemplate();
    private final GlobalLockTemplate<Object> globalLockTemplate = new GlobalLockTemplate<>();
    private final FailureHandler failureHandler;
    private volatile boolean disable;

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

    /**
     * 全局锁开启的处理
     * @param methodInvocation
     * @return
     * @throws Exception
     */
    private Object handleGlobalLock(final MethodInvocation methodInvocation) throws Exception {
        /** 锁模板 */
        return globalLockTemplate.execute(() -> {
            try {
                return methodInvocation.proceed();
            } catch (Exception e) {
                throw e;
            } catch (Throwable e) {
                throw new RuntimeException(e);
            }
        });
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

    private <T extends Annotation> T getAnnotation(Method method, Class<?> targetClass, Class<T> annotationClass) {
        return method == null ? targetClass == null ? null : targetClass.getAnnotation(annotationClass)
            : method.getAnnotation(annotationClass);
    }

    private String formatMethod(Method method) {
        StringBuilder sb = new StringBuilder(method.getName()).append("(");

        Class<?>[] params = method.getParameterTypes();
        int in = 0;
        for (Class<?> clazz : params) {
            sb.append(clazz.getName());
            if (++in < params.length) {
                sb.append(", ");
            }
        }
        return sb.append(")").toString();
    }

    @Override
    public void onChangeEvent(ConfigurationChangeEvent event) {
        if (ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION.equals(event.getDataId())) {
            LOGGER.info("{} config changed, old value:{}, new value:{}", ConfigurationKeys.DISABLE_GLOBAL_TRANSACTION,
                disable, event.getNewValue());
            disable = Boolean.parseBoolean(event.getNewValue().trim());
        }
    }
}
