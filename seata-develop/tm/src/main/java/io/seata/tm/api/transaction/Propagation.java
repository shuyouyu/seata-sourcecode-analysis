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
package io.seata.tm.api.transaction;

/**
 * Propagation level of global transactions.
 *
 * @author haozhibei
 */
public enum Propagation {
    /**
     * 需要
     * The REQUIRED.
     */
    REQUIRED,

    /**
     * 新的需要
     * The REQUIRES_NEW.
     */
    REQUIRES_NEW,

    /**
     * 不需要
     * The NOT_SUPPORTED
     */
    NOT_SUPPORTED,

    /**
     * 支持
     * The SUPPORTS
     */
    SUPPORTS,

    /**
     * 从不
     * The NEVER
     */
    NEVER,

    /**
     * 强制性
     * The MANDATORY
     */
    MANDATORY

}

