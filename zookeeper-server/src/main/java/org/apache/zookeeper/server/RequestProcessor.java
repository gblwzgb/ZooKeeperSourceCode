/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server;

/**
 * RequestProcessor链接在一起以处理事务。请求始终按顺序处理。
 * 独立服务器，跟随者和领导者都具有略微不同的RequestProcessor链接在一起。
 *
 * 请求始终在RequestProcessor链中向前移动。请求通过processRequest()传递给RequestProcessor。
 * 通常，方法总是由单个线程调用。
 *
 * 调用shutdown时，请求RequestProcessor也应关闭与其连接的所有RequestProcessor。
 */

/**
 * RequestProcessors are chained together to process transactions. Requests are
 * always processed in order. The standalone server, follower, and leader all
 * have slightly different RequestProcessors chained together.
 *
 * Requests always move forward through the chain of RequestProcessors. Requests
 * are passed to a RequestProcessor through processRequest(). Generally method
 * will always be invoked by a single thread.
 *
 * When shutdown is called, the request RequestProcessor should also shutdown
 * any RequestProcessors that it is connected to.
 */
public interface RequestProcessor {

    @SuppressWarnings("serial")
    class RequestProcessorException extends Exception {

        public RequestProcessorException(String msg, Throwable t) {
            super(msg, t);
        }

    }

    void processRequest(Request request) throws RequestProcessorException;

    void shutdown();

}
