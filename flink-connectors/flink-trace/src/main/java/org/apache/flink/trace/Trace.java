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

package org.apache.flink.trace;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.InvocationTargetException;

/**
 * @author FredGao
 * @version 1.0.0
 * @ClassName TraceWrapper.java
 * @Description
 * @createTime 2022-01-27 14:15:00
 */
public interface Trace {

	Logger LOG = LoggerFactory.getLogger(Trace.class);

    String TRACE_KIND_CUSTOM = "custom";


    void traceEvent(long time, long value, String key, String url, int delay, String sinkType);

    void traceError(long time, long value, String key, String url, String error, String sinkType);

    /**
     * Create a BiliTraceClient from config.
     */
    static Trace createTraceClient(ClassLoader cl, String traceKind, String customClass, String traceId) {
        switch (traceKind) {
            case TRACE_KIND_CUSTOM:
                try {
                    return (Trace) cl.loadClass(customClass).getDeclaredConstructor(String.class).newInstance(traceId);
                } catch (ClassNotFoundException | IllegalAccessException | InstantiationException | NoSuchMethodException | InvocationTargetException e) {
                    LOG.error("init custom traceClient error, cl = {}, traceKind = {}, customClass = {}, traceId = {}",
                        cl, traceKind, customClass, traceId);
					throw new RuntimeException(
                            "Can not new instance for custom class from " + customClass, e);
                }
            default:
                return new DefaultTrace(traceId);
        }
    }
}
