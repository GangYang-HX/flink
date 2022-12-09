package com.bilibili.bsql.trace;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.lang.reflect.InvocationTargetException;

/** Trace. */
public interface Trace extends Serializable {

    Logger LOG = LoggerFactory.getLogger(Trace.class);

    String TRACE_KIND_CUSTOM = "custom";

    void traceEvent(
            String traceId,
            long time,
            long value,
            String key,
            String url,
            int delay,
            String sinkType,
            String pipeline,
            String color);

    void traceError(
            String traceId,
            long time,
            long value,
            String key,
            String url,
            String error,
            String sinkType,
            String pipeline,
            String color);

    /** Create a BiliTraceClient from config. */
    static Trace createTraceClient(
            ClassLoader cl, String traceKind, String customClass, String traceId) {
        switch (traceKind) {
            case TRACE_KIND_CUSTOM:
                try {
                    LOG.info("init LancerTrace");
                    return (Trace) cl.loadClass(customClass).getDeclaredConstructor().newInstance();
                } catch (ClassNotFoundException
                        | IllegalAccessException
                        | InstantiationException
                        | NoSuchMethodException
                        | InvocationTargetException e) {
                    LOG.error(
                            "init custom traceClient error, cl = {}, traceKind = {}, customClass = {}, traceId = {}",
                            cl,
                            traceKind,
                            customClass,
                            traceId);
                    throw new RuntimeException(
                            "Can not new instance for custom class from " + customClass, e);
                }
            default:
                LOG.info("init DefaultTrace");
                return new DefaultTrace();
        }
    }
}
