/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2019 All Rights Reserved.
 */
package com.bilibili.bsql.kfc.proxy;

import com.baidu.brpc.protocol.BrpcMeta;

import ai.kfc.KfcMessage;

public interface KfcService {

    @BrpcMeta(serviceName = "ai.kfc.KFC", methodName = "GetValues")
    KfcMessage.KFCResponse GetValues(KfcMessage.KFCRequest request);
}
