/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2019 All Rights Reserved.
 */
package com.bilibili.bsql.kfc.proxy;

import java.util.concurrent.Future;

import com.baidu.brpc.client.RpcCallback;

import ai.kfc.KfcMessage;

public interface KfcServiceAsync extends KfcService {
    Future<KfcMessage.KFCResponse> GetValues(KfcMessage.KFCRequest request,
                                             RpcCallback<KfcMessage.KFCResponse> callback);
}
