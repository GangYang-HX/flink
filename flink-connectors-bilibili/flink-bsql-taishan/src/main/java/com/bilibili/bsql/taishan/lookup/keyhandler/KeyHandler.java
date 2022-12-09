package com.bilibili.bsql.taishan.lookup.keyhandler;


import com.google.common.util.concurrent.ListenableFuture;

public interface KeyHandler<K, V> {
    String createCacheKey(Object... inputs);

    K buildReq(Object... inputs);

    ListenableFuture<V> query(K key);

    default boolean shouldReturnDirectly(Object... inputs) {
        String key = inputs[0].toString();
        return key == null || key.length() == 0;
    }
}
