package com.bilibili.bsql.taishan.lookup.keyhandler;

import com.bilibili.bsql.taishan.format.TaishanRPC;

public abstract class AbstractKeyHandler {
    protected TaishanRPC taishanRPC;
    protected String table;
    protected String token;
    protected long timeout;

    public AbstractKeyHandler(TaishanRPC taishanRPC, String table, String token, long timeout) {
        this.taishanRPC = taishanRPC;
        this.table = table;
        this.token = token;
        this.timeout = timeout;
    }

    public String createCacheKey(Object... inputs) {
        throw new UnsupportedOperationException("taishan multi key query not support the cache for now!");
    }
}
