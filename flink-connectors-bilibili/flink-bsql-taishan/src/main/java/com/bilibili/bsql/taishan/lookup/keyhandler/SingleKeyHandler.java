package com.bilibili.bsql.taishan.lookup.keyhandler;

import com.bapis.infra.service.taishan.Auth;
import com.bapis.infra.service.taishan.GetReq;
import com.bapis.infra.service.taishan.GetResp;
import com.bapis.infra.service.taishan.Record;
import com.bilibili.bsql.taishan.format.TaishanRPC;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;

import java.nio.charset.StandardCharsets;

public class SingleKeyHandler extends AbstractKeyHandler implements KeyHandler<GetReq, GetResp> {

    public SingleKeyHandler(TaishanRPC taishanRPC, String table, String token, long timeout) {
        super(taishanRPC, table, token, timeout);
    }

    @Override
    public String createCacheKey(Object... inputs) {
        return inputs[0].toString();
    }

    @Override
    public GetReq buildReq(Object... inputs) {
        return GetReq.newBuilder()
                .setTable(table)
                .setAuth(Auth.newBuilder().setToken(token).build())
                .setRecord(Record.newBuilder()
                        .setKey(ByteString.copyFrom(inputs[0].toString().getBytes(StandardCharsets.UTF_8)))
                        .build())
                .build();
    }

    @Override
    public ListenableFuture<GetResp> query(GetReq key) {
        return taishanRPC.getAsync(key, timeout);
    }

}
