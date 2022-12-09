package com.bilibili.bsql.taishan.lookup.keyhandler;

import com.bapis.infra.service.taishan.Auth;
import com.bapis.infra.service.taishan.BatchGetReq;
import com.bapis.infra.service.taishan.BatchGetResp;
import com.bapis.infra.service.taishan.Record;
import com.bilibili.bsql.taishan.format.TaishanRPC;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import org.apache.commons.lang3.StringUtils;

import java.nio.charset.StandardCharsets;

public class MultipleKeyHandler extends AbstractKeyHandler implements KeyHandler<BatchGetReq, BatchGetResp> {

    private final String delimitKey;

    public MultipleKeyHandler(String delimitKey, TaishanRPC taishanRPC, String table, String token, long timeout) {
        super(taishanRPC, table, token, timeout);
        this.delimitKey = delimitKey;
    }

    @Override
    public BatchGetReq buildReq(Object... inputs) {
        BatchGetReq.Builder reqBuilder = BatchGetReq.newBuilder();
        reqBuilder.setTable(table).setAuth(Auth.newBuilder().setToken(token).build());
        String[] keys = StringUtils.splitPreserveAllTokens(inputs[0].toString(), this.delimitKey);
        for (String key : keys) {
            Record record = Record.newBuilder()
                    .setKey(ByteString.copyFrom(key.getBytes(StandardCharsets.UTF_8)))
                    .build();
            reqBuilder.addRecords(record);
        }
        return reqBuilder.build();
    }

    @Override
    public ListenableFuture<BatchGetResp> query(BatchGetReq key) {
        return taishanRPC.batchGetAsync(key, timeout);
    }
}
