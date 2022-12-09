package com.bilibili.bsql.taishan.format;

import com.bilibili.bsql.taishan.rpc.NamingClientBootstrap;
import pleiades.venus.naming.client.NamingClient;
import pleiades.venus.naming.client.Namings;
import pleiades.venus.naming.client.resolve.NamingResolver;

/**
 * @author zhuzhengjun
 * @date 2020/10/29 2:39 下午
 */
public class TaishanNamingResolver {
    private static volatile NamingResolver resolver;
    public static final String TAISHAN_API_ID = "inf.taishan.proxy";

    public static NamingResolver getResolver() {
        if (resolver == null) {
            synchronized (TaishanNamingResolver.class) {
                if (resolver == null) {
                    NamingClient namingClient = NamingClientBootstrap.getInstance().getNamingClient();
                    resolver = namingClient.resolveForReady(TAISHAN_API_ID, Namings.Scheme.GRPC);
                }
            }
        }
        return resolver;
    }
}
