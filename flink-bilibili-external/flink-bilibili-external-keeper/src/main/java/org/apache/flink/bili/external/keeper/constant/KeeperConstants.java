package org.apache.flink.bili.external.keeper.constant;

/** KeeperConstants. */
public class KeeperConstants {

    // Access information
    public static final String APP_ID = "datacenter.keeper.keeper";
    public static final String URL = "http://berserker.bilibili.co/voyager/v1/invocation/grpc";
    public static final String ACCOUNT = "p_flink_connector";
    public static final String SECRET_KEY = "1a585c3d4cb94ffdbe08e50d1788985f";

    // Interface information

    // Keeper Group start
    public static final String GROUP_NAME_KEEPER = "Keeper";
    // get table info api
    public static final String API_GET_TABLE_INFO = "GetTableInfo";
    // Keeper Group end
}
