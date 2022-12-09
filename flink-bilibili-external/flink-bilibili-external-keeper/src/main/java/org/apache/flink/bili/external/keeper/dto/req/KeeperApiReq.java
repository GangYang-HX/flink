package org.apache.flink.bili.external.keeper.dto.req;

import cn.hutool.core.lang.TypeReference;
import lombok.Builder;
import lombok.Data;

/** Common request dto for keeper by open api. */
@Data
@Builder
public class KeeperApiReq {
    /** keeper method group. * */
    private String groupName;
    /** keeper method name. * */
    private String apiName;
    /** UUID for identify each individual request. * */
    private String requestId;
    /** Request parameters serialized into Json. * */
    private String data;

    private TypeReference<?> type;
}
