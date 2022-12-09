package org.apache.flink.bili.external.archer.integrate;

import lombok.Data;

import java.util.List;

/** archer get job ids by unique relateUid response */
@Data
public class ArcherGetJobIdByRelateUIDResp {

    private List<Long> jobIds;
}
