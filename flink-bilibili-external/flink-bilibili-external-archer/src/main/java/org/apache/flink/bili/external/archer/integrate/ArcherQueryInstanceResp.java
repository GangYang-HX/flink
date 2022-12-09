package org.apache.flink.bili.external.archer.integrate;

import lombok.Data;

import java.util.List;

/** archer query instance response */
@Data
public class ArcherQueryInstanceResp {

    private List<InstanceQuery> instances;
}
