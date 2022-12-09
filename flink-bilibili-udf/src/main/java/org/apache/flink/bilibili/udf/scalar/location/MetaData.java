package org.apache.flink.bilibili.udf.scalar.location;

import java.util.Map;

/**
 * @copyright IPIP.net
 */
public class MetaData {
    public int build;
    public int ip_version;
    public int node_count;
    public Map<String, Integer> languages;
    public String[] fields;
    public int total_size;
}
