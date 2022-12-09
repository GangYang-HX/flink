package org.apache.flink.bilibili.udf.scalar.location;

/**
 * @author elsa on 2018/11/14.
 */
public enum IpPart {
    CLASSIFICATION(-1),
    COUNTRY(0),
    PROVINCE(1),
    CITY(2),
    DISTRICT(3),
    ISP(4),
    LATITUDE(5),
    LONGITUDE(6),
    TIMEZONE(7),
    CONTINENT(12);

    private int idx;

    IpPart(int idx) {
        this.idx = idx;
    }

    public int getIdx() {
        return idx;
    }

    public static IpPart parse(String part) {
        return Enum.valueOf(IpPart.class, part);
    }

}
