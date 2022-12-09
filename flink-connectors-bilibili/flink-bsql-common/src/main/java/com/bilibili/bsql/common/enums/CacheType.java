package com.bilibili.bsql.common.enums;

/** CacheType. */
public enum CacheType {
    NONE,
    LRU,
    ALL;

    public static boolean isValid(String type) {
        for (CacheType tmpType : CacheType.values()) {
            if (tmpType.name().equalsIgnoreCase(type)) {
                return true;
            }
        }
        return false;
    }
}
