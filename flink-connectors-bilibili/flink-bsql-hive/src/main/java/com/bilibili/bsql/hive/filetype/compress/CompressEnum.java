package com.bilibili.bsql.hive.filetype.compress;

import org.apache.commons.lang3.StringUtils;


/**
 * @Description the compression support for textfile,and now only {@link CompressEnum#LZO_COMPRESS} is supported.
 */
public enum CompressEnum {

    LZO_COMPRESS("lzo", "lzop", ".lzo"),
    ORC_COMPRESS("orc", "zstd", ".orc");

    private String codecName;
    private String compression;
    private String compressFileSuffix;

    CompressEnum(String codecName, String compression, String compressFileSuffix) {
        this.codecName = codecName;
        this.compression = compression;
        this.compressFileSuffix = compressFileSuffix;
    }

    public static CompressEnum getCompressEnumByName(String codecName) {
        if (StringUtils.isBlank(codecName)) {
            return null;
        }

        switch (codecName){
            case "lzo":
            case "LZO":
                return LZO_COMPRESS;
            case "zstd":
            case "ZSTD":
                return ORC_COMPRESS;

            default:
                return null;
        }

    }

    public String getCodecName() {
        return codecName;
    }

    public String getCompression() {
        return compression;
    }

    public String getCompressFileSuffix() {
        return compressFileSuffix;
    }
}
