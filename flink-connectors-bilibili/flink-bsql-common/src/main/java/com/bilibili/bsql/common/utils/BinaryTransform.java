/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */

package com.bilibili.bsql.common.utils;

/**
 *
 * @author zhouxiaogang
 * @version $Id: BinaryTransform.java, v 0.1 2020-03-11 17:57
zhouxiaogang Exp $$
 */
public class BinaryTransform {
    /**
     * 大端序编码,java为高字节在前(数组0表示最前)
     */
    public static byte[] encodeBigEndian(int n) {
        byte[] b = new byte[4];
        b[0] = (byte) (n >> 24 & 0xff);
        b[1] = (byte) (n >> 16 & 0xff);
        b[2] = (byte) (n >> 8 & 0xff);
        b[3] = (byte) (n & 0xff);
        return b;
    }

    /**
     * 小端序编码,java为高字节在后(数组0表示最前)
     */
    public static byte[] encodeLittleEndian(int n) {
        byte[] b = new byte[4];
        b[0] = (byte) (n & 0xff);
        b[1] = (byte) (n >> 8 & 0xff);
        b[2] = (byte) (n >> 16 & 0xff);//高字节在后是与java存放内存相反, 与书写顺序相反
        b[3] = (byte) (n >> 24 & 0xff);//数据组结束位,存放内存起始位, 即:高字节在后
        return b;
    }

    /**
     * 整数到字节数组转换,java为高字节在前(数组0表示最前)
     */
    public static int decodeBigEndian(byte b[]) {
        int s = 0;
        s = ((((b[0] & 0xff) << 24 | (b[1] & 0xff)) << 16) | (b[2] & 0xff)) << 8| (b[3] & 0xff);
        return s;
    }

    /**
     * 整数到字节数组转换,java为高字节在后(数组3表示最前)
     */
    public static int decodeLittleEndian(byte b[]) {
        int s = 0;
        s = ((((b[3] & 0xff) << 24 | (b[2] & 0xff)) << 16) | (b[1] & 0xff)) << 8| (b[0] & 0xff);
        return s;
    }
}
