package org.apache.flink.table.runtime.operators.join.latency.util;

import java.nio.ByteBuffer;

/**
 * Created by jackt on 2017/12/9.
 */
public class BytesUtil {

    public static byte bytes2Byte(byte[] byteNum){
        if(byteNum.length != 1){
            throw new RuntimeException("bytes2Byte input bytes length need == 1");
        }

        return byteNum[0];
    }

    public static byte[] intToBytes(int x){
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(0, x);
        return buffer.array();
    }

    public static int bytesToInt(byte[] bytes){
        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.put(bytes, 0, bytes.length);
        buffer.flip();//need flip
        return buffer.getInt();
    }

    /**
     * 把long类型的value转为8个byte字节，放到byte数组的off开始的位置，高位在前
     *
     * @param value
     * @param bytes
     * @param off
     */
    public static void long2bytes(long value, byte[] bytes, int off) {
        bytes[off + 7] = (byte) value;
        bytes[off + 6] = (byte) (value >>> 8);
        bytes[off + 5] = (byte) (value >>> 16);
        bytes[off + 4] = (byte) (value >>> 24);
        bytes[off + 3] = (byte) (value >>> 32);
        bytes[off + 2] = (byte) (value >>> 40);
        bytes[off + 1] = (byte) (value >>> 48);
        bytes[off] = (byte) (value >>> 56);
    }

    public static byte[] long2bytes(final long value) {
        byte[] bytes = new byte[8];
        long2bytes(value, bytes, 0);
        return bytes;
    }

    /**
     * 把byte数组中off开始的8个字节，转为long类型，高位在前
     *
     * @param bytes
     * @param off
     */
    public static long bytes2long(byte[] bytes, int off) {
        return ((bytes[off + 7] & 0xFFL)) + ((bytes[off + 6] & 0xFFL) << 8) + ((bytes[off + 5] & 0xFFL) << 16)
                + ((bytes[off + 4] & 0xFFL) << 24) + ((bytes[off + 3] & 0xFFL) << 32) + ((bytes[off + 2] & 0xFFL) << 40)
                + ((bytes[off + 1] & 0xFFL) << 48) + (((long) bytes[off]) << 56);
    }

    /**
     * 把int类型的value转为4个byte字节，放到byte数组的off开始的位置，高位在前
     *
     * @param value
     * @param bytes
     * @param off
     */
    public static void int2bytes(int value, byte[] bytes, int off) {
        bytes[off + 3] = (byte) value;
        bytes[off + 2] = (byte) (value >>> 8);
        bytes[off + 1] = (byte) (value >>> 16);
        bytes[off] = (byte) (value >>> 24);
    }

    public static byte[] int2bytes(final int value) {
        final byte[] bytes = new byte[4];
        int2bytes(value, bytes, 0);
        return bytes;
    }

    /**
     * 把byte数组中off开始的4个字节，转为int类型，高位在前
     *
     * @param bytes
     * @param off
     */
    public static int bytes2int(byte[] bytes, int off) {
        return ((bytes[off + 3] & 0xFF)) + ((bytes[off + 2] & 0xFF) << 8) + ((bytes[off + 1] & 0xFF) << 16) + ((bytes[off]) << 24);
    }

    /**
     * 把short类型的value转为2个byte字节，放到byte数组的off开始的位置，高位在前
     *
     * @param value
     * @param bytes
     * @param off
     */
    public static void short2bytes(short value, byte[] bytes, int off) {
        bytes[off + 1] = (byte) value;
        bytes[off] = (byte) (value >>> 8);
    }

    public static byte[] short2bytes(final short value) {
        final byte[] bytes = new byte[2];
        short2bytes(value, bytes, 0);
        return bytes;
    }

    /**
     * 把byte数组中off开始的2个字节，转为short类型，高位在前
     *
     * @param b
     * @param off
     */
    public static short bytes2short(byte[] b, int off) {
        return (short) (((b[off + 1] & 0xFF)) + ((b[off] & 0xFF) << 8));
    }
}
