/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2019 All Rights Reserved.
 */
package com.bilibili.bsql.redis.lettucecodec;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.Charset;
import java.nio.charset.CharsetEncoder;
import java.nio.charset.CoderResult;

import io.lettuce.core.codec.RedisCodec;
import io.lettuce.core.codec.ToByteBufEncoder;
import io.lettuce.core.internal.LettuceAssert;
import io.lettuce.core.protocol.LettuceCharsets;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.ByteBufUtil;
import io.netty.buffer.Unpooled;
import io.netty.util.CharsetUtil;

/**
 *
 * @author zhouxiaogang
 * @version $Id: ByteValueCodec.java, v 0.1 2019-12-04 15:51
zhouxiaogang Exp $$
 */
public class ByteValueCodec implements RedisCodec<String, byte[]>, ToByteBufEncoder<String, byte[]> {
    public static final ByteValueCodec UTF8;
    public static final ByteValueCodec ASCII;
    private static final byte[] EMPTY;
    private final Charset charset;
    private final boolean ascii;
    private final boolean utf8;

    public ByteValueCodec() {
        this(Charset.defaultCharset());
    }

    public ByteValueCodec(Charset charset) {
        LettuceAssert.notNull(charset, "Charset must not be null");
        this.charset = charset;
        if (charset.name().equals("UTF-8")) {
            this.utf8 = true;
            this.ascii = false;
        } else if (charset.name().contains("ASCII")) {
            this.utf8 = false;
            this.ascii = true;
        } else {
            this.ascii = false;
            this.utf8 = false;
        }

    }

    public void encodeKey(String key, ByteBuf target) {
        this.encode(key, target);
    }

    public void encode(String str, ByteBuf target) {
        if (str != null) {
            if (this.utf8) {
                ByteBufUtil.writeUtf8(target, str);
            } else if (this.ascii) {
                ByteBufUtil.writeAscii(target, str);
            } else {
                CharsetEncoder encoder = CharsetUtil.encoder(this.charset);
                int length = (int)((double)str.length() * (double)encoder.maxBytesPerChar());
                target.ensureWritable(length);

                try {
                    ByteBuffer dstBuf = target.nioBuffer(0, length);
                    int pos = dstBuf.position();
                    CoderResult cr = encoder.encode(CharBuffer.wrap(str), dstBuf, true);
                    if (!cr.isUnderflow()) {
                        cr.throwException();
                    }

                    cr = encoder.flush(dstBuf);
                    if (!cr.isUnderflow()) {
                        cr.throwException();
                    }

                    target.writerIndex(target.writerIndex() + dstBuf.position() - pos);
                } catch (CharacterCodingException var8) {
                    throw new IllegalStateException(var8);
                }
            }
        }
    }

    public int estimateSize(Object keyOrValue) {
        if (keyOrValue instanceof String) {
            CharsetEncoder encoder = CharsetUtil.encoder(this.charset);
            return (int)(encoder.averageBytesPerChar() * (float)((String)keyOrValue).length());
        } else if (keyOrValue instanceof byte[]) {
            return keyOrValue == null ? 0 : ((byte[])((byte[])keyOrValue)).length;
        } else {
            return 0;
        }
    }

    public void encodeValue(byte[] value, ByteBuf target) {
        if (value != null) {
            target.writeBytes(value);
        }
    }

    public String decodeKey(ByteBuffer bytes) {
        return Unpooled.wrappedBuffer(bytes).toString(this.charset);
    }

    public byte[] decodeValue(ByteBuffer bytes) {
        return getBytes(bytes);
    }

    public ByteBuffer encodeKey(String key) {
        return this.encodeAndAllocateBuffer(key);
    }

    public ByteBuffer encodeValue(byte[] value) {
        return value == null ? ByteBuffer.wrap(EMPTY) : ByteBuffer.wrap(value);
    }

    private ByteBuffer encodeAndAllocateBuffer(String key) {
        if (key == null) {
            return ByteBuffer.wrap(EMPTY);
        } else {
            CharsetEncoder encoder = CharsetUtil.encoder(this.charset);
            ByteBuffer buffer = ByteBuffer.allocate((int)(encoder.maxBytesPerChar() * (float)key.length()));
            ByteBuf byteBuf = Unpooled.wrappedBuffer(buffer);
            byteBuf.clear();
            this.encode(key, byteBuf);
            buffer.limit(byteBuf.writerIndex());
            return buffer;
        }
    }

    private static byte[] getBytes(ByteBuffer buffer) {
        int remaining = buffer.remaining();
        if (remaining == 0) {
            return EMPTY;
        } else {
            byte[] b = new byte[remaining];
            buffer.get(b);
            return b;
        }
    }

    static {
        UTF8 = new ByteValueCodec(LettuceCharsets.UTF8);
        ASCII = new ByteValueCodec(LettuceCharsets.ASCII);
        EMPTY = new byte[0];
    }
}
