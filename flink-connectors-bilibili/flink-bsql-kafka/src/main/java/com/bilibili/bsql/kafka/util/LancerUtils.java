package com.bilibili.bsql.kafka.util;

import com.bilibili.recordio.IoBuf;
import com.bilibili.recordio.RecordReader;
import io.netty.handler.codec.http.cookie.Cookie;
import io.netty.handler.codec.http.cookie.ServerCookieDecoder;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.*;
import java.util.zip.GZIPInputStream;

import com.bilibili.recordio.Record;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;

/**
 * @Author weizefeng
 * @Date 2022/3/29 16:20
 **/
@Slf4j
public class LancerUtils {
    public static final int LOG_REPORT_PROTOCOL_LENGTH = 19;

    public static final String DATA_REAL_IP = "DATA-REAL-IP";
    public static final String SLB_IP = "SLB_IP";

    public static final String BUVID3 = "buvid3";

    public static final String DEDE_USER_ID = "DedeUserID";

    public final static String VERTICAL_LINE_TRANSFORM = "%7C";

    public final static String LINE_THROUGH = "-";

    /**
     * bfe 版本号
     */
    public static final String LANCER_PROTOCOL_VERSION = "lancer-protocol-version";
    /**
     * 多条聚合版本
     */
    public static final String VERSION_2_1 = "2.1";

    /**
     * 2.1版本带的配置
     */
    public static final String LANCER_PAYLOAD_COMPRESS = "lancer-payload-compress";


    public final static String VERTICAL_LINE = "|";

    public final static String SPLIT = ",";

    public final static String VERSION = "version";

    public static final String VERSION_1_0 = "1.0";
    public static final String VERSION_1_1 = "1.1";
    public static final String LOG_ID = "logId";
    public static final String ORIGIN_REQUEST_PATH = "request-path";
    public static final String CREATE_TIME = "ctime";
    public static final String REQUEST_TIME = "request_time";
    public static final String UPDATE_TIME = "update_time";

    private static byte[] jointCommonFiled(byte[] res, String commonField) {
        byte[] commonFiledByte = commonField.getBytes();
        byte[] newBody = new byte[res.length + commonFiledByte.length];
        System.arraycopy(commonFiledByte, 0, newBody, 0, commonFiledByte.length);
        System.arraycopy(res, 0, newBody, commonFiledByte.length, res.length);
        return newBody;
    }

    private static byte[] takeOutCommonHeader(byte[] res) {
        byte[] realData = new byte[res.length - LOG_REPORT_PROTOCOL_LENGTH];
        System.arraycopy(res, LOG_REPORT_PROTOCOL_LENGTH, realData, 0, res.length - LOG_REPORT_PROTOCOL_LENGTH);
        return realData;
    }

    private String getExtraField(Map<String, byte[]> header, String strs) {
        StringBuilder extraField = new StringBuilder();
        String[] splitStr = strs.split(SPLIT);
        for (String str : splitStr) {
            String split;
            switch (str) {
                case "user-agent":
                    split = replaceWithDefault(bytesToStr(header.get("user-agent"), ""), LINE_THROUGH);
                    break;
                case "referer":
                    split = replaceWithDefault(bytesToStr(header.get("referer"), ""), LINE_THROUGH);
                    break;
                case "DedeUserID":
                    String cookieStr = bytesToStr(header.get("cookie"), "");
                    Set<Cookie> cookies = ServerCookieDecoder.LAX.decode(cookieStr);
                    String dedeUserID = getCookieByName(cookies, DEDE_USER_ID);
                    split = (dedeUserID == null) ? replaceWithDefault(bytesToStr(header.get(DEDE_USER_ID), ""), LINE_THROUGH) : dedeUserID;
                    break;
                case "buvid3":
                    String cookie = bytesToStr(header.get("cookie"), "");
                    Set<Cookie> cookieLists = ServerCookieDecoder.LAX.decode(cookie);
                    String buvid3 = getCookieByName(cookieLists, BUVID3);
                    split = (buvid3 == null) ? replaceWithDefault(bytesToStr(header.get(BUVID3), ""), LINE_THROUGH) : buvid3;
                    break;
                case "DATA-REAL-IP":
                    String ip = bytesToStr(header.get(DATA_REAL_IP), "");
                    if (StringUtils.isEmpty(ip)) {
                        ip = bytesToStr(header.get(SLB_IP), "");
                    }
                    split = ip;
                    break;
                default:
                    throw new RuntimeException("Unsupported fields " + str);
            }
            extraField.append(split).append(VERTICAL_LINE);
        }
        return extraField.toString();
    }

    private byte[] getNewBody(byte[] body, String commonField) {
        if (body.length < LOG_REPORT_PROTOCOL_LENGTH) {
            log.info("content length is illegal,less than: {}", LOG_REPORT_PROTOCOL_LENGTH);
            return null;
        }
        if (commonField != null && !commonField.isEmpty()) {
            byte[] commonFiledByte = commonField.getBytes();
            byte[] newBody = new byte[body.length - LOG_REPORT_PROTOCOL_LENGTH + commonFiledByte.length];
            System.arraycopy(commonFiledByte, 0, newBody, 0, commonFiledByte.length);
            System.arraycopy(body, LOG_REPORT_PROTOCOL_LENGTH, newBody, commonFiledByte.length,
                body.length - LOG_REPORT_PROTOCOL_LENGTH);

            return newBody;
        } else {
            byte[] realData = new byte[body.length - LOG_REPORT_PROTOCOL_LENGTH];
            System.arraycopy(body, LOG_REPORT_PROTOCOL_LENGTH, realData, 0, body.length - LOG_REPORT_PROTOCOL_LENGTH);
            return realData;
        }

    }

    public static String bytesToStr(byte[] bytes, String def) {
        if (bytes == null) {
            return def;
        } else {
            return new String(bytes);
        }
    }

    public static long bytesToLong(byte[] bytes, long def) {
        if (bytes == null) {
            return def;
        } else {
            String s = new String(bytes);
            return Long.parseLong(s);
        }
    }

    private static String replaceWithDefault(String param, String defaultValue) {
        // 如果为null则返回默认值.
        if (param == null) {
            return defaultValue;
        }
        // 如果存在竖杠,则替换为
        if (param.contains(VERTICAL_LINE)) {
            param = StringUtils.replace(param, VERTICAL_LINE, VERTICAL_LINE_TRANSFORM);
        }
        return param;
    }

    private static String getCookieByName(Set<Cookie> cookies, String cookieName) {
        for (Cookie cookie : cookies) {
            if (cookieName.equals(cookie.name())) {
                return StringUtils.replace(cookie.value(), "|", "%7C");
            }
        }
        return null;
    }

    private static byte[] decode(byte[] content, String codec) {
        byte[] result = null;
        if (StringUtils.isEmpty(codec)) {
            result = content;
        } else if ("gzip".equals(codec)) {
            result = uncompress(content);
        } else {
            log.warn("unsupported compress code: {}", codec);
        }
        return result;
    }

    /**
     * GZIP解压缩
     *
     * @param bytes
     * @return
     */
    public static byte[] uncompress(byte[] bytes) {
        if (bytes == null || bytes.length == 0) {
            return null;
        }
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        ByteArrayInputStream in = new ByteArrayInputStream(bytes);

        try (GZIPInputStream gzipInputStream = new GZIPInputStream(in)) {
            byte[] buffer = new byte[1024];
            int n;
            while ((n = gzipInputStream.read(buffer)) >= 0) {
                out.write(buffer, 0, n);
            }
            return out.toByteArray();
        } catch (Exception e) {
            log.error("gzip uncompress error:", e);
        }
        return null;
    }

    /**
     * bytes 转 List<Record>
     *
     * @param content
     * @return
     */
    private static List<Record> parseRecords(byte[] content) {
        if (content == null) {
            return Lists.newArrayList();
        }
        RecordReader recordReader = new RecordReader(
            Channels.newChannel(new ByteArrayInputStream(content)));
        List<Record> recordios = new LinkedList<>();
        while (true) {
            try {
                Record recordio = recordReader.read();
                if (recordio != null) {
                    recordios.add(recordio);
                }
            } catch (IOException ioException) {
                break;
            }
        }
        return recordios;
    }

    public static List<ConsumerRecord<byte[], byte[]>> splitConsumerRecord(ConsumerRecord<byte[], byte[]> compressRecord) {
        List<ConsumerRecord<byte[], byte[]>> result = new ArrayList<>();
        Map<String, byte[]> header = KafkaMetadataParser.parseHeaderWithBytes(compressRecord.headers());
        String protocolVersion = bytesToStr(header.get(LANCER_PROTOCOL_VERSION), null);
        String url = bytesToStr(header.get(ORIGIN_REQUEST_PATH), null);
        String logId = bytesToStr(header.get(LOG_ID), null);
        long current = Clock.systemUTC().millis();
        long utime = bytesToLong(header.get(UPDATE_TIME), current);
        long ctime = bytesToLong(header.get(CREATE_TIME), current);
        byte[] body = compressRecord.value();
        //parse eventBody，解析完成后得到list，for循环
        if (VERSION_2_1.equals(protocolVersion)) {
            //解压
            byte[] bytes = decode(body, bytesToStr(header.get(LANCER_PAYLOAD_COMPRESS), ""));
            //解recordio
            List<Record> recordios = parseRecords(bytes);
            for (Record record : recordios) {
                // 把recordIo的meta全部塞进header里面
                if (record.hasMeta()) {
                    for (String key : record.metaNameIterable()) {
                        IoBuf ioBuf = record.getMeta(key);
                        byte[] byteValue = ioBuf.peekBytes(ioBuf.calculateBytes());
                        header.put(key, byteValue);
                    }
                }
                header.put("update_time", String.valueOf(utime).getBytes(StandardCharsets.UTF_8));
                IoBuf payload = record.getPayload();
                byte[] recordBody = payload.peekBytes(payload.calculateBytes());
                if (VERSION_1_0.equals(bytesToStr(header.get(VERSION), "")))
                    recordBody = takeOutCommonHeader(recordBody);//截取公共头部
                url = url == null ? bytesToStr(header.get(ORIGIN_REQUEST_PATH), "") : url;
                logId = logId == null ? bytesToStr(header.get(LOG_ID), null) : logId;
                result.add(new ConsumerRecord<byte[], byte[]>(
                    compressRecord.topic(),
                    compressRecord.partition(),
                    compressRecord.offset(),
                    compressRecord.timestamp(),
                    compressRecord.timestampType(),
                    compressRecord.checksum(),
                    compressRecord.serializedKeySize(),
                    compressRecord.serializedValueSize(),
                    compressRecord.key(),
                    recordBody,
                    generateLancerKafkaHeader(header)
                ));
            }
        } else {
            result.add(compressRecord);
        }
        return result;
    }

    private static RecordHeaders generateLancerKafkaHeader(Map<String, byte[]> oldHeader) {
        RecordHeaders results = new RecordHeaders();
        oldHeader.forEach(results::add);
        return results;
    }

}
