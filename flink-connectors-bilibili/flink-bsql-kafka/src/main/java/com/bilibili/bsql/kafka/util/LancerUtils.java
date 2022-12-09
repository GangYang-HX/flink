package com.bilibili.bsql.kafka.util;

import com.bilibili.recordio.IoBuf;
import com.bilibili.recordio.Record;
import com.bilibili.recordio.RecordReader;
import org.apache.commons.compress.utils.Lists;
import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.channels.Channels;
import java.nio.charset.StandardCharsets;
import java.time.Clock;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.zip.GZIPInputStream;

/** Lancer parse util. */
public class LancerUtils {

    private static final Logger LOG = LoggerFactory.getLogger(LancerUtils.class);
    public static final int LOG_REPORT_PROTOCOL_LENGTH = 19;

    public static final String DATA_REAL_IP = "DATA-REAL-IP";
    public static final String SLB_IP = "SLB_IP";

    public static final String BUVID3 = "buvid3";

    public static final String DEDE_USER_ID = "DedeUserID";

    public static final String VERTICAL_LINE_TRANSFORM = "%7C";

    public static final String LINE_THROUGH = "-";

    /** bfe version. */
    public static final String LANCER_PROTOCOL_VERSION = "lancer-protocol-version";
    /** Multiple aggregated version. */
    public static final String VERSION_2_1 = "2.1";

    /** 2.1 version config. */
    public static final String LANCER_PAYLOAD_COMPRESS = "lancer-payload-compress";

    public static final String VERTICAL_LINE = "|";

    public static final String SPLIT = ",";

    public static final String VERSION = "version";

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
        System.arraycopy(
                res,
                LOG_REPORT_PROTOCOL_LENGTH,
                realData,
                0,
                res.length - LOG_REPORT_PROTOCOL_LENGTH);
        return realData;
    }

    private byte[] getNewBody(byte[] body, String commonField) {
        if (body.length < LOG_REPORT_PROTOCOL_LENGTH) {
            LOG.info("content length is illegal,less than: {}", LOG_REPORT_PROTOCOL_LENGTH);
            return null;
        }
        if (commonField != null && !commonField.isEmpty()) {
            byte[] commonFiledByte = commonField.getBytes();
            byte[] newBody =
                    new byte[body.length - LOG_REPORT_PROTOCOL_LENGTH + commonFiledByte.length];
            System.arraycopy(commonFiledByte, 0, newBody, 0, commonFiledByte.length);
            System.arraycopy(
                    body,
                    LOG_REPORT_PROTOCOL_LENGTH,
                    newBody,
                    commonFiledByte.length,
                    body.length - LOG_REPORT_PROTOCOL_LENGTH);

            return newBody;
        } else {
            byte[] realData = new byte[body.length - LOG_REPORT_PROTOCOL_LENGTH];
            System.arraycopy(
                    body,
                    LOG_REPORT_PROTOCOL_LENGTH,
                    realData,
                    0,
                    body.length - LOG_REPORT_PROTOCOL_LENGTH);
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
        if (param == null) {
            return defaultValue;
        }
        if (param.contains(VERTICAL_LINE)) {
            param = StringUtils.replace(param, VERTICAL_LINE, VERTICAL_LINE_TRANSFORM);
        }
        return param;
    }

    private static byte[] decode(byte[] content, String codec) {
        byte[] result = null;
        if (StringUtils.isEmpty(codec)) {
            result = content;
        } else if ("gzip".equals(codec)) {
            result = uncompress(content);
        } else {
            LOG.warn("unsupported compress code: {}", codec);
        }
        return result;
    }

    /**
     * GZIP ucompress.
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
            LOG.error("gzip uncompress error:", e);
        }
        return null;
    }

    /**
     * bytes transform to record list.
     *
     * @param content
     * @return
     */
    private static List<Record> parseRecords(byte[] content) {
        if (content == null) {
            return Lists.newArrayList();
        }
        RecordReader recordReader =
                new RecordReader(Channels.newChannel(new ByteArrayInputStream(content)));
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

    public static List<ConsumerRecord<byte[], byte[]>> splitConsumerRecord(
            ConsumerRecord<byte[], byte[]> compressRecord) {
        List<ConsumerRecord<byte[], byte[]>> result = new ArrayList<>();
        Map<String, byte[]> header =
                KafkaMetadataParser.parseHeaderWithBytes(compressRecord.headers());
        String protocolVersion = bytesToStr(header.get(LANCER_PROTOCOL_VERSION), null);
        String url = bytesToStr(header.get(ORIGIN_REQUEST_PATH), null);
        String logId = bytesToStr(header.get(LOG_ID), null);
        long current = Clock.systemUTC().millis();
        long utime = bytesToLong(header.get(UPDATE_TIME), current);
        long ctime = bytesToLong(header.get(CREATE_TIME), current);
        byte[] body = compressRecord.value();
        // parse eventBody
        if (VERSION_2_1.equals(protocolVersion)) {
            // uncompress
            byte[] bytes = decode(body, bytesToStr(header.get(LANCER_PAYLOAD_COMPRESS), ""));
            // parse recordio
            List<Record> recordios = parseRecords(bytes);
            for (Record record : recordios) {
                // put meta into header from recordIo
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
                if (VERSION_1_0.equals(bytesToStr(header.get(VERSION), ""))) {
                    recordBody = takeOutCommonHeader(recordBody);
                }
                url = url == null ? bytesToStr(header.get(ORIGIN_REQUEST_PATH), "") : url;
                logId = logId == null ? bytesToStr(header.get(LOG_ID), null) : logId;
                result.add(
                        new ConsumerRecord<byte[], byte[]>(
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
                                generateLancerKafkaHeader(header)));
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
