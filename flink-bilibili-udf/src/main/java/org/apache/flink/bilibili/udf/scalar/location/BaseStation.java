package org.apache.flink.bilibili.udf.scalar.location;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.HashMap;
import java.util.Map;

public class BaseStation {

    /**
     * @var Reader
     */
    private Reader reader;

    public BaseStation(String path) throws IOException, InvalidDatabaseException {
        this.reader = new Reader(path);
    }

    public String[] find(String addr, String language) throws IPFormatException, InvalidDatabaseException, UnsupportedEncodingException {
        return this.reader.find(addr, language);
    }

    public Map<String, String> findMap(String addr, String language) throws IPFormatException, InvalidDatabaseException, UnsupportedEncodingException {
        String[] data = this.reader.find(addr, language);
        if (data == null) {
            return null;
        }
        Map<String, String> m = new HashMap<String, String>();

        String[] fields = this.reader.getSupportFields();

        for (int i = 0, l = data.length; i < l; i++) {
            m.put(fields[i], data[i]);
        }

        return m;
    }

    public BaseStationInfo findInfo(String addr, String language) throws IPFormatException, InvalidDatabaseException, UnsupportedEncodingException {

        String[] data = this.reader.find(addr, language);
        if (data == null) {
            return null;
        }

        return new BaseStationInfo(data);
    }

    public boolean isIPv4() {
        return (this.reader.getMeta().ip_version & 0x01) == 0x01;
    }

    public boolean isIPv6() {
        return (this.reader.getMeta().ip_version & 0x02) == 0x02;
    }

    public String[] fields() {
        return this.reader.getSupportFields();
    }

    public int buildTime() {
        return this.reader.getBuildUTCTime();
    }
}
