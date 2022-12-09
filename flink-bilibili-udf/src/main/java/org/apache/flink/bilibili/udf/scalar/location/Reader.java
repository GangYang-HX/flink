package org.apache.flink.bilibili.udf.scalar.location;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.PropertyNamingStrategy;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sun.net.util.IPAddressUtil;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.Arrays;


public class Reader {

    private static final Logger LOG = LoggerFactory.getLogger(Reader.class);

    private int fileSize;
    private int nodeCount;

    private MetaData meta;
    private byte[] data;

    private int v4offset;


    public Reader(String path) throws IOException {

        Configuration hadoopConf = new Configuration();

        String hadoopConfDir = "/etc/second-hadoop";

        if (!hadoopConfDir.endsWith("/")) {
            hadoopConfDir = hadoopConfDir + "/";
        }

        hadoopConf.addResource(new Path(hadoopConfDir + "hdfs-site.xml"));
        hadoopConf.addResource(new Path(hadoopConfDir + "core-site.xml"));
        hadoopConf.addResource(new Path(hadoopConfDir + "mount-table.xml"));

        FileSystem fs = null;
        FSDataInputStream in = null;
        ByteArrayOutputStream out = new ByteArrayOutputStream();

        try {
            fs = FileSystem.get(hadoopConf);
            FileStatus status = fs.getFileStatus(new Path(path));
            this.fileSize = new Long(status.getLen()).intValue();
            in = fs.open(status.getPath());
            byte[] buf = new byte[1024];
            int length;
            while ((length = in.read(buf)) != -1) {
                out.write(buf, 0, length);
            }
        } catch (Exception e) {
            LOG.error("read file exception", e);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
        }

        this.data = out.toByteArray();

        long metaLength = bytesToLong(
            this.data[0],
            this.data[1],
            this.data[2],
            this.data[3]
        );

        byte[] metaBytes = Arrays.copyOfRange(this.data, 4, Long.valueOf(metaLength).intValue() + 4);

        ObjectMapper mapper = new ObjectMapper();
        mapper.setPropertyNamingStrategy(PropertyNamingStrategy.CAMEL_CASE_TO_LOWER_CASE_WITH_UNDERSCORES);

        MetaData meta = mapper.readValue(new String(metaBytes), MetaData.class);
        this.nodeCount = meta.node_count;
        this.meta = meta;
        if ((meta.total_size + Long.valueOf(metaLength).intValue() + 4) != this.data.length) {
            throw new InvalidDatabaseException("database file size error");
        }

        this.data = Arrays.copyOfRange(this.data, Long.valueOf(metaLength).intValue() + 4, this.fileSize);

        if (this.v4offset == 0) {
            int node = 0;
            for (int i = 0; i < 96 && node < this.nodeCount; i++) {
                if (i >= 80) {
                    node = this.readNode(node, 1);
                } else {
                    node = this.readNode(node, 0);
                }
            }

            this.v4offset = node;
        }
    }

    public String[] find(String addr, String language) throws IPFormatException, InvalidDatabaseException, UnsupportedEncodingException {

        int off;
        try {
            off = this.meta.languages.get(language);
        } catch (NullPointerException e) {
            return null;
        }

        byte[] ipv;

        if (addr.indexOf(":") > 0) {
            ipv = IPAddressUtil.textToNumericFormatV6(addr);
            if (ipv == null) {
                throw new IPFormatException("ipv6 format error");
            }
            if ((this.meta.ip_version & 0x02) != 0x02){
                throw new IPFormatException("no support ipv6");
            }

        } else if (addr.indexOf(".") > 0) {
            ipv = IPAddressUtil.textToNumericFormatV4(addr);
            if (ipv == null) {
                throw new IPFormatException("ipv4 format error");
            }
            if ((this.meta.ip_version & 0x01) != 0x01){
                throw new IPFormatException("no support ipv4");
            }
        } else {
            throw new IPFormatException("ip format error");
        }

        int node = 0;
        try {
            node = this.findNode(ipv);
        } catch (NotFoundException nfe) {
            return null;
        }

        final String data = this.resolve(node);

        return Arrays.copyOfRange(data.split("\t", this.meta.fields.length * this.meta.languages.size()), off, off+this.meta.fields.length);
    }

    private int findNode(byte[] binary) throws NotFoundException {

        int node = 0;

        final int bit = binary.length * 8;

        if (bit == 32) {
            node = this.v4offset;
        }

        for (int i = 0; i < bit; i++) {
            if (node > this.nodeCount) {
                break;
            }

            node = this.readNode(node, 1 & ((0xFF & binary[i / 8]) >> 7 - (i % 8)));
        }

        if (node == this.nodeCount) {
            return 0;
        } else if (node > this.nodeCount) {
            return node;
        }

        throw new NotFoundException("ip not found");
    }

    private String resolve(int node) throws InvalidDatabaseException, UnsupportedEncodingException {
        final int resoloved = node - this.nodeCount + this.nodeCount * 8;
        if (resoloved >= this.fileSize) {
            throw new InvalidDatabaseException("database resolve error");
        }

        byte b = 0;
        int size = Long.valueOf(bytesToLong(
            b,
            b,
            this.data[resoloved],
            this.data[resoloved+1]
        )).intValue();

        if (this.data.length < (resoloved + 2 + size)) {
            throw new InvalidDatabaseException("database resolve error");
        }

        return new String(this.data, resoloved + 2, size, "UTF-8");
    }

    private int readNode(int node, int index) {
        int off = node * 8 + index * 4;

        return Long.valueOf(bytesToLong(
            this.data[off],
            this.data[off+1],
            this.data[off+2],
            this.data[off+3]
        )).intValue();
    }

    private static long bytesToLong(byte a, byte b, byte c, byte d) {
        return int2long((((a & 0xff) << 24) | ((b & 0xff) << 16) | ((c & 0xff) << 8) | (d & 0xff)));
    }

    private static long int2long(int i) {
        long l = i & 0x7fffffffL;
        if (i < 0) {
            l |= 0x080000000L;
        }
        return l;
    }

    public MetaData getMeta() {
        return this.meta;
    }

    public int getBuildUTCTime() {
        return this.meta.build;
    }

    public String[] getSupportFields() {
        return this.meta.fields;
    }
}
