package com.bilibili.bsql.hive.tableinfo;

import com.alibaba.fastjson.JSON;
import lombok.Data;
import org.apache.http.HttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.util.EntityUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URI;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by haozhugogo on 2020/4/28.
 */
public class HiveRewriter {
    private static final String HIVE_META_DATA_URL = "http://berserker.bilibili.co/api/hive/meta/%s/%s";
    private static final String HIVE_DATA_URL = "http://berserker.bilibili.co/api/hive/data/%s/%s";
    private static final Logger logger = LoggerFactory.getLogger(HiveRewriter.class);
    private static final String SPLIT_INFO_STR = "^字段分隔符: ([\\s\\S]*?); 行分隔符: ([\\s\\S]*?)$";
    private final static Pattern SPLIT_INFO_PATTERN = Pattern.compile(SPLIT_INFO_STR);

    public static Map<String, String> rewrite(Map<String, String> props) throws Exception {
        String[] tableName = props.get("tableName").split("\\.");
		return rewrite(props, tableName[0], tableName[1]);

    }

	public static Map<String, String> rewrite(Map<String, String> props, String databaseName, String tableName) throws Exception {
		HiveMetaDetail hiveMetaDetail = getMetaData(databaseName, tableName);
		props.put("location", hiveMetaDetail.getLocation());
		props.put("partitionKey", hiveMetaDetail.getPartitionKey());
		String splitInfo = hiveMetaDetail.getSplitInfo();
		Matcher matcher = SPLIT_INFO_PATTERN.matcher(splitInfo);
		if (matcher.find()) {
			props.put("fieldDelim", decodeEscape(matcher.group(1)));
			props.put("rowDelim", decodeEscape(matcher.group(2)));
		}
		props.put("owner", hiveMetaDetail.getOwner());
        //lancer bsql which contains traceId will not overwrite by hms,and so is the hmsOverwirte conf.
        boolean is_hms_overwrite = !(props.containsKey("traceId") || props.containsKey("hmsOverwrite"));
        if ("OrcSerde".equals(hiveMetaDetail.getStoreName())) {
            if (is_hms_overwrite) {
                props.put("format", "orc");
                if (hiveMetaDetail.getStates().containsKey("orc.compress")) {
                    props.put("compress", hiveMetaDetail.getStates().get("orc.compress"));
                }
            }
        } else if ("LazySimpleSerDe".equals(hiveMetaDetail.getStoreName())) {
            if (is_hms_overwrite) {
                props.put("format", "text");
            }
        } else if ("ParquetHiveSerDe".equals(hiveMetaDetail.getStoreName())) {
			props.put("format", "parquet");
		} else {
			props.put("format", "text");
		}
		return props;

	}

    public static HiveMetaDetail getMetaData(String db, String table) throws Exception {
        try {
            CloseableHttpClient httpClient = HttpClients.createDefault();
            HttpGet httpGet = new HttpGet();
            httpGet.setURI(new URI(String.format(HIVE_META_DATA_URL, db, table)));
            HttpResponse httpResponse = httpClient.execute(httpGet);
            String responseString = EntityUtils.toString(httpResponse.getEntity(), "UTF-8");
            HiveMetaDetail hiveMetaDetail = JSON.parseObject(responseString, HiveMetaDetail.class);
            if (hiveMetaDetail.location == null) {
                throw new RuntimeException("找不到hive表:" + table + " 信息,请确认表已经创建");
            }
            httpGet = new HttpGet();
            httpGet.setURI(new URI(String.format(HIVE_DATA_URL, db, table)));
            HttpResponse dataHttpResponse = httpClient.execute(httpGet);
            String dataResponseString = EntityUtils.toString(dataHttpResponse.getEntity(), "UTF-8");
            HiveDataDetail hiveDataDetail = JSON.parseObject(dataResponseString, HiveDataDetail.class);
            hiveMetaDetail.setStoreName(hiveDataDetail.getStoreName());

            httpClient.close();
            return hiveMetaDetail;
        } catch (Exception e) {
            logger.error("Get hive meta data error", e);
            throw e;
        }
    }

    @Data
    public static class HiveMetaDetail {

        private String database;
        private String tableType;
        private String table;
        private String location;
        private String partitionKey;
        private String latestPartition;
        private String earliestPartition;
        private Map<String, String> states;
        private List<HiveMetaColumn> columns;
        private String splitInfo;
        private String owner;
        private String storeName;
    }

    @Data
    public static class HiveDataDetail {

        private String storeName; // hive表存储格式
    }

    @Data
    public static class HiveMetaColumn {

        private int index;
        private String column;
        private String columnType;
        private String columnDesc;
    }

    /**
     * 将前端展示的分隔符转换成程序中的分隔符
     *
     * @param s
     * @return
     */
    private static String decodeEscape(String s) {
        return s.replaceAll("\\\\t", "\t")
                .replaceAll("\\\\r", "\r")
                .replaceAll("\\\\n", "\n")
                .replaceAll("\\\\u0001", "\u0001");
    }
}
