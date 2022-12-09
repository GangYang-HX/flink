package com.bilibili.bsql.util;

import com.bilibili.bsql.kafka.util.KafkaMetadataParser;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.common.header.Header;
import org.apache.kafka.common.header.internals.RecordHeaders;
import org.junit.Test;

import java.util.Map;

/**
 * @Description
 * @Author weizefeng
 * @Date 2022/2/15 21:58
 **/
@Slf4j
public class KafkaMetadataParserTest {
    @Test
    public void nullHeaderTest(){
        RecordHeaders headers = new RecordHeaders((Iterable<Header>) null);
        Map<String, String> map = KafkaMetadataParser.parseKafkaHeader(headers);
    }

}
