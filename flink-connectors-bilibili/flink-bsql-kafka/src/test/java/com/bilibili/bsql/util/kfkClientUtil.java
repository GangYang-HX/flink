/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2019 All Rights Reserved.
 */
package com.bilibili.bsql.util;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.net.URL;
import java.net.URLClassLoader;
import java.nio.channels.Channels;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.commons.compress.utils.Lists;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.PartitionInfo;
import org.junit.Test;


/**
 *
 * @author zhouxiaogang
 * @version $Id: kfkClient.java, v 0.1 2019-08-26 11:20
zhouxiaogang Exp $$
 */
public class kfkClientUtil {

	@Test
    public void generateStaff() throws Exception{

        Properties props = new Properties();
//        props.put("bootstrap.servers", "172.22.33.99:9092,172.22.33.97:9092");
        props.put("bootstrap.servers", "172.22.33.94:9092,172.22.33.99:9092,172.22.33.97:9092");
        props.put("acks", "all");
        props.put("retries", 0);
        props.put("batch.size", 16384);
        props.put("linger.ms", 1);
        props.put("buffer.memory", 33554432);
        props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
        //生产者发送消息
        String topic = "zzs-source";
        Producer<String, MyTable> procuder = new KafkaProducer<String,MyTable>(props);
        long standardTime = System.currentTimeMillis();
        System.err.println(standardTime);
        for (int i = 0; i <= 2; i++) {
            int j = i%6;
            MyTable table = new MyTable();

            table.setName("name1");
            table.setNum((long)i);
            table.setXtime(new Timestamp(standardTime + i*50));


            String value = table.hivelize();
            ProducerRecord<String, MyTable> msg = new ProducerRecord(topic, value);
            procuder.send(msg);
            Thread.sleep(100);
        }
        //列出topic的相关信息
        List<PartitionInfo> partitions = new ArrayList<PartitionInfo>() ;
        System.out.println("send message over.");
        procuder.close(1000, TimeUnit.MILLISECONDS);
    }

	@Test
	public void lancerKakaTest() throws Exception{

		Properties props = new Properties();
//        props.put("bootstrap.servers", "172.22.33.99:9092,172.22.33.97:9092");
		props.put("bootstrap.servers", "10.221.51.174:9092,10.221.50.145:9092,10.221.50.131:9092");
		props.put("acks", "all");
		props.put("retries", 0);
		props.put("batch.size", 16384);
		props.put("linger.ms", 1);
		props.put("buffer.memory", 33554432);
		props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		props.put("value.serializer", "org.apache.kafka.common.serialization.ByteArraySerializer");
		//生产者发送消息
		String topic = "zzj_test";
		Producer<String, MyTable> procuder = new KafkaProducer<String,MyTable>(props);
		long standardTime = System.currentTimeMillis();
		System.err.println(standardTime);
		for (int i = 0; i <= 2; i++) {
			int j = i%6;
			MyTable table = new MyTable();

			table.setName("name1");
			table.setNum((long)i);
			table.setXtime(new Timestamp(standardTime + i*50));


			String value = table.hivelize();
			ProducerRecord<String, MyTable> msg = new ProducerRecord(topic, value.getBytes());
			procuder.send(msg);
			Thread.sleep(100);
		}
		//列出topic的相关信息
		List<PartitionInfo> partitions = new ArrayList<PartitionInfo>() ;
		System.out.println("send message over.");
		procuder.close(1000, TimeUnit.MILLISECONDS);
	}




	public static class MyTable{
        public String name;
        public Long num;
        public Timestamp xtime;

        public String getName() {
            return name;
        }

        public void setName(String name) {
            this.name = name;
        }

        public Long getNum() {
            return num;
        }

        public void setNum(Long num) {
            this.num = num;
        }

        public Timestamp getXtime() {
            return xtime;
        }

        public void setXtime(Timestamp xtime) {
            this.xtime = xtime;
        }

        public String hivelize(){
            StringBuilder stringBuilder = new StringBuilder();
            stringBuilder.append(name);
            stringBuilder.append("\u0001");
            stringBuilder.append(num);
            stringBuilder.append("\u0001");
            stringBuilder.append(xtime);
            System.err.println(stringBuilder.toString());

            return stringBuilder.toString();
        }

        public String toString() {
        	return String.format("%s,%s,%s", name, num, xtime);
		}
    }



}
