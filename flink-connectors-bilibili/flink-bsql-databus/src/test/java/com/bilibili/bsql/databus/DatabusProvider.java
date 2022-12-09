/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2021 All Rights Reserved.
 */
package com.bilibili.bsql.databus;

import org.junit.Test;

import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
//import com.bilibili.bsql.taishan.rpc.NamingClientBootstrap;

import pleiades.component.databus.model.DatabusProperties;
import pleiades.component.databus.pub.DatabusPub;
import pleiades.component.model.DatabusClient;

/**
 *
 * @author xiaoyu
 * @version $Id: DatabusTest.java, v 0.1 2020-04-27 4:49 下午 xiaoyu Exp $$
 */
public class DatabusProvider {

	@Test
	public void produce() throws InterruptedException {
//		System.setProperty("deploy_env","uat");
//		System.setProperty("zone","sh001");
//		NamingClientBootstrap bootstrap = NamingClientBootstrap.getInstance();
//		//NamingClient client = bootstrap.getNamingClient();
//		//bootstrap.start();
//		DatabusProperties properties = new DatabusProperties();
//        /*properties.setAppkey("fbc8b9b29c26286f");
//        properties.setSecret("d92eb00d4a373cf4e1950f85b7851127");
//        properties.getPub().setGroup("PgcReviewCal-MainBangumi-P");
//        properties.getPub().setTopic("PgcReviewCal-T");*/
//
//
//		properties.setAppkey("f87a235e8d559f3c");
//		properties.setSecret("dc952bc42a8393383551d211f7892699");
//		properties.getPub().setGroup("Xiaoyu-DatacenterBigdataUat-P");
//		properties.getPub().setTopic("Xiaoyu-T");
//
//		DatabusPub pub = new DatabusPub(properties, bootstrap.getNamingClient());
//
//		DatabusClient.register();
//
//		for (int i = 0;; i++) {
//		System.out.println(i);
//		//for (int i = 0; i < 10; i++) {
//		System.out.println("java");
//		JSONObject jsontest = new JSONObject();
//		JSONObject jsontest1 = new JSONObject();
//		jsontest1.put("test-1", "java");
//		jsontest1.put("test-2", "scala");
//		jsontest.put("test", "jsontest1");
//		JSONArray jsonArray = new JSONArray();
//		jsonArray.add(jsontest);
//		jsonArray.add(jsontest1);
//		pub.pub(String.valueOf(jsonArray));
////		pub.pub("100", String.valueOf(jsonArray));
//
//            /*JSONObject jsontest1 = new JSONObject();
//            jsontest1.put("test", "scala");
//            pub.pub("200", String.valueOf(jsontest1));*/
//		Thread.sleep(100);
//		}
//
//		//}
//
//
//		//client.close();
	}
}
