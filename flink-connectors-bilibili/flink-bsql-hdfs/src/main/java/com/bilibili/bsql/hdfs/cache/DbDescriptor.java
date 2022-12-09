package com.bilibili.bsql.hdfs.cache;

import com.bilibili.bsql.common.TableInfo;

import java.util.Iterator;

public interface DbDescriptor {

	public boolean loadData2Db(String path) throws Exception;

	public void put(String key, String value) throws Exception;

	public byte[] get(String key) throws Exception;

	public void close() throws Exception;

	Iterator newIterator();

	boolean loadData2Db(String path, TableInfo tableInfo) throws Exception;
}
