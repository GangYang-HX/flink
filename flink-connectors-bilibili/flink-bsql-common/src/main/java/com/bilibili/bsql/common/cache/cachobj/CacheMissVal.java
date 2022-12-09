/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

 

package com.bilibili.bsql.common.cache.cachobj;


import org.apache.flink.table.data.RowData;

/**
 * Only the data marked to dimension table miss
 * Date: 2018/8/28
 */
public class CacheMissVal extends CacheObj<RowData> {

    private static CacheObj missObj = new CacheMissVal(null);

    public static CacheObj getMissKeyObj(){
        return missObj;
    }

    public CacheMissVal(RowData content) {
    	super(content);
	}

	public CacheContentType getType() {
    	return CacheContentType.MissVal;
	}

	public RowData getContent() {
    	return null;
	}

	public void setContent(RowData content) {
		throw new UnsupportedOperationException();
	}
}
