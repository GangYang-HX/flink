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

package com.bilibili.bsql.hive.filetype.compress.writer.extractor;

import org.apache.flink.types.Row;

import java.nio.charset.Charset;
import java.util.List;
import org.apache.commons.compress.utils.Lists;

/**
 * A {@link Extractor} implementation that extracts element to string with fieldDelim.
 *
 */
public class DefaultExtractor implements Extractor<Row> {

	private String fieldDelim;
	private int eventTimePos;
	private boolean timeFieldVirtual;

	public DefaultExtractor( String fieldDelim, int eventTimePos, boolean timeFieldVirtual) {
		this.fieldDelim = fieldDelim;
		this.eventTimePos = eventTimePos;
		this.timeFieldVirtual = timeFieldVirtual;
	}

	@Override
	public byte[] extract(Row row) {
		List<byte[]> bytesList = Lists.newArrayList();
		int totalLength = 0;

		Charset charset = Charset.forName("UTF-8");
		byte[] fieldDelimBytes = fieldDelim.getBytes(charset);
		for (int i = 0; i < row.getArity(); i++) {
			if (timeFieldVirtual && i == eventTimePos) {
				continue;
			}

			Object field = row.getField(i);
			byte[] fieldBytes = field instanceof byte[] ? (byte[]) field : field.toString().getBytes(charset);
			bytesList.add(fieldBytes);
			totalLength += fieldBytes.length;

			//add fieldDelim only before the last physical field
			if (timeFieldVirtual && eventTimePos == row.getArity() - 1) {
				//eg.[physicalField1, physicalField2, physicalField3, virtualField] only add fieldDelim before physicalField3.
				if (i < row.getArity() - 2) {
					bytesList.add(fieldDelimBytes);
					totalLength += fieldDelimBytes.length;
				}
			} else if (i < row.getArity() - 1) {
				//eg.[physicalField1, physicalField2, virtualField, physicalField3] cause virtualField will be skiped, add fieldDelim normally.
				bytesList.add(fieldDelimBytes);
				totalLength += fieldDelimBytes.length;
			}
		}

		byte[] bytes = new byte[totalLength];
		int index = 0;
		for (byte[] item : bytesList) {
			 System.arraycopy(item, 0, bytes, index, item.length);
			 index += item.length;
		}

		return bytes;
	}

}
