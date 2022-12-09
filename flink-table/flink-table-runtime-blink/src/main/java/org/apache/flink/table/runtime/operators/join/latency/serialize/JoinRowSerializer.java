package org.apache.flink.table.runtime.operators.join.latency.serialize;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;

import org.apache.commons.lang3.StringUtils;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.operators.join.latency.cache.JoinRow;
import org.apache.flink.table.runtime.operators.join.latency.util.BytesUtil;
import org.apache.flink.table.runtime.operators.join.latency.util.SymbolsConstant;
import org.apache.flink.table.runtime.typeutils.RowDataSerializer;
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo;

import static org.apache.flink.table.runtime.operators.join.latency.cache.JoinRow.*;


/**
 * 只序列化必要的字段,row和timestamp,其他字段丢弃
 * 
 * @author zhangyang
 * @Date:2019/10/29
 * @Time:11:07 AM
 */
public class JoinRowSerializer extends SerializeService<JoinRow> {

	HashMap<String, RowDataSerializer> rowDataSerializerHashMap = new HashMap<>();
	DataOutputSerializer dataOutputSerializer = new DataOutputSerializer(128);

    public JoinRowSerializer(MetricGroup metricGroup,
							 RowDataTypeInfo leftType,
							 RowDataTypeInfo rightType,
							 RowDataTypeInfo outputType,
							 ExecutionConfig conf) {

        super(metricGroup);
        rowDataSerializerHashMap.put(LEFT_INPUT, new RowDataSerializer(conf, leftType.toRowType()));
        rowDataSerializerHashMap.put(RIGHT_INPUT, new RowDataSerializer(conf, rightType.toRowType()));
        rowDataSerializerHashMap.put(OUTPUT, new RowDataSerializer(conf, outputType.toRowType()));
    }

    @Override
    public byte[] doSerialize(JoinRow row) throws IOException {
        String header = row.getJoined() + SymbolsConstant._U0001 + row.getTimestamp() + SymbolsConstant._U0001
                        + row.getCondition() + SymbolsConstant._U0001 + row.getTableName();
        byte[] headerBytes = header.getBytes(StandardCharsets.UTF_8);
        int headerLength = headerBytes.length;

		dataOutputSerializer.clear();
		RowDataSerializer serializer = rowDataSerializerHashMap.get(row.getTableName());
		serializer.serialize(row.getRow(), dataOutputSerializer);

		byte[] fieldsBytes = dataOutputSerializer.getCopyOfBuffer();
		int fieldsLength = fieldsBytes.length;

        int bytesLength = headerLength + fieldsLength + 4;
        byte[] bytes = new byte[bytesLength];

        ByteBuffer buffer = ByteBuffer.allocate(4);
        buffer.putInt(0, headerLength);
        System.arraycopy(buffer.array(), 0, bytes, 0, 4);
        System.arraycopy(headerBytes, 0, bytes, 4, headerLength);
        System.arraycopy(fieldsBytes, 0, bytes, 4 + headerLength, fieldsLength);

        return bytes;
    }

    @Override
    public JoinRow doDeserialize(byte[] bytes) throws IOException {
        byte[] headerLengthBytes = new byte[4];
        System.arraycopy(bytes, 0, headerLengthBytes, 0, 4);

        int headerLength = BytesUtil.bytesToInt(headerLengthBytes);
        byte[] headerBytes = new byte[headerLength];
        System.arraycopy(bytes, 4, headerBytes, 0, headerLength);

        byte[] bodyBytes = new byte[bytes.length - 4 - headerLength];
        System.arraycopy(bytes, 4 + headerLength, bodyBytes, 0, bodyBytes.length);

        String headerStr = new String(headerBytes, StandardCharsets.UTF_8);
//        String fieldsStr = new String(bodyBytes, StandardCharsets.UTF_8);

        String[] headerArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(headerStr,
			SymbolsConstant._U0001);
//        String[] fieldsArray = StringUtils.splitByWholeSeparatorPreserveAllTokens(fieldsStr,
//			SymbolsConstant._U0001);
//		GenericRowData rawRow = new GenericRowData(fieldsArray.length);
//        for (int i = 0; i < fieldsArray.length; i++) {
//            if (fieldsArray[i].length() > 0) {
//                rawRow.setField(i, fieldsArray[i]);
//            }
//        }
		RowData rawRow = rowDataSerializerHashMap.get(headerArray[3])
			.deserialize(new DataInputDeserializer(bodyBytes));

        JoinRow row = new JoinRow(rawRow, null, null);
        row.setJoined(Integer.valueOf(headerArray[0]));
        row.setTimestamp(Long.valueOf(headerArray[1]));
        row.setCondition(headerArray[2]);
        row.setTableName(headerArray[3]);
        return row;
    }
}
