package com.bilibili.bsql.hive.filetype.text;

import org.apache.commons.lang.StringUtils;
import org.apache.flink.api.common.serialization.Encoder;
import org.apache.flink.types.Row;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.OutputStream;
import java.nio.charset.Charset;

/**
 * Created by haozhugogo on 2020/5/10.
 */
public class BiliSimpleStringEncoder implements Encoder<Row> {

    private static final Logger LOG = LoggerFactory.getLogger(BiliSimpleStringEncoder.class);

    private String charsetName;
    private transient Charset charset;
    private String fieldDelim;
    private String rowDelim;
    private int eventTimePos;
    private boolean timeFieldVirtual;

    /**
     * Creates a new {@code StringWriter} that uses {@code "UTF-8"} charset to convert strings to bytes.
     */
    public BiliSimpleStringEncoder(String fieldDelim, String rowDelim, int eventTimePos, boolean timeFieldVirtual) {
        this("UTF-8", fieldDelim, rowDelim, eventTimePos, timeFieldVirtual);
    }

    /**
     * Creates a new {@code StringWriter} that uses the given charset to convert strings to bytes.
     *
     * @param charsetName Name of the charset to be used, must be valid input for {@code Charset.forName(charsetName)}
     */
    public BiliSimpleStringEncoder(String charsetName, String fieldDelim, String rowDelim, int eventTimePos,boolean timeFieldVirtual) {
        this.charsetName = charsetName;
        this.fieldDelim = fieldDelim;
        this.eventTimePos = eventTimePos;
        this.timeFieldVirtual = timeFieldVirtual;
        this.rowDelim = StringUtils.isEmpty(rowDelim) ? "\n" : rowDelim;
    }

    @Override
    public void encode(Row row, OutputStream stream) throws IOException {
        if (charset == null) {
            charset = Charset.forName(charsetName);
        }

        for (int i = 0; i < row.getArity(); i++) {
            if (timeFieldVirtual && i == eventTimePos) {
                continue;
            }
            Object field = row.getField(i);
            byte[] encodeByteArray = field instanceof byte[] ? (byte[]) field : field.toString().getBytes(charset);

            stream.write(encodeByteArray);
            //add fieldDelim only before the last physical field
            if (timeFieldVirtual && eventTimePos == row.getArity() - 1) {
                //eg.[physicalField1, physicalField2, physicalField3, virtualField] only add fieldDelim before physicalField3.
                if (i < row.getArity() - 2) {
                    stream.write(fieldDelim.getBytes(charset));
                }
            } else if (i < row.getArity() - 1 ) {
                //eg.[physicalField1, physicalField2, virtualField, physicalField3] cause virtualField will be skiped, add fieldDelim normally.
                stream.write(fieldDelim.getBytes(charset));
            }
        }

        stream.write(rowDelim.getBytes());
    }

}
