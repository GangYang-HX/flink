/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package com.bilibili.bsql.common.format.converter;

import java.io.IOException;
import java.io.Serializable;
import java.math.BigDecimal;
import java.sql.Timestamp;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.format.DateTimeFormatter;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.table.data.*;
import org.apache.flink.table.data.util.DataFormatConverters;
import org.apache.flink.table.types.logical.*;

import com.bilibili.bsql.common.utils.ObjectUtil;


/**
 * @author zhouxiaogang
 * @version $Id: CustomRowConverter.java, v 0.1 2020-10-25 16:13
 * zhouxiaogang Exp $$
 */
public class CustomRowConverter implements Serializable {
    private final RowType rowType;
    private static final DateTimeFormatter DATA_FORMAT_20 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ssz");
    private static final DateTimeFormatter DATA_FORMAT_24 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSz");
    private static final DateTimeFormatter DATA_FORMAT_21 = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss");
    private MetricGroup rowConverterMetricGroup;
    private Counter abnormalInputCounter;
	private Boolean NO_DEFAULT_VALUE = false;

    public CustomRowConverter(RowType rowType) {
        this.rowType = rowType;
    }

    public void setRowConverterMetricGroup(MetricGroup rowConverterMetricGroup) {
        this.rowConverterMetricGroup = rowConverterMetricGroup;
    }

	public void setNoDefaultValue(Boolean noDefaultValue) {
		this.NO_DEFAULT_VALUE = noDefaultValue;
	}

    public RowData deserializeString(Object[] fields) {
        if (abnormalInputCounter == null && rowConverterMetricGroup != null) {
            abnormalInputCounter = rowConverterMetricGroup.counter("source_abnormal_input");
        }
        boolean hasEncounterAbnormal = false;
        GenericRowData row = new GenericRowData(rowType.getFieldCount());
        for (int i = 0; i < rowType.getFieldCount(); i++) {
            if (i >= fields.length) {
                LogicalType type = rowType.getTypeAt(i);
                try {
					row.setField(i, getDefaultValue(type));
                } catch (Exception e) {
                    // ignore
                }
                continue;
            }
            Object item = fields[i];
            LogicalType type = rowType.getTypeAt(i);
            try {
                switch (type.getTypeRoot()) {
                    case CHAR:
                    case VARCHAR:
						if (null == item) {
							row.setField(i, null);
							continue;
						}

                        row.setField(i, StringData.fromString(String.valueOf(item)));
                        continue;
                    case BIGINT:
                    case INTERVAL_DAY_TIME:
						if (null == item) {
							row.setField(i, null);
							continue;
						}

						Long value = new Long(String.valueOf(item));
						row.setField(i, value);
                        continue;
                    case TIMESTAMP_WITHOUT_TIME_ZONE:
                        row.setField(i, convertToTimestamp(String.valueOf(item)));
                        continue;
                    case BOOLEAN:
                        Boolean valueBool = Boolean.valueOf(String.valueOf(item));
                        row.setField(i, valueBool);
                        continue;
                    case INTEGER:
                    case INTERVAL_YEAR_MONTH:
                    	if (null == item) {
							row.setField(i, null);
							continue;
						}

						Integer valueInt = new Integer(String.valueOf(item));
						row.setField(i, valueInt);
                        continue;
                    case TINYINT:
						if (null == item) {
							row.setField(i, null);
							continue;
						}

                        Byte valueByte = new Byte(String.valueOf(item));
                        row.setField(i, valueByte);
                        continue;
                    case SMALLINT:
						if (null == item) {
							row.setField(i, null);
							continue;
						}

						Short valueShort = new Short(String.valueOf(item));
						row.setField(i, valueShort);
                        continue;
                    case FLOAT:
						if (null == item) {
							row.setField(i, null);
							continue;
						}

                        Float valueFloat = new Float(String.valueOf(item));
                        row.setField(i, valueFloat);
                        continue;
                    case DOUBLE:
						if (null == item) {
							row.setField(i, null);
							continue;
						}

                        Double valueDouble = new Double(String.valueOf(item));
                        row.setField(i, valueDouble);
                        continue;
                    case BINARY:
                    case VARBINARY:
                        if (item instanceof String) {
                            row.setField(i, ((String) item).getBytes());
                        } else {
                            row.setField(i, item);
                        }
                        continue;
                    case DECIMAL:
                        if (null == item){
                            row.setField(i, null);
                            continue;
                        }
                        BigDecimal valueBigDecimal = new BigDecimal(String.valueOf(item));
                        DecimalData decimalData = DecimalData.fromBigDecimal(
                            valueBigDecimal,
                            valueBigDecimal.precision(),
                            valueBigDecimal.scale()
                        );
                        row.setField(i, decimalData);
                        continue;
                }
                throw new IOException(String.format("unexpected Type %d th field in %s", i, item));
            } catch (Exception e) {
                try {
                    if (!hasEncounterAbnormal) {
						if (abnormalInputCounter != null) {
							abnormalInputCounter.inc();
						}
                        hasEncounterAbnormal = true;
                    }
					row.setField(i, getDefaultValue(type));
                } catch (Exception ex) {
                    // ignore
                }
            }
        }
        return row;
    }

	private Object getDefaultValue(LogicalType type) {
		return this.NO_DEFAULT_VALUE ? null : ObjectUtil.getDefaultValue(type);
	}

    public TimestampData convertToTimestamp(String timeStampStr) throws Exception {
        if (StringUtils.isNumeric(timeStampStr)) {
            return TimestampData.fromEpochMillis(new Timestamp(Long.parseLong(timeStampStr)).getTime());
        } else if (timeStampStr.contains("T")) {
            if (timeStampStr.length() == 20) {
                return TimestampData.fromEpochMillis(formatByPattern(timeStampStr, DATA_FORMAT_20));
            } else if (timeStampStr.length() == 21 || timeStampStr.length() == 19) {
                return TimestampData.fromEpochMillis(formatByPattern(timeStampStr, DATA_FORMAT_21));
            } else if (timeStampStr.length() == 24) {
                return TimestampData.fromEpochMillis(formatByPattern(timeStampStr, DATA_FORMAT_24));
            } else {
                throw new IOException(String.format("Can not format timestamp %s", timeStampStr));
            }
        } else {
            return TimestampData.fromEpochMillis(Timestamp.valueOf(timeStampStr).getTime());
        }
    }

    private long formatByPattern(String timestamp, DateTimeFormatter dateTimeFormatter) {
        // Temporal values with UTC, need to plus 8 hour
        // https://github.com/vert-x3/vertx-jdbc-client/issues/20
        return LocalDateTime.from(dateTimeFormatter.parse(timestamp)).atZone(ZoneId.systemDefault()).toInstant().toEpochMilli();
    }
}
