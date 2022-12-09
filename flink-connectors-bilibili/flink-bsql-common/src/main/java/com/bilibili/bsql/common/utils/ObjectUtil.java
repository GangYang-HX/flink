/**
 * Alipay.com Inc.
 * Copyright (c) 2004-2016 All Rights Reserved.
 */
package com.bilibili.bsql.common.utils;

import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import java.io.Serializable;
import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.*;
import java.util.HashMap;
import java.util.Map;
import java.util.TimeZone;

import org.apache.flink.table.types.logical.*;

/**
 * 有关<code>Object</code>处理的工具类。
 *
 * <p>
 * 这个类中的每个方法都可以“安全”地处理<code>null</code>，而不会抛出<code>NullPointerException</code>。
 * </p>
 *
 * @author Xinux
 * @version $Id: ObjectUtil.java, v 0.1 2016-06-21 5:40 PM Xinux Exp $$
 */
public class ObjectUtil {
    private static final long MILLIS_PER_DAY = 86400000;
    private static final long TIMEZONE_OFFSET = TimeZone.getDefault().getRawOffset();

    public static Object getDefaultValue(LogicalType type) {
        switch (type.getTypeRoot()) {
            case CHAR:
            case VARCHAR:
                return StringData.fromString("");
            case BIGINT:
            case INTERVAL_DAY_TIME:
                return new Long(0);
            case TIMESTAMP_WITHOUT_TIME_ZONE:
            	// when play back from checkpoint, if a 'now' value appear, can push the window forward
				// which will cause many record dropped
                return TimestampData.fromEpochMillis(0);
            case BOOLEAN:
                return new Boolean(false);
            case INTEGER:
            case INTERVAL_YEAR_MONTH:
                return new Integer(0);
            case TINYINT:
                return new Byte((byte) 0);
            case SMALLINT:
                return new Short((short) 0);
            case FLOAT:
                return new Float(0);
            case DOUBLE:
                return new Double(0);
        }

        return "";
    }

    /* ============================================================================ */
    /*  常量和singleton。                                                           */
    /* ============================================================================ */

    //用于全局存储
    public static Map<String, String> GLOBAL_STORE = new HashMap<String, String>();

    /**
     * 用于表示<code>null</code>的常量。
     *
     * <p>
     * 例如，<code>HashMap.get(Object)</code>方法返回<code>null</code>有两种可能：
     * 值不存在或值为<code>null</code>。而这个singleton可用来区别这两种情形。
     * </p>
     *
     * <p>
     * 另一个例子是，<code>Hashtable</code>的值不能为<code>null</code>。
     * </p>
     */
    public static final Object NULL = new Serializable() {
        private static final long serialVersionUID = 7092611880189329093L;

        private Object readResolve() {
            return NULL;
        }
    };

    /* ============================================================================ */
    /*  默认值函数。                                                                */
    /*                                                                              */
    /*  当对象为null时，将对象转换成指定的默认对象。                                */
    /* ============================================================================ */

    /**
     * 如果对象为<code>null</code>，则返回指定默认对象，否则返回对象本身。
     * <pre>
     * ObjectUtil.defaultIfNull(null, null)      = null
     * ObjectUtil.defaultIfNull(null, "")        = ""
     * ObjectUtil.defaultIfNull(null, "zz")      = "zz"
     * ObjectUtil.defaultIfNull("abc", *)        = "abc"
     * ObjectUtil.defaultIfNull(Boolean.TRUE, *) = Boolean.TRUE
     * </pre>
     *
     * @param object       要测试的对象
     * @param defaultValue 默认值
     * @return 对象本身或默认对象
     */
    public static Object defaultIfNull(Object object, Object defaultValue) {
        return (object != null) ? object : defaultValue;
    }

    /* ============================================================================ */
    /*  比较函数。                                                                  */
    /*                                                                              */
    /*  以下方法用来比较两个对象是否相同。                                          */
    /* ============================================================================ */

    /**
     * 比较两个对象是否完全相等。
     *
     * <p>
     * 此方法可以正确地比较多维数组。
     * <pre>
     * ObjectUtil.equals(null, null)                  = true
     * ObjectUtil.equals(null, "")                    = false
     * ObjectUtil.equals("", null)                    = false
     * ObjectUtil.equals("", "")                      = true
     * ObjectUtil.equals(Boolean.TRUE, null)          = false
     * ObjectUtil.equals(Boolean.TRUE, "true")        = false
     * ObjectUtil.equals(Boolean.TRUE, Boolean.TRUE)  = true
     * ObjectUtil.equals(Boolean.TRUE, Boolean.FALSE) = false
     * </pre>
     * </p>
     *
     * @param object1 对象1
     * @param object2 对象2
     * @return 如果相等, 则返回<code>true</code>
     */
    public static boolean equals(Object object1, Object object2) {
        return ArrayUtil.equals(object1, object2);
    }

    /* ============================================================================ */
    /*  Hashcode函数。                                                              */
    /*                                                                              */
    /*  以下方法用来取得对象的hash code。                                           */
    /* ============================================================================ */

    /**
     * 取得对象的hash值, 如果对象为<code>null</code>, 则返回<code>0</code>。
     *
     * <p>
     * 此方法可以正确地处理多维数组。
     * </p>
     *
     * @param object 对象
     * @return hash值
     */
    public static int hashCode(Object object) {
        return ArrayUtil.hashCode(object);
    }

    /**
     * 取得对象的原始的hash值, 如果对象为<code>null</code>, 则返回<code>0</code>。
     *
     * <p>
     * 该方法使用<code>System.identityHashCode</code>来取得hash值，该值不受对象本身的<code>hashCode</code>方法的影响。
     * </p>
     *
     * @param object 对象
     * @return hash值
     */
    public static int identityHashCode(Object object) {
        return (object == null) ? 0 : System.identityHashCode(object);
    }

    /* ============================================================================ */
    /*  取得对象的identity。                                                        */
    /* ============================================================================ */

    /**
     * 取得对象自身的identity，如同对象没有覆盖<code>toString()</code>方法时，<code>Object.toString()</code>的原始输出。
     * <pre>
     * ObjectUtil.identityToString(null)          = null
     * ObjectUtil.identityToString("")            = "java.lang.String@1e23"
     * ObjectUtil.identityToString(Boolean.TRUE)  = "java.lang.Boolean@7fa"
     * ObjectUtil.identityToString(new int[0])    = "int[]@7fa"
     * ObjectUtil.identityToString(new Object[0]) = "java.lang.Object[]@7fa"
     * </pre>
     *
     * @param object 对象
     * @return 对象的identity，如果对象是<code>null</code>，则返回<code>null</code>
     */
    public static String identityToString(Object object) {
        if (object == null) {
            return null;
        }

        return appendIdentityToString(null, object).toString();
    }

    /**
     * 取得对象自身的identity，如同对象没有覆盖<code>toString()</code>方法时，<code>Object.toString()</code>的原始输出。
     * <pre>
     * ObjectUtil.identityToString(null, "NULL")            = "NULL"
     * ObjectUtil.identityToString("", "NULL")              = "java.lang.String@1e23"
     * ObjectUtil.identityToString(Boolean.TRUE, "NULL")    = "java.lang.Boolean@7fa"
     * ObjectUtil.identityToString(new int[0], "NULL")      = "int[]@7fa"
     * ObjectUtil.identityToString(new Object[0], "NULL")   = "java.lang.Object[]@7fa"
     * </pre>
     *
     * @param object  对象
     * @param nullStr 如果对象为<code>null</code>，则返回该字符串
     * @return 对象的identity，如果对象是<code>null</code>，则返回指定字符串
     */
    public static String identityToString(Object object, String nullStr) {
        if (object == null) {
            return nullStr;
        }

        return appendIdentityToString(null, object).toString();
    }

    /**
     * 将对象自身的identity——如同对象没有覆盖<code>toString()</code>方法时，<code>Object.toString()</code>的原始输出——追加到<code>StringBuffer</code>中。
     * <pre>
     * ObjectUtil.appendIdentityToString(*, null)            = null
     * ObjectUtil.appendIdentityToString(null, "")           = "java.lang.String@1e23"
     * ObjectUtil.appendIdentityToString(null, Boolean.TRUE) = "java.lang.Boolean@7fa"
     * ObjectUtil.appendIdentityToString(buf, Boolean.TRUE)  = buf.append("java.lang.Boolean@7fa")
     * ObjectUtil.appendIdentityToString(buf, new int[0])    = buf.append("int[]@7fa")
     * ObjectUtil.appendIdentityToString(buf, new Object[0]) = buf.append("java.lang.Object[]@7fa")
     * </pre>
     *
     * @param buffer <code>StringBuffer</code>对象，如果是<code>null</code>，则创建新的
     * @param object 对象
     * @return <code>StringBuffer</code>对象，如果对象为<code>null</code>，则返回<code>null</code>
     */
    public static StringBuffer appendIdentityToString(StringBuffer buffer, Object object) {
        if (object == null) {
            return null;
        }

        if (buffer == null) {
            buffer = new StringBuffer();
        }

        buffer.append(ClassUtil.getClassNameForObject(object));

        return buffer.append('@').append(Integer.toHexString(identityHashCode(object)));
    }

    /* ============================================================================ */
    /*  Clone函数。                                                                 */
    /*                                                                              */
    /*  以下方法调用Object.clone方法，默认是“浅复制”（shallow copy）。            */
    /* ============================================================================ */



    /* ============================================================================ */
    /*  toString方法。                                                              */
    /* ============================================================================ */

    /**
     * 取得对象的<code>toString()</code>的值，如果对象为<code>null</code>，则返回空字符串<code>""</code>。
     * <pre>
     * ObjectUtil.toString(null)         = ""
     * ObjectUtil.toString("")           = ""
     * ObjectUtil.toString("bat")        = "bat"
     * ObjectUtil.toString(Boolean.TRUE) = "true"
     * ObjectUtil.toString([1, 2, 3])    = "[1, 2, 3]"
     * </pre>
     *
     * @param object 对象
     * @return 对象的<code>toString()</code>的返回值，或空字符串<code>""</code>
     */
    public static String toString(Object object) {
        return (object == null) ? ""
                : (object.getClass().isArray() ? ArrayUtil.toString(object) : object.toString());
    }

    /**
     * 取得对象的<code>toString()</code>的值，如果对象为<code>null</code>，则返回指定字符串。
     * <pre>
     * ObjectUtil.toString(null, null)           = null
     * ObjectUtil.toString(null, "null")         = "null"
     * ObjectUtil.toString("", "null")           = ""
     * ObjectUtil.toString("bat", "null")        = "bat"
     * ObjectUtil.toString(Boolean.TRUE, "null") = "true"
     * ObjectUtil.toString([1, 2, 3], "null")    = "[1, 2, 3]"
     * </pre>
     *
     * @param object  对象
     * @param nullStr 如果对象为<code>null</code>，则返回该字符串
     * @return 对象的<code>toString()</code>的返回值，或指定字符串
     */
    public static String toString(Object object, String nullStr) {
        return (object == null) ? nullStr : (object.getClass().isArray() ? ArrayUtil
                .toString(object) : object.toString());
    }

    public static Object defaultIfBlank(Object object, Object defaultObject) {
        if (ObjectUtil.isBlank(object))
            return defaultObject;
        return object;
    }

    /**
     * 是否null
     */
    public static boolean isBlank(Object object) {
        return null == object;
    }

    /***
     * check if originalObjectArray
     * @return true if have one blank at least.
     */
    public static boolean isBlank(Object... originalObjectArray) {

        if (null == originalObjectArray || 0 == originalObjectArray.length)
            return true;
        for (int i = 0; i < originalObjectArray.length; i++) {
            if (isBlank(originalObjectArray[i]))
                return true;
        }
        return false;
    }

    public static String getString(Object obj, String defaultVal) {
        if (null == obj) {
            return defaultVal;
        }
        return getString(obj);
    }


    public static String getString(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return (String) obj;
        } else {
            return obj.toString();
        }
    }

    public static Boolean getBoolean(Object obj, boolean defaultVal) {
        if (obj == null) {
            return defaultVal;
        }

        return getBoolean(obj);
    }

    public static Boolean getBoolean(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Boolean.valueOf((String) obj);
        } else if (obj instanceof Boolean) {
            return (Boolean) obj;
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Boolean.");
    }

    public static Integer getInteger(Object obj, Integer defaultVal) {
        if (null == obj) {
            return defaultVal;
        }
        return getInteger(obj);
    }

    public static Integer getInteger(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Integer.valueOf((String) obj);
        } else if (obj instanceof Integer) {
            return (Integer) obj;
        } else if (obj instanceof Long) {
            return ((Long) obj).intValue();
        } else if (obj instanceof Double) {
            return ((Double) obj).intValue();
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).intValue();
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Integer.");
    }

    public static Long getLong(Object obj, Long defaultVal) {
        if (null == obj) {
            return defaultVal;
        }

        return getLong(obj);
    }

    public static Long getLong(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Long.valueOf((String) obj);
        } else if (obj instanceof Long) {
            return (Long) obj;
        } else if (obj instanceof Integer) {
            return Long.valueOf(obj.toString());
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Long.");
    }

    public static Short getShort(Object obj, Short defaultVal) {
        if (null == obj) {
            return defaultVal;
        }

        return getShort(obj);
    }

    public static Short getShort(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Short.valueOf((String) obj);
        } else if (obj instanceof Short) {
            return (Short) obj;
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Short.");
    }

    public static BigDecimal getBigDecimal(Object obj, BigDecimal defaultVal) {
        if (null == obj) {
            return defaultVal;
        }
        return getBigDecimal(obj);
    }

    public static BigDecimal getBigDecimal(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            return new BigDecimal((String) obj);
        } else if (obj instanceof BigDecimal) {
            return (BigDecimal) obj;
        } else if (obj instanceof BigInteger) {
            return new BigDecimal((BigInteger) obj);
        } else if (obj instanceof Number) {
            return new BigDecimal(((Number) obj).doubleValue());
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to BigDecimal.");
    }

    public static Date getDate(Object obj, Date defaultVal) {
        if (null == obj) {
            return defaultVal;
        }
        return getDate(obj);
    }

    public static Date getDate(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof String) {
            SimpleDateFormat format = new SimpleDateFormat("yyyy-MM-dd");
            try {
                return new Date(format.parse((String) obj).getTime());
            } catch (ParseException e) {
                throw new RuntimeException("String convert to Date fail.");
            }
        } else if (obj instanceof Timestamp) {
            return new Date(((Timestamp) obj).getTime());
        } else if (obj instanceof Date) {
            return (Date) obj;
        } else if (obj instanceof LocalDate) {
            return Date.valueOf((LocalDate) obj);
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Date.");
    }

    public static Timestamp getTimestamp(Object obj, Timestamp defaultVal) {
        if (null == obj) {
            return defaultVal;
        }

        return getTimestamp(obj);
    }

    public static Timestamp getTimestamp(Object obj) {
        if (obj == null) {
            return null;
        }
        if (obj instanceof Timestamp) {
            return (Timestamp) obj;
        } else if (obj instanceof Date) {
            return new Timestamp(((Date) obj).getTime());
        } else if (obj instanceof String) {
            return new Timestamp(getDate(obj).getTime());
        } else if (obj instanceof LocalDateTime) {
            return getLocalDateTimestamp((LocalDateTime) obj);
        } else if (obj instanceof Instant) {
            return new Timestamp(((Instant) obj).toEpochMilli());
        }
        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Timestamp.");
    }

    public static Timestamp getLocalDateTimestamp(LocalDateTime dateTime) {
        long epochDay = dateTime.toLocalDate().toEpochDay();
        long nanoOfDay = dateTime.toLocalTime().toNanoOfDay();
        long millisecond = epochDay * MILLIS_PER_DAY + nanoOfDay / 1_000_000 - TIMEZONE_OFFSET;
        return new Timestamp(millisecond);
    }

    public static Byte getByte(Object obj, Byte defaultVal) {
        if (null == obj) {
            return defaultVal;
        }
        return getByte(obj);
    }

    public static Byte getByte(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Byte.valueOf((String) obj);
        } else if (obj instanceof Byte) {
            return (Byte) obj;
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Byte.");
    }

    public static Float getFloat(Object obj, Float defaultVal) {
        if (null == obj) {
            return defaultVal;
        }
        return getFloat(obj);
    }

    public static Float getFloat(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Float.valueOf((String) obj);
        } else if (obj instanceof Float) {
            return (Float) obj;
        } else if (obj instanceof Double) {
            return ((Double) obj).floatValue();
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).floatValue();
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Float.");
    }

    public static Double getDouble(Object obj, Double defaultVal) {
        if (null == obj) {
            return defaultVal;
        }

        return getDouble(obj);
    }

    public static Double getDouble(Object obj) {
        if (obj == null) {
            return null;
        }

        if (obj instanceof String) {
            return Double.valueOf((String) obj);
        } else if (obj instanceof Double) {
            return (Double) obj;
        } else if (obj instanceof Float) {
            return (Double) obj;
        } else if (obj instanceof BigDecimal) {
            return ((BigDecimal) obj).doubleValue();
        }

        throw new RuntimeException("not support type of " + obj.getClass() + " convert to Double.");
    }


    public static Object getTarget(Object obj, Class tClass) {

        if (tClass == VarCharType.class) {
            return getString(obj);
        }
        if (tClass == BigIntType.class) {
            return getLong(obj);
        }
        if (tClass == TimestampType.class) {
            return getTimestamp(obj);
        }
        if (tClass == BooleanType.class) {
            return getBoolean(obj);
        }
        if (tClass == IntType.class) {
            return getInteger(obj);
        }
        if (tClass == TinyIntType.class) {
            return getByte(obj);
        }
        if (tClass == SmallIntType.class) {
            return getShort(obj);
        }
        if (tClass == FloatType.class) {
            return getFloat(obj);
        }
        if (tClass == DoubleType.class) {
            return getDouble(obj);
        }
        if (tClass == DecimalType.class) {
            return getBigDecimal(obj);
        }


        throw new ClassCastException("类型转换失败");
    }


}
