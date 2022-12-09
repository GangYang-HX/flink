package com.bilibili.bsql.hive.utils;

import org.apache.flink.table.functions.UserDefinedFunction;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Method;

/**
 * @author: zhuzhengjun
 * @date: 2022/3/1 8:27 下午
 */
public class InvokeUtils {

	private final static Logger LOG = LoggerFactory.getLogger(InvokeUtils.class);

	public static UserDefinedFunction initInstance(Class<?> udfClass) {
		try {
			UserDefinedFunction udfInstance = (UserDefinedFunction) udfClass.newInstance();
			udfInstance.open(null);
			return udfInstance;
		} catch (Exception e) {
			throw new RuntimeException("create instance error.");
		}
	}

	public static Method initMethod(Class<?> udfClass) {
		Method udfMethod = null;
		for (Method method : udfClass.getMethods()) {
			if ("eval".equals(method.getName())) {
				udfMethod = method;
				break;
			}
		}
		return udfMethod;
	}

	public static Class<?> getAndValidUdf(String udfClassName, String udfProperty) {
		String propertyClassName = udfProperty.split("\\(")[0];
		if (org.apache.flink.util.StringUtils.isNullOrWhitespaceOnly(udfClassName)) {
			return null;
		} else {
			if (propertyClassName.equals(udfClassName.substring(udfClassName.lastIndexOf(".") + 1))) {
				return getUdf(udfClassName);
			} else {
				throw new IllegalArgumentException("Property class name invoke error: " + propertyClassName);
			}
		}
	}

//	public static Class<?> getUdf(String udfClassName) throws IllegalArgumentException {
//		try {
//			return Class.forName(udfClassName);
//		} catch (ClassNotFoundException e) {
//			throw new IllegalArgumentException("hive partition can not find udf class named '" + udfClassName + "'");
//		}
//	}

	public static Class<?> getUdf(String udfClassName) throws IllegalArgumentException {
		ClassLoader invokeUtilClassLoader = InvokeUtils.class.getClassLoader();
		ClassLoader currentThreadClassLoader = Thread.currentThread().getContextClassLoader();
		LOG.info("when sink hive,ClassLoader load info,invokeUtilClassLoader={},currentThreadClassLoader={},udfClassName={}"
			, invokeUtilClassLoader.toString(), currentThreadClassLoader.toString(), udfClassName);
		try {
			return Class.forName(udfClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("when sink hive for name can not find udf class named '" + udfClassName + "'");
		}
		try {
			return currentThreadClassLoader.loadClass(udfClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("when sink hive threadUdfLoadClass can not find udf class named '" + udfClassName + "'");
		}
		try {
			return invokeUtilClassLoader.loadClass(udfClassName);
		} catch (ClassNotFoundException e) {
			LOG.error("when sink hive invokeUtilClassLoader can not find udf class named '" + udfClassName + "'");
		}
		throw new IllegalArgumentException("hive partition can not find udf class named '" + udfClassName + "'");
	}

	public static void validMethod(Class<?> udfClass, String udfProperty) {
		int evalMethodCount = 0;
		Method evalMethod = null;
		for (Method method : udfClass.getMethods()) {
			if ("eval".equals(method.getName())) {
				evalMethod = method;
				evalMethodCount++;
			}
		}
		if (evalMethodCount != 1) {
			throw new IllegalArgumentException("the " + udfClass + " class don't have unique eval method");
		}
		if (evalMethod.getParameterCount() != udfProperty.split("\\(")[1].replaceAll("\\)", "").trim().split(",").length) {
			throw new IllegalArgumentException(udfClass.getSimpleName() + ": The number of parameters does not match.");
		}
	}

}
