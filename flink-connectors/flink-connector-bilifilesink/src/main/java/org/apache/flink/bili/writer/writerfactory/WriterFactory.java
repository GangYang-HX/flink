/**
 * Bilibili.com Inc.
 * Copyright (c) 2009-2020 All Rights Reserved.
 */
package org.apache.flink.bili.writer.writerfactory;

import org.apache.flink.api.common.typeinfo.TypeInformation;

/**
 *
 * @author zhouxiaogang
 * @version $Id: WriterFactory.java, v 0.1 2020-05-22 10:53
zhouxiaogang Exp $$
 */
public interface WriterFactory {
	Object createWriter(String type, String[] fieldNames, TypeInformation<?>[] fieldTypes);
}
