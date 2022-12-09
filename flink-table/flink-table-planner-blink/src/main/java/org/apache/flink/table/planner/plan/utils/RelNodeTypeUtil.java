package org.apache.flink.table.planner.plan.utils;

import org.apache.calcite.rel.RelNode;
import org.apache.flink.table.planner.plan.nodes.FlinkRelNode;
import org.apache.flink.table.planner.plan.nodes.exec.ExecNodeBase;

/**
 * @author Dove
 * @Date 2022/6/1 6:01 下午
 */
public class RelNodeTypeUtil {
	/**
	 * all the supported prefixes of RelNode class name (i.e. all the implementation of {@link FlinkRelNode}).
	 */
	private static final String[] REL_TYPE_NAME_PREFIXES = {"StreamExec", "BatchExec", "FlinkLogical", "StreamPhysical"};

	public static String getNodeTypeName(RelNode rel) {
		String typeName = rel.getRelTypeName();
		for (String prefix : REL_TYPE_NAME_PREFIXES) {
			if (typeName.startsWith(prefix)) {
				return typeName.substring(prefix.length());
			}
		}
		throw new IllegalStateException("Unsupported RelNode class name '" + typeName + "'");
	}

	public static String getNodeTypeName(ExecNodeBase rel) {
		String typeName = getRelTypeName(rel);
		for (String prefix : REL_TYPE_NAME_PREFIXES) {
			if (typeName.startsWith(prefix)) {
				return typeName.substring(prefix.length());
			}
		}
		throw new IllegalStateException("Unsupported ExecNodeBase class name '" + typeName + "'");
	}

	private static String getRelTypeName(ExecNodeBase rel) {
		String cn = rel.getClass().getName();
		int i = cn.length();
		do {
			--i;
			if (i < 0) {
				return cn;
			}
		} while (cn.charAt(i) != '$' && cn.charAt(i) != '.');
		return cn.substring(i + 1);
	}

}
