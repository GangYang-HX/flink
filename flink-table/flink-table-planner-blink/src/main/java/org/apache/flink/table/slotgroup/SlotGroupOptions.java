package org.apache.flink.table.slotgroup;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * @author zhangyang
 * @Date:2022/3/23
 * @Time:2:14 下午
 */
public class SlotGroupOptions {
    public static final ConfigOption<String> SLOT_SLOT = key("slot-group")
            .stringType()
            .noDefaultValue()
            .withDescription("the slot group string");
}
