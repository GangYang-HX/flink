package org.apache.flink.api.common;

import org.apache.commons.lang3.RandomStringUtils;

import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TaskInfoTest {
    @Test
    public void testTruncateTaskName() {
        String mockTaskName = RandomStringUtils.randomAlphanumeric(2000);
        assertEquals(mockTaskName.length(), 2000);
        TaskInfo taskInfo = new TaskInfo(mockTaskName, 10, 0, 10, 1);
        assertTrue(taskInfo.getTaskNameWithSubtasks(true).endsWith("[truncated by flink] (1/10)"));
    }
}
