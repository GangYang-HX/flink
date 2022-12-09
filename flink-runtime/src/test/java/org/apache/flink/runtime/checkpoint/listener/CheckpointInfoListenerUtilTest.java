package org.apache.flink.runtime.checkpoint.listener;

import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class CheckpointInfoListenerUtilTest {

    @Test
    public void testEnableRegistListener() {
        List<CheckpointInfoListener> registeredListeners =
                CheckpointInfoListenerUtil.getRegisteredListeners(true);
        assert registeredListeners.size() == 1;
        String testClassName = "TestCheckpointInfoListener";
        assertEquals(registeredListeners.get(0).getClass().getSimpleName(), testClassName);
    }

    @Test
    public void testDisableRegistListener() {
        List<CheckpointInfoListener> registeredListeners =
                CheckpointInfoListenerUtil.getRegisteredListeners(false);
        assert registeredListeners.size() == 0;
    }
}
