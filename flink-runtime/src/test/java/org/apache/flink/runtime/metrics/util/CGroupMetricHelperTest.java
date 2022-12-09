package org.apache.flink.runtime.metrics.util;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.runtime.clusterframework.types.ResourceID;

import org.junit.Assert;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.UUID;

public class CGroupMetricHelperTest {
    @Test
    public void testReadFile() throws IOException, InterruptedException {
        String mockContainerID = UUID.randomUUID().toString();
        Tuple2<File, TemporaryFolder> fileAndTempFolder = writeMockData(mockContainerID);
        String mockPath = fileAndTempFolder.f0.getAbsolutePath().replace(mockContainerID, CGroupMetricHelper.CONTAINER_ID_TAG);
        CGroupMetricHelper helper = new CGroupMetricHelper(new ResourceID(mockContainerID), mockPath, 15);
        //wait
        Thread.sleep(2000L);
        Assert.assertEquals(helper.getCgroupCpuNrPeriods(), 1);
        Assert.assertEquals(helper.getCgroupCpuNrThrottled(), 2);
        Assert.assertEquals(helper.getCgroupCpuThrottle(), 3);
        fileAndTempFolder.f1.delete();
    }

    @Test
    public void testEmptyFile() throws IOException, InterruptedException {
        CGroupMetricHelper helper = new CGroupMetricHelper(new ResourceID(UUID.randomUUID().toString()), "", 15);
        //wait
        Thread.sleep(2000L);
        Assert.assertEquals(helper.getCgroupCpuNrPeriods(), 0);
        Assert.assertEquals(helper.getCgroupCpuNrThrottled(), 0);
        Assert.assertEquals(helper.getCgroupCpuThrottle(), 0);
    }

    private Tuple2<File, TemporaryFolder> writeMockData(String mockContainerID) throws IOException {
        TemporaryFolder parent = new TemporaryFolder();
        parent.create();
        File folder = parent.newFolder(mockContainerID);
        File file = File.createTempFile("cpu", ".stat", folder);
        BufferedWriter out = new BufferedWriter(new FileWriter(file));
        out.write("nr_periods 1");
        out.write("\n");
        out.write("nr_throttled 2");
        out.write("\n");
        out.write("throttled_time 3");
        out.flush();
        out.close();
        return Tuple2.of(file, parent);
    }
}
