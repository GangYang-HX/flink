/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.bili.external.archer.utils;

import org.junit.Test;

public class DataFormatUtilsTest {

    @Test
    public void testGetCreateDummyData() {
        String json =
                DataFormatUtils.getCreateDummyData(
                        "test", "test", "test", "15 0 0 * * ?", "test", "test");
        assert json != null;
    }

    @Test
    public void testGetGetJobIdByRelateUIDData() {
        String json = DataFormatUtils.getGetJobIdByRelateUIDData("test");
        assert json != null;
    }

    @Test
    public void testGetInstanceQueryData() {
        String json =
                DataFormatUtils.getInstanceQueryData(
                        "test", "2022-10-17 17:00:00", "2022-10-17 17:59:59", "test");
        assert json != null;
    }

    @Test
    public void testGetInstanceByUidQueryData() {
        String json =
                DataFormatUtils.getInstanceByUidQueryData(
                        "test", "2022-10-17 17:00:00", "2022-10-17 17:59:59", "test");
        assert json != null;
    }

    @Test
    public void testGetSuccessData() {
        String json = DataFormatUtils.getSuccessData("test", "test");
        assert json != null;
    }
}
