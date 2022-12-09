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

package org.apache.flink.runtime.checkpoint;

public enum ExpiredType {

    BARRIER_TRIGGER_EXPIRED(new DisplayMessageProvider() {
        @Override
        public String getMessage(String... args) {
            String template = "Expired Type => BARRIER_TRIGGER_EXPIRED, triggered tasks ({}/{}).";
            return template.replaceFirst("\\{}", args[0]).replace("{}", args[1]);
        }
    }),

    SNAPSHOT_EXPIRED(new DisplayMessageProvider() {
        @Override
        public String getMessage(String... args) {
            String template = "Expired Type => SNAPSHOT_EXPIRED, the maximum of barrier trigger delay is {} ms.";
            return template.replace("{}", args[0]);
        }
    });

    private final DisplayMessageProvider messageProvider;

    ExpiredType(DisplayMessageProvider messageProvider) {
        this.messageProvider = messageProvider;
    }

    public DisplayMessageProvider getMessageProvider() {
        return messageProvider;
    }

    interface DisplayMessageProvider {
        String getMessage(String... args);
    }
}
