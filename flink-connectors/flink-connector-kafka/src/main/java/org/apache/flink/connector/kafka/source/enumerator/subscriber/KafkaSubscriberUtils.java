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

package org.apache.flink.connector.kafka.source.enumerator.subscriber;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.TopicDescription;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

/** The base implementations of {@link KafkaSubscriber}. */
class KafkaSubscriberUtils {

    private static final Logger LOG = LoggerFactory.getLogger(KafkaSubscriberUtils.class);

    private KafkaSubscriberUtils() {}

    private static final Integer DO_NOT_RETRY = 0;

    static Map<String, TopicDescription> getAllTopicMetadata(AdminClient adminClient) {
        return getAllTopicMetadata(adminClient, DO_NOT_RETRY);
    }

    static Map<String, TopicDescription> getAllTopicMetadata(
            AdminClient adminClient, int retryTimes) {
        int i = 0;
        Exception finalException;
        do {
            try {
                Set<String> allTopicNames = adminClient.listTopics().names().get();
                return getTopicMetadata(adminClient, allTopicNames);
            } catch (Exception e) {
                LOG.warn(
                        "KafkaSubscriberUtils#getAllTopicMetadata encounter exception,totalTimes:{},current for the {} time.",
                        retryTimes + 1,
                        i + 1,
                        e);
                finalException = e;
            }
        } while (i++ < retryTimes);
        throw new RuntimeException("Failed to get metadata for all topics.", finalException);
    }

    static Map<String, TopicDescription> getTopicMetadata(
            AdminClient adminClient, Set<String> topicNames) {
        return getTopicMetadata(adminClient, topicNames, DO_NOT_RETRY);
    }

    static Map<String, TopicDescription> getTopicMetadata(
            AdminClient adminClient, Set<String> topicNames, int retryTimes) {
        int i = 0;
        Exception finalException;
        do {
            try {
                return adminClient.describeTopics(topicNames).all().get();
            } catch (Exception e) {
                LOG.warn(
                        "KafkaSubscriberUtils#getTopicMetadata encounter exception,totalTimes:{},current for the {} time.",
                        retryTimes + 1,
                        i + 1,
                        e);
                finalException = e;
            }
        } while (i++ < retryTimes);
        throw new RuntimeException(
                String.format("Failed to get metadata for topics %s.", topicNames), finalException);
    }
}
