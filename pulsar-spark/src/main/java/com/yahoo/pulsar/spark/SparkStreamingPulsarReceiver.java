/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.yahoo.pulsar.spark;

import org.apache.spark.storage.StorageLevel;
import org.apache.spark.streaming.receiver.Receiver;

import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.Consumer;
import com.yahoo.pulsar.client.api.ConsumerConfiguration;
import com.yahoo.pulsar.client.api.Message;
import com.yahoo.pulsar.client.api.MessageListener;
import com.yahoo.pulsar.client.api.PulsarClient;
import com.yahoo.pulsar.client.api.PulsarClientException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class SparkStreamingPulsarReceiver extends Receiver<byte[]> {

    private ClientConfiguration clientConfiguration;
    private ConsumerConfiguration consumerConfiguration;
    private PulsarClient pulsarClient;
    private String url;
    private String topic;
    private String subscription;

    public SparkStreamingPulsarReceiver(ClientConfiguration clientConfiguration,
            ConsumerConfiguration consumerConfiguration, String url, String topic, String subscription) {
        this(StorageLevel.MEMORY_AND_DISK_2(), clientConfiguration, consumerConfiguration, url, topic, subscription);
    }

    public SparkStreamingPulsarReceiver(StorageLevel storageLevel, ClientConfiguration clientConfiguration,
            ConsumerConfiguration consumerConfiguration, String url, String topic, String subscription) {
        super(storageLevel);
        this.clientConfiguration = clientConfiguration;
        this.url = url;
        this.topic = topic;
        this.subscription = subscription;
        if (consumerConfiguration.getAckTimeoutMillis() == 0) {
            consumerConfiguration.setAckTimeout(60, TimeUnit.SECONDS);
        }
        consumerConfiguration.setMessageListener((consumer, msg) -> {
            try {
                store(msg.getData());
                consumer.acknowledgeAsync(msg);
            } catch (Exception e) {
                log.error("Failed to store a message : {}", e.getMessage());
            }
        });
        this.consumerConfiguration = consumerConfiguration;
    }

    public void onStart() {
        try {
            pulsarClient = PulsarClient.create(url, clientConfiguration);
            pulsarClient.subscribe(topic, subscription, consumerConfiguration);
        } catch (PulsarClientException e) {
            log.error("Failed to start subscription : {}", e.getMessage());
            restart("Restart a consumer");
        }
    }

    public void onStop() {
        try {
            if (pulsarClient != null) {
                pulsarClient.close();
            }
        } catch (PulsarClientException e) {
            log.error("Failed to close client : {}", e.getMessage());
        }
    }

    private static final Logger log = LoggerFactory.getLogger(SparkStreamingPulsarReceiver.class);
}