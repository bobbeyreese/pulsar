package com.yahoo.pulsar.broker.loadbalance.impl;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.TimeAverageMessageData;
import com.yahoo.pulsar.broker.loadbalance.NewPlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public final class LeastLongTermMessageRate implements NewPlacementStrategy {
    private static Logger log = LoggerFactory.getLogger(LeastLongTermMessageRate.class);

    private LeastLongTermMessageRate(){}
    public static final LeastLongTermMessageRate instance = new LeastLongTermMessageRate();

    private static double getScore(final BrokerData brokerData, final Map<String, BundleData> preallocatedData) {
        double totalMessageRate = 0;
        if (preallocatedData != null) {
            for (BundleData bundleData: preallocatedData.values()) {
                final TimeAverageMessageData longTermData = bundleData.getLongTermData();
                totalMessageRate += longTermData.getMsgRateIn() + longTermData.getMsgRateOut();
            }
        }
        return totalMessageRate + brokerData.getMsgRateIn() + brokerData.getMsgRateOut();
    }

    @Override
    public String selectBroker(final Map<String, BrokerData> brokerData, final BundleData bundleToAssign,
                               final Map<String, Map<String, BundleData>> preallocatedData) {
        double minScore = Double.POSITIVE_INFINITY;
        String bestBroker = null;
        for (Map.Entry<String, BrokerData> entry: brokerData.entrySet()) {
            final String broker = entry.getKey();
            final BrokerData currentBrokerData = entry.getValue();
            final Map<String, BundleData> currentPreallocatedData = preallocatedData.get(broker);
            final double score = getScore(currentBrokerData, currentPreallocatedData);
            log.info("{} got score {}", broker, score);
            if (score < minScore) {
                minScore = score;
                bestBroker = broker;
            }
        }
        return bestBroker;
    }
}
