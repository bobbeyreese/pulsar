package com.yahoo.pulsar.broker.loadbalance.impl;

import com.yahoo.pulsar.broker.*;
import com.yahoo.pulsar.broker.loadbalance.NewPlacementStrategy;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.Set;

public final class LeastLongTermMessageRate implements NewPlacementStrategy {
    private static Logger log = LoggerFactory.getLogger(LeastLongTermMessageRate.class);

    private LeastLongTermMessageRate(){}
    public static final LeastLongTermMessageRate instance = new LeastLongTermMessageRate();

    private static double getScore(final Map<String, BundleData> preallocatedData,
                                   final TimeAverageBrokerData timeAverageData) {
        double totalMessageRate = 0;
        if (preallocatedData != null) {
            for (BundleData bundleData: preallocatedData.values()) {
                final TimeAverageMessageData longTermData = bundleData.getLongTermData();
                totalMessageRate += longTermData.getMsgRateIn() + longTermData.getMsgRateOut();
            }
        }
        return totalMessageRate + timeAverageData.getLongTermMsgRateIn() + timeAverageData.getLongTermMsgRateOut();
    }

    @Override
    public String selectBroker(final Set<String> candidates,
                               final Map<String, BrokerData> brokerData,
                               final BundleData bundleToAssign,
                               final Map<String, Map<String, BundleData>> preallocatedData,
                               final Map<String, TimeAverageBrokerData> timeAverageData,
                               final ServiceConfiguration conf) {
        double minScore = Double.POSITIVE_INFINITY;
        String bestBroker = null;
        for (String broker: candidates) {
            final Map<String, BundleData> currentPreallocatedData = preallocatedData.get(broker);
            final TimeAverageBrokerData currentTimeAverageData = timeAverageData.get(broker);
            final double score = getScore(currentPreallocatedData, currentTimeAverageData);
            log.debug("{} got score {}", broker, score);
            if (score < minScore) {
                minScore = score;
                bestBroker = broker;
            }
        }
        return bestBroker;
    }
}
