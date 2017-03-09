package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.TimeAverageBrokerData;

import java.util.Map;
import java.util.Set;

public interface LoadSheddingStrategy {

    Set<String> selectBundlesForUnloading(Map<String, BrokerData> brokerDataMap,
                                          Map<String, Map<String, BundleData>> preallocatedBundles,
                                          Map<String, TimeAverageBrokerData> timeAverageData,
                                          ServiceConfiguration conf);

    static LoadSheddingStrategy create(final ServiceConfiguration conf) {
        // TODO
        return null;
    }
}
