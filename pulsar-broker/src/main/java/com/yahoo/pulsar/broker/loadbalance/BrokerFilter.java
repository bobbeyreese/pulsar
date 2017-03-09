package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.TimeAverageBrokerData;

import java.util.Map;
import java.util.Set;

public interface BrokerFilter {

    void filter(Set<String> brokers,
                Map<String, BrokerData> brokerDataMap,
                BundleData bundleToAssign,
                Map<String, Map<String, BundleData>> preallocatedBundles,
                Map<String, TimeAverageBrokerData> timeAverageData,
                ServiceConfiguration conf);

    static BrokerFilter create(final ServiceConfiguration conf) {
        // TODO
        return null;
    }
}
