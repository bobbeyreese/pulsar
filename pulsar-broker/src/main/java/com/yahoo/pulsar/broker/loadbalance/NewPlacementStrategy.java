package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.BundleData;
import com.yahoo.pulsar.broker.ServiceConfiguration;
import com.yahoo.pulsar.broker.TimeAverageBrokerData;
import com.yahoo.pulsar.broker.loadbalance.impl.LeastLongTermMessageRate;

import java.util.Map;
import java.util.Set;

public interface NewPlacementStrategy {
    String selectBroker(Set<String> candidates,
                        Map<String, BrokerData> brokerDataMap,
                        BundleData bundleToAssign,
                        Map<String, Map<String, BundleData>> preallocatedBundles,
                        Map<String, TimeAverageBrokerData> timeAverageData,
                        ServiceConfiguration conf);

    static NewPlacementStrategy create(final ServiceConfiguration conf) {
        switch (conf.getLoadBalancerPlacementStrategy()) {
            case "LeastLongTermMessageRate":
            default:
                return LeastLongTermMessageRate.instance;
        }
    }
}
