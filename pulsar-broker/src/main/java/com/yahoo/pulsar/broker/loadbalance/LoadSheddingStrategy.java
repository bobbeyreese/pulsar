package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.ServiceConfiguration;

import java.util.Set;

/**
 * Load management component which determines the criteria for unloading bundles.
 */
public interface LoadSheddingStrategy {

    /**
     * Recommend that all of the returned bundles be unloaded.
     * @param loadData The load data to used to make the unloading decision.
     * @param conf The service configuration.
     * @return A set of all the bundles that should be unloaded.
     */
    Set<String> selectBundlesForUnloading(LoadData loadData, ServiceConfiguration conf);

    /**
     * Create a LoadSheddingStrategy from the given configuration.
     * @param conf The configuration to create the strategy from.
     * @return The created strategy.
     */
    static LoadSheddingStrategy create(final ServiceConfiguration conf) {
        // TODO
        return null;
    }
}