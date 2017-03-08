package com.yahoo.pulsar.broker.loadbalance;

import com.yahoo.pulsar.broker.PulsarServerException;

public interface NewLoadManager {

    void disableBroker() throws PulsarServerException;

    void doLoadShedding();

    void doNamespaceBundleSplit();

    String selectBrokerForAssignment(String bundleToAssign);

    void updateLocalBrokerData();

    void start() throws PulsarServerException;

    void stop() throws PulsarServerException;

    void writeBrokerDataOnZooKeeper();

    void writeBundleDataOnZooKeeper();
}
