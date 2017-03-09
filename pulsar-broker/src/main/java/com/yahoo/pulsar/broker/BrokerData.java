package com.yahoo.pulsar.broker;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;
import com.yahoo.pulsar.common.policies.data.loadbalancer.ResourceUsage;
import com.yahoo.pulsar.common.policies.data.loadbalancer.ServiceLookupData;
import com.yahoo.pulsar.common.policies.data.loadbalancer.SystemResourceUsage;

import java.util.*;

public class BrokerData extends JSONWritable implements ServiceLookupData {

    private final String webServiceUrl;
    private final String webServiceUrlTls;
    private final String pulsarServiceUrl;
    private final String pulsarServiceUrlTls;
    private ResourceUsage cpu;
    private ResourceUsage memory;
    private ResourceUsage directMemory;
    private TimeAverageMessageData shortTermData;
    private TimeAverageMessageData longTermData;
    private long lastUpdate;
    private Map<String, NamespaceBundleStats> lastStats;
    private int numTopics;
    private int numBundles;
    private int numConsumers;
    private int numProducers;

    private Set<String> bundles;

    private Set<String> lastBundleGains;
    private Set<String> lastBundleLosses;

    // For JSON only.
    public BrokerData(){
        this(0, 0, null, null, null, null);
    }

    public BrokerData(final int numShortSamples, final int numLongSamples, final String webServiceUrl,
                      final String webServiceUrlTls, final String pulsarServiceUrl, final String pulsarServiceUrlTls) {
        this.webServiceUrl = webServiceUrl;
        this.webServiceUrlTls = webServiceUrlTls;
        this.pulsarServiceUrl = pulsarServiceUrl;
        this.pulsarServiceUrlTls = pulsarServiceUrlTls;
        lastStats = new HashMap<>();
        lastUpdate = System.currentTimeMillis();
        cpu = new ResourceUsage();
        memory = new ResourceUsage();
        directMemory = new ResourceUsage();
        bundles = new HashSet<>();
        lastBundleGains = new HashSet<>();
        lastBundleLosses = new HashSet<>();
        shortTermData = new TimeAverageMessageData(numShortSamples);
        longTermData = new TimeAverageMessageData(numLongSamples);
    }

    public void update(final SystemResourceUsage systemResourceUsage,
                       final Map<String, NamespaceBundleStats> bundleStats) {
        updateSystemResourceUsage(systemResourceUsage);
        updateBundleData(bundleStats);
        lastStats = bundleStats;
        lastUpdate = System.currentTimeMillis();
    }

    private void updateSystemResourceUsage(final SystemResourceUsage systemResourceUsage) {
        this.cpu = systemResourceUsage.cpu;
        this.memory = systemResourceUsage.memory;
        this.directMemory = systemResourceUsage.directMemory;
    }

    private void updateBundleData(final Map<String, NamespaceBundleStats> bundleStats) {
        double totalMsgThroughputIn = 0;
        double totalMsgThroughputOut = 0;
        double totalMsgRateIn = 0;
        double totalMsgRateOut = 0;
        int totalNumTopics = 0;
        int totalNumBundles = 0;
        int totalNumConsumers = 0;
        int totalNumProducers = 0;
        lastBundleGains.clear();
        lastBundleLosses.clear();
        final Iterator<String> oldBundleIterator = bundles.iterator();
        while (oldBundleIterator.hasNext()) {
            final String bundle = oldBundleIterator.next();
            if (!bundleStats.containsKey(bundle)) {
                lastBundleLosses.add(bundle);
                oldBundleIterator.remove();
            }
        }
        for (Map.Entry<String, NamespaceBundleStats> entry: bundleStats.entrySet()) {
            final String bundle = entry.getKey();
            final NamespaceBundleStats stats = entry.getValue();
            if (!bundles.contains(bundle)) {
                lastBundleGains.add(bundle);
                bundles.add(bundle);
            }
            totalMsgThroughputIn += stats.msgThroughputIn;
            totalMsgThroughputOut += stats.msgThroughputOut;
            totalMsgRateIn += stats.msgRateIn;
            totalMsgRateOut += stats.msgRateOut;
            totalNumTopics += stats.topics;
            ++totalNumBundles;
            totalNumConsumers += stats.consumerCount;
            totalNumProducers += stats.producerCount;
        }
        numTopics = totalNumTopics;
        numBundles = totalNumBundles;
        numConsumers = totalNumConsumers;
        numProducers = totalNumProducers;
        shortTermData.update(totalMsgThroughputIn, totalMsgThroughputOut, totalMsgRateIn, totalMsgRateOut);
        longTermData.update(totalMsgThroughputIn, totalMsgThroughputOut, totalMsgRateIn, totalMsgRateOut);
    }

    public ResourceUsage getCpu() {
        return cpu;
    }

    public void setCpu(ResourceUsage cpu) {
        this.cpu = cpu;
    }

    public ResourceUsage getMemory() {
        return memory;
    }

    public void setMemory(ResourceUsage memory) {
        this.memory = memory;
    }

    public ResourceUsage getDirectMemory() {
        return directMemory;
    }

    public void setDirectMemory(ResourceUsage directMemory) {
        this.directMemory = directMemory;
    }

    public Set<String> getLastBundleGains() {
        return lastBundleGains;
    }

    public void setLastBundleGains(Set<String> lastBundleGains) {
        this.lastBundleGains = lastBundleGains;
    }

    public Set<String> getLastBundleLosses() {
        return lastBundleLosses;
    }

    public void setLastBundleLosses(Set<String> lastBundleLosses) {
        this.lastBundleLosses = lastBundleLosses;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public Set<String> getBundles() {
        return bundles;
    }

    public void setBundles(Set<String> bundles) {
        this.bundles = bundles;
    }

    public TimeAverageMessageData getShortTermData() {
        return shortTermData;
    }

    public TimeAverageMessageData getLongTermData() {
        return longTermData;
    }

    public void setShortTermData(TimeAverageMessageData shortTermData) {
        this.shortTermData = shortTermData;
    }

    public void setLongTermData(TimeAverageMessageData longTermData) {
        this.longTermData = longTermData;
    }

    public Map<String, NamespaceBundleStats> getLastStats() {
        return lastStats;
    }

    public void setLastStats(Map<String, NamespaceBundleStats> lastStats) {
        this.lastStats = lastStats;
    }

    public int getNumTopics() {
        return numTopics;
    }

    public void setNumTopics(int numTopics) {
        this.numTopics = numTopics;
    }

    public int getNumBundles() {
        return numBundles;
    }

    public void setNumBundles(int numBundles) {
        this.numBundles = numBundles;
    }

    public int getNumConsumers() {
        return numConsumers;
    }

    public void setNumConsumers(int numConsumers) {
        this.numConsumers = numConsumers;
    }

    public int getNumProducers() {
        return numProducers;
    }

    public void setNumProducers(int numProducers) {
        this.numProducers = numProducers;
    }

    @Override
    public String getWebServiceUrl() {
        return webServiceUrl;
    }

    @Override
    public String getWebServiceUrlTls() {
        return webServiceUrlTls;
    }

    @Override
    public String getPulsarServiceUrl() {
        return pulsarServiceUrl;
    }

    @Override
    public String getPulsarServiceUrlTls() {
        return pulsarServiceUrlTls;
    }
}
