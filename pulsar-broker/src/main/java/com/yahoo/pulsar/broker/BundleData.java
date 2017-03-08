package com.yahoo.pulsar.broker;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;

public class BundleData extends JSONWritable {
    private TimeAverageMessageData shortTermData;
    private TimeAverageMessageData longTermData;

    // For JSON only.
    public BundleData(){}

    public BundleData(final int numShortSamples, final int numLongSamples) {
        shortTermData = new TimeAverageMessageData(numShortSamples);
        longTermData = new TimeAverageMessageData(numLongSamples);
    }

    public BundleData(final int numShortSamples, final int numLongSamples, final NamespaceBundleStats defaultStats) {
        shortTermData = new TimeAverageMessageData(numShortSamples, defaultStats);
        longTermData = new TimeAverageMessageData(numLongSamples, defaultStats);
    }

    public void update(final NamespaceBundleStats newSample) {
        shortTermData.update(newSample);
        longTermData.update(newSample);
    }

    public TimeAverageMessageData getShortTermData() {
        return shortTermData;
    }

    public void setShortTermData(TimeAverageMessageData shortTermData) {
        this.shortTermData = shortTermData;
    }

    public TimeAverageMessageData getLongTermData() {
        return longTermData;
    }

    public void setLongTermData(TimeAverageMessageData longTermData) {
        this.longTermData = longTermData;
    }
}
