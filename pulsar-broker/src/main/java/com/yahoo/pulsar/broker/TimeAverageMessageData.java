package com.yahoo.pulsar.broker;

import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;

public class TimeAverageMessageData {
    private int maxSamples;
    private int numSamples;

    private double msgThroughputIn;
    private double msgThroughputOut;
    private double msgRateIn;
    private double msgRateOut;

    // For JSON only.
    public TimeAverageMessageData(){}

    public TimeAverageMessageData(final int maxSamples){
        this.maxSamples = maxSamples;
    }

    public TimeAverageMessageData(final int maxSamples, final NamespaceBundleStats defaultStats) {
        this.maxSamples = maxSamples;
        msgThroughputIn = defaultStats.msgThroughputIn;
        msgThroughputOut = defaultStats.msgThroughputOut;
        msgRateIn = defaultStats.msgRateIn;
        msgRateOut = defaultStats.msgRateOut;
    }

    public void update(final double newMsgThroughputIn, final double newMsgThroughputOut, final double newMsgRateIn,
                       final double newMsgRateOut) {
        numSamples = Math.min(numSamples + 1, maxSamples);
        msgThroughputIn = getUpdatedValue(msgThroughputIn, newMsgThroughputIn);
        msgThroughputOut = getUpdatedValue(msgThroughputOut, newMsgThroughputOut);
        msgRateIn = getUpdatedValue(msgRateIn, newMsgRateIn);
        msgRateOut = getUpdatedValue(msgRateOut, newMsgRateOut);
    }

    public void update(final NamespaceBundleStats newSample) {
        update(newSample.msgThroughputIn, newSample.msgThroughputOut, newSample.msgRateIn, newSample.msgRateOut);
    }

    private double getUpdatedValue(final double oldSample, final double newSample) {
        return ((numSamples - 1) * oldSample + newSample) / numSamples;
    }

    public int getMaxSamples() {
        return maxSamples;
    }

    public void setMaxSamples(int maxSamples) {
        this.maxSamples = maxSamples;
    }

    public int getNumSamples() {
        return numSamples;
    }

    public void setNumSamples(int numSamples) {
        this.numSamples = numSamples;
    }

    public double getMsgThroughputIn() {
        return msgThroughputIn;
    }

    public void setMsgThroughputIn(double msgThroughputIn) {
        this.msgThroughputIn = msgThroughputIn;
    }

    public double getMsgThroughputOut() {
        return msgThroughputOut;
    }

    public void setMsgThroughputOut(double msgThroughputOut) {
        this.msgThroughputOut = msgThroughputOut;
    }

    public double getMsgRateIn() {
        return msgRateIn;
    }

    public void setMsgRateIn(double msgRateIn) {
        this.msgRateIn = msgRateIn;
    }

    public double getMsgRateOut() {
        return msgRateOut;
    }

    public void setMsgRateOut(double msgRateOut) {
        this.msgRateOut = msgRateOut;
    }
}
