package com.yahoo.pulsar.broker.loadbalance.impl;

import com.yahoo.pulsar.broker.*;
import com.yahoo.pulsar.broker.loadbalance.*;
import com.yahoo.pulsar.common.policies.data.loadbalancer.NamespaceBundleStats;
import com.yahoo.pulsar.common.policies.data.loadbalancer.SystemResourceUsage;
import com.yahoo.pulsar.common.util.ObjectMapperFactory;
import com.yahoo.pulsar.zookeeper.ZooKeeperCacheListener;
import com.yahoo.pulsar.zookeeper.ZooKeeperChildrenCache;
import com.yahoo.pulsar.zookeeper.ZooKeeperDataCache;
import org.apache.bookkeeper.util.ZkUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.SystemUtils;
import org.apache.zookeeper.CreateMode;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.ZooDefs;
import org.apache.zookeeper.ZooKeeper;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class NewLoadManagerImpl implements NewLoadManager, ZooKeeperCacheListener<BrokerData> {
    public static final String TIME_AVERAGE_BROKER_ZPATH = "/loadbalance/broker-time-average";
    public static final String BUNDLE_DATA_ZPATH = "/loadbalance/bundle-data";
    public static final String DEFAULT_BUNDLE_DATA_ZPATH = "/loadbalance/bundle-data/default";

    private static final int MIBI = 1024 * 1024;
    private static final Logger log = LoggerFactory.getLogger(NewLoadManagerImpl.class);

    private final BrokerData localData;
    private final Map<String, BrokerData> brokerData;
    private final Map<String, TimeAverageBrokerData> timeAverageBrokerData;
    private final Map<String, BundleData> bundleData;
    private final Map<String, Map<String, BundleData>> preallocatedBundleData;
    private final Map<String, String> preallocatedBundleToBroker;
    private final Set<String> brokerCandidateCache;
    private final List<BrokerFilter> filterPipeline;
    private final List<LoadSheddingStrategy> loadSheddingPipeline;
    private final NewPlacementStrategy placementStrategy;
    private final PulsarService pulsar;
    private final ZooKeeper zkClient;
    private final ServiceConfiguration conf;
    private final BrokerHostUsage brokerHostUsage;
    private final ZooKeeperDataCache<BrokerData> brokerDataCache;
    private final ZooKeeperChildrenCache availableActiveBrokers;
    private final ScheduledExecutorService scheduler;
    private final NamespaceBundleStats defaultStats;
    private long lastBundleDataUpdate;

    private String brokerZnodePath;
    private SystemResourceUsage baselineSystemResourceUsage;

    public NewLoadManagerImpl(final PulsarService pulsar) {
        this.pulsar = pulsar;
        zkClient = pulsar.getZkClient();
        conf = pulsar.getConfiguration();
        brokerData = new ConcurrentHashMap<>();
        bundleData = new ConcurrentHashMap<>();
        timeAverageBrokerData = new ConcurrentHashMap<>();
        preallocatedBundleData = new ConcurrentHashMap<>();
        preallocatedBundleToBroker = new ConcurrentHashMap<>();
        brokerCandidateCache = new HashSet<>();
        filterPipeline = new ArrayList<>();
        loadSheddingPipeline = new ArrayList<>();
        localData = new BrokerData(conf.getNumShortSamples(), conf.getNumLongSamples(), pulsar.getWebServiceAddress(),
                pulsar.getWebServiceAddressTls(), pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls());
        placementStrategy = NewPlacementStrategy.create(conf);
        defaultStats = new NamespaceBundleStats();
        defaultStats.msgThroughputIn = conf.getDefaultMsgThroughputIn();
        defaultStats.msgThroughputOut = conf.getDefaultMsgThroughputOut();
        defaultStats.msgRateIn = conf.getDefaultMsgRateIn();
        defaultStats.msgRateOut = conf.getDefaultMsgRateOut();
        if (SystemUtils.IS_OS_LINUX) {
            brokerHostUsage = new LinuxBrokerHostUsageImpl(pulsar);
        } else {
            brokerHostUsage = new GenericBrokerHostUsageImpl(pulsar);
        }

        brokerDataCache = new ZooKeeperDataCache<BrokerData>(pulsar.getLocalZkCache()) {
            @Override
            public BrokerData deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, BrokerData.class);
            }
        };
        brokerDataCache.registerListener(this);
        availableActiveBrokers = new ZooKeeperChildrenCache(pulsar.getLocalZkCache(),
                SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT);
        availableActiveBrokers.registerListener(new ZooKeeperCacheListener<Set<String>>() {
            @Override
            public void onUpdate(String path, Set<String> data, Stat stat) {
                if (log.isDebugEnabled()) {
                    log.debug("Update Received for path {}", path);
                }
                scheduler.submit(NewLoadManagerImpl.this::updateAllBrokerData);
            }
        });
        scheduler = Executors.newScheduledThreadPool(1);
    }

    private void updateAllBrokerData() {
        try {
            synchronized (brokerData) {
                brokerData.clear();
                Set<String> activeBrokers = availableActiveBrokers.get();
                for (String broker : activeBrokers) {
                    try {
                        String key = String.format("%s/%s", SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT, broker);
                        final BrokerData data = brokerDataCache.get(key)
                                .orElseThrow(KeeperException.NoNodeException::new);
                        brokerData.put(broker, data);
                        if (preallocatedBundleData.containsKey(broker)) {
                            final Iterator<Map.Entry<String, BundleData>> preallocatedIterator =
                                    preallocatedBundleData.get(broker).entrySet().iterator();
                            while (preallocatedIterator.hasNext()) {
                                final String bundle = preallocatedIterator.next().getKey();
                                if (data.getLastStats().containsKey(bundle)) {
                                    preallocatedIterator.remove();
                                    preallocatedBundleToBroker.remove(bundle);
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Error reading broker data from cache for broker - [{}], [{}]", broker, e);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error reading active brokers list from zookeeper while updating broker data [{}]", e);
        }
    }

    private Map<String, NamespaceBundleStats> getBundleStats() {
        return pulsar.getBrokerService().getBundleStats();
    }

    public void updateLocalBrokerData() {
        try {
            final SystemResourceUsage systemResourceUsage = getSystemResourceUsage();
            localData.update(systemResourceUsage, getBundleStats());
        } catch (Exception e) {
            log.warn("Error when attempting to update local broker data: {}", e);
        }
    }

    public void updateBundleData() {
        for (Map.Entry<String, BrokerData> brokerEntry: brokerData.entrySet()) {
            final String broker = brokerEntry.getKey();
            final BrokerData data = brokerEntry.getValue();
            final Map<String, NamespaceBundleStats> statsMap = data.getLastStats();
            for (Map.Entry<String, NamespaceBundleStats> entry: statsMap.entrySet()) {
                final String bundle = entry.getKey();
                final NamespaceBundleStats stats = entry.getValue();
                if (bundleData.containsKey(bundle)) {
                    bundleData.get(bundle).update(stats);
                } else {
                    BundleData currentBundleData = null;
                    try {
                        final String bundleZPath = getBundleDataZooKeeperPath(bundle);
                        if (zkClient.exists(bundleZPath, null) != null) {
                            currentBundleData = ObjectMapperFactory.getThreadLocal()
                                    .readValue(zkClient.getData(bundleZPath, null, null), BundleData.class);
                        }
                    } catch (Exception e) {
                        log.warn("Error when trying to check for existing bundle data for bundle {}: {}", bundle, e);
                    }
                    if (currentBundleData == null) {
                        currentBundleData = new BundleData(conf.getNumShortSamples(), conf.getNumLongSamples());
                    }
                    currentBundleData.update(stats);
                    bundleData.put(bundle, currentBundleData);
                }
            }
            if (timeAverageBrokerData.containsKey(broker)) {
                timeAverageBrokerData.get(broker).reset(statsMap.keySet(), bundleData, defaultStats);
            } else {
                timeAverageBrokerData.put(broker, new TimeAverageBrokerData(statsMap.keySet(), bundleData,
                        defaultStats));
            }
        }
    }

    private boolean needBrokerDataUpdate() {
        return System.currentTimeMillis() >
                localData.getLastUpdate() + conf.getBrokerDataUpdateIntervalSeconds() * 1000;
    }

    private boolean needBundleDataUpdate() {
        return System.currentTimeMillis() > lastBundleDataUpdate + conf.getBundleDataUpdateIntervalSeconds() * 1000;
    }

    private static void createZPathIfNotExists(final ZooKeeper zkClient, final String path) throws Exception {
        if (zkClient.exists(path, false) == null) {
            try {
                ZkUtils.createFullPathOptimistic(zkClient, path, new byte[0], ZooDefs.Ids.OPEN_ACL_UNSAFE,
                        CreateMode.PERSISTENT);
            } catch (KeeperException.NodeExistsException e) {
                // Ignore if already exists.
            }
        }
    }

    public static String getBundleDataZooKeeperPath(final String bundle) {
        return BUNDLE_DATA_ZPATH + "/" + bundle;
    }



    private static long getRealtimeJvmHeapUsageBytes() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    private SystemResourceUsage getSystemResourceUsage() throws IOException {
        SystemResourceUsage systemResourceUsage = brokerHostUsage.getBrokerHostUsage();

        // Override System memory usage and limit with JVM heap usage and limit
        long maxHeapMemoryInBytes = Runtime.getRuntime().maxMemory();
        long memoryUsageInBytes = getRealtimeJvmHeapUsageBytes();
        systemResourceUsage.memory.usage = (double) memoryUsageInBytes / MIBI;
        systemResourceUsage.memory.limit = (double) maxHeapMemoryInBytes / MIBI;

        // Collect JVM direct memory
        systemResourceUsage.directMemory.usage = (double) (sun.misc.SharedSecrets.getJavaNioAccess()
                .getDirectBufferPool().getMemoryUsed() / MIBI);
        systemResourceUsage.directMemory.limit = (double) (sun.misc.VM.maxDirectMemory() / MIBI);

        return systemResourceUsage;
    }

    @Override
    public void disableBroker() throws PulsarServerException {
        if (StringUtils.isNotEmpty(brokerZnodePath)) {
            try {
                pulsar.getZkClient().delete(brokerZnodePath, -1);
            } catch (Exception e) {
                throw new PulsarServerException(e);
            }
        }
    }

    @Override
    public void doLoadShedding() {
        for (LoadSheddingStrategy strategy: loadSheddingPipeline) {
            final Set<String> namespacesToUnload = strategy.selectBundlesForUnloading(brokerData,
                    preallocatedBundleData, timeAverageBrokerData, conf);
            if (namespacesToUnload != null && !namespacesToUnload.isEmpty()) {
                // TODO: Write code to unload the bundles.
                return;
            }
        }
    }

    @Override
    public void doNamespaceBundleSplit() {
        // TODO?
    }

    @Override
    public void onUpdate(final String path, final BrokerData data, final Stat stat) {
        scheduler.submit(this::updateAllBrokerData);
    }

    @Override
    public synchronized String selectBrokerForAssignment(final String bundle) {
        if (preallocatedBundleToBroker.containsKey(bundle)) {
            return preallocatedBundleToBroker.get(bundle);
        }
        final BundleData data = bundleData.computeIfAbsent(bundle,
                key -> new BundleData(conf.getNumShortSamples(), conf.getNumLongSamples(), defaultStats));
        brokerCandidateCache.clear();
        brokerCandidateCache.addAll(brokerData.keySet());
        for (BrokerFilter filter: filterPipeline) {
            filter.filter(brokerCandidateCache, brokerData, data, preallocatedBundleData, timeAverageBrokerData, conf);
        }
        final String broker = placementStrategy.selectBroker(brokerCandidateCache, brokerData, data,
                preallocatedBundleData, timeAverageBrokerData, conf);

        // Add new bundle to preallocated.
        preallocatedBundleData.computeIfAbsent(broker, key -> new ConcurrentHashMap<>()).put(bundle, data);
        preallocatedBundleToBroker.put(bundle, broker);
        return broker;
    }

    @Override
    public void start() throws PulsarServerException {
        try {
            // Register the brokers in zk list
            createZPathIfNotExists(zkClient, SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT);

            String lookupServiceAddress = pulsar.getAdvertisedAddress() + ":" + conf.getWebServicePort();
            brokerZnodePath = SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT + "/" + lookupServiceAddress;
            final String timeAverageZPath = TIME_AVERAGE_BROKER_ZPATH + "/" + lookupServiceAddress;
            updateLocalBrokerData();
            try {
                ZkUtils.createFullPathOptimistic(pulsar.getZkClient(), brokerZnodePath, localData.getJsonBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (Exception e) {
                // Catching exception here to print the right error message
                log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
                throw e;
            }
            createZPathIfNotExists(zkClient, timeAverageZPath);
            zkClient.setData(timeAverageZPath, (new TimeAverageBrokerData()).getJsonBytes(), -1);
            updateAllBrokerData();
            lastBundleDataUpdate = System.currentTimeMillis();
            baselineSystemResourceUsage = getSystemResourceUsage();
        } catch (Exception e) {
            log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
            throw new PulsarServerException(e);
        }
    }

    @Override
    public void stop() throws PulsarServerException {
        // Do nothing.
    }

    @Override
    public void writeBrokerDataOnZooKeeper() {
        try {
            if (needBrokerDataUpdate()) {
                updateLocalBrokerData();
                zkClient.setData(brokerZnodePath, localData.getJsonBytes(), -1);
            }
        } catch(Exception e) {
            log.warn("Error writing broker data on ZooKeeper: {}", e);
        }
    }

    @Override
    public void writeBundleDataOnZooKeeper() {
        if (needBundleDataUpdate()) {
            updateBundleData();
            for (Map.Entry<String, BundleData> entry: bundleData.entrySet()) {
                final String bundle = entry.getKey();
                final BundleData data = entry.getValue();
                try {
                    final String zooKeeperPath = getBundleDataZooKeeperPath(bundle);
                    createZPathIfNotExists(zkClient, zooKeeperPath);
                    zkClient.setData(zooKeeperPath, data.getJsonBytes(), -1);
                } catch (Exception e) {
                    log.warn("Error when writing data for bundle {} to ZooKeeper: {}", bundle, e);
                }
            }
            for (Map.Entry<String, TimeAverageBrokerData> entry: timeAverageBrokerData.entrySet()) {
                final String broker = entry.getKey();
                final TimeAverageBrokerData data = entry.getValue();
                try {
                    final String zooKeeperPath = TIME_AVERAGE_BROKER_ZPATH + "/" + broker;
                    createZPathIfNotExists(zkClient, zooKeeperPath);
                    zkClient.setData(zooKeeperPath, data.getJsonBytes(), -1);
                } catch (Exception e) {
                    log.warn("Error when writing time average broker data for {} to ZooKeeper: {}", broker, e);
                }
            }
        }
    }

}
