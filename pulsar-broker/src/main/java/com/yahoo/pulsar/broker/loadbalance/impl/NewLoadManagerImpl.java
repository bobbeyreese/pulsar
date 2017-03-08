package com.yahoo.pulsar.broker.loadbalance.impl;

import com.yahoo.pulsar.broker.*;
import com.yahoo.pulsar.broker.loadbalance.BrokerHostUsage;
import com.yahoo.pulsar.broker.loadbalance.NewLoadManager;
import com.yahoo.pulsar.broker.loadbalance.NewPlacementStrategy;
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
import java.util.Iterator;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;

public class NewLoadManagerImpl implements NewLoadManager, ZooKeeperCacheListener<BrokerData> {
    public static final String BUNDLE_DATA_ZPATH = "/loadbalance/bundle-data";
    public static final String DEFAULT_BUNDLE_DATA_ZPATH = "/loadbalance/bundle-data/default";

    private static final int MIBI = 1024 * 1024;
    private static final Logger log = LoggerFactory.getLogger(NewLoadManagerImpl.class);

    private final BrokerData localData;
    private final Map<String, BrokerData> brokerData;
    private final Map<String, BundleData> bundleData;
    private final Map<String, Map<String, BundleData>> preallocatedBundleData;
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
        preallocatedBundleData = new ConcurrentHashMap<>();
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
                                if (data.getLastStats().containsKey(preallocatedIterator.next().getKey())) {
                                    preallocatedIterator.remove();
                                }
                            }
                        }
                    } catch (Exception e) {
                        log.warn("Error reading load report from Cache for broker - [{}], [{}]", broker, e);
                    }
                }
            }
        } catch (Exception e) {
            log.warn("Error reading active brokers list from zookeeper while re-ranking load reports [{}]", e);
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
                    final BundleData newBundleData =
                            new BundleData(conf.getNumShortSamples(), conf.getNumLongSamples());
                    newBundleData.update(stats);
                    bundleData.put(bundle, newBundleData);
                }
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
        // TODO
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
    public String selectBrokerForAssignment(final String bundle) {
        final BundleData data = bundleData.computeIfAbsent(bundle,
                key -> new BundleData(conf.getNumShortSamples(), conf.getNumLongSamples(), defaultStats));
        final String broker = placementStrategy.selectBroker(brokerData, data, preallocatedBundleData);

        // Add new broker to preallocated.
        preallocatedBundleData.computeIfAbsent(broker, key -> new ConcurrentHashMap<>()).put(bundle, data);
        return broker;
    }

    @Override
    public void start() throws PulsarServerException {
        try {
            // Register the brokers in zk list
            createZPathIfNotExists(zkClient, SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT);

            String lookupServiceAddress = pulsar.getAdvertisedAddress() + ":" + conf.getWebServicePort();
            brokerZnodePath = SimpleLoadManagerImpl.LOADBALANCE_BROKERS_ROOT + "/" + lookupServiceAddress;
            updateLocalBrokerData();
            try {
                ZkUtils.createFullPathOptimistic(pulsar.getZkClient(), brokerZnodePath, localData.getJsonBytes(),
                        ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.EPHEMERAL);
            } catch (Exception e) {
                // Catching exception here to print the right error message
                log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
                throw e;
            }
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
        }
    }

}
