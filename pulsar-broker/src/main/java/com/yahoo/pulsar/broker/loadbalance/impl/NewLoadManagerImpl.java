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

public class NewLoadManagerImpl implements NewLoadManager, ZooKeeperCacheListener<LocalBrokerData> {
    public static final String LOADBALANCE_BROKERS_ROOT = "/loadbalance/new-brokers";
    public static final String TIME_AVERAGE_BROKER_ZPATH = "/loadbalance/broker-time-average";
    public static final String BUNDLE_DATA_ZPATH = "/loadbalance/bundle-data";

    private static final int MIBI = 1024 * 1024;
    private static final Logger log = LoggerFactory.getLogger(NewLoadManagerImpl.class);

    private final LocalBrokerData localData;
    private final LoadData loadData;

    // Used to determine whether a bundle is preallocated.
    private final Map<String, String> preallocatedBundleToBroker;

    // Set of broker candidates to reuse so that object creation is avoided.
    private final Set<String> brokerCandidateCache;

    // Used to filter brokers from being selected for assignment.
    private final List<BrokerFilter> filterPipeline;

    // Pipeline used to determine what namespaces, if any, should be unloaded.
    private final List<LoadSheddingStrategy> loadSheddingPipeline;

    // Strategy used to determine where new topics should be placed.
    private final NewPlacementStrategy placementStrategy;

    private final PulsarService pulsar;
    private final ZooKeeper zkClient;
    private final ServiceConfiguration conf;
    private final BrokerHostUsage brokerHostUsage;
    private final ZooKeeperDataCache<LocalBrokerData> brokerDataCache;
    private final ZooKeeperChildrenCache availableActiveBrokers;
    private final ScheduledExecutorService scheduler;

    // The default bundle stats which are used to initialize historic data.
    // This data is overriden after the bundle receives its first sample.
    private final NamespaceBundleStats defaultStats;

    // Timestamp of last invocation of updateBundleData.
    private long lastBundleDataUpdate;

    private String brokerZnodePath;
    private final String brokerRoot;

    // System resource usage directly after starting.
    private SystemResourceUsage baselineSystemResourceUsage;

    public NewLoadManagerImpl(final PulsarService pulsar) {
        this(pulsar, LOADBALANCE_BROKERS_ROOT);
    }

    /**
     * Initialize this load manager.
     * @param pulsar Client to construct this manager from.
     * @param brokerRoot ZooKeeper path containing some data implementing ServiceLookup.
     */
    public NewLoadManagerImpl(final PulsarService pulsar, final String brokerRoot) {
        this.brokerRoot = brokerRoot;
        this.pulsar = pulsar;
        zkClient = pulsar.getZkClient();
        conf = pulsar.getConfiguration();

        loadData = new LoadData();
        preallocatedBundleToBroker = new ConcurrentHashMap<>();
        brokerCandidateCache = new HashSet<>();
        filterPipeline = new ArrayList<>();
        loadSheddingPipeline = new ArrayList<>();
        localData = new LocalBrokerData(pulsar.getWebServiceAddress(), pulsar.getWebServiceAddressTls(),
                pulsar.getBrokerServiceUrl(), pulsar.getBrokerServiceUrlTls());
        placementStrategy = NewPlacementStrategy.create(conf);

        // Initialize the default
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

        brokerDataCache = new ZooKeeperDataCache<LocalBrokerData>(pulsar.getLocalZkCache()) {
            @Override
            public LocalBrokerData deserialize(String key, byte[] content) throws Exception {
                return ObjectMapperFactory.getThreadLocal().readValue(content, LocalBrokerData.class);
            }
        };
        brokerDataCache.registerListener(this);
        availableActiveBrokers = new ZooKeeperChildrenCache(pulsar.getLocalZkCache(), brokerRoot);
        availableActiveBrokers.registerListener(new ZooKeeperCacheListener<Set<String>>() {
            @Override
            public void onUpdate(String path, Set<String> data, Stat stat) {
                if (log.isDebugEnabled()) {
                    log.debug("Update Received for path {}", path);
                }
                scheduler.submit(NewLoadManagerImpl.this::updateAll);
            }
        });
        scheduler = Executors.newScheduledThreadPool(1);
    }

    /*
     * As the leader broker, update the broker data map in loadData by querying ZooKeeper for the broker data put there
     * by each broker via updateLocalBrokerData.
     */
    private void updateAllBrokerData() {
        try {
            Set<String> activeBrokers = availableActiveBrokers.get();
            final Map<String, BrokerData> brokerDataMap = loadData.getBrokerData();
            for (String broker: activeBrokers) {
                try {
                    String key = String.format("%s/%s", brokerRoot, broker);
                    final LocalBrokerData localData = brokerDataCache.get(key)
                            .orElseThrow(KeeperException.NoNodeException::new);

                    if (brokerDataMap.containsKey(broker)) {
                        // Replace previous local broker data.
                        loadData.getBrokerData().get(broker).setLocalData(localData);
                    } else {
                        // Initialize BrokerData object for previously unseen brokers.
                        brokerDataMap.put(broker, new BrokerData(localData));
                    }
                } catch (Exception e) {
                    log.warn("Error reading broker data from cache for broker - [{}], [{}]", broker, e);
                }
            }
        } catch (Exception e) {
            log.warn("Error reading active brokers list from zookeeper while updating broker data [{}]", e);
        }
    }

    /*
     * Use the Pulsar client to acquire the namespace bundle stats.
     */
    private Map<String, NamespaceBundleStats> getBundleStats() {
        return pulsar.getBrokerService().getBundleStats();
    }

    /**
     * Update both the broker data and the bundle data.
     */
    public void updateAll() {
        updateAllBrokerData();
        updateBundleData();
    }

    /**
     * As the leader broker, use the local broker data saved on ZooKeeper to update the bundle stats so that better
     * load management decisions may be made.
     */
    public void updateBundleData() {
        final Map<String, BundleData> bundleData = loadData.getBundleData();
        // Iterate over the broker data.
        for (Map.Entry<String, BrokerData> brokerEntry: loadData.getBrokerData().entrySet()) {
            final String broker = brokerEntry.getKey();
            final BrokerData brokerData = brokerEntry.getValue();
            final Map<String, NamespaceBundleStats> statsMap = brokerData.getLocalData().getLastStats();

            // Iterate over the last bundle stats available to the current broker to update the bundle data.
            for (Map.Entry<String, NamespaceBundleStats> entry: statsMap.entrySet()) {
                final String bundle = entry.getKey();
                final NamespaceBundleStats stats = entry.getValue();
                if (bundleData.containsKey(bundle)) {
                    // If we recognize the bundle, add these stats as a new sample.
                    bundleData.get(bundle).update(stats);
                } else {
                    // Otherwise, attempt to find the bundle data on ZooKeeper.
                    // If it cannot be found, use the latest stats as the first sample.
                    BundleData currentBundleData = getBundleDataOrDefault(bundle);
                    currentBundleData.update(stats);
                    bundleData.put(bundle, currentBundleData);
                }
            }

            // Remove all loaded bundles from the preallocated maps.
            final Map<String, BundleData> preallocatedBundleData = brokerData.getPreallocatedBundleData();
            if (preallocatedBundleData.containsKey(broker)) {
                final Iterator<Map.Entry<String, BundleData>> preallocatedIterator =
                        preallocatedBundleData.entrySet().iterator();
                while (preallocatedIterator.hasNext()) {
                    final String bundle = preallocatedIterator.next().getKey();
                    if (bundleData.containsKey(bundle)) {
                        preallocatedIterator.remove();
                        preallocatedBundleToBroker.remove(bundle);
                    }
                }
            }

            // Using the newest data, update the aggregated time-average data for the current broker.
            brokerData.getTimeAverageData().reset(statsMap.keySet(), bundleData, defaultStats);
        }
    }

    // Determine if the broker data requires an update by measuring the time past since the last update.
    private boolean needBrokerDataUpdate() {
        return System.currentTimeMillis() >
                localData.getLastUpdate() + conf.getBrokerDataUpdateIntervalSeconds() * 1000;
    }

    // Determine if the bundle data requires an update by measuring the time past since the last update.
    private boolean needBundleDataUpdate() {
        return System.currentTimeMillis() > lastBundleDataUpdate + conf.getBundleDataUpdateIntervalSeconds() * 1000;
    }

    // Attempt to create a ZooKeeper path if it does not exist.
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

    // Get the ZooKeeper path for the given bundle full name.
    public static String getBundleDataZooKeeperPath(final String bundle) {
        return BUNDLE_DATA_ZPATH + "/" + bundle;
    }

    // Get the total number of used bytes in the JVM.
    private static long getRealtimeJVMHeapUsageBytes() {
        return Runtime.getRuntime().totalMemory() - Runtime.getRuntime().freeMemory();
    }

    // Get the system resource usage for this broker.
    private SystemResourceUsage getSystemResourceUsage() throws IOException {
        SystemResourceUsage systemResourceUsage = brokerHostUsage.getBrokerHostUsage();

        // Override System memory usage and limit with JVM heap usage and limit
        long maxHeapMemoryInBytes = Runtime.getRuntime().maxMemory();
        long memoryUsageInBytes = getRealtimeJVMHeapUsageBytes();
        systemResourceUsage.memory.usage = (double) memoryUsageInBytes / MIBI;
        systemResourceUsage.memory.limit = (double) maxHeapMemoryInBytes / MIBI;

        // Collect JVM direct memory
        systemResourceUsage.directMemory.usage = (double) (sun.misc.SharedSecrets.getJavaNioAccess()
                .getDirectBufferPool().getMemoryUsed() / MIBI);
        systemResourceUsage.directMemory.limit = (double) (sun.misc.VM.maxDirectMemory() / MIBI);

        return systemResourceUsage;
    }

    // Use the thread local ObjectMapperFactory to read the given json data into an instance of the given class.
    private static <T> T readJson(final byte[] data, final Class<T> clazz) throws IOException {
        return ObjectMapperFactory.getThreadLocal().readValue(data, clazz);
    }

    // Attempt to local the data for the given bundle in ZooKeeper.
    // If it cannot be found, return the default bundle data.
    private BundleData getBundleDataOrDefault(final String bundle) {
        BundleData bundleData = null;
        try {
            final String bundleZPath = getBundleDataZooKeeperPath(bundle);
            if (zkClient.exists(bundleZPath, null) != null) {
                bundleData = readJson(zkClient.getData(bundleZPath, null, null), BundleData.class);
            }
        } catch (Exception e) {
            log.warn("Error when trying to find bundle {} on zookeeper: {}", bundle, e);
        }
        if (bundleData == null) {
            bundleData = new BundleData(conf.getNumShortSamples(), conf.getNumLongSamples(), defaultStats);
        }
        return bundleData;
    }

    /**
     * As any broker, disable the broker this manager is running on.
     * @throws PulsarServerException If ZooKeeper failed to disable the broker.
     */
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

    /**
     * As the leader broker, select bundles for the namespace service to unload so that they may be reassigned to new
     * brokers.
     */
    @Override
    public void doLoadShedding() {
        for (LoadSheddingStrategy strategy: loadSheddingPipeline) {
            final Set<String> namespacesToUnload = strategy.selectBundlesForUnloading(loadData, conf);
            if (namespacesToUnload != null && !namespacesToUnload.isEmpty()) {
                // TODO: Write code to unload the bundles.
                return;
            }
        }
    }

    /**
     * As the leader broker, attempt to automatically detect and split hot namespace bundles.
     */
    @Override
    public void doNamespaceBundleSplit() {
        // TODO?
    }

    @Override
    public String getBrokerRoot() {
        return brokerRoot;
    }

    /**
     * When the broker data ZooKeeper nodes are updated, update the broker data map.
     */
    @Override
    public void onUpdate(final String path, final LocalBrokerData data, final Stat stat) {
        scheduler.submit(this::updateAll);
    }

    /**
     * As the leader broker, find a suitable broker for the assignment of the given bundle.
     * @param bundle Full name of the bundle to assign.
     * @return The name of the selected broker, as it appears on ZooKeeper.
     */
    @Override
    public synchronized String selectBrokerForAssignment(final String bundle) {
        // ?: Is it too inefficient to make this synchronized? If so, it may be a good idea to use weighted random
        // or atomic data.
        if (preallocatedBundleToBroker.containsKey(bundle)) {
            // If the given bundle is already in preallocated, return the selected broker.
            return preallocatedBundleToBroker.get(bundle);
        }
        final BundleData data = loadData.getBundleData().computeIfAbsent(bundle, key -> getBundleDataOrDefault(bundle));
        brokerCandidateCache.clear();
        brokerCandidateCache.addAll(loadData.getBrokerData().keySet());

        // Use the filter pipeline to finalize broker candidates.
        for (BrokerFilter filter: filterPipeline) {
            filter.filter(brokerCandidateCache, data, loadData, conf);
        }
        final String broker = placementStrategy.selectBroker(brokerCandidateCache, data, loadData, conf);

        // Add new bundle to preallocated.
        loadData.getBrokerData().get(broker).getPreallocatedBundleData().put(bundle, data);
        preallocatedBundleToBroker.put(bundle, broker);
        return broker;
    }

    /**
     * As any broker, start the load manager.
     * @throws PulsarServerException If an unexpected error prevented the load manager from being started.
     */
    @Override
    public void start() throws PulsarServerException {
        try {
            // Register the brokers in zk list
            createZPathIfNotExists(zkClient, brokerRoot);

            String lookupServiceAddress = pulsar.getAdvertisedAddress() + ":" + conf.getWebServicePort();
            brokerZnodePath = brokerRoot + "/" + lookupServiceAddress;
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
            updateAll();
            lastBundleDataUpdate = System.currentTimeMillis();
            baselineSystemResourceUsage = getSystemResourceUsage();
        } catch (Exception e) {
            log.error("Unable to create znode - [{}] for load balance on zookeeper ", brokerZnodePath, e);
            throw new PulsarServerException(e);
        }
    }

    /**
     * As any broker, stop the load manager.
     * @throws PulsarServerException If an unexpected error occurred when attempting to stop the load manager.
     */
    @Override
    public void stop() throws PulsarServerException {
        // Do nothing.
    }

    /**
     * As any broker, retrieve the namespace bundle stats and system resource usage to update data local to this broker.
     */
    @Override
    public void updateLocalBrokerData() {
        try {
            final SystemResourceUsage systemResourceUsage = getSystemResourceUsage();
            localData.update(systemResourceUsage, getBundleStats());
        } catch (Exception e) {
            log.warn("Error when attempting to update local broker data: {}", e);
        }
    }

    /**
     * As any broker, write the local broker data to ZooKeeper.
     */
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

    /**
     * As the leader broker, write bundle data aggregated from all brokers to ZooKeeper.
     */
    @Override
    public void writeBundleDataOnZooKeeper() {
        if (needBundleDataUpdate()) {
            updateBundleData();
            // Write the bundle data to ZooKeeper.
            for (Map.Entry<String, BundleData> entry: loadData.getBundleData().entrySet()) {
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
            // Write the time average broker data to ZooKeeper.
            for (Map.Entry<String, BrokerData> entry: loadData.getBrokerData().entrySet()) {
                final String broker = entry.getKey();
                final TimeAverageBrokerData data = entry.getValue().getTimeAverageData();
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
