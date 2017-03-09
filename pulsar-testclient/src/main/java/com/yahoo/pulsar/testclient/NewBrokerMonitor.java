package com.yahoo.pulsar.testclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.google.gson.Gson;
import com.yahoo.pulsar.broker.BrokerData;
import com.yahoo.pulsar.broker.TimeAverageMessageData;
import org.apache.zookeeper.WatchedEvent;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.ZooKeeper;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;

public class NewBrokerMonitor {
    private ServerSocket socket;
    private static final String BROKER_ROOT = "/loadbalance/brokers";
    private static final int ZOOKEEPER_TIMEOUT_MILLIS = 5000;
    private final ZooKeeper zkClient;
    private static final Gson gson = new Gson();

    private static class BrokerWatcher implements Watcher {
        public final ZooKeeper zkClient;
        public Set<String> brokers;

        public BrokerWatcher(final ZooKeeper zkClient) {
            this.zkClient = zkClient;
            this.brokers = Collections.EMPTY_SET;
        }

        public synchronized void process(final WatchedEvent event) {
            try {
                if (event.getType() == Event.EventType.NodeChildrenChanged) {
                    updateBrokers(event.getPath());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        public synchronized void updateBrokers(final String path) {
            final Set<String> newBrokers = new HashSet<>();
            try {
                newBrokers.addAll(zkClient.getChildren(path, this));
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
            for (String oldBroker: brokers) {
                if (!newBrokers.contains(oldBroker)) {
                    System.out.println("Lost broker: " + oldBroker);
                }
            }
            for (String newBroker: newBrokers) {
                if (!brokers.contains(newBroker)) {
                    System.out.println("Gained broker: " + newBroker);
                    final BrokerDataWatcher brokerDataWatcher = new BrokerDataWatcher(zkClient);
                    brokerDataWatcher.printBrokerData(path + "/" + newBroker);
                }
            }
            this.brokers = newBrokers;
        }
    }

    private static class BrokerDataWatcher implements Watcher {
        private final ZooKeeper zkClient;

        public BrokerDataWatcher(final ZooKeeper zkClient) {
            this.zkClient = zkClient;
        }

        public synchronized void process(final WatchedEvent event) {
            try {
                if (event.getType() == Event.EventType.NodeDataChanged) {
                    printBrokerData(event.getPath());
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }
        }

        private static void printTimeAverageData(final TimeAverageMessageData data) {
            System.out.println("Num Samples: " + data.getNumSamples());
            System.out.format("Message Throughput In: %.2f bytes/s\n", data.getMsgThroughputIn());
            System.out.format("Message Throughput Out: %.2f bytes/s\n", data.getMsgThroughputOut());
            System.out.format("Message Rate In: %.2f msgs/s\n", data.getMsgRateIn());
            System.out.format("Message Rate Out: %.2f msgs/s\n", data.getMsgRateOut());
        }

        public synchronized void printBrokerData(final String path) {
            final String brokerName = path.substring(path.lastIndexOf('/') + 1);
            BrokerData brokerData;
            try {
                brokerData = gson.fromJson(new String(zkClient.getData(path, this, null)), BrokerData.class);
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            }

            System.out.println("\nBroker Data for " + brokerName + ":");
            System.out.println("---------------");


            System.out.println("\nNum Topics: " + brokerData.getNumTopics());
            System.out.println("Num Bundles: " + brokerData.getNumBundles());
            System.out.println("Num Consumers: " + brokerData.getNumConsumers());
            System.out.println("Num Producers: " + brokerData.getNumProducers());

            System.out.println(String.format("\nCPU: %.2f%%", brokerData.getCpu().percentUsage()));

            System.out.println(String.format("Memory: %.2f%%", brokerData.getMemory().percentUsage()));

            System.out.println(String.format("Direct Memory: %.2f%%", brokerData.getDirectMemory().percentUsage()));

            System.out.println("\nShort Term Data:");
            printTimeAverageData(brokerData.getShortTermData());

            System.out.println("\nLong Term Data:");
            printTimeAverageData(brokerData.getLongTermData());

            System.out.println();
            if (!brokerData.getLastBundleGains().isEmpty()) {
                for (String bundle: brokerData.getLastBundleGains()) {
                    System.out.println("Gained Bundle: " + bundle);
                }
                System.out.println();
            }
            if (!brokerData.getLastBundleLosses().isEmpty()) {
                for (String bundle: brokerData.getLastBundleLosses()) {
                    System.out.println("Lost Bundle: " + bundle);
                }
                System.out.println();
            }
        }
    }

    static class Arguments {
        @Parameter(names = {"--listen-port"}, description = "Port to listen on", required = true)
        public int listenPort = 0;

        @Parameter(names = {"--connect-string"}, description = "Zookeeper connect string", required = true)
        public String connectString = null;
    }

    public NewBrokerMonitor(final ZooKeeper zkClient) {
        this.zkClient = zkClient;
    }

    private static double percentUsage(final double usage, final double limit) {
        return limit > 0  && usage >= 0 ? 100 * Math.min(1, usage / limit): 0;
    }

    private void listen(final int listenPort) throws Exception {
        socket = new ServerSocket(listenPort);
        final ServerSocket localSocketReference = socket;
        new Thread(() -> {
            try {
                while (true) {
                    final Socket clientSocket = localSocketReference.accept();
                    System.out.println(String.format("Accepted connection from %s",
                            clientSocket.getInetAddress().getHostName()));
                    final BufferedReader reader = new BufferedReader(new InputStreamReader(clientSocket.getInputStream()));
                    new Thread(() -> {
                        try {
                            while (true) {
                                final String line = reader.readLine();
                                if (!(line == null || line.isEmpty())) {
                                    System.out.println(line);
                                }
                            }
                        } catch (Exception ex) {
                            throw new RuntimeException(ex);
                        }
                    }).start();
                }
            } catch (Exception ex) {
                throw new RuntimeException(ex);
            } finally {
                try {
                    socket.close();
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            }
        }).start();
    }

    private void start(final int listenPort) {
        try {
            listen(listenPort);
            final BrokerWatcher brokerWatcher = new BrokerWatcher(zkClient);
            brokerWatcher.updateBrokers(BROKER_ROOT);
            while (true) {}
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }

    public static void main(String[] args) {
        try {
            final Arguments arguments = new Arguments();
            final JCommander jc = new JCommander(arguments);
            jc.parse(args);
            final ZooKeeper zkClient = new ZooKeeper(arguments.connectString, ZOOKEEPER_TIMEOUT_MILLIS, null);
            final NewBrokerMonitor monitor = new NewBrokerMonitor(zkClient);
            monitor.start(arguments.listenPort);
        } catch (Exception ex) {
            throw new RuntimeException(ex);
        }
    }
}
