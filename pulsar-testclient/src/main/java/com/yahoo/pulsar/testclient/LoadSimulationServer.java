package com.yahoo.pulsar.testclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;
import com.google.common.util.concurrent.RateLimiter;
import com.yahoo.pulsar.client.api.*;
import com.yahoo.pulsar.client.impl.PulsarClientImpl;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.util.concurrent.DefaultThreadFactory;
import org.apache.commons.lang.SystemUtils;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Map;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

public class LoadSimulationServer {
    public static final byte FOUND_TOPIC = 0;
    public static final byte NO_SUCH_TOPIC = 1;
    public static final byte REDUNDANT_COMMAND = 2;
    public static final byte CHANGE_COMMAND = 0;
    public static final byte STOP_COMMAND = 1;
    public static final byte TRADE_COMMAND = 2;
    public static final byte CHANGE_GROUP_COMMAND = 3;
    public static final byte STOP_GROUP_COMMAND = 4;
    private final ExecutorService executor;
    private final Map<Integer, byte[]> payloadCache;
    private final Map<String, TradeUnit> topicsToTradeUnits;
    private final PulsarClient client;
    private final ProducerConfiguration producerConf;
    private final ConsumerConfiguration consumerConf;
    private final ClientConfiguration clientConf;
    private final int port;

    private static class TradeUnit {
        final Future<Producer> producerFuture;
        final Future<Consumer> consumerFuture;
        final AtomicBoolean stop;
        final RateLimiter rateLimiter;
        final AtomicReference<byte[]> payload;
        final Map<Integer, byte[]> payloadCache;

        public TradeUnit(final TradeConfiguration tradeConf, final PulsarClient client,
                         final ProducerConfiguration producerConf, final ConsumerConfiguration consumerConf,
                         final Map<Integer, byte[]> payloadCache) throws Exception {
            consumerFuture = client.subscribeAsync(tradeConf.topic, "Subscriber-" + tradeConf.topic, consumerConf);
            producerFuture = client.createProducerAsync(tradeConf.topic, producerConf);
            this.payload = new AtomicReference<>();
            this.payloadCache = payloadCache;
            this.payload.set(payloadCache.computeIfAbsent(tradeConf.size, byte[]::new));
            rateLimiter = RateLimiter.create(tradeConf.rate);
            stop = new AtomicBoolean(false);
        }

        public void change(final TradeConfiguration tradeConf) {
            rateLimiter.setRate(tradeConf.rate);
            this.payload.set(payloadCache.computeIfAbsent(tradeConf.size, byte[]::new));
        }

        public void start() throws Exception {
            final Producer producer = producerFuture.get();
            final Consumer consumer = consumerFuture.get();
            while (!stop.get()) {
                producer.sendAsync(payload.get());
                rateLimiter.acquire();
            }
            producer.closeAsync();
            consumer.closeAsync();
        }
    }

    private static class MainArguments {
        @Parameter(names = { "-h", "--help" }, description = "Help message", help = true)
        boolean help;

        @Parameter(names = {"--port"}, description = "Port to listen on for controller", required = true)
        public int port;

        @Parameter(names = {"--service-url" }, description = "Pulsar Service URL", required = true)
        public String serviceURL;
    }

    private static class TradeConfiguration {
        public byte command;
        public String topic;
        public double rate;
        public int size;
        public String tenant;
        public String group;
        public TradeConfiguration() {
            command = -1;
            rate = 100;
            size = 1024;
        }
    }

    private void handle(final Socket socket) throws Exception {
        final DataInputStream inputStream = new DataInputStream(socket.getInputStream());
        int command;
        while ((command = inputStream.read()) != -1) {
            handle((byte) command, inputStream, new DataOutputStream(socket.getOutputStream()));
        }
    }

    private void decodeProducerOptions(final TradeConfiguration tradeConf, final DataInputStream inputStream)
            throws Exception {
        tradeConf.topic = inputStream.readUTF();
        tradeConf.size = inputStream.readInt();
        tradeConf.rate = inputStream.readDouble();
    }

    private void decodeGroupOptions(final TradeConfiguration tradeConf, final DataInputStream inputStream)
        throws Exception {
        tradeConf.tenant = inputStream.readUTF();
        tradeConf.group = inputStream.readUTF();
    }

    private void handle(final byte command, final DataInputStream inputStream, final DataOutputStream outputStream)
            throws Exception {
        final TradeConfiguration tradeConf = new TradeConfiguration();
        tradeConf.command = command;
        switch(command) {
            case CHANGE_COMMAND:
                decodeProducerOptions(tradeConf, inputStream);
                if (topicsToTradeUnits.containsKey(tradeConf.topic)) {
                    topicsToTradeUnits.get(tradeConf.topic).change(tradeConf);
                    outputStream.write(FOUND_TOPIC);
                } else {
                    outputStream.write(NO_SUCH_TOPIC);
                }
                break;
            case STOP_COMMAND:
                tradeConf.topic = inputStream.readUTF();
                if (topicsToTradeUnits.containsKey(tradeConf.topic)) {
                    final boolean wasStopped = topicsToTradeUnits.get(tradeConf.topic).stop.getAndSet(true);
                    outputStream.write(wasStopped ? REDUNDANT_COMMAND: FOUND_TOPIC);
                } else {
                    outputStream.write(NO_SUCH_TOPIC);
                }
                break;
            case TRADE_COMMAND:
                decodeProducerOptions(tradeConf, inputStream);
                final TradeUnit tradeUnit = new TradeUnit(tradeConf, client, producerConf, consumerConf, payloadCache);
                topicsToTradeUnits.put(tradeConf.topic, tradeUnit);
                executor.submit(() -> {
                    try {
                        tradeUnit.start();
                    } catch (Exception ex) {
                        throw new RuntimeException(ex);
                    }
                });
                outputStream.write(NO_SUCH_TOPIC);
                break;
            case CHANGE_GROUP_COMMAND:
                decodeGroupOptions(tradeConf, inputStream);
                tradeConf.size = inputStream.readInt();
                tradeConf.rate = inputStream.readDouble();
                final String groupRegex = ".*://.*/" + tradeConf.tenant + "/" + tradeConf.group + "-.*/.*";
                int numFound = 0;
                for (Map.Entry<String, TradeUnit> entry: topicsToTradeUnits.entrySet()) {
                    final String destination = entry.getKey();
                    final TradeUnit unit = entry.getValue();
                    if (destination.matches(groupRegex)) {
                        ++numFound;
                        unit.change(tradeConf);
                    }
                }
                outputStream.writeInt(numFound);
                break;
            case STOP_GROUP_COMMAND:
                decodeGroupOptions(tradeConf, inputStream);
                final String regex = ".*://.*/" + tradeConf.tenant + "/" + tradeConf.group + "-.*/.*";
                int numStopped = 0;
                for (Map.Entry<String, TradeUnit> entry: topicsToTradeUnits.entrySet()) {
                    final String destination = entry.getKey();
                    final TradeUnit unit = entry.getValue();
                    if (destination.matches(regex) && !unit.stop.getAndSet(true)) {
                        ++numStopped;
                    }
                }
                outputStream.writeInt(numStopped);
                break;
            default:
                throw new IllegalArgumentException("Unrecognized command code received: " + command);
        }
        outputStream.flush();
    }

    private static final MessageListener ackListener = Consumer::acknowledgeAsync;

    public LoadSimulationServer(final MainArguments arguments) throws Exception {
        payloadCache = new ConcurrentHashMap<>();
        topicsToTradeUnits = new ConcurrentHashMap<>();
        final EventLoopGroup eventLoopGroup = SystemUtils.IS_OS_LINUX ?
                new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors(),
                        new DefaultThreadFactory("pulsar-test-client")):
                new NioEventLoopGroup(Runtime.getRuntime().availableProcessors(),
                        new DefaultThreadFactory("pulsar-test-client"));
        clientConf = new ClientConfiguration();
        clientConf.setConnectionsPerBroker(0);
        producerConf = new ProducerConfiguration();
        producerConf.setSendTimeout(0, TimeUnit.SECONDS);
        producerConf.setMessageRoutingMode(ProducerConfiguration.MessageRoutingMode.RoundRobinPartition);
        producerConf.setBatchingMaxPublishDelay(1, TimeUnit.MILLISECONDS);
        producerConf.setBatchingEnabled(true);
        consumerConf = new ConsumerConfiguration();
        consumerConf.setMessageListener(ackListener);
        client = new PulsarClientImpl(arguments.serviceURL, clientConf, eventLoopGroup);
        port = arguments.port;
        executor = Executors.newCachedThreadPool(new DefaultThreadFactory("test-client"));
    }

    public static void main(String[] args) throws Exception {
        final MainArguments mainArguments = new MainArguments();
        final JCommander jc = new JCommander(mainArguments);
        try {
            jc.parse(args);
        } catch (ParameterException e) {
            jc.usage();
            throw e;
        }
        (new LoadSimulationServer(mainArguments)).run();
    }

    public void run() throws Exception {
        final ServerSocket serverSocket = new ServerSocket(port);

        while (true) {
            System.out.println("Listening for controller command...");
            final Socket socket = serverSocket.accept();
            System.out.format("Connected to %s\n", socket.getInetAddress().getHostName());
            executor.submit(() -> {
                try {
                    handle(socket);
                } catch (Exception ex) {
                    throw new RuntimeException(ex);
                }
            });
        }
    }
}
