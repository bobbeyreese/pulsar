package com.yahoo.pulsar.testclient;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.ParameterException;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.InputStreamReader;
import java.net.Socket;
import java.util.List;
import java.util.Random;

/**
 * To use:
 * 1. Delegate a list of server machines which act as zookeeper clients.
 * 2. Choose a port for those machines.
 * 3. On each of these machines, get them to listen via pulsar-perf simulation-server --port <chosen port>
 *     --service-url <broker service url>
 * 4. Start the controller with pulsar-perf simulation-controller --cluster <cluster name>
 *     --servers <comma separated list of <hostname>:<port> --server-port <chosen port>
 * 5. You will get a shell on the controller, where you can use the commands trade, change, stop, trade_group,
 *    change_group, stop_group. You can enter "help" to see the syntax for the commands. Note that tenant, namespace,
 *    and topic refer to persistent://cluster/tenant/namespace/topic/bundle. For instance, to start trading for
 *    topic with destination persistent://mycluster/mytenant/mynamespace/mytopic/bundle at rate 200 msgs/s, you would
 *    type "trade mytenant mynamespace mytopic --rate 200".
 *    The group commands also refer to a "group_name" parameter. This is a string that is prefixed to the namespaces
 *    when trade_group is invoked so they may be identified by other group commands. At the moment, groups may not
 *    be modified after they have been created via trade_group.
 *
 */
public class LoadSimulationController {
    private final DataInputStream[] inputStreams;
    private final DataOutputStream[] outputStreams;
    private final String[] servers;
    private final int serverPort;
    private final String cluster;
    private final Random random;

    private static class MainArguments {
        @Parameter(names = {"--cluster"}, description = "Cluster to test on", required = true)
        String cluster;

        @Parameter(names = {"--servers"}, description = "Comma separated list of server hostnames", required = true)
        String serverHostNames;

        @Parameter(names = {"--server-port"}, description = "Port that the servers are listening on", required = true)
        int serverPort;
    }

    private static class ShellArguments {
        @Parameter(description = "Command arguments:\n" +
                "trade tenant namespace topic\n" +
                "change tenant namespace topic\n" +
                "stop tenant namespace topic\n" +
                "trade_group tenant group_name num_namespaces\n" +
                "change_group tenant group_name\n" +
                "stop_group tenant group_name\n", required = true)
        List<String> commandArguments;

        @Parameter(names = {"--rate"}, description = "Messages per second")
        double rate = 1;

        @Parameter(names = {"--size"}, description = "Message size in bytes")
        int size = 1024;

        @Parameter(names = {"--separation"}, description = "Separation time in ms for trade_group actions " +
        "(0 for no separation)")
        int separation = 0;

        @Parameter(names = {"--topics-per-namespace"}, description = "Number of topics to create per namespace in " +
        "trade_group (total number of topics is num_namespaces X num_topics)")
        int topicsPerNamespace = 1;
    }

    public LoadSimulationController(final MainArguments arguments) throws Exception {
        random = new Random();
        serverPort = arguments.serverPort;
        cluster = arguments.cluster;
        servers = arguments.serverHostNames.split(",");
        final Socket[] sockets = new Socket[servers.length];
        inputStreams = new DataInputStream[servers.length];
        outputStreams = new DataOutputStream[servers.length];
        System.out.format("Found %d servers:\n", servers.length);
        for (int i = 0; i < servers.length; ++i) {
            sockets[i] = new Socket(servers[i], serverPort);
            inputStreams[i] = new DataInputStream(sockets[i].getInputStream());
            outputStreams[i] = new DataOutputStream(sockets[i].getOutputStream());
            System.out.format("Connected to %s\n", servers[i]);
        }
    }

    private boolean checkAppArgs(final int numAppArgs, final int numRequired) {
        if (numAppArgs != numRequired) {
            System.out.format("ERROR: Wrong number of application arguments (found %d, required %d)\n",
                    numAppArgs, numRequired);
            return false;
        }
        return true;
    }

    private String makeDestination(final String tenant, final String namespace, final String topic) {
        return String.format("persistent://%s/%s/%s/%s", cluster, tenant, namespace, topic);
    }

    private void writeProducerOptions(final DataOutputStream outputStream, final ShellArguments arguments,
                                      final String destination)
            throws Exception {
        outputStream.writeUTF(destination);
        outputStream.writeInt(arguments.size);
        outputStream.writeDouble(arguments.rate);
    }

    private void trade(final ShellArguments arguments, final String destination) throws Exception {
        final int i = random.nextInt(servers.length);
        System.out.println("Sending trade request to " + servers[i]);
        outputStreams[i].write(LoadSimulationServer.TRADE_COMMAND);
        writeProducerOptions(outputStreams[i], arguments, destination);
        outputStreams[i].flush();
        if (inputStreams[i].read() != -1) {
            System.out.println("Created producer and consumer for " + destination);
        } else {
            System.out.format("ERROR: Socket to %s closed\n", servers[i]);
        }
    }

    private void handleTrade(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        if (checkAppArgs(commandArguments.size() - 1, 3)) {
            final String destination = makeDestination(commandArguments.get(1), commandArguments.get(2),
                    commandArguments.get(3));
            trade(arguments, destination);
        }
    }

    private void handleChange(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        if (checkAppArgs(commandArguments.size() - 1, 3)) {
            final String destination = makeDestination(commandArguments.get(1), commandArguments.get(2),
                    commandArguments.get(3));
            System.out.println("Searching for server with topic " + destination);
            for (DataOutputStream outputStream : outputStreams) {
                outputStream.write(LoadSimulationServer.CHANGE_COMMAND);
                writeProducerOptions(outputStream, arguments, destination);
                outputStream.flush();
            }
            boolean foundTopic = false;
            for (int i = 0; i < servers.length; ++i) {
                int readValue;
                switch (readValue = inputStreams[i].read()) {
                    case LoadSimulationServer.FOUND_TOPIC:
                        System.out.format("Found topic %s on server %s\n", destination, servers[i]);
                        foundTopic = true;
                        break;
                    case LoadSimulationServer.NO_SUCH_TOPIC:
                        break;
                    case -1:
                        System.out.format("ERROR: Socket to %s closed\n", servers[i]);
                        break;
                    default:
                        System.out.println("ERROR: Unknown response signal received: " + readValue);
                }
            }
            if (!foundTopic) {
                System.out.format("ERROR: Topic %s not found\n", destination);
            }
        }
    }

    private void handleStop(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        if (checkAppArgs(commandArguments.size() - 1, 3)) {
            final String destination = makeDestination(commandArguments.get(1), commandArguments.get(2),
                    commandArguments.get(3));
            System.out.println("Searching for server with topic " + destination);
            for (DataOutputStream outputStream : outputStreams) {
                outputStream.write(LoadSimulationServer.STOP_COMMAND);
                outputStream.writeUTF(destination);
                outputStream.flush();
            }
            boolean foundTopic = false;
            for (int i = 0; i < servers.length; ++i) {
                int readValue;
                switch (readValue = inputStreams[i].read()) {
                    case LoadSimulationServer.FOUND_TOPIC:
                        System.out.format("Found topic %s on server %s\n", destination, servers[i]);
                        foundTopic = true;
                        break;
                    case LoadSimulationServer.NO_SUCH_TOPIC:
                        break;
                    case LoadSimulationServer.REDUNDANT_COMMAND:
                        System.out.format("ERROR: Topic %s already stopped on %s\n", destination, servers[i]);
                        foundTopic = true;
                        break;
                    case -1:
                        System.out.format("ERROR: Socket to %s closed\n", servers[i]);
                        break;
                    default:
                        System.out.println("ERROR: Unknown response signal received: " + readValue);
                }
            }
            if (!foundTopic) {
                System.out.format("ERROR: Topic %s not found\n", destination);
            }
        }
    }

    private void handleGroupTrade(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        if (checkAppArgs(commandArguments.size() - 1, 3)) {
            final String tenant = commandArguments.get(1);
            final String group = commandArguments.get(2);
            final int numNamespaces = Integer.parseInt(commandArguments.get(3));
            for (int i = 0; i < numNamespaces; ++i) {
                for (int j = 0; j < arguments.topicsPerNamespace; ++j) {
                    final String destination = makeDestination(tenant, String.format("%s-%s", group, i), Integer.toString(j));
                    trade(arguments, destination);
                    Thread.sleep(arguments.separation);
                }
            }
        }
    }

    private void handleGroupChange(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        if (checkAppArgs(commandArguments.size() - 1, 2)) {
            final String tenant = commandArguments.get(1);
            final String group = commandArguments.get(2);
            for (DataOutputStream outputStream: outputStreams) {
                outputStream.write(LoadSimulationServer.CHANGE_GROUP_COMMAND);
                outputStream.writeUTF(tenant);
                outputStream.writeUTF(group);
                outputStream.writeInt(arguments.size);
                outputStream.writeDouble(arguments.rate);
                outputStream.flush();
            }
            accumulateAndReport(tenant, group);
        }
    }

    private void accumulateAndReport(final String tenant, final String group) throws Exception {
        int numFound = 0;
        for (int i = 0; i < servers.length; ++i) {
            final int foundOnServer = inputStreams[i].readInt();
            if (foundOnServer == -1) {
                System.out.format("ERROR: Socket to %s closed\n", servers[i]);
            } else if (foundOnServer == 0) {
                System.out.format("Found no topics belonging to tenant %s and group %s on %s\n", tenant, group,
                        servers[i]);
            } else if (foundOnServer > 0){
                System.out.format("Found %d topics belonging to tenant %s and group %s on %s\n", foundOnServer,
                        tenant, group, servers[i]);
                numFound += foundOnServer;
            } else {
                System.out.format("ERROR: Negative value %d received for topic count on %s\n", foundOnServer,
                        servers[i]);
            }
        }
        if (numFound == 0) {
            System.out.format("ERROR: Found no topics belonging to tenant %s and group %s\n", tenant, group);
        } else {
            System.out.format("Found %d topics belonging to tenant %s and group %s\n", numFound, tenant, group);
        }
    }

    private void handleGroupStop(final ShellArguments arguments) throws Exception {
        final List<String> commandArguments = arguments.commandArguments;
        if (checkAppArgs(commandArguments.size() - 1, 2)) {
            final String tenant = commandArguments.get(1);
            final String group = commandArguments.get(2);
            for (DataOutputStream outputStream: outputStreams) {
                outputStream.write(LoadSimulationServer.STOP_GROUP_COMMAND);
                outputStream.writeUTF(tenant);
                outputStream.writeUTF(group);
                outputStream.flush();
            }
            accumulateAndReport(tenant, group);
        }
    }

    public void run() throws Exception {
        final BufferedReader inReader = new BufferedReader(new InputStreamReader(System.in));
        while (true) {
            System.out.println();
            System.out.print("> ");
            final String[] args = inReader.readLine().split("\\s+");
            if (args.length > 0 && !(args.length == 1 && args[0].isEmpty())) {
                final ShellArguments arguments = new ShellArguments();
                final JCommander jc = new JCommander(arguments);
                try {
                    jc.parse(args);
                    final String command = arguments.commandArguments.get(0);
                    switch (command) {
                        case "trade":
                            handleTrade(arguments);
                            break;
                        case "change":
                            handleChange(arguments);
                            break;
                        case "stop":
                            handleStop(arguments);
                            break;
                        case "trade_group":
                            handleGroupTrade(arguments);
                            break;
                        case "change_group":
                            handleGroupChange(arguments);
                            break;
                        case "stop_group":
                            handleGroupStop(arguments);
                            break;
                        case "quit":
                        case "exit":
                            System.exit(0);
                            break;
                        default:
                            System.out.format("ERROR: Unknown command \"%s\"\n", command);
                    }
                } catch (ParameterException ex) {
                    ex.printStackTrace();
                    jc.usage();
                } catch (Exception ex) {
                    ex.printStackTrace();
                }
            }
        }
    }

    public static void main(String[] args) throws Exception {
        final MainArguments arguments = new MainArguments();
        final JCommander jc = new JCommander(arguments);
        try {
            jc.parse(args);
        } catch (Exception ex) {
            jc.usage();
            ex.printStackTrace();
            System.exit(1);
        }
        (new LoadSimulationController(arguments)).run();
    }
}
