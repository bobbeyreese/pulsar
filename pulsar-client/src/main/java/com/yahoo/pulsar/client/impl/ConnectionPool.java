/**
 * Copyright 2016 Yahoo Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.yahoo.pulsar.client.impl;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.security.cert.X509Certificate;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

import org.apache.commons.lang3.SystemUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.yahoo.pulsar.client.api.AuthenticationDataProvider;
import com.yahoo.pulsar.client.api.ClientConfiguration;
import com.yahoo.pulsar.client.api.PulsarClientException;
import com.yahoo.pulsar.common.api.PulsarLengthFieldFrameDecoder;

import io.netty.bootstrap.Bootstrap;
import io.netty.buffer.PooledByteBufAllocator;
import io.netty.channel.ChannelException;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelInitializer;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollSocketChannel;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioSocketChannel;
import io.netty.handler.ssl.SslContext;
import io.netty.handler.ssl.SslContextBuilder;
import io.netty.handler.ssl.util.InsecureTrustManagerFactory;

public class ConnectionPool implements Closeable {
    private final ConcurrentHashMap<InetSocketAddress, ConcurrentMap<Integer, CompletableFuture<ClientCnx>>> pool;

    private final Bootstrap bootstrap;
    private final EventLoopGroup eventLoopGroup;
    private final int maxConnectionsPerHosts;

    private static final int MaxMessageSize = 5 * 1024 * 1024;
    public static final String TLS_HANDLER = "tls";

    public ConnectionPool(final PulsarClientImpl client, EventLoopGroup eventLoopGroup) {
        this.eventLoopGroup = eventLoopGroup;
        this.maxConnectionsPerHosts = client.getConfiguration().getConnectionsPerBroker();

        pool = new ConcurrentHashMap<>();
        bootstrap = new Bootstrap();
        bootstrap.group(eventLoopGroup);
        if (SystemUtils.IS_OS_LINUX && eventLoopGroup instanceof EpollEventLoopGroup) {
            bootstrap.channel(EpollSocketChannel.class);
        } else {
            bootstrap.channel(NioSocketChannel.class);
        }

        bootstrap.option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 10000);
        bootstrap.option(ChannelOption.TCP_NODELAY, client.getConfiguration().isUseTcpNoDelay());
        bootstrap.option(ChannelOption.ALLOCATOR, PooledByteBufAllocator.DEFAULT);
        bootstrap.handler(new ChannelInitializer<SocketChannel>() {
            public void initChannel(SocketChannel ch) throws Exception {
                ClientConfiguration clientConfig = client.getConfiguration();
                if (clientConfig.isUseTls()) {
                    SslContextBuilder builder = SslContextBuilder.forClient();
                    if (clientConfig.isTlsAllowInsecureConnection()) {
                        builder.trustManager(InsecureTrustManagerFactory.INSTANCE);
                    } else {
                        if (clientConfig.getTlsTrustCertsFilePath().isEmpty()) {
                            // Use system default
                            builder.trustManager((File) null);
                        } else {
                            File trustCertCollection = new File(clientConfig.getTlsTrustCertsFilePath());
                            builder.trustManager(trustCertCollection);
                        }
                    }

                    // Set client certificate if available
                    AuthenticationDataProvider authData = clientConfig.getAuthentication().getAuthData();
                    if (authData.hasDataForTls()) {
                        builder.keyManager(authData.getTlsPrivateKey(),
                                (X509Certificate[]) authData.getTlsCertificates());
                    }

                    SslContext sslCtx = builder.build();
                    ch.pipeline().addLast(TLS_HANDLER, sslCtx.newHandler(ch.alloc()));
                }
                ch.pipeline().addLast("frameDecoder", new PulsarLengthFieldFrameDecoder(MaxMessageSize, 0, 4, 0, 4));
                ch.pipeline().addLast("handler", new ClientCnx(client));
            }
        });
    }

    private static final Random random = new Random();

    public CompletableFuture<ClientCnx> getConnection(final InetSocketAddress address) {
        if (maxConnectionsPerHosts == 0) {
            // Disable pooling
            return createConnection(address, -1);
        }

        final int randomKey = signSafeMod(random.nextInt(), maxConnectionsPerHosts);

        return pool.computeIfAbsent(address, a -> new ConcurrentHashMap<>()) //
                .computeIfAbsent(randomKey, k -> createConnection(address, randomKey));
    }

    private CompletableFuture<ClientCnx> createConnection(InetSocketAddress address, int connectionKey) {
        if (log.isDebugEnabled()) {
            log.debug("Connection for {} not found in cache", address);
        }

        final CompletableFuture<ClientCnx> cnxFuture = new CompletableFuture<ClientCnx>();

        // Trigger async connect to broker
        bootstrap.connect(address).addListener((ChannelFuture future) -> {
            if (!future.isSuccess()) {
                log.warn("Failed to open connection to {} : {}", address, future.cause().getClass().getSimpleName());
                cnxFuture.completeExceptionally(new PulsarClientException(future.cause()));
                cleanupConnection(address, connectionKey, cnxFuture);
                return;
            }

            log.info("[{}] Connected to server", future.channel());

            future.channel().closeFuture().addListener(v -> {
                // Remove connection from pool when it gets closed
                if (log.isDebugEnabled()) {
                    log.debug("Removing closed connection from pool: {}", v);
                }
                cleanupConnection(address, connectionKey, cnxFuture);
            });

            // We are connected to broker, but need to wait until the connect/connected handshake is
            // complete
            final ClientCnx cnx = (ClientCnx) future.channel().pipeline().get("handler");
            if (!future.channel().isActive() || cnx == null) {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Connection was already closed by the time we got notified", future.channel());
                }
                cnxFuture.completeExceptionally(new ChannelException("Connection already closed"));
                return;
            }

            cnx.connectionFuture().thenRun(() -> {
                if (log.isDebugEnabled()) {
                    log.debug("[{}] Connection handshake completed", cnx.channel());
                }
                cnxFuture.complete(cnx);
            }).exceptionally(exception -> {
                log.warn("[{}] Connection handshake failed: {}", cnx.channel(), exception.getMessage());
                cnxFuture.completeExceptionally(exception);
                cleanupConnection(address, connectionKey, cnxFuture);
                cnx.ctx().close();
                return null;
            });
        });

        return cnxFuture;
    }

    @Override
    public void close() throws IOException {
        eventLoopGroup.shutdownGracefully();
    }

    private void cleanupConnection(InetSocketAddress address, int connectionKey,
            CompletableFuture<ClientCnx> connectionFuture) {
        ConcurrentMap<Integer, CompletableFuture<ClientCnx>> map = pool.get(address);
        if (map != null) {
            map.remove(connectionKey, connectionFuture);
        }
    }

    public static int signSafeMod(long dividend, int divisor) {
        int mod = (int) (dividend % (long) divisor);
        if (mod < 0) {
            mod += divisor;
        }
        return mod;
    }

    private static final Logger log = LoggerFactory.getLogger(ConnectionPool.class);
}
