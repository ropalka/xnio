/*
 * JBoss, Home of Professional Open Source.
 *
 * Copyright 2013 Red Hat, Inc. and/or its affiliates, and individual
 * contributors as indicated by the @author tags.
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

package org.xnio.ssl;

import static java.security.AccessController.doPrivileged;
import static org.xnio.IoUtils.safeClose;
import static org.xnio._private.Messages.msg;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import java.security.NoSuchProviderException;
import java.security.PrivilegedAction;
import java.util.concurrent.TimeUnit;

import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLEngine;
import org.xnio.ChannelListener;
import org.xnio.ChannelListeners;
import org.xnio.FutureResult;
import org.xnio.IoFuture;
import org.xnio.IoUtils;
import org.xnio.Option;
import org.xnio.OptionMap;
import org.xnio.Options;
import org.xnio.ByteBufferPool;
import org.xnio.StreamConnection;
import org.xnio.Xnio;
import org.xnio.XnioExecutor;
import org.xnio.XnioIoThread;
import org.xnio.XnioWorker;
import org.xnio.channels.AcceptingChannel;
import org.xnio.channels.AssembledConnectedSslStreamChannel;
import org.xnio.channels.BoundChannel;
import org.xnio.channels.ConnectedSslStreamChannel;
import org.xnio.channels.ConnectedStreamChannel;

/**
 * An XNIO SSL provider based on JSSE.  Works with any XNIO provider.
 *
 * @author <a href="mailto:david.lloyd@redhat.com">David M. Lloyd</a>
 * @author <a href="mailto:frainone@redhat.com">Flavia Rainone</a>
 */
public final class JsseXnioSsl extends XnioSsl {
    public static final boolean NEW_IMPL = doPrivileged((PrivilegedAction<Boolean>) () -> Boolean.valueOf(Boolean.parseBoolean(System.getProperty("org.xnio.ssl.new", "false")))).booleanValue();

    static final ByteBufferPool bufferPool = ByteBufferPool.LARGE_DIRECT;
    private final SSLContext sslContext;

    /**
     * Construct a new instance.
     *
     * @param xnio the XNIO instance to associate with
     * @param optionMap the options for this provider
     * @throws NoSuchProviderException if the given SSL provider is not found
     * @throws NoSuchAlgorithmException if the given SSL algorithm is not supported
     * @throws KeyManagementException if the SSL context could not be initialized
     */
    public JsseXnioSsl(final Xnio xnio, final OptionMap optionMap) throws NoSuchProviderException, NoSuchAlgorithmException, KeyManagementException {
        this(xnio, optionMap, JsseSslUtils.createSSLContext(optionMap));
    }

    /**
     * Construct a new instance.
     *
     * @param xnio the XNIO instance to associate with
     * @param optionMap the options for this provider
     * @param sslContext the SSL context to use for this instance
     */
    public JsseXnioSsl(final Xnio xnio, final OptionMap optionMap, final SSLContext sslContext) {
        super(xnio, sslContext, optionMap);
        this.sslContext = sslContext;
    }

    /**
     * Get the JSSE SSL context for this provider instance.
     *
     * @return the SSL context
     */
    @SuppressWarnings("unused")
    public SSLContext getSslContext() {
        return sslContext;
    }

    /**
     * Get the SSL engine for a given connection.
     *
     * @return the SSL engine
     */
    public static SSLEngine getSslEngine(SslConnection connection) {
        if (connection instanceof JsseSslStreamConnection) {
            return ((JsseSslStreamConnection) connection).getEngine();
        } else if (connection instanceof JsseSslConnection) {
            return ((JsseSslConnection) connection).getEngine();
        } else {
            throw msg.notFromThisProvider();
        }
    }

    @SuppressWarnings("deprecation")
    public IoFuture<ConnectedSslStreamChannel> connectSsl(final XnioWorker worker, final InetSocketAddress bindAddress, final InetSocketAddress destination, final ChannelListener<? super ConnectedSslStreamChannel> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        final FutureResult<ConnectedSslStreamChannel> futureResult = new FutureResult<ConnectedSslStreamChannel>(IoUtils.directExecutor());
        final IoFuture<SslConnection> futureSslConnection = openSslConnection(worker, bindAddress, destination, new ChannelListener<SslConnection>() {
                    public void handleEvent(final SslConnection sslConnection) {
                        final ConnectedSslStreamChannel assembledChannel = new AssembledConnectedSslStreamChannel(sslConnection, sslConnection.getSourceChannel(), sslConnection.getSinkChannel());
                        if (!futureResult.setResult(assembledChannel)) {
                            safeClose(assembledChannel);
                        } else {
                            ChannelListeners.invokeChannelListener(assembledChannel, openListener);
                        }
                    }
                }, bindListener, optionMap).addNotifier(new IoFuture.HandlingNotifier<SslConnection, FutureResult<ConnectedSslStreamChannel>>() {
            public void handleCancelled(final FutureResult<ConnectedSslStreamChannel> result) {
                result.setCancelled();
            }

            public void handleFailed(final IOException exception, final FutureResult<ConnectedSslStreamChannel> result) {
                result.setException(exception);
            }
        }, futureResult);
        futureResult.getIoFuture().addNotifier(new IoFuture.HandlingNotifier<ConnectedStreamChannel, IoFuture<SslConnection>>() {
            public void handleCancelled(final IoFuture<SslConnection> result) {
                result.cancel();
            }
        }, futureSslConnection);
        futureResult.addCancelHandler(futureSslConnection);
        return futureResult.getIoFuture();
    }

    public IoFuture<SslConnection> openSslConnection(final XnioWorker worker, final InetSocketAddress bindAddress, final InetSocketAddress destination, final ChannelListener<? super SslConnection> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        return openSslConnection(worker.getIoThread(), bindAddress, destination, openListener, bindListener, optionMap);
    }

    public IoFuture<SslConnection> openSslConnection(final XnioIoThread ioThread, final InetSocketAddress bindAddress, final InetSocketAddress destination, final ChannelListener<? super SslConnection> openListener, final ChannelListener<? super BoundChannel> bindListener, final OptionMap optionMap) {
        final FutureResult<SslConnection> futureResult = new FutureResult<>(ioThread);
        final IoFuture<StreamConnection> connection = ioThread.openStreamConnection(bindAddress, destination, new ChannelListener<StreamConnection>() {
            public void handleEvent(final StreamConnection connection) {
                final SSLEngine sslEngine = JsseSslUtils.createSSLEngine(sslContext, optionMap, destination);
                final boolean startTls = optionMap.get(Options.SSL_STARTTLS, false);
                final SslConnection wrappedConnection;
                try {
                    wrappedConnection = NEW_IMPL ? new JsseSslConnection(connection, sslEngine, bufferPool, bufferPool) : new JsseSslStreamConnection(connection, sslEngine, bufferPool, bufferPool, startTls);
                } catch (RuntimeException e) {
                    futureResult.setCancelled();
                    throw e;
                }
                if (NEW_IMPL && ! startTls) {
                    try {
                        wrappedConnection.startHandshake();
                    } catch (IOException e) {
                        if (futureResult.setException(e)) {
                            IoUtils.safeClose(connection);
                        }
                    }
                }
                if (! futureResult.setResult(wrappedConnection)) {
                    IoUtils.safeClose(connection);
                } else {
                    ChannelListeners.invokeChannelListener(wrappedConnection, openListener);
                }
            }
        }, bindListener, optionMap);
        connection.addNotifier(new IoFuture.HandlingNotifier<StreamConnection, FutureResult<SslConnection>>() {
            public void handleCancelled(final FutureResult<SslConnection> attachment) {
                attachment.setCancelled();
            }

            public void handleFailed(final IOException exception, final FutureResult<SslConnection> attachment) {
                attachment.setException(exception);
            }
        }, futureResult);
        futureResult.addCancelHandler(connection);
        return futureResult.getIoFuture();
    }

    @SuppressWarnings("deprecation")
    public AcceptingChannel<ConnectedSslStreamChannel> createSslTcpServer(final XnioWorker worker, final InetSocketAddress bindAddress, final ChannelListener<? super AcceptingChannel<ConnectedSslStreamChannel>> acceptListener, final OptionMap optionMap) throws IOException {
        final AcceptingChannel<SslConnection> server = createSslConnectionServer(worker, bindAddress, null, optionMap);
        final AcceptingChannel<ConnectedSslStreamChannel> acceptingChannel = new AcceptingChannel<ConnectedSslStreamChannel>() {
            public ConnectedSslStreamChannel accept() throws IOException {
                final SslConnection connection = server.accept();
                return connection == null ? null : new AssembledConnectedSslStreamChannel(connection, connection.getSourceChannel(), connection.getSinkChannel());
            }

            public ChannelListener.Setter<? extends AcceptingChannel<ConnectedSslStreamChannel>> getAcceptSetter() {
                return ChannelListeners.getDelegatingSetter(server.getAcceptSetter(), this);
            }

            public ChannelListener.Setter<? extends AcceptingChannel<ConnectedSslStreamChannel>> getCloseSetter() {
                return ChannelListeners.getDelegatingSetter(server.getCloseSetter(), this);
            }

            public SocketAddress getLocalAddress() {
                return server.getLocalAddress();
            }

            public <A extends SocketAddress> A getLocalAddress(final Class<A> type) {
                return server.getLocalAddress(type);
            }

            public void suspendAccepts() {
                server.suspendAccepts();
            }

            public void resumeAccepts() {
                server.resumeAccepts();
            }

            public boolean isAcceptResumed() {
                return server.isAcceptResumed();
            }

            public void wakeupAccepts() {
                server.wakeupAccepts();
            }

            public void awaitAcceptable() throws IOException {
                server.awaitAcceptable();
            }

            public void awaitAcceptable(final long time, final TimeUnit timeUnit) throws IOException {
                server.awaitAcceptable(time, timeUnit);
            }

            public XnioWorker getWorker() {
                return server.getWorker();
            }

            @Deprecated
            public XnioExecutor getAcceptThread() {
                return server.getAcceptThread();
            }

            public XnioIoThread getIoThread() {
                return server.getIoThread();
            }

            public void close() throws IOException {
                server.close();
            }

            public boolean isOpen() {
                return server.isOpen();
            }

            public boolean supportsOption(final Option<?> option) {
                return server.supportsOption(option);
            }

            public <T> T getOption(final Option<T> option) throws IOException {
                return server.getOption(option);
            }

            public <T> T setOption(final Option<T> option, final T value) throws IllegalArgumentException, IOException {
                return server.setOption(option, value);
            }
        };
        acceptingChannel.getAcceptSetter().set(acceptListener);
        return acceptingChannel;
    }

    public AcceptingChannel<SslConnection> createSslConnectionServer(final XnioWorker worker, final InetSocketAddress bindAddress, final ChannelListener<? super AcceptingChannel<SslConnection>> acceptListener, final OptionMap optionMap) throws IOException {
       final JsseAcceptingSslStreamConnection server = new JsseAcceptingSslStreamConnection(sslContext, worker.createStreamConnectionServer(bindAddress,  null,  optionMap), optionMap, bufferPool, bufferPool, optionMap.get(Options.SSL_STARTTLS, false));
        if (acceptListener != null) server.getAcceptSetter().set(acceptListener);
        return server;
    }
}
