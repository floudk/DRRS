package org.apache.flink.runtime.scale.io.network;

import org.apache.flink.runtime.io.network.netty.NettyClient;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyServer;
import org.apache.flink.runtime.scale.ScaleConfig;
import org.apache.flink.runtime.scale.io.ScaleCommManager;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.Bootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelException;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelFuture;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioSocketChannel;

import java.net.InetSocketAddress;

import static org.apache.flink.util.Preconditions.checkState;

public class ScaleNettyClient extends NettyClient {
    private static final String SCALE_THREAD_GROUP_NAME = "Flink Scale Netty Client";
    private static final int port = ScaleConfig.Instance.SCALE_PORT;
    private final ScaleNettyProtocol scaleNettyProtocol;

    private final ScaleCommManager scaleCommManager;

    public ScaleNettyClient(
            ScaleCommManager scaleCommManager,
            NettyConfig nettyConfig,
            ScaleNettyProtocol scaleNettyProtocol) {
        super(nettyConfig);
        this.scaleCommManager = scaleCommManager;
        this.scaleNettyProtocol = scaleNettyProtocol;

        bootstrap = new Bootstrap();
        switch (config.getTransportType()) {
            case NIO:
                initNioBootstrap();
                break;
            case EPOLL:
                initEpollBootstrap();
                break;
            case AUTO:
                if (Epoll.isAvailable()) {
                    initEpollBootstrap();

                } else {
                    initNioBootstrap();
                }
        }
        bootstrap.option(ChannelOption.TCP_NODELAY, true);
        bootstrap.option(ChannelOption.SO_KEEPALIVE, true);
        bootstrap.option(
                ChannelOption.CONNECT_TIMEOUT_MILLIS,
                config.getClientConnectTimeoutSeconds() * 1000);
        bootstrap.option(ChannelOption.ALLOCATOR, scaleCommManager.getBufferPool());
    }


    @Override
    protected void initNioBootstrap() {
        NioEventLoopGroup nioGroup =
                new NioEventLoopGroup(
                        config.getClientNumThreads(),
                        NettyServer.getNamedThreadFactory(SCALE_THREAD_GROUP_NAME+" (" + port + ")"));
        bootstrap.group(nioGroup).channel(NioSocketChannel.class);
    }
    @Override
    protected void initEpollBootstrap() {
        EpollEventLoopGroup epollGroup =
                new EpollEventLoopGroup(
                        config.getClientNumThreads(),
                        NettyServer.getNamedThreadFactory(SCALE_THREAD_GROUP_NAME+" (" + port + ")"));
        bootstrap.group(epollGroup).channel(EpollSocketChannel.class);
    }

    @Override
    public ChannelFuture connect(final InetSocketAddress serverSocketAddress) {
        checkState(bootstrap != null, "Client has not been initialized yet.");

        // --------------------------------------------------------------------
        // Child channel pipeline for accepted connections
        // --------------------------------------------------------------------

        bootstrap.handler(
                new ChannelInitializer<SocketChannel>() {
                    @Override
                    public void initChannel(SocketChannel channel) throws Exception {
                        channel.pipeline().addLast(scaleNettyProtocol.getHandlers(scaleCommManager));
                    }
                });
        try {
            return bootstrap.connect(serverSocketAddress);
        } catch (ChannelException e) {
            if ((e.getCause() instanceof java.net.SocketException
                    && e.getCause().getMessage().equals("Too many open files"))
                    || (e.getCause() instanceof ChannelException
                    && e.getCause().getCause() instanceof java.net.SocketException
                    && e.getCause()
                    .getCause()
                    .getMessage()
                    .equals("Too many open files"))) {
                throw new ChannelException(
                        "The operating system does not offer enough file handles to open the network connection. "
                                + "Please increase the number of available file handles.",
                        e.getCause());
            } else {
                throw e;
            }
        }
    }

}
