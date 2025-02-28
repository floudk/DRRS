package org.apache.flink.runtime.scale.io.network;

import org.apache.flink.runtime.io.network.netty.NettyBufferPool;
import org.apache.flink.runtime.io.network.netty.NettyConfig;
import org.apache.flink.runtime.io.network.netty.NettyServer;

import org.apache.flink.runtime.scale.ScaleConfig;

import org.apache.flink.runtime.scale.io.ScaleCommManager;

import org.apache.flink.shaded.netty4.io.netty.bootstrap.ServerBootstrap;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelInitializer;
import org.apache.flink.shaded.netty4.io.netty.channel.ChannelOption;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.Epoll;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.epoll.EpollServerSocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.nio.NioEventLoopGroup;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.SocketChannel;
import org.apache.flink.shaded.netty4.io.netty.channel.socket.nio.NioServerSocketChannel;

import java.net.InetSocketAddress;

public class NettyScaleServer extends NettyServer {
    private final String SCALE_THREAD_GROUP_NAME = "Flink Scale Netty Server";

    private final int port = ScaleConfig.Instance.SCALE_PORT;
    private final ScaleCommManager scaleCommManager;
    private final ScaleNettyProtocol scaleNettyProtocol;

    public NettyScaleServer(
            ScaleCommManager scaleCommManager, NettyConfig config, ScaleNettyProtocol scaleNettyProtocol) {
        super(config);
        this.scaleCommManager = scaleCommManager;
        this.scaleNettyProtocol = scaleNettyProtocol;
        final NettyBufferPool nettyBufferPool = scaleCommManager.getBufferPool();

        bootstrap = new ServerBootstrap();
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

        bootstrap.localAddress(config.getServerAddress(), port);
        bootstrap.option(ChannelOption.ALLOCATOR, nettyBufferPool);
        bootstrap.childOption(ChannelOption.ALLOCATOR, nettyBufferPool);
        if (config.getServerConnectBacklog() > 0) {
            bootstrap.option(ChannelOption.SO_BACKLOG, config.getServerConnectBacklog());
        }
        int receiveAndSendBufferSize = config.getSendAndReceiveBufferSize();
        if (receiveAndSendBufferSize > 0) {
            bootstrap.childOption(ChannelOption.SO_SNDBUF, receiveAndSendBufferSize);
            bootstrap.childOption(ChannelOption.SO_RCVBUF, receiveAndSendBufferSize);
        }

        bootstrap.childHandler(new ScaleServerChannelInitializer(scaleNettyProtocol, scaleCommManager));

        bindFuture = bootstrap.bind().syncUninterruptibly();
        localAddress = (InetSocketAddress) bindFuture.channel().localAddress();
        LOG.info("Successful initialization scale server, listening on SocketAddress {}.", localAddress);

    }

    @Override
    protected void initNioBootstrap() {
        NioEventLoopGroup nioGroup =
                new NioEventLoopGroup(
                        config.getServerNumThreads(),
                        getNamedThreadFactory(SCALE_THREAD_GROUP_NAME+" (" + port + ")"));
        bootstrap.group(nioGroup).channel(NioServerSocketChannel.class);
    }
    @Override
    protected void initEpollBootstrap() {

        EpollEventLoopGroup epollGroup =
                new EpollEventLoopGroup(
                        config.getServerNumThreads(),
                        getNamedThreadFactory(SCALE_THREAD_GROUP_NAME+" (" + port + ")"));
        bootstrap.group(epollGroup).channel(EpollServerSocketChannel.class);
    }

    private static class ScaleServerChannelInitializer extends ChannelInitializer<SocketChannel> {
        private final ScaleNettyProtocol protocol;
        private final ScaleCommManager manager;

        public ScaleServerChannelInitializer(ScaleNettyProtocol protocol, ScaleCommManager manager) {
            this.protocol = protocol;
            this.manager = manager;
        }

        @Override
        public void initChannel(SocketChannel channel) throws Exception {
            LOG.info("New scale connection established: {}", channel);
            channel.pipeline().addLast(protocol.getHandlers(manager));
        }
    }


}
