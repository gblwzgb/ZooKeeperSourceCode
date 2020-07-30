/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.zookeeper.server.quorum;

import java.io.IOException;
import javax.management.JMException;
import javax.security.sasl.SaslException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.zookeeper.audit.ZKAuditProvider;
import org.apache.zookeeper.jmx.ManagedUtil;
import org.apache.zookeeper.metrics.MetricsProvider;
import org.apache.zookeeper.metrics.MetricsProviderLifeCycleException;
import org.apache.zookeeper.metrics.impl.MetricsProviderBootstrap;
import org.apache.zookeeper.server.DatadirCleanupManager;
import org.apache.zookeeper.server.ExitCode;
import org.apache.zookeeper.server.ServerCnxnFactory;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperServerMain;
import org.apache.zookeeper.server.admin.AdminServer.AdminServerException;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog;
import org.apache.zookeeper.server.persistence.FileTxnSnapLog.DatadirException;
import org.apache.zookeeper.server.quorum.QuorumPeerConfig.ConfigException;
import org.apache.zookeeper.server.util.JvmPauseMonitor;
import org.apache.zookeeper.util.ServiceUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
/**
 * 配置文件
 *
 * 当使用此类的main（）方法启动程序时，第一个参数用作配置文件的路径，该文件将用于获取配置信息。
 * 该文件是Properties文件，因此键和值之间用等号（=）分隔，而键/值对之间用换行符分隔。
 * 以下是配置文件中使用的键的一般摘要。有关此内容的完整详细信息，请参阅docs/index.html中的文档
 *
 * dataDir - ZooKeeper数据的存储目录。
 * dataLogDir - ZooKeeper事务日志的存储目录。
 * clientPort - 用于与客户端通信的端口。
 * tickTime - 滴答的持续时间（以毫秒为单位）。这是ZooKeeper中的基本时间单位。
 * initLimit - follower最初与leader同步所等待的最大滴答数。
 * syncLimit - follower将等待leader发出的消息（包括心跳）的最大滴答声数量。
 * server.id - 这是具有给定ID的服务器将用于仲裁协议的host：port [：port]。
 * 除了配置文件。dataDir中有一个名为"myid"的文件，其中包含作为服务器id的ASCII十进制值。
 */
/* 核心作用
 * 1、作为集群部署启动的入口，提供main方法
 * 2、根据main的入参（配置文件路径），获取配置文件并解析到QuorumPeerConfig中
 * 3、创建QuorumPeer，为其初始化各种参数QuorumPeer，然后启动它，并join在QuorumPeer上，让main线程挂起。
 *      （如果main没传入参，则降级成单机部署模式ZooKeeperServerMain.main）
 */

/**
 *
 * <h2>Configuration file</h2>
 *
 * When the main() method of this class is used to start the program, the first
 * argument is used as a path to the config file, which will be used to obtain
 * configuration information. This file is a Properties file, so keys and
 * values are separated by equals (=) and the key/value pairs are separated
 * by new lines. The following is a general summary of keys used in the
 * configuration file. For full details on this see the documentation in
 * docs/index.html
 * <ol>
 * <li>dataDir - The directory where the ZooKeeper data is stored.</li>
 * <li>dataLogDir - The directory where the ZooKeeper transaction log is stored.</li>
 * <li>clientPort - The port used to communicate with clients.</li>
 * <li>tickTime - The duration of a tick in milliseconds. This is the basic
 * unit of time in ZooKeeper.</li>
 * <li>initLimit - The maximum number of ticks that a follower will wait to
 * initially synchronize with a leader.</li>
 * <li>syncLimit - The maximum number of ticks that a follower will wait for a
 * message (including heartbeats) from the leader.</li>
 * <li>server.<i>id</i> - This is the host:port[:port] that the server with the
 * given id will use for the quorum protocol.</li>
 * </ol>
 * In addition to the config file. There is a file in the data directory called
 * "myid" that contains the server id as an ASCII decimal value.
 *
 */
@InterfaceAudience.Public
public class QuorumPeerMain {

    private static final Logger LOG = LoggerFactory.getLogger(QuorumPeerMain.class);

    private static final String USAGE = "Usage: QuorumPeerMain configfile";

    protected QuorumPeer quorumPeer;

    /**
     * 要启动复制的服务器，请在命令行上指定配置文件名。
     *
     * @param args 配置文件的路径
     */
    /**
     * To start the replicated server specify the configuration file name on
     * the command line.
     * @param args path to the configfile
     */
    public static void main(String[] args) {
        QuorumPeerMain main = new QuorumPeerMain();
        try {
            // 初始化并运行
            main.initializeAndRun(args);
        } catch (IllegalArgumentException e) {
            LOG.error("Invalid arguments, exiting abnormally", e);
            LOG.info(USAGE);
            System.err.println(USAGE);
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (ConfigException e) {
            LOG.error("Invalid config, exiting abnormally", e);
            System.err.println("Invalid config, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.INVALID_INVOCATION.getValue());
        } catch (DatadirException e) {
            LOG.error("Unable to access datadir, exiting abnormally", e);
            System.err.println("Unable to access datadir, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNABLE_TO_ACCESS_DATADIR.getValue());
        } catch (AdminServerException e) {
            LOG.error("Unable to start AdminServer, exiting abnormally", e);
            System.err.println("Unable to start AdminServer, exiting abnormally");
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.ERROR_STARTING_ADMIN_SERVER.getValue());
        } catch (Exception e) {
            LOG.error("Unexpected exception, exiting abnormally", e);
            ZKAuditProvider.addServerStartFailureAuditLog();
            ServiceUtils.requestSystemExit(ExitCode.UNEXPECTED_ERROR.getValue());
        }
        LOG.info("Exiting normally");
        ServiceUtils.requestSystemExit(ExitCode.EXECUTION_FINISHED.getValue());
    }

    protected void initializeAndRun(String[] args) throws ConfigException, IOException, AdminServerException {
        QuorumPeerConfig config = new QuorumPeerConfig();
        if (args.length == 1) {
            // 从配置文件中读取配置，并设置到QuorumPeerConfig中
            config.parse(args[0]);
        }

        // Start and schedule the the purge task  （启动并调度清除任务）
        DatadirCleanupManager purgeMgr = new DatadirCleanupManager(
            config.getDataDir(),
            config.getDataLogDir(),
            config.getSnapRetainCount(),  // 默认3个
            config.getPurgeInterval());  // 默认0
        // 因为默认PurgeInterval为0，代表不清楚，所以这里会被忽略
        purgeMgr.start();

        if (args.length == 1 && config.isDistributed()) {
            runFromConfig(config);
        } else {
            LOG.warn("Either no config or no quorum defined in config, running in standalone mode");
            // there is only server in the quorum -- run as standalone  （quorum中只有一台服务器-独立运行）
            ZooKeeperServerMain.main(args);
        }
    }

    public void runFromConfig(QuorumPeerConfig config) throws IOException, AdminServerException {
        try {
            // 注册日志bean？
            ManagedUtil.registerLog4jMBeans();
        } catch (JMException e) {
            LOG.warn("Unable to register log4j JMX control", e);
        }

        LOG.info("Starting quorum peer");
        MetricsProvider metricsProvider;
        try {
            // 启动指标提供者？
            metricsProvider = MetricsProviderBootstrap.startMetricsProvider(
                config.getMetricsProviderClassName(),
                config.getMetricsProviderConfiguration());
        } catch (MetricsProviderLifeCycleException error) {
            throw new IOException("Cannot boot MetricsProvider " + config.getMetricsProviderClassName(), error);
        }
        try {
            // 初始化（内部创建了一个ServerMetrics）
            ServerMetrics.metricsProviderInitialized(metricsProvider);
            ServerCnxnFactory cnxnFactory = null;
            ServerCnxnFactory secureCnxnFactory = null;

            if (config.getClientPortAddress() != null) {  // 这里会有一个0.0.0.0:2181
                // 反射创建一个ServerCnxnFactory，默认NIO模式
                cnxnFactory = ServerCnxnFactory.createFactory();
                // 配置ServerCnxnFactory
                cnxnFactory.configure(config.getClientPortAddress(), config.getMaxClientCnxns(), config.getClientPortListenBacklog(), false);
            }

            if (config.getSecureClientPortAddress() != null) {
                secureCnxnFactory = ServerCnxnFactory.createFactory();
                secureCnxnFactory.configure(config.getSecureClientPortAddress(), config.getMaxClientCnxns(), config.getClientPortListenBacklog(), true);
            }

            // new一个QuorumPeer()
            quorumPeer = getQuorumPeer();
            // 给QuorumPeer设置各种属性
            // 事务工厂
            quorumPeer.setTxnFactory(new FileTxnSnapLog(config.getDataLogDir(), config.getDataDir()));
            quorumPeer.enableLocalSessions(config.areLocalSessionsEnabled());
            quorumPeer.enableLocalSessionsUpgrading(config.isLocalSessionsUpgradingEnabled());
            //quorumPeer.setQuorumPeers(config.getAllMembers());
            // 设置选举算法，默认3
            quorumPeer.setElectionType(config.getElectionAlg());
            quorumPeer.setMyid(config.getServerId());
            // 默认3000
            quorumPeer.setTickTime(config.getTickTime());
            // 默认-1，表示没指定
            quorumPeer.setMinSessionTimeout(config.getMinSessionTimeout());
            // 默认-1，表示没指定
            quorumPeer.setMaxSessionTimeout(config.getMaxSessionTimeout());
            quorumPeer.setInitLimit(config.getInitLimit());
            quorumPeer.setSyncLimit(config.getSyncLimit());
            quorumPeer.setConnectToLearnerMasterLimit(config.getConnectToLearnerMasterLimit());
            quorumPeer.setObserverMasterPort(config.getObserverMasterPort());
            quorumPeer.setConfigFileName(config.getConfigFilename());
            quorumPeer.setClientPortListenBacklog(config.getClientPortListenBacklog());
            // 设置"DB"，在内存中
            quorumPeer.setZKDatabase(new ZKDatabase(quorumPeer.getTxnFactory()));
            // todo:这里看下
            quorumPeer.setQuorumVerifier(config.getQuorumVerifier(), false);
            if (config.getLastSeenQuorumVerifier() != null) {
                quorumPeer.setLastSeenQuorumVerifier(config.getLastSeenQuorumVerifier(), false);
            }
            quorumPeer.initConfigInZKDatabase();
            // 连接工厂
            quorumPeer.setCnxnFactory(cnxnFactory);
            // 安全连接工厂
            quorumPeer.setSecureCnxnFactory(secureCnxnFactory);
            quorumPeer.setSslQuorum(config.isSslQuorum());
            quorumPeer.setUsePortUnification(config.shouldUsePortUnification());
            // Learner的类型，默认PARTICIPANT（参与者），可以设置成OBSERVER（不参与选举）
            quorumPeer.setLearnerType(config.getPeerType());
            // 默认true
            quorumPeer.setSyncEnabled(config.getSyncEnabled());
            // 默认false
            quorumPeer.setQuorumListenOnAllIPs(config.getQuorumListenOnAllIPs());
            if (config.sslQuorumReloadCertFiles) {
                quorumPeer.getX509Util().enableCertFileReloading();
            }
            // 默认false
            quorumPeer.setMultiAddressEnabled(config.isMultiAddressEnabled());
            // 默认true
            quorumPeer.setMultiAddressReachabilityCheckEnabled(config.isMultiAddressReachabilityCheckEnabled());
            quorumPeer.setMultiAddressReachabilityCheckTimeoutMs(config.getMultiAddressReachabilityCheckTimeoutMs());

            // sets quorum sasl authentication configurations
            quorumPeer.setQuorumSaslEnabled(config.quorumEnableSasl);
            if (quorumPeer.isQuorumSaslAuthEnabled()) {
                quorumPeer.setQuorumServerSaslRequired(config.quorumServerRequireSasl);
                quorumPeer.setQuorumLearnerSaslRequired(config.quorumLearnerRequireSasl);
                quorumPeer.setQuorumServicePrincipal(config.quorumServicePrincipal);
                quorumPeer.setQuorumServerLoginContext(config.quorumServerLoginContext);
                quorumPeer.setQuorumLearnerLoginContext(config.quorumLearnerLoginContext);
            }
            quorumPeer.setQuorumCnxnThreadsSize(config.quorumCnxnThreadsSize);
            // 初始化quorumPeer
            quorumPeer.initialize();

            if (config.jvmPauseMonitorToRun) {
                quorumPeer.setJvmPauseMonitor(new JvmPauseMonitor(config));
            }

            // 启动QuorumPeer
            quorumPeer.start();
            // 添加ZK开始停止审核日志
            ZKAuditProvider.addZKStartStopAuditLog();
            // 挂起直到QuorumPeer挂掉为止
            quorumPeer.join();
        } catch (InterruptedException e) {
            // warn, but generally this is ok
            LOG.warn("Quorum Peer interrupted", e);
        } finally {
            if (metricsProvider != null) {
                try {
                    metricsProvider.stop();
                } catch (Throwable error) {
                    LOG.warn("Error while stopping metrics", error);
                }
            }
        }
    }

    // @VisibleForTesting
    protected QuorumPeer getQuorumPeer() throws SaslException {
        return new QuorumPeer();
    }

}
