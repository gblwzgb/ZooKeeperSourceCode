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

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.IOException;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Queue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.ReadLock;
import javax.security.sasl.SaslException;
import org.apache.jute.BinaryInputArchive;
import org.apache.jute.BinaryOutputArchive;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.TxnLogProposalIterator;
import org.apache.zookeeper.server.ZKDatabase;
import org.apache.zookeeper.server.ZooKeeperThread;
import org.apache.zookeeper.server.ZooTrace;
import org.apache.zookeeper.server.quorum.Leader.Proposal;
import org.apache.zookeeper.server.quorum.QuorumPeer.LearnerType;
import org.apache.zookeeper.server.quorum.auth.QuorumAuthServer;
import org.apache.zookeeper.server.util.MessageTracker;
import org.apache.zookeeper.server.util.ZxidUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Q：创建时机？
 * A：LearnerCnxAcceptorHandler收到Learner的连接请求时。
 *
 * Q：Learner什么时候发起的连接请求？
 * A：QuorumPeer在run方法中选举完后，知道自己是FOLLOWING的状态，然后调用Follower.followLeader();
 *
 * Q：LearnerCnxAcceptorHandler的创建时机
 * A：QuorumPeer在run方法中选举完后，知道自己是LEADING的状态，然后调用Leader#lead()，创建一个LearnerCnxAcceptor线程
 *    该线程会创建LearnerCnxAcceptorHandler，来处理Learner发过来的连接请求。
 */

/**
 * There will be an instance of this class created by the Leader for each
 * learner. All communication with a learner is handled by this
 * class.
 * （译：Leader将为每个learner创建一个此类的实例。与learner的所有交流都由该class处理。）
 */
public class LearnerHandler extends ZooKeeperThread {

    private static final Logger LOG = LoggerFactory.getLogger(LearnerHandler.class);

    protected final Socket sock;

    public Socket getSocket() {
        return sock;
    }

    final LearnerMaster learnerMaster;

    /** Deadline for receiving the next ack. If we are bootstrapping then
     * it's based on the initLimit, if we are done bootstrapping it's based
     * on the syncLimit. Once the deadline is past this learner should
     * be considered no longer "sync'd" with the leader. */
    volatile long tickOfNextAckDeadline;

    /**
     * ZooKeeper server identifier of this learner
     */
    protected long sid = 0;

    long getSid() {
        return sid;
    }

    String getRemoteAddress() {
        return sock == null ? "<null>" : sock.getRemoteSocketAddress().toString();
    }

    protected int version = 0x1;

    int getVersion() {
        return version;
    }

    /**
     * The packets to be sent to the learner
     */
    final LinkedBlockingQueue<QuorumPacket> queuedPackets = new LinkedBlockingQueue<QuorumPacket>();
    private final AtomicLong queuedPacketsSize = new AtomicLong();

    protected final AtomicLong packetsReceived = new AtomicLong();
    protected final AtomicLong packetsSent = new AtomicLong();

    protected final AtomicLong requestsReceived = new AtomicLong();

    protected volatile long lastZxid = -1;

    public synchronized long getLastZxid() {
        return lastZxid;
    }

    protected final Date established = new Date();

    public Date getEstablished() {
        return (Date) established.clone();
    }

    /**
     * Marker packets would be added to quorum packet queue after every
     * markerPacketInterval packets.
     * It is ok if packetCounter overflows.
     */
    private final int markerPacketInterval = 1000;
    private AtomicInteger packetCounter = new AtomicInteger();

    /**
     * 此类控制Leader等待该Learner的提案得到ack的时间。
     * 如果时间超过syncLimit，则连接将关闭。
     * 它一次只跟踪一个提案，当该提案的ACK到达时，它将切换到收到的最后一个提案，或者如果没有待处理的提案，则清除该值。
     */
    /**
     * This class controls the time that the Leader has been
     * waiting for acknowledgement of a proposal from this Learner.
     * If the time is above syncLimit, the connection will be closed.
     * It keeps track of only one proposal at a time, when the ACK for
     * that proposal arrives, it switches to the last proposal received
     * or clears the value if there is no pending proposal.
     */
    private class SyncLimitCheck {

        private boolean started = false;
        private long currentZxid = 0;
        private long currentTime = 0;
        private long nextZxid = 0;
        private long nextTime = 0;

        public synchronized void start() {
            started = true;
        }

        public synchronized void updateProposal(long zxid, long time) {
            if (!started) {
                return;
            }
            if (currentTime == 0) {
                currentTime = time;
                currentZxid = zxid;
            } else {
                nextTime = time;
                nextZxid = zxid;
            }
        }

        public synchronized void updateAck(long zxid) {
            if (currentZxid == zxid) {
                currentTime = nextTime;
                currentZxid = nextZxid;
                nextTime = 0;
                nextZxid = 0;
            } else if (nextZxid == zxid) {
                LOG.warn(
                    "ACK for 0x{} received before ACK for 0x{}",
                    Long.toHexString(zxid),
                    Long.toHexString(currentZxid));
                nextTime = 0;
                nextZxid = 0;
            }
        }

        public synchronized boolean check(long time) {
            if (currentTime == 0) {
                return true;
            } else {
                long msDelay = (time - currentTime) / 1000000;
                return (msDelay < learnerMaster.syncTimeout());
            }
        }

    }

    private SyncLimitCheck syncLimitCheck = new SyncLimitCheck();

    private static class MarkerQuorumPacket extends QuorumPacket {

        long time;
        MarkerQuorumPacket(long time) {
            this.time = time;
        }

        @Override
        public int hashCode() {
            return Objects.hash(time);
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            MarkerQuorumPacket that = (MarkerQuorumPacket) o;
            return time == that.time;
        }

    }

    private BinaryInputArchive ia;

    private BinaryOutputArchive oa;

    private final BufferedInputStream bufferedInput;
    private BufferedOutputStream bufferedOutput;

    protected final MessageTracker messageTracker;

    // for test only
    protected void setOutputArchive(BinaryOutputArchive oa) {
        this.oa = oa;
    }
    protected void setBufferedOutput(BufferedOutputStream bufferedOutput) {
        this.bufferedOutput = bufferedOutput;
    }

    /**
     * Keep track of whether we have started send packets thread
     */
    private volatile boolean sendingThreadStarted = false;

    /**
     * For testing purpose, force learnerMaster to use snapshot to sync with followers
     */
    public static final String FORCE_SNAP_SYNC = "zookeeper.forceSnapshotSync";
    private boolean forceSnapSync = false;

    /**
     * Keep track of whether we need to queue TRUNC or DIFF into packet queue
     * that we are going to blast it to the learner
     */
    // 跟踪是否需要将 TRUNC 或 DIFF 排入数据包队列，然后将其发送给 learner
    private boolean needOpPacket = true;

    /**
     * Last zxid sent to the learner as part of synchronization
     */
    private long leaderLastZxid;

    /**
     * for sync throttling
     */
    private LearnerSyncThrottler syncThrottler = null;

    LearnerHandler(Socket sock, BufferedInputStream bufferedInput, LearnerMaster learnerMaster) throws IOException {
        super("LearnerHandler-" + sock.getRemoteSocketAddress());
        this.sock = sock;
        this.learnerMaster = learnerMaster;
        this.bufferedInput = bufferedInput;

        if (Boolean.getBoolean(FORCE_SNAP_SYNC)) {
            forceSnapSync = true;
            LOG.info("Forcing snapshot sync is enabled");
        }

        try {
            QuorumAuthServer authServer = learnerMaster.getQuorumAuthServer();
            if (authServer != null) {
                authServer.authenticate(sock, new DataInputStream(bufferedInput));
            }
        } catch (IOException e) {
            LOG.error("Server failed to authenticate quorum learner, addr: {}, closing connection", sock.getRemoteSocketAddress(), e);
            try {
                sock.close();
            } catch (IOException ie) {
                LOG.error("Exception while closing socket", ie);
            }
            throw new SaslException("Authentication failure: " + e.getMessage());
        }

        this.messageTracker = new MessageTracker(MessageTracker.BUFFERED_MESSAGE_SIZE);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("LearnerHandler ").append(sock);
        sb.append(" tickOfNextAckDeadline:").append(tickOfNextAckDeadline());
        sb.append(" synced?:").append(synced());
        sb.append(" queuedPacketLength:").append(queuedPackets.size());
        return sb.toString();
    }

    /**
     * If this packet is queued, the sender thread will exit
     */
    final QuorumPacket proposalOfDeath = new QuorumPacket();

    private LearnerType learnerType = LearnerType.PARTICIPANT;
    public LearnerType getLearnerType() {
        return learnerType;
    }

    /**
     * This method will use the thread to send packets added to the
     * queuedPackets list
     *
     * @throws InterruptedException
     */
    private void sendPackets() throws InterruptedException {
        long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
        while (true) {
            try {
                QuorumPacket p;
                p = queuedPackets.poll();
                if (p == null) {
                    bufferedOutput.flush();
                    p = queuedPackets.take();
                }

                ServerMetrics.getMetrics().LEARNER_HANDLER_QP_SIZE.add(Long.toString(this.sid), queuedPackets.size());

                if (p instanceof MarkerQuorumPacket) {
                    MarkerQuorumPacket m = (MarkerQuorumPacket) p;
                    ServerMetrics.getMetrics().LEARNER_HANDLER_QP_TIME
                        .add(Long.toString(this.sid), (System.nanoTime() - m.time) / 1000000L);
                    continue;
                }

                queuedPacketsSize.addAndGet(-packetSize(p));
                if (p == proposalOfDeath) {
                    // Packet of death!
                    break;
                }
                if (p.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (p.getType() == Leader.PROPOSAL) {
                    syncLimitCheck.updateProposal(p.getZxid(), System.nanoTime());
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'o', p);
                }

                // Log the zxid of the last request, if it is a valid zxid.
                if (p.getZxid() > 0) {
                    lastZxid = p.getZxid();
                }
                oa.writeRecord(p, "packet");
                packetsSent.incrementAndGet();
                messageTracker.trackSent(p.getType());
            } catch (IOException e) {
                if (!sock.isClosed()) {
                    LOG.warn("Unexpected exception at {}", this, e);
                    try {
                        // this will cause everything to shutdown on
                        // this learner handler and will help notify
                        // the learner/observer instantaneously
                        sock.close();
                    } catch (IOException ie) {
                        LOG.warn("Error closing socket for handler {}", this, ie);
                    }
                }
                break;
            }
        }
    }

    public static String packetToString(QuorumPacket p) {
        String type;
        String mess = null;

        switch (p.getType()) {
        case Leader.ACK:
            type = "ACK";
            break;
        case Leader.COMMIT:
            type = "COMMIT";
            break;
        case Leader.FOLLOWERINFO:
            type = "FOLLOWERINFO";
            break;
        case Leader.NEWLEADER:
            type = "NEWLEADER";
            break;
        case Leader.PING:
            type = "PING";
            break;
        case Leader.PROPOSAL:
            type = "PROPOSAL";
            break;
        case Leader.REQUEST:
            type = "REQUEST";
            break;
        case Leader.REVALIDATE:
            type = "REVALIDATE";
            ByteArrayInputStream bis = new ByteArrayInputStream(p.getData());
            DataInputStream dis = new DataInputStream(bis);
            try {
                long id = dis.readLong();
                mess = " sessionid = " + id;
            } catch (IOException e) {
                LOG.warn("Unexpected exception", e);
            }

            break;
        case Leader.UPTODATE:
            type = "UPTODATE";
            break;
        case Leader.DIFF:
            type = "DIFF";
            break;
        case Leader.TRUNC:
            type = "TRUNC";
            break;
        case Leader.SNAP:
            type = "SNAP";
            break;
        case Leader.ACKEPOCH:
            type = "ACKEPOCH";
            break;
        case Leader.SYNC:
            type = "SYNC";
            break;
        case Leader.INFORM:
            type = "INFORM";
            break;
        case Leader.COMMITANDACTIVATE:
            type = "COMMITANDACTIVATE";
            break;
        case Leader.INFORMANDACTIVATE:
            type = "INFORMANDACTIVATE";
            break;
        default:
            type = "UNKNOWN" + p.getType();
        }
        String entry = null;
        if (type != null) {
            entry = type + " " + Long.toHexString(p.getZxid()) + " " + mess;
        }
        return entry;
    }

    /**
     * This thread will receive packets from the peer and process them and
     * also listen to new connections from new peers.
     */
    @Override
    public void run() {
        try {
            // 添加到set中
            learnerMaster.addLearnerHandler(this);
            tickOfNextAckDeadline = learnerMaster.getTickOfInitialAckDeadline();

            ia = BinaryInputArchive.getArchive(bufferedInput);
            bufferedOutput = new BufferedOutputStream(sock.getOutputStream());
            oa = BinaryOutputArchive.getArchive(bufferedOutput);

            // 从流里读数据到QuorumPacket中
            QuorumPacket qp = new QuorumPacket();
            ia.readRecord(qp, "packet");

            messageTracker.trackReceived(qp.getType());
            if (qp.getType() != Leader.FOLLOWERINFO && qp.getType() != Leader.OBSERVERINFO) {
                // 如果第一个数据包不是FOLLOWERINFO或者OBSERVERINFO类型的，直接返回
                LOG.error("First packet {} is not FOLLOWERINFO or OBSERVERINFO!", qp.toString());

                return;
            }

            if (learnerMaster instanceof ObserverMaster && qp.getType() != Leader.OBSERVERINFO) {
                // 如果learnerMaster是ObserverMaster，则第一个数据包必须是OBSERVERINFO类型的
                throw new IOException("Non observer attempting to connect to ObserverMaster. type = " + qp.getType());
            }
            byte[] learnerInfoData = qp.getData();
            if (learnerInfoData != null) {
                ByteBuffer bbsid = ByteBuffer.wrap(learnerInfoData);
                if (learnerInfoData.length >= 8) {
                    this.sid = bbsid.getLong();
                }
                if (learnerInfoData.length >= 12) {
                    this.version = bbsid.getInt(); // protocolVersion
                }
                if (learnerInfoData.length >= 20) {
                    long configVersion = bbsid.getLong();
                    if (configVersion > learnerMaster.getQuorumVerifierVersion()) {
                        throw new IOException("Follower is ahead of the leader (has a later activated configuration)");
                    }
                }
            } else {
                this.sid = learnerMaster.getAndDecrementFollowerCounter();
            }

            // 获取 Learner 的信息，用于打印日志
            String followerInfo = learnerMaster.getPeerInfo(this.sid);
            if (followerInfo.isEmpty()) {
                LOG.info(
                    "Follower sid: {} not in the current config {}",
                    this.sid,
                    Long.toHexString(learnerMaster.getQuorumVerifierVersion()));
            } else {
                LOG.info("Follower sid: {} : info : {}", this.sid, followerInfo);
            }

            if (qp.getType() == Leader.OBSERVERINFO) {
                // Learner 的类型，默认是 LearnerType.PARTICIPANT
                learnerType = LearnerType.OBSERVER;
            }

            learnerMaster.registerLearnerHandlerBean(this, sock);

            // 最新接收到的Learner的Epoch
            long lastAcceptedEpoch = ZxidUtils.getEpochFromZxid(qp.getZxid());

            long peerLastZxid;
            StateSummary ss = null;
            // Learner的zxid
            long zxid = qp.getZxid();
            long newEpoch = learnerMaster.getEpochToPropose(this.getSid(), lastAcceptedEpoch);
            long newLeaderZxid = ZxidUtils.makeZxid(newEpoch, 0);

            if (this.getVersion() < 0x10000) {
                // we are going to have to extrapolate the epoch information  （译：我们将不得不推断epoch信息）
                long epoch = ZxidUtils.getEpochFromZxid(zxid);
                ss = new StateSummary(epoch, zxid);
                // fake the message  （译：伪造消息）
                learnerMaster.waitForEpochAck(this.getSid(), ss);
            } else {
                byte[] ver = new byte[4];
                ByteBuffer.wrap(ver).putInt(0x10000);
                // 创建Leader给Follower的第一个数据包，包含Leader的信息
                QuorumPacket newEpochPacket = new QuorumPacket(Leader.LEADERINFO, newLeaderZxid, ver, null);
                // 写到流里
                oa.writeRecord(newEpochPacket, "packet");
                messageTracker.trackSent(Leader.LEADERINFO);
                // 直接发送
                bufferedOutput.flush();
                // 读一个Follower的ack数据包
                QuorumPacket ackEpochPacket = new QuorumPacket();
                ia.readRecord(ackEpochPacket, "packet");
                messageTracker.trackReceived(ackEpochPacket.getType());
                if (ackEpochPacket.getType() != Leader.ACKEPOCH) {
                    // 不等于ACKEPOCH，退出线程
                    LOG.error("{} is not ACKEPOCH", ackEpochPacket.toString());
                    return;
                }
                ByteBuffer bbepoch = ByteBuffer.wrap(ackEpochPacket.getData());
                // 入参：Follower的epoch，Follower的zxid
                ss = new StateSummary(bbepoch.getInt(), ackEpochPacket.getZxid());
                // 阻塞到超过半数回复了Epoch（type为Leader.LEADERINFO）的ack，
                learnerMaster.waitForEpochAck(this.getSid(), ss);
            }
            peerLastZxid = ss.getLastZxid();

            // Take any necessary action if we need to send TRUNC or DIFF
            // startForwarding() will be called in all cases  （译：如果需要发送TRUNC或DIFF，请采取任何必要的措施startForwarding()在所有情况下都会被调用）
            /** 同步数据给 Follower */
            boolean needSnap = syncFollower(peerLastZxid, learnerMaster);

            // syncs between followers and the leader are exempt from throttling because it
            // is importatnt to keep the state of quorum servers up-to-date. The exempted syncs
            // are counted as concurrent syncs though
            // （译：followers和leader之间的同步不受限制，因为保持仲裁服务器状态为最新很重要。 不过，豁免的同步算作并发同步）
            boolean exemptFromThrottle = getLearnerType() != LearnerType.OBSERVER;
            /* if we are not truncating or sending a diff just send a snapshot */  // （译：如果我们不 truncating 或发送 diff，则仅发送快照）
            if (needSnap) {
                /** 兜底：直接发送整个内存中的快照数据，什么时候触发？看 syncFollower 方法，默认应该不会触发的 */
                syncThrottler = learnerMaster.getLearnerSnapSyncThrottler();
                syncThrottler.beginSync(exemptFromThrottle);
                try {
                    long zxidToSend = learnerMaster.getZKDatabase().getDataTreeLastProcessedZxid();
                    oa.writeRecord(new QuorumPacket(Leader.SNAP, zxidToSend, null, null), "packet");
                    messageTracker.trackSent(Leader.SNAP);
                    bufferedOutput.flush();

                    LOG.info(
                        "Sending snapshot last zxid of peer is 0x{}, zxid of leader is 0x{}, "
                            + "send zxid of db as 0x{}, {} concurrent snapshot sync, "
                            + "snapshot sync was {} from throttle",
                        Long.toHexString(peerLastZxid),
                        Long.toHexString(leaderLastZxid),
                        Long.toHexString(zxidToSend),
                        syncThrottler.getSyncInProgress(),
                        exemptFromThrottle ? "exempt" : "not exempt");
                    // Dump data to peer  （译：dump数据到对端）
                    learnerMaster.getZKDatabase().serializeSnapshot(oa);
                    oa.writeString("BenWasHere", "signature");
                    bufferedOutput.flush();
                } finally {
                    ServerMetrics.getMetrics().SNAP_COUNT.add(1);
                }
            } else {
                syncThrottler = learnerMaster.getLearnerDiffSyncThrottler();
                syncThrottler.beginSync(exemptFromThrottle);
                ServerMetrics.getMetrics().DIFF_COUNT.add(1);
            }

            LOG.debug("Sending NEWLEADER message to {}", sid);
            // the version of this quorumVerifier will be set by leader.lead() in case
            // the leader is just being established. waitForEpochAck makes sure that readyToStart is true if
            // we got here, so the version was set
            /** 发送 NEWLEADER 数据包  **/
            if (getVersion() < 0x10000) {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, newLeaderZxid, null, null);
                oa.writeRecord(newLeaderQP, "packet");
            } else {
                QuorumPacket newLeaderQP = new QuorumPacket(Leader.NEWLEADER, newLeaderZxid, learnerMaster.getQuorumVerifierBytes(), null);
                // 加到队列中异步线程发送数据包（马上会启动这个异步线程）
                queuedPackets.add(newLeaderQP);
            }
            // 应该是给getVersion() < 0x10000发送数据包用的。
            bufferedOutput.flush();

            // Start thread that blast packets in the queue to learner  （译：启动线程，将队列中的数据包推给给learner）
            /** 启动异步线程，用于异步发送数据包给 Learner */
            startSendingPackets();

            /*
             * Have to wait for the first ACK, wait until
             * the learnerMaster is ready, and only then we can
             * start processing messages.
             * （译：必须等待第一个ACK，等到LearnerMaster准备好，然后我们才能开始处理消息。）
             */
            // 获取NEWLEADER的ack
            qp = new QuorumPacket();
            ia.readRecord(qp, "packet");

            messageTracker.trackReceived(qp.getType());
            if (qp.getType() != Leader.ACK) {
                LOG.error("Next packet was supposed to be an ACK, but received packet: {}", packetToString(qp));
                return;
            }

            LOG.debug("Received NEWLEADER-ACK message from {}", sid);

            /** 阻塞，直到NEWLEADER的ack超过一半 */
            learnerMaster.waitForNewLeaderAck(getSid(), qp.getZxid());

            syncLimitCheck.start();
            // sync ends when NEWLEADER-ACK is received
            syncThrottler.endSync();
            syncThrottler = null;

            // now that the ack has been processed expect the syncLimit  （译：现在，ack已被处理，期望syncLimit）
            sock.setSoTimeout(learnerMaster.syncTimeout());

            /*
             * Wait until learnerMaster starts up  （译：等到LearnerMaster启动）
             */
            learnerMaster.waitForStartup();

            // Mutation packets will be queued during the serialize,
            // so we need to mark when the peer can actually start
            // using the data
            // （译：变动数据包将在序列化过程中排队，因此我们需要标记对等方实际上可以开始使用数据的时间）
            LOG.debug("Sending UPTODATE message to {}", sid);
            queuedPackets.add(new QuorumPacket(Leader.UPTODATE, -1, null, null));

            /** 死循环，一直处理 follower 的请求 **/
            while (true) {
                qp = new QuorumPacket();
                ia.readRecord(qp, "packet");
                messageTracker.trackReceived(qp.getType());

                long traceMask = ZooTrace.SERVER_PACKET_TRACE_MASK;
                if (qp.getType() == Leader.PING) {
                    traceMask = ZooTrace.SERVER_PING_TRACE_MASK;
                }
                if (LOG.isTraceEnabled()) {
                    ZooTrace.logQuorumPacket(LOG, traceMask, 'i', qp);
                }
                tickOfNextAckDeadline = learnerMaster.getTickOfNextAckDeadline();

                // 收到的数据包数量+1
                packetsReceived.incrementAndGet();

                ByteBuffer bb;
                long sessionId;
                int cxid;
                int type;

                switch (qp.getType()) {
                case Leader.ACK:
                    if (this.learnerType == LearnerType.OBSERVER) {
                        LOG.debug("Received ACK from Observer {}", this.sid);
                    }
                    syncLimitCheck.updateAck(qp.getZxid());
                    learnerMaster.processAck(this.sid, qp.getZxid(), sock.getLocalSocketAddress());
                    break;
                case Leader.PING:
                    // Process the touches
                    ByteArrayInputStream bis = new ByteArrayInputStream(qp.getData());
                    DataInputStream dis = new DataInputStream(bis);
                    while (dis.available() > 0) {
                        long sess = dis.readLong();
                        int to = dis.readInt();
                        learnerMaster.touch(sess, to);
                    }
                    break;
                case Leader.REVALIDATE:
                    ServerMetrics.getMetrics().REVALIDATE_COUNT.add(1);
                    learnerMaster.revalidateSession(qp, this);
                    break;
                case Leader.REQUEST:
                    bb = ByteBuffer.wrap(qp.getData());
                    sessionId = bb.getLong();
                    cxid = bb.getInt();
                    type = bb.getInt();
                    bb = bb.slice();
                    Request si;
                    if (type == OpCode.sync) {
                        // 人工通过命令行请求的。
                        si = new LearnerSyncRequest(this, sessionId, cxid, type, bb, qp.getAuthinfo());
                    } else {
                        si = new Request(null, sessionId, cxid, type, bb, qp.getAuthinfo());
                    }
                    si.setOwner(this);
                    // 提交到 Leader 的 RequestProcessor 中，绕过 LeaderRequestProcessor，直接进入 PrepRequestProcessor
                    learnerMaster.submitLearnerRequest(si);
                    // 收到的请求数+1
                    requestsReceived.incrementAndGet();
                    break;
                default:
                    LOG.warn("unexpected quorum packet, type: {}", packetToString(qp));
                    break;
                }
            }
        } catch (IOException e) {
            if (sock != null && !sock.isClosed()) {
                LOG.error("Unexpected exception causing shutdown while sock still open", e);
                //close the socket to make sure the
                //other side can see it being close
                try {
                    sock.close();
                } catch (IOException ie) {
                    // do nothing
                }
            }
        } catch (InterruptedException e) {
            LOG.error("Unexpected exception in LearnerHandler.", e);
        } catch (SyncThrottleException e) {
            LOG.error("too many concurrent sync.", e);
            syncThrottler = null;
        } catch (Exception e) {
            LOG.error("Unexpected exception in LearnerHandler.", e);
            throw e;
        } finally {
            if (syncThrottler != null) {
                syncThrottler.endSync();
                syncThrottler = null;
            }
            String remoteAddr = getRemoteAddress();
            LOG.warn("******* GOODBYE {} ********", remoteAddr);
            messageTracker.dumpToLog(remoteAddr);
            shutdown();
        }
    }

    /**
     * Start thread that will forward any packet in the queue to the follower
     */
    protected void startSendingPackets() {
        if (!sendingThreadStarted) {
            // Start sending packets
            new Thread() {
                public void run() {
                    Thread.currentThread().setName("Sender-" + sock.getRemoteSocketAddress());
                    try {
                        sendPackets();
                    } catch (InterruptedException e) {
                        LOG.warn("Unexpected interruption", e);
                    }
                }
            }.start();
            sendingThreadStarted = true;
        } else {
            LOG.error("Attempting to start sending thread after it already started");
        }
    }

    /**
     * Tests need not send marker packets as they are only needed to
     * log quorum packet delays
     */
    protected boolean shouldSendMarkerPacketForLogging() {
        return true;
    }

    /**
     * 确定我们是否需要使用DIFF/TRUNC/SNAP和设置follower来与follower同步，以从commit processor接收数据包
     *
     * @param peerLastZxid 对端 learner 的最大 zxid
     * @param learnerMaster 从 learnerMaster 中同步数据，一般是 Leader
     * @return 如果需要快照传输，则为true。
     */
    /**
     * Determine if we need to sync with follower using DIFF/TRUNC/SNAP
     * and setup follower to receive packets from commit processor
     *
     * @param peerLastZxid
     * @param learnerMaster
     * @return true if snapshot transfer is needed.
     */
    boolean syncFollower(long peerLastZxid, LearnerMaster learnerMaster) {
        /*
         * When leader election is completed, the leader will set its
         * lastProcessedZxid to be (epoch < 32). There will be no txn associated
         * with this zxid.
         *
         * The learner will set its lastProcessedZxid to the same value if
         * it get DIFF or SNAP from the learnerMaster. If the same learner come
         * back to sync with learnerMaster using this zxid, we will never find this
         * zxid in our history. In this case, we will ignore TRUNC logic and
         * always send DIFF if we have old enough history
         *
         * 译：
         * leader选举完成后，leader会将其lastProcessedZxid设置为（epoch <32）。  没有与该zxid相关的txn。
         *
         * 如果learner从learnerMaster获得DIFF或SNAP，则它将其lastProcessedZxid设置为相同的值。
         * 如果同一个learner返回使用此zxid与learningerMaster进行同步，则我们将永远不会在此历史记录中找到该zxid。
         * 在这种情况下，如果我们有足够的历史记录，我们将忽略TRUNC逻辑并始终发送DIFF
         */
        // 低32位为0，todo：该字段的作用？
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        // Keep track of the latest zxid which already queued  （译：跟踪已经排队的最新zxid）
        long currentZxid = peerLastZxid;
        boolean needSnap = true;
        ZKDatabase db = learnerMaster.getZKDatabase();
        // 默认是true
        boolean txnLogSyncEnabled = db.isTxnLogSyncEnabled();
        // 获取读写锁
        ReentrantReadWriteLock lock = db.getLogLock();
        // 获取读锁
        ReadLock rl = lock.readLock();
        try {
            // 锁
            rl.lock();
            // 获取最大CommittedLog
            long maxCommittedLog = db.getmaxCommittedLog();
            // 获取最小CommittedLog
            long minCommittedLog = db.getminCommittedLog();
            // 获取本机最后处理的zxid
            long lastProcessedZxid = db.getDataTreeLastProcessedZxid();

            LOG.info("Synchronizing with Learner sid: {} maxCommittedLog=0x{}"
                     + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                     + " peerLastZxid=0x{}",
                     getSid(),
                     Long.toHexString(maxCommittedLog),
                     Long.toHexString(minCommittedLog),
                     Long.toHexString(lastProcessedZxid),
                     Long.toHexString(peerLastZxid));

            if (db.getCommittedLog().isEmpty()) {
                /*
                 * It is possible that committedLog is empty. In that case
                 * setting these value to the latest txn in learnerMaster db
                 * will reduce the case that we need to handle
                 *
                 * Here is how each case handle by the if block below
                 * 1. lastProcessZxid == peerZxid -> Handle by (2)
                 * 2. lastProcessZxid < peerZxid -> Handle by (3)
                 * 3. lastProcessZxid > peerZxid -> Handle by (5)
                 *
                 * 译：
                 * commitLog可能为空。 在这种情况下，将这些值设置为learnerMaster db中的最新txn可以减少我们需要处理的情况
                 *
                 * 这是下面的if块处理每种情况的方式
                 * 1. lastProcessZxid == peerZxid -> Handle by (2)
                 * 2. lastProcessZxid < peerZxid -> Handle by (3)
                 * 3. lastProcessZxid > peerZxid -> Handle by (5)
                 */
                minCommittedLog = lastProcessedZxid;
                maxCommittedLog = lastProcessedZxid;
            }

            /*
             * Here are the cases that we want to handle
             *
             * 1. Force sending snapshot (for testing purpose)
             * 2. Peer and learnerMaster is already sync, send empty diff
             * 3. Follower has txn that we haven't seen. This may be old leader
             *    so we need to send TRUNC. However, if peer has newEpochZxid,
             *    we cannot send TRUNC since the follower has no txnlog
             * 4. Follower is within committedLog range or already in-sync.
             *    We may need to send DIFF or TRUNC depending on follower's zxid
             *    We always send empty DIFF if follower is already in-sync
             * 5. Follower missed the committedLog. We will try to use on-disk
             *    txnlog + committedLog to sync with follower. If that fail,
             *    we will send snapshot
             *
             * 译：
             * 这是我们要处理的情况
             *
             * 1.强制发送快照（用于测试）
             * 2.对方和learnerMaster已经同步，发送空diff
             * 3.Follower拥有我们没有看到的txn。 这可能是老leader
             *     因此我们需要发送TRUNC。 但是，如果对等方具有newEpochZxid，
             *     我们无法发送TRUNC，因为follower没有txnlog
             * 4.Follower在commitLog范围内或已处于同步状态。
             *     我们可能需要发送DIFF或TRUNC，具体取决于关注者的zxid
             *     如果 Follower 已经同步，我们总是发送空的DIFF
             * 5. Follower 错过了 commitLog。 我们将尝试使用磁盘上的
             *    txnlog + commitLog 与 follower 同步。 如果失败了，我们将发送快照
             */

            if (forceSnapSync) {  // 默认 false
                // Force learnerMaster to use snapshot to sync with follower
                LOG.warn("Forcing snapshot sync - should not see this in production");
            } else if (lastProcessedZxid == peerLastZxid) {
                // Follower is already sync with us, send empty diff  （译：Follower已经与我们同步，发送空差异）
                LOG.info(
                    "Sending DIFF zxid=0x{} for peer sid: {}",
                    Long.toHexString(peerLastZxid),
                    getSid());
                // 发送一个DIFF给Follower
                queueOpPacket(Leader.DIFF, peerLastZxid);
                needOpPacket = false;
                needSnap = false;
            } else if (peerLastZxid > maxCommittedLog && !isPeerNewEpochZxid) {
                // Newer than committedLog, send trunc and done  （译：比commitLog新，发送trunc并完成）
                LOG.debug(
                    "Sending TRUNC to follower zxidToSend=0x{} for peer sid:{}",
                    Long.toHexString(maxCommittedLog),
                    getSid());
                // 发送一个 TRUNC 给Follower
                queueOpPacket(Leader.TRUNC, maxCommittedLog);
                currentZxid = maxCommittedLog;
                needOpPacket = false;
                needSnap = false;
            } else if ((maxCommittedLog >= peerLastZxid) && (minCommittedLog <= peerLastZxid)) {  /** 直接使用 commitlog 还原出提案，发给 follower */
                // Follower is within commitLog range  （Follower在commitLog范围内）
                LOG.info("Using committedLog for peer sid: {}", getSid());
                Iterator<Proposal> itr = db.getCommittedLog().iterator();
                // todo：
                currentZxid = queueCommittedProposals(itr, peerLastZxid, null, maxCommittedLog);
                needSnap = false;
            } else if (peerLastZxid < minCommittedLog && txnLogSyncEnabled) {  /** 读取磁盘还原出提案，发给 follower */
                // Use txnlog and committedLog to sync  （译：使用txnlog和commitLog进行同步）

                // Calculate sizeLimit that we allow to retrieve txnlog from disk  （译：计算sizeLimit，我们允许从磁盘检索txnlog）
                long sizeLimit = db.calculateTxnLogSizeLimit();
                // This method can return empty iterator if the requested zxid is older than on-disk txnlog
                // （译：如果请求的zxid早于磁盘txnlog，则此方法可以返回空迭代器）
                Iterator<Proposal> txnLogItr = db.getProposalsFromTxnLog(peerLastZxid, sizeLimit);
                if (txnLogItr.hasNext()) {
                    LOG.info("Use txnlog and committedLog for peer sid: {}", getSid());
                    currentZxid = queueCommittedProposals(txnLogItr, peerLastZxid, minCommittedLog, maxCommittedLog);

                    if (currentZxid < minCommittedLog) {
                        LOG.info(
                            "Detected gap between end of txnlog: 0x{} and start of committedLog: 0x{}",
                            Long.toHexString(currentZxid),
                            Long.toHexString(minCommittedLog));
                        currentZxid = peerLastZxid;
                        // Clear out currently queued requests and revert
                        // to sending a snapshot.
                        queuedPackets.clear();
                        needOpPacket = true;
                    } else {
                        LOG.debug("Queueing committedLog 0x{}", Long.toHexString(currentZxid));
                        Iterator<Proposal> committedLogItr = db.getCommittedLog().iterator();
                        currentZxid = queueCommittedProposals(committedLogItr, currentZxid, null, maxCommittedLog);
                        needSnap = false;
                    }
                }
                // closing the resources
                if (txnLogItr instanceof TxnLogProposalIterator) {
                    TxnLogProposalIterator txnProposalItr = (TxnLogProposalIterator) txnLogItr;
                    txnProposalItr.close();
                }
            } else {
                LOG.warn(
                    "Unhandled scenario for peer sid: {} maxCommittedLog=0x{}"
                        + " minCommittedLog=0x{} lastProcessedZxid=0x{}"
                        + " peerLastZxid=0x{} txnLogSyncEnabled={}",
                    getSid(),
                    Long.toHexString(maxCommittedLog),
                    Long.toHexString(minCommittedLog),
                    Long.toHexString(lastProcessedZxid),
                    Long.toHexString(peerLastZxid),
                    txnLogSyncEnabled);
            }
            if (needSnap) {
                currentZxid = db.getDataTreeLastProcessedZxid();
            }

            LOG.debug("Start forwarding 0x{} for peer sid: {}", Long.toHexString(currentZxid), getSid());
            // todo
            leaderLastZxid = learnerMaster.startForwarding(this, currentZxid);
        } finally {
            rl.unlock();
        }

        if (needOpPacket && !needSnap) {
            // This should never happen, but we should fall back to sending
            // snapshot just in case.
            LOG.error("Unhandled scenario for peer sid: {} fall back to use snapshot",  getSid());
            needSnap = true;
        }

        return needSnap;
    }

    /**
     * 将提交的提议排队到数据包队列中。将要排队的数据包范围是（peerLaxtZxid，maxZxid]
     *
     * @param itr  指向提议的迭代器
     * @param peerLastZxid  follower看到的最后一个zxid
     * @param maxZxid  要排队的提议的最大zxid，如果没有限制，则为null
     * @param lastCommittedZxid 发送diff时，我们需要在leader上发送lastCommittedZxid以遵循Zab 1.0协议。
     * @return 排队的提议的最后一个zxid
     */
    /**
     * Queue committed proposals into packet queue. The range of packets which
     * is going to be queued are (peerLaxtZxid, maxZxid]
     *
     * @param itr  iterator point to the proposals
     * @param peerLastZxid  last zxid seen by the follower
     * @param maxZxid  max zxid of the proposal to queue, null if no limit
     * @param lastCommittedZxid when sending diff, we need to send lastCommittedZxid
     *        on the leader to follow Zab 1.0 protocol.
     * @return last zxid of the queued proposal
     */
    protected long queueCommittedProposals(Iterator<Proposal> itr, long peerLastZxid, Long maxZxid, Long lastCommittedZxid) {
        boolean isPeerNewEpochZxid = (peerLastZxid & 0xffffffffL) == 0;
        long queuedZxid = peerLastZxid;
        // as we look through proposals, this variable keeps track of previous proposal Id.
        // （译：当我们浏览提议时，此变量会跟踪先前的提议ID。）
        long prevProposalZxid = -1;
        while (itr.hasNext()) {
            Proposal propose = itr.next();

            long packetZxid = propose.packet.getZxid();
            // abort if we hit the limit  （译：如果达到极限，则中止）
            if ((maxZxid != null) && (packetZxid > maxZxid)) {
                break;
            }

            // skip the proposals the peer already has  （跳过对方已经拥有的提议）
            if (packetZxid < peerLastZxid) {
                prevProposalZxid = packetZxid;
                continue;
            }

            // If we are sending the first packet, figure out whether to trunc or diff
            // （译：如果我们要发送第一个数据包，请确定是trunc还是diff）
            if (needOpPacket) {

                // Send diff when we see the follower's zxid in our history  （译：当我们在历史记录中看到follower的zxid时发送diff）
                if (packetZxid == peerLastZxid) {
                    LOG.info(
                        "Sending DIFF zxid=0x{}  for peer sid: {}",
                        Long.toHexString(lastCommittedZxid),
                        getSid());
                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                    continue;
                }

                if (isPeerNewEpochZxid) {
                    // Send diff and fall through if zxid is of a new-epoch  （译：如果zxid是新epoch，则发送diff并通过）
                    LOG.info(
                        "Sending DIFF zxid=0x{}  for peer sid: {}",
                        Long.toHexString(lastCommittedZxid),
                        getSid());
                    queueOpPacket(Leader.DIFF, lastCommittedZxid);
                    needOpPacket = false;
                } else if (packetZxid > peerLastZxid) {
                    // Peer have some proposals that the learnerMaster hasn't seen yet it may used to be a leader
                    // （译：对方提出了一些提议，learnerMaster 尚未见到，该 peer 它可能曾经是 leader）
                    if (ZxidUtils.getEpochFromZxid(packetZxid) != ZxidUtils.getEpochFromZxid(peerLastZxid)) {
                        // We cannot send TRUNC that cross epoch boundary.
                        // The learner will crash if it is asked to do so.
                        // We will send snapshot this those cases.
                        LOG.warn("Cannot send TRUNC to peer sid: " + getSid() + " peer zxid is from different epoch");
                        return queuedZxid;
                    }

                    LOG.info(
                        "Sending TRUNC zxid=0x{}  for peer sid: {}",
                        Long.toHexString(prevProposalZxid),
                        getSid());
                    queueOpPacket(Leader.TRUNC, prevProposalZxid);
                    needOpPacket = false;
                }
            }

            if (packetZxid <= queuedZxid) {
                // We can get here, if we don't have op packet to queue or there is a duplicate txn in a given iterator
                // （译：如果没有op数据包排队或给定的迭代器中有重复的txn，我们可以到达这里）
                continue;
            }

            // Since this is already a committed proposal, we need to follow it by a commit packet
            // （译：由于这已经是一个已提交的提议，因此我们需要在其后跟随一个提交包）
            queuePacket(propose.packet);
            queueOpPacket(Leader.COMMIT, packetZxid);
            queuedZxid = packetZxid;

        }

        if (needOpPacket && isPeerNewEpochZxid) {
            // We will send DIFF for this kind of zxid in any case. This if-block
            // is the catch when our history older than learner and there is
            // no new txn since then. So we need an empty diff
            LOG.info(
                "Sending TRUNC zxid=0x{}  for peer sid: {}",
                Long.toHexString(lastCommittedZxid),
                getSid());
            queueOpPacket(Leader.DIFF, lastCommittedZxid);
            needOpPacket = false;
        }

        return queuedZxid;
    }

    public void shutdown() {
        // Send the packet of death
        try {
            queuedPackets.clear();
            queuedPackets.put(proposalOfDeath);
        } catch (InterruptedException e) {
            LOG.warn("Ignoring unexpected exception", e);
        }
        try {
            if (sock != null && !sock.isClosed()) {
                sock.close();
            }
        } catch (IOException e) {
            LOG.warn("Ignoring unexpected exception during socket close", e);
        }
        this.interrupt();
        learnerMaster.removeLearnerHandler(this);
        learnerMaster.unregisterLearnerHandlerBean(this);
    }

    public long tickOfNextAckDeadline() {
        return tickOfNextAckDeadline;
    }

    /**
     * ping calls from the learnerMaster to the peers
     */
    public void ping() {
        // If learner hasn't sync properly yet, don't send ping packet
        // otherwise, the learner will crash
        if (!sendingThreadStarted) {
            return;
        }
        long id;
        if (syncLimitCheck.check(System.nanoTime())) {
            id = learnerMaster.getLastProposed();
            QuorumPacket ping = new QuorumPacket(Leader.PING, id, null, null);
            queuePacket(ping);
        } else {
            LOG.warn("Closing connection to peer due to transaction timeout.");
            shutdown();
        }
    }

    /**
     * Queue leader packet of a given type
     * @param type
     * @param zxid
     */
    private void queueOpPacket(int type, long zxid) {
        QuorumPacket packet = new QuorumPacket(type, zxid, null, null);
        queuePacket(packet);
    }

    void queuePacket(QuorumPacket p) {
        queuedPackets.add(p);
        // Add a MarkerQuorumPacket at regular intervals.
        if (shouldSendMarkerPacketForLogging() && packetCounter.getAndIncrement() % markerPacketInterval == 0) {
            queuedPackets.add(new MarkerQuorumPacket(System.nanoTime()));
        }
        queuedPacketsSize.addAndGet(packetSize(p));
    }

    static long packetSize(QuorumPacket p) {
        /* Approximate base size of QuorumPacket: int + long + byte[] + List */
        long size = 4 + 8 + 8 + 8;
        byte[] data = p.getData();
        if (data != null) {
            size += data.length;
        }
        return size;
    }

    public boolean synced() {
        return isAlive() && learnerMaster.getCurrentTick() <= tickOfNextAckDeadline;
    }

    public synchronized Map<String, Object> getLearnerHandlerInfo() {
        Map<String, Object> info = new LinkedHashMap<>(9);
        info.put("remote_socket_address", getRemoteAddress());
        info.put("sid", getSid());
        info.put("established", getEstablished());
        info.put("queued_packets", queuedPackets.size());
        info.put("queued_packets_size", queuedPacketsSize.get());
        info.put("packets_received", packetsReceived.longValue());
        info.put("packets_sent", packetsSent.longValue());
        info.put("requests", requestsReceived.longValue());
        info.put("last_zxid", getLastZxid());

        return info;
    }

    public synchronized void resetObserverConnectionStats() {
        packetsReceived.set(0);
        packetsSent.set(0);
        requestsReceived.set(0);

        lastZxid = -1;
    }

    /**
     * For testing, return packet queue
     * @return
     */
    public Queue<QuorumPacket> getQueuedPackets() {
        return queuedPackets;
    }

    /**
     * For testing, we need to reset this value
     */
    public void setFirstPacket(boolean value) {
        needOpPacket = value;
    }

}
