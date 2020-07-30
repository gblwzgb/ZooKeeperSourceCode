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

package org.apache.zookeeper.server;

import java.io.IOException;
import java.io.PrintWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.SocketException;
import java.nio.ByteBuffer;
import java.nio.channels.SelectionKey;
import java.nio.channels.Selector;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * 核心职责：
 * 1、配置一些信息
 * 2、创建启动一个AcceptThread，用于接收客户端的connect请求，并分发给SelectorThread
 * 3、创建启动一组SelectorThread，用来select后续的客户端读、写请求
 * 4、创建一个WorkerService线程池，SelectorThread收到请求后，封装成IOWorkRequest，丢入该线程池处理
 * 5、创建并启动ConnectionExpirerThread，用于清理过期的连接
 * 6、创建NIOServerCnxn（这个发生在SelectorThread中，SelectorThread拿到AcceptThread的分发的socket后，会封装成NIOServerCnxn）
 *
 * 内部线程类：
 * 1、AcceptThread
 * 2、SelectorThread
 * 3、IOWorkRequest
 * 4、ConnectionExpirerThread
 */

/**
 * NIOServerCnxnFactory implements a multi-threaded ServerCnxnFactory using
 * NIO non-blocking socket calls. Communication between threads is handled via
 * queues.
 *
 *   - 1   accept thread, which accepts new connections and assigns to a
 *         selector thread
 *   - 1-N selector threads, each of which selects on 1/N of the connections.
 *         The reason the factory supports more than one selector thread is that
 *         with large numbers of connections, select() itself can become a
 *         performance bottleneck.
 *   - 0-M socket I/O worker threads, which perform basic socket reads and
 *         writes. If configured with 0 worker threads, the selector threads
 *         do the socket I/O directly.
 *   - 1   connection expiration thread, which closes idle connections; this is
 *         necessary to expire connections on which no session is established.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 accept thread,
 * 1 connection expiration thread, 4 selector threads, and 64 worker threads.
 */
public class NIOServerCnxnFactory extends ServerCnxnFactory {

    private static final Logger LOG = LoggerFactory.getLogger(NIOServerCnxnFactory.class);

    /** Default sessionless connection timeout in ms: 10000 (10s) */
    public static final String ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT = "zookeeper.nio.sessionlessCnxnTimeout";
    /**
     * With 500 connections to an observer with watchers firing on each, is
     * unable to exceed 1GigE rates with only 1 selector.
     * Defaults to using 2 selector threads with 8 cores and 4 with 32 cores.
     * Expressed as sqrt(numCores/2). Must have at least 1 selector thread.
     */
    public static final String ZOOKEEPER_NIO_NUM_SELECTOR_THREADS = "zookeeper.nio.numSelectorThreads";
    /** Default: 2 * numCores */
    public static final String ZOOKEEPER_NIO_NUM_WORKER_THREADS = "zookeeper.nio.numWorkerThreads";
    /** Default: 64kB */
    public static final String ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES = "zookeeper.nio.directBufferBytes";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT = "zookeeper.nio.shutdownTimeout";

    static {
        Thread.setDefaultUncaughtExceptionHandler(new Thread.UncaughtExceptionHandler() {
            public void uncaughtException(Thread t, Throwable e) {
                LOG.error("Thread {} died", t, e);
            }
        });
        /**
         * this is to avoid the jvm bug:
         * NullPointerException in Selector.open()
         * http://bugs.sun.com/view_bug.do?bug_id=6427854
         */
        try {
            Selector.open().close();
        } catch (IOException ie) {
            LOG.error("Selector failed to open", ie);
        }

        /**
         * Value of 0 disables use of direct buffers and instead uses
         * gathered write call.
         *
         * Default to using 64k direct buffers.
         */
        directBufferBytes = Integer.getInteger(ZOOKEEPER_NIO_DIRECT_BUFFER_BYTES, 64 * 1024);
    }

    /**
     * AbstractSelectThread is an abstract base class containing a few bits
     * of code shared by the AcceptThread (which selects on the listen socket)
     * and SelectorThread (which selects on client connections) classes.
     */
    private abstract class AbstractSelectThread extends ZooKeeperThread {

        protected final Selector selector;

        public AbstractSelectThread(String name) throws IOException {
            super(name);
            // Allows the JVM to shutdown even if this thread is still running.
            setDaemon(true);
            this.selector = Selector.open();
        }

        public void wakeupSelector() {
            selector.wakeup();
        }

        /**
         * Close the selector. This should be called when the thread is about to
         * exit and no operation is going to be performed on the Selector or
         * SelectionKey
         */
        protected void closeSelector() {
            try {
                selector.close();
            } catch (IOException e) {
                LOG.warn("ignored exception during selector close.", e);
            }
        }

        protected void cleanupSelectionKey(SelectionKey key) {
            if (key != null) {
                try {
                    key.cancel();
                } catch (Exception ex) {
                    LOG.debug("ignoring exception during selectionkey cancel", ex);
                }
            }
        }

        protected void fastCloseSock(SocketChannel sc) {
            if (sc != null) {
                try {
                    // Hard close immediately, discarding buffers
                    sc.socket().setSoLinger(true, 0);
                } catch (SocketException e) {
                    LOG.warn("Unable to set socket linger to 0, socket close may stall in CLOSE_WAIT", e);
                }
                NIOServerCnxn.closeSock(sc);
            }
        }

    }

    /**
     * 有一个AcceptThread可以接受新的连接，并使用简单的循环机制(round-robin)将它们分配给SelectorThread，以将其分布在SelectorThreads中。
     * 它强制每个IP设置最大连接数，并尝试通过在重试前短暂休眠来应对文件描述符用尽的情况。
     */
    /**
     * There is a single AcceptThread which accepts new connections and assigns
     * them to a SelectorThread using a simple round-robin scheme to spread
     * them across the SelectorThreads. It enforces maximum number of
     * connections per IP and attempts to cope with running out of file
     * descriptors by briefly sleeping before retrying.
     */
    private class AcceptThread extends AbstractSelectThread {

        private final ServerSocketChannel acceptSocket;
        private final SelectionKey acceptKey;
        private final RateLogger acceptErrorLogger = new RateLogger(LOG);
        private final Collection<SelectorThread> selectorThreads;
        private Iterator<SelectorThread> selectorIterator;
        private volatile boolean reconfiguring = false;

        public AcceptThread(ServerSocketChannel ss, InetSocketAddress addr, Set<SelectorThread> selectorThreads) throws IOException {
            super("NIOServerCxnFactory.AcceptThread:" + addr);
            this.acceptSocket = ss;
            this.acceptKey = acceptSocket.register(selector, SelectionKey.OP_ACCEPT);
            this.selectorThreads = Collections.unmodifiableList(new ArrayList<SelectorThread>(selectorThreads));
            selectorIterator = this.selectorThreads.iterator();
        }

        public void run() {
            try {
                while (!stopped && !acceptSocket.socket().isClosed()) {
                    try {
                        select();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }
            } finally {
                closeSelector();
                // This will wake up the selector threads, and tell the
                // worker thread pool to begin shutdown.
                if (!reconfiguring) {
                    NIOServerCnxnFactory.this.stop();
                }
                LOG.info("accept thread exitted run method");
            }
        }

        public void setReconfiguring() {
            reconfiguring = true;
        }

        private void select() {
            try {
                selector.select();

                Iterator<SelectionKey> selectedKeys = selector.selectedKeys().iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selectedKeys.remove();

                    if (!key.isValid()) {
                        continue;
                    }
                    if (key.isAcceptable()) {
                        if (!doAccept()) {
                            // If unable to pull a new connection off the accept
                            // queue, pause accepting to give us time to free
                            // up file descriptors and so the accept thread
                            // doesn't spin in a tight loop.
                            pauseAccept(10);
                        }
                    } else {
                        LOG.warn("Unexpected ops in accept select {}", key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Mask off the listen socket interest ops and use select() to sleep
         * so that other threads can wake us up by calling wakeup() on the
         * selector.
         */
        private void pauseAccept(long millisecs) {
            acceptKey.interestOps(0);
            try {
                selector.select(millisecs);
            } catch (IOException e) {
                // ignore
            } finally {
                acceptKey.interestOps(SelectionKey.OP_ACCEPT);
            }
        }

        /**
         * Accept new socket connections. Enforces maximum number of connections
         * per client IP address. Round-robin assigns to selector thread for
         * handling. Returns whether pulled a connection off the accept queue
         * or not. If encounters an error attempts to fast close the socket.
         *
         * @return whether was able to accept a connection or not
         */
        private boolean doAccept() {
            boolean accepted = false;
            SocketChannel sc = null;
            try {
                sc = acceptSocket.accept();
                accepted = true;
                if (limitTotalNumberOfCnxns()) {
                    throw new IOException("Too many connections max allowed is " + maxCnxns);
                }
                InetAddress ia = sc.socket().getInetAddress();
                int cnxncount = getClientCnxnCount(ia);

                if (maxClientCnxns > 0 && cnxncount >= maxClientCnxns) {
                    throw new IOException("Too many connections from " + ia + " - max is " + maxClientCnxns);
                }

                LOG.debug("Accepted socket connection from {}", sc.socket().getRemoteSocketAddress());

                sc.configureBlocking(false);

                // Round-robin assign this connection to a selector thread
                if (!selectorIterator.hasNext()) {
                    selectorIterator = selectorThreads.iterator();
                }
                // 获取下一个SelectorThread
                SelectorThread selectorThread = selectorIterator.next();
                // 将socket连接，交给SelectorThread，让它执行select
                if (!selectorThread.addAcceptedConnection(sc)) {
                    throw new IOException("Unable to add connection to selector queue"
                                          + (stopped ? " (shutdown in progress)" : ""));
                }
                acceptErrorLogger.flush();
            } catch (IOException e) {
                // accept, maxClientCnxns, configureBlocking
                ServerMetrics.getMetrics().CONNECTION_REJECTED.add(1);
                acceptErrorLogger.rateLimitLog("Error accepting new connection: " + e.getMessage());
                fastCloseSock(sc);
            }
            return accepted;
        }

    }

    /**
     * SelectorThread从AcceptThread接收新accepted的连接，并负责选择连接中的I/O准备情况。
     * 该线程是唯一对选择器执行任何非线程安全或可能阻止的调用的线程（注册新连接和读取/写入兴趣操作）。
     *
     * 将连接分配给SelectorThread是永久的，只有一个SelectorThread会与该连接进行交互。
     * 有1-N个SelectorThreads，在SelectorThreads之间平均分配连接。
     *
     * 如果存在工作线程池，则当连接具有I/O执行时，SelectorThread会通过清除其感兴趣的操作将其从选择中删除，并安排I/O进行工作线程的处理。
     * 工作完成后，将连接置于就绪队列中，以恢复其兴趣操作并继续选择。
     *
     * 如果没有工作线程池，则SelectorThread将直接执行I/O。
     */
    // Accepter有个SelectorThread集合，每个SelectorThread维护一批连接
    /**
     * The SelectorThread receives newly accepted connections from the
     * AcceptThread and is responsible for selecting for I/O readiness
     * across the connections. This thread is the only thread that performs
     * any non-threadsafe or potentially blocking calls on the selector
     * (registering new connections and reading/writing interest ops).
     *
     * Assignment of a connection to a SelectorThread is permanent and only
     * one SelectorThread will ever interact with the connection. There are
     * 1-N SelectorThreads, with connections evenly apportioned between the
     * SelectorThreads.
     *
     * If there is a worker thread pool, when a connection has I/O to perform
     * the SelectorThread removes it from selection by clearing its interest
     * ops and schedules the I/O for processing by a worker thread. When the
     * work is complete, the connection is placed on the ready queue to have
     * its interest ops restored and resume selection.
     *
     * If there is no worker thread pool, the SelectorThread performs the I/O
     * directly.
     */
    class SelectorThread extends AbstractSelectThread {

        private final int id;
        private final Queue<SocketChannel> acceptedQueue;
        private final Queue<SelectionKey> updateQueue;

        public SelectorThread(int id) throws IOException {
            super("NIOServerCxnFactory.SelectorThread-" + id);
            this.id = id;
            acceptedQueue = new LinkedBlockingQueue<SocketChannel>();
            updateQueue = new LinkedBlockingQueue<SelectionKey>();
        }

        /**
         * Place new accepted connection onto a queue for adding. Do this
         * so only the selector thread modifies what keys are registered
         * with the selector.
         */
        public boolean addAcceptedConnection(SocketChannel accepted) {
            if (stopped || !acceptedQueue.offer(accepted)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * Place interest op update requests onto a queue so that only the
         * selector thread modifies interest ops, because interest ops
         * reads/sets are potentially blocking operations if other select
         * operations are happening.
         */
        public boolean addInterestOpsUpdateRequest(SelectionKey sk) {
            if (stopped || !updateQueue.offer(sk)) {
                return false;
            }
            wakeupSelector();
            return true;
        }

        /**
         * 线程的主循环在连接上执行selects()并分派就绪的I/O工作请求，
         * 然后注册所有未决的新接受连接并更新队列上的所有兴趣操作。
         */
        /**
         * The main loop for the thread selects() on the connections and
         * dispatches ready I/O work requests, then registers all pending
         * newly accepted connections and updates any interest ops on the
         * queue.
         */
        public void run() {
            try {
                while (!stopped) {
                    try {
                        // select并处理准备就绪的事件。
                        // 可能为了处理下面两个操作而被唤醒。见addInterestOpsUpdateRequest、addAcceptedConnection
                        select();
                        // 处理Accepter新分配过来的连接（如果有的话）
                        processAcceptedConnections();
                        // 某些连接想要更新InterestOps，处理这些请求
                        processInterestOpsUpdateRequests();
                    } catch (RuntimeException e) {
                        LOG.warn("Ignoring unexpected runtime exception", e);
                    } catch (Exception e) {
                        LOG.warn("Ignoring unexpected exception", e);
                    }
                }

                // Close connections still pending on the selector. Any others
                // with in-flight work, let drain out of the work queue.
                for (SelectionKey key : selector.keys()) {
                    NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                    if (cnxn.isSelectable()) {
                        cnxn.close(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
                    }
                    cleanupSelectionKey(key);
                }
                SocketChannel accepted;
                while ((accepted = acceptedQueue.poll()) != null) {
                    fastCloseSock(accepted);
                }
                updateQueue.clear();
            } finally {
                closeSelector();
                // This will wake up the accept thread and the other selector
                // threads, and tell the worker thread pool to begin shutdown.
                NIOServerCnxnFactory.this.stop();
                LOG.info("selector thread exitted run method");
            }
        }

        private void select() {
            try {
                selector.select();

                Set<SelectionKey> selected = selector.selectedKeys();
                ArrayList<SelectionKey> selectedList = new ArrayList<SelectionKey>(selected);
                Collections.shuffle(selectedList);
                Iterator<SelectionKey> selectedKeys = selectedList.iterator();
                while (!stopped && selectedKeys.hasNext()) {
                    SelectionKey key = selectedKeys.next();
                    selected.remove(key);

                    if (!key.isValid()) {
                        cleanupSelectionKey(key);
                        continue;
                    }
                    if (key.isReadable() || key.isWritable()) {  // 可读或者可写
                        handleIO(key);
                    } else {
                        LOG.warn("Unexpected ops in select {}", key.readyOps());
                    }
                }
            } catch (IOException e) {
                LOG.warn("Ignoring IOException while selecting", e);
            }
        }

        /**
         * Schedule I/O for processing on the connection associated with
         * the given SelectionKey. If a worker thread pool is not being used,
         * I/O is run directly by this thread.
         */
        private void handleIO(SelectionKey key) {
            IOWorkRequest workRequest = new IOWorkRequest(this, key);
            NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();

            // Stop selecting this key while processing on its connection  (在处理它的连接时停止选择此键) （顺序执行？）
            cnxn.disableSelectable();
            key.interestOps(0);
            touchCnxn(cnxn);
            // 丢到线程池里执行
            workerPool.schedule(workRequest);
        }

        /**
         * 遍历已分配给该线程但尚未放置在选择器上的接受连接的队列。
         */
        /**
         * Iterate over the queue of accepted connections that have been
         * assigned to this thread but not yet placed on the selector.
         */
        private void processAcceptedConnections() {
            SocketChannel accepted;
            while (!stopped && (accepted = acceptedQueue.poll()) != null) {
                SelectionKey key = null;
                try {
                    // 将SocketChannel注册到selector上，并设置感兴趣的事情为：读
                    key = accepted.register(selector, SelectionKey.OP_READ);
                    NIOServerCnxn cnxn = createConnection(accepted, key, this);
                    key.attach(cnxn);
                    addCnxn(cnxn);
                } catch (IOException e) {
                    // register, createConnection
                    cleanupSelectionKey(key);
                    fastCloseSock(accepted);
                }
            }
        }

        /**
         * 遍历准备恢复选择的连接队列，并恢复其兴趣操作选择掩码。
         */
        /**
         * Iterate over the queue of connections ready to resume selection,
         * and restore their interest ops selection mask.
         */
        private void processInterestOpsUpdateRequests() {
            SelectionKey key;
            while (!stopped && (key = updateQueue.poll()) != null) {
                if (!key.isValid()) {
                    cleanupSelectionKey(key);
                }
                // 获取SelectionKey上关联的对象
                NIOServerCnxn cnxn = (NIOServerCnxn) key.attachment();
                if (cnxn.isSelectable()) {
                    // 更新兴趣事件
                    key.interestOps(cnxn.getInterestOps());
                }
            }
        }

    }

    /**
     * IOWorkRequest is a small wrapper class to allow doIO() calls to be
     * run on a connection using a WorkerService.
     */
    private class IOWorkRequest extends WorkerService.WorkRequest {

        private final SelectorThread selectorThread;
        private final SelectionKey key;
        private final NIOServerCnxn cnxn;

        IOWorkRequest(SelectorThread selectorThread, SelectionKey key) {
            this.selectorThread = selectorThread;
            this.key = key;
            this.cnxn = (NIOServerCnxn) key.attachment();
        }

        public void doWork() throws InterruptedException {
            if (!key.isValid()) {
                selectorThread.cleanupSelectionKey(key);
                return;
            }

            if (key.isReadable() || key.isWritable()) {
                cnxn.doIO(key);

                // Check if we shutdown or doIO() closed this connection
                if (stopped) {
                    cnxn.close(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);
                    return;
                }
                if (!key.isValid()) {
                    selectorThread.cleanupSelectionKey(key);
                    return;
                }
                touchCnxn(cnxn);
            }

            // Mark this connection as once again ready for selection  （将此连接标记为再次可供选择）
            cnxn.enableSelectable();
            // Push an update request on the queue to resume selecting
            // on the current set of interest ops, which may have changed
            // as a result of the I/O operations we just performed.
            if (!selectorThread.addInterestOpsUpdateRequest(key)) {
                cnxn.close(ServerCnxn.DisconnectReason.CONNECTION_MODE_CHANGED);
            }
        }

        @Override
        public void cleanup() {
            cnxn.close(ServerCnxn.DisconnectReason.CLEAN_UP);
        }

    }

    /**
     * 该线程负责关闭过时的连接，以便未建立任何会话的连接适当的到期。
     */
    /**
     * This thread is responsible for closing stale connections so that
     * connections on which no session is established are properly expired.
     */
    private class ConnectionExpirerThread extends ZooKeeperThread {

        ConnectionExpirerThread() {
            super("ConnnectionExpirer");
        }

        public void run() {
            try {
                while (!stopped) {
                    long waitTime = cnxnExpiryQueue.getWaitTime();
                    if (waitTime > 0) {
                        Thread.sleep(waitTime);
                        continue;
                    }
                    for (NIOServerCnxn conn : cnxnExpiryQueue.poll()) {
                        ServerMetrics.getMetrics().SESSIONLESS_CONNECTIONS_EXPIRED.add(1);
                        conn.close(ServerCnxn.DisconnectReason.CONNECTION_EXPIRED);
                    }
                }

            } catch (InterruptedException e) {
                LOG.info("ConnnectionExpirerThread interrupted");
            }
        }

    }

    ServerSocketChannel ss;

    /**
     * We use this buffer to do efficient socket I/O. Because I/O is handled
     * by the worker threads (or the selector threads directly, if no worker
     * thread pool is created), we can create a fixed set of these to be
     * shared by connections.
     */
    private static final ThreadLocal<ByteBuffer> directBuffer = new ThreadLocal<ByteBuffer>() {
        @Override
        protected ByteBuffer initialValue() {
            return ByteBuffer.allocateDirect(directBufferBytes);
        }
    };

    public static ByteBuffer getDirectBuffer() {
        return directBufferBytes > 0 ? directBuffer.get() : null;
    }

    // ipMap is used to limit connections per IP
    private final ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>> ipMap = new ConcurrentHashMap<InetAddress, Set<NIOServerCnxn>>();

    protected int maxClientCnxns = 60;
    int listenBacklog = -1;

    int sessionlessCnxnTimeout;
    private ExpiryQueue<NIOServerCnxn> cnxnExpiryQueue;

    protected WorkerService workerPool;

    private static int directBufferBytes;
    private int numSelectorThreads;
    private int numWorkerThreads;
    private long workerShutdownTimeoutMS;

    /**
     * Construct a new server connection factory which will accept an unlimited number
     * of concurrent connections from each client (up to the file descriptor
     * limits of the operating system). startup(zks) must be called subsequently.
     */
    public NIOServerCnxnFactory() {
    }

    private volatile boolean stopped = true;
    private ConnectionExpirerThread expirerThread;
    private AcceptThread acceptThread;
    private final Set<SelectorThread> selectorThreads = new HashSet<SelectorThread>();

    /**
     * 配置一些属性，主线操作
     * 1、绑定到对客户端暴露的端口上（熟悉的2181）
     * 2、创建SelectorThread，用于select（只是创建，未start）
     * 3、创建AcceptThread，用于接收连接请求，建立连接后把socket转给SelectorThread处理（只是创建，未start）
     * 4、创建ConnectionExpirerThread
     */
    @Override
    public void configure(InetSocketAddress addr, int maxcc, int backlog, boolean secure) throws IOException {
        if (secure) {
            // NIO不支持SSL
            throw new UnsupportedOperationException("SSL isn't supported in NIOServerCnxn");
        }
        // 配置SASL登录
        configureSaslLogin();

        // 默认60
        maxClientCnxns = maxcc;
        initMaxCnxns();
        sessionlessCnxnTimeout = Integer.getInteger(ZOOKEEPER_NIO_SESSIONLESS_CNXN_TIMEOUT, 10000);
        // We also use the sessionlessCnxnTimeout as expiring interval for
        // cnxnExpiryQueue. These don't need to be the same, but the expiring
        // interval passed into the ExpiryQueue() constructor below should be
        // less than or equal to the timeout.
        cnxnExpiryQueue = new ExpiryQueue<NIOServerCnxn>(sessionlessCnxnTimeout);
        expirerThread = new ConnectionExpirerThread();

        // 获取机器的CPU核数
        int numCores = Runtime.getRuntime().availableProcessors();
        // 32 cores sweet spot seems to be 4 selector threads  (32核心的最佳选择似乎是4个选择器线程)
        // 4核 = 1，8核 = 2
        numSelectorThreads = Integer.getInteger(
            ZOOKEEPER_NIO_NUM_SELECTOR_THREADS,
            Math.max((int) Math.sqrt((float) numCores / 2), 1));
        if (numSelectorThreads < 1) {
            throw new IOException("numSelectorThreads must be at least 1");
        }

        // Worker的数量，CPU核数*2
        numWorkerThreads = Integer.getInteger(ZOOKEEPER_NIO_NUM_WORKER_THREADS, 2 * numCores);
        workerShutdownTimeoutMS = Long.getLong(ZOOKEEPER_NIO_SHUTDOWN_TIMEOUT, 5000);

        String logMsg = "Configuring NIO connection handler with "
            + (sessionlessCnxnTimeout / 1000) + "s sessionless connection timeout, "
            + numSelectorThreads + " selector thread(s), "
            + (numWorkerThreads > 0 ? numWorkerThreads : "no") + " worker threads, and "
            + (directBufferBytes == 0 ? "gathered writes." : ("" + (directBufferBytes / 1024) + " kB direct buffers."));
        LOG.info(logMsg);
        for (int i = 0; i < numSelectorThreads; ++i) {
            // 添加SelectorThread
            selectorThreads.add(new SelectorThread(i));
        }

        listenBacklog = backlog;
        // open一个ServerSocketChannel
        this.ss = ServerSocketChannel.open();
        ss.socket().setReuseAddress(true);
        LOG.info("binding to port {}", addr);
        if (listenBacklog == -1) {
            // 绑定到端口上
            ss.socket().bind(addr);
        } else {
            ss.socket().bind(addr, listenBacklog);
        }
        // 设置非阻塞
        ss.configureBlocking(false);
        // 创建一个Accepter，用于接收连接
        acceptThread = new AcceptThread(ss, addr, selectorThreads);
    }

    private void tryClose(ServerSocketChannel s) {
        try {
            s.close();
        } catch (IOException sse) {
            LOG.error("Error while closing server socket.", sse);
        }
    }

    @Override
    public void reconfigure(InetSocketAddress addr) {
        ServerSocketChannel oldSS = ss;
        try {
            acceptThread.setReconfiguring();
            tryClose(oldSS);
            acceptThread.wakeupSelector();
            try {
                acceptThread.join();
            } catch (InterruptedException e) {
                LOG.error("Error joining old acceptThread when reconfiguring client port.", e);
                Thread.currentThread().interrupt();
            }
            this.ss = ServerSocketChannel.open();
            ss.socket().setReuseAddress(true);
            LOG.info("binding to port {}", addr);
            ss.socket().bind(addr);
            ss.configureBlocking(false);
            acceptThread = new AcceptThread(ss, addr, selectorThreads);
            acceptThread.start();
        } catch (IOException e) {
            LOG.error("Error reconfiguring client port to {}", addr, e);
            tryClose(oldSS);
        }
    }

    /** {@inheritDoc} */
    public int getMaxClientCnxnsPerHost() {
        return maxClientCnxns;
    }

    /** {@inheritDoc} */
    public void setMaxClientCnxnsPerHost(int max) {
        maxClientCnxns = max;
    }

    /** {@inheritDoc} */
    public int getSocketListenBacklog() {
        return listenBacklog;
    }

    @Override
    public void start() {
        stopped = false;
        if (workerPool == null) {
            // 创建一个fixed线程池，线程数为numWorkerThreads
            workerPool = new WorkerService("NIOWorker", numWorkerThreads, false);
        }
        for (SelectorThread thread : selectorThreads) {
            // 遍历启动SelectorThread
            if (thread.getState() == Thread.State.NEW) {
                thread.start();
            }
        }
        // ensure thread is started once and only once
        if (acceptThread.getState() == Thread.State.NEW) {
            // 遍历启动AcceptThread
            acceptThread.start();
        }
        if (expirerThread.getState() == Thread.State.NEW) {
            // 遍历启动expirerThread
            expirerThread.start();
        }
    }

    @Override
    public void startup(ZooKeeperServer zks, boolean startServer) throws IOException, InterruptedException {
        start();
        setZooKeeperServer(zks);
        if (startServer) {
            zks.startdata();
            zks.startup();
        }
    }

    @Override
    public InetSocketAddress getLocalAddress() {
        return (InetSocketAddress) ss.socket().getLocalSocketAddress();
    }

    @Override
    public int getLocalPort() {
        return ss.socket().getLocalPort();
    }

    /**
     * De-registers the connection from the various mappings maintained
     * by the factory.
     */
    public boolean removeCnxn(NIOServerCnxn cnxn) {
        // If the connection is not in the master list it's already been closed
        if (!cnxns.remove(cnxn)) {
            return false;
        }
        cnxnExpiryQueue.remove(cnxn);

        removeCnxnFromSessionMap(cnxn);

        InetAddress addr = cnxn.getSocketAddress();
        if (addr != null) {
            Set<NIOServerCnxn> set = ipMap.get(addr);
            if (set != null) {
                set.remove(cnxn);
                // Note that we make no effort here to remove empty mappings
                // from ipMap.
            }
        }

        // unregister from JMX
        unregisterConnection(cnxn);
        return true;
    }

    /**
     * Add or update cnxn in our cnxnExpiryQueue
     * @param cnxn
     */
    // // 更新这个连接的过期时间
    public void touchCnxn(NIOServerCnxn cnxn) {
        cnxnExpiryQueue.update(cnxn, cnxn.getSessionTimeout());
    }

    private void addCnxn(NIOServerCnxn cnxn) throws IOException {
        InetAddress addr = cnxn.getSocketAddress();
        if (addr == null) {
            throw new IOException("Socket of " + cnxn + " has been closed");
        }
        Set<NIOServerCnxn> set = ipMap.get(addr);
        if (set == null) {
            // in general we will see 1 connection from each
            // host, setting the initial cap to 2 allows us
            // to minimize mem usage in the common case
            // of 1 entry --  we need to set the initial cap
            // to 2 to avoid rehash when the first entry is added
            // Construct a ConcurrentHashSet using a ConcurrentHashMap
            set = Collections.newSetFromMap(new ConcurrentHashMap<NIOServerCnxn, Boolean>(2));
            // Put the new set in the map, but only if another thread
            // hasn't beaten us to it
            Set<NIOServerCnxn> existingSet = ipMap.putIfAbsent(addr, set);
            if (existingSet != null) {
                set = existingSet;
            }
        }
        set.add(cnxn);

        cnxns.add(cnxn);
        // 更新这个连接的过期时间
        touchCnxn(cnxn);
    }

    protected NIOServerCnxn createConnection(SocketChannel sock, SelectionKey sk, SelectorThread selectorThread) throws IOException {
        return new NIOServerCnxn(zkServer, sock, sk, this, selectorThread);
    }

    private int getClientCnxnCount(InetAddress cl) {
        Set<NIOServerCnxn> s = ipMap.get(cl);
        if (s == null) {
            return 0;
        }
        return s.size();
    }

    /**
     * clear all the connections in the selector
     *
     */
    @Override
    @SuppressWarnings("unchecked")
    public void closeAll(ServerCnxn.DisconnectReason reason) {
        // clear all the connections on which we are selecting
        for (ServerCnxn cnxn : cnxns) {
            try {
                // This will remove the cnxn from cnxns
                cnxn.close(reason);
            } catch (Exception e) {
                LOG.warn(
                    "Ignoring exception closing cnxn session id 0x{}",
                    Long.toHexString(cnxn.getSessionId()),
                    e);
            }
        }
    }

    public void stop() {
        stopped = true;

        // Stop queuing connection attempts
        try {
            ss.close();
        } catch (IOException e) {
            LOG.warn("Error closing listen socket", e);
        }

        if (acceptThread != null) {
            if (acceptThread.isAlive()) {
                acceptThread.wakeupSelector();
            } else {
                acceptThread.closeSelector();
            }
        }
        if (expirerThread != null) {
            expirerThread.interrupt();
        }
        for (SelectorThread thread : selectorThreads) {
            if (thread.isAlive()) {
                thread.wakeupSelector();
            } else {
                thread.closeSelector();
            }
        }
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        try {
            // close listen socket and signal selector threads to stop
            stop();

            // wait for selector and worker threads to shutdown
            join();

            // close all open connections
            closeAll(ServerCnxn.DisconnectReason.SERVER_SHUTDOWN);

            if (login != null) {
                login.shutdown();
            }
        } catch (InterruptedException e) {
            LOG.warn("Ignoring interrupted exception during shutdown", e);
        } catch (Exception e) {
            LOG.warn("Ignoring unexpected exception during shutdown", e);
        }

        if (zkServer != null) {
            zkServer.shutdown();
        }
    }

    @Override
    public void join() throws InterruptedException {
        if (acceptThread != null) {
            acceptThread.join();
        }
        for (SelectorThread thread : selectorThreads) {
            thread.join();
        }
        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }
    }

    @Override
    public Iterable<ServerCnxn> getConnections() {
        return cnxns;
    }

    public void dumpConnections(PrintWriter pwriter) {
        pwriter.print("Connections ");
        cnxnExpiryQueue.dump(pwriter);
    }

    @Override
    public void resetAllConnectionStats() {
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            c.resetStats();
        }
    }

    @Override
    public Iterable<Map<String, Object>> getAllConnectionInfo(boolean brief) {
        HashSet<Map<String, Object>> info = new HashSet<Map<String, Object>>();
        // No need to synchronize since cnxns is backed by a ConcurrentHashMap
        for (ServerCnxn c : cnxns) {
            info.add(c.getConnectionInfo(brief));
        }
        return info;
    }

}
