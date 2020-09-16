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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import java.util.ArrayDeque;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.zookeeper.ZooDefs.OpCode;
import org.apache.zookeeper.common.Time;
import org.apache.zookeeper.server.Request;
import org.apache.zookeeper.server.RequestProcessor;
import org.apache.zookeeper.server.ServerMetrics;
import org.apache.zookeeper.server.WorkerService;
import org.apache.zookeeper.server.ZooKeeperCriticalThread;
import org.apache.zookeeper.server.ZooKeeperServerListener;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * 该RequestProcessor将传入的提交请求与本地提交的请求进行匹配。
 * 诀窍是，本地提交的更改系统状态的请求将作为传入的已提交请求返回，因此我们需要将它们匹配。
 * 我们不仅等待已提交的请求，还处理属于其他会话的未提交的请求。
 *
 * CommitProcessor是多线程的。线程之间的通信是通过队列，atomics,和在处理器上同步的wait/notifyAll处理的。
 * CommitProcessor充当网关，用于允许请求继续处理管道的其余部分。
 * 它将允许许多读取请求，但只有一个写入请求可以同时进行，从而确保以事务ID顺序处理写入请求。
 *
 * -1     CommitProcessor主线程，该线程监视请求队列，并根据工作线程的sessionId将请求分配给工作线程，以便始终将特定会话的读写请求分配给同一线程（因此保证按顺序运行）。
 * -0-N   工作线程，将请求转给下一个RequestProcessor处理。如果配置了0个工作线程，则由CommitProcessor主线程干这个活。
 *
 * 典型的（默认）线程计数是：在32核计算机上，1个提交处理器线程和32个工作线程。
 *
 * 多线程约束：
 *      - 必须按顺序处理每个会话的请求。
 *      - 必须以zxid顺序处理写请求
 *      - 必须确保一个会话中的写操作之间没有竞争条件，否则将触发另一会话中的读请求设置监视。
 *
 * 当前实现通过简单地不允许处理任何读请求来解决第三个约束与写请求并行。
 */
// 同步转异步

/**
 * This RequestProcessor matches the incoming committed requests with the
 * locally submitted requests. The trick is that locally submitted requests that
 * change the state of the system will come back as incoming committed requests,
 * so we need to match them up. Instead of just waiting for the committed requests,
 * we process the uncommitted requests that belong to other sessions.
 *
 * The CommitProcessor is multi-threaded. Communication between threads is
 * handled via queues, atomics, and wait/notifyAll synchronized on the
 * processor. The CommitProcessor acts as a gateway for allowing requests to
 * continue with the remainder of the processing pipeline. It will allow many
 * read requests but only a single write request to be in flight simultaneously,
 * thus ensuring that write requests are processed in transaction id order.
 *
 *   - 1   commit processor main thread, which watches the request queues and
 *         assigns requests to worker threads based on their sessionId so that
 *         read and write requests for a particular session are always assigned
 *         to the same thread (and hence are guaranteed to run in order).
 *   - 0-N worker threads, which run the rest of the request processor pipeline
 *         on the requests. If configured with 0 worker threads, the primary
 *         commit processor thread runs the pipeline directly.
 *
 * Typical (default) thread counts are: on a 32 core machine, 1 commit
 * processor thread and 32 worker threads.
 *
 * Multi-threading constraints:
 *   - Each session's requests must be processed in order.
 *   - Write requests must be processed in zxid order
 *   - Must ensure no race condition between writes in one session that would
 *     trigger a watch being set by a read request in another session
 *
 * The current implementation solves the third constraint by simply allowing no
 * read requests to be processed in parallel with write requests.
 */
public class CommitProcessor extends ZooKeeperCriticalThread implements RequestProcessor {

    private static final Logger LOG = LoggerFactory.getLogger(CommitProcessor.class);

    /** Default: numCores */
    public static final String ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS = "zookeeper.commitProcessor.numWorkerThreads";
    /** Default worker pool shutdown timeout in ms: 5000 (5s) */
    public static final String ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT = "zookeeper.commitProcessor.shutdownTimeout";
    /** Default max read batch size: -1 to disable the feature */
    public static final String ZOOKEEPER_COMMIT_PROC_MAX_READ_BATCH_SIZE = "zookeeper.commitProcessor.maxReadBatchSize";
    /** Default max commit batch size: 1 */
    public static final String ZOOKEEPER_COMMIT_PROC_MAX_COMMIT_BATCH_SIZE = "zookeeper.commitProcessor.maxCommitBatchSize";

    /**
     * Incoming requests.
     * （译：传入的请求。）
     */
    // 待处理的请求，可读可写
    protected LinkedBlockingQueue<Request> queuedRequests = new LinkedBlockingQueue<Request>();

    /**
     * Incoming requests that are waiting on a commit,
     * contained in order of arrival
     * （译：等待commit的传入请求，按到达顺序包含）
     */
    // 等待 commit 的请求会记录到这里面，投票通过了，收到 commit 请求后，会出队
    // 这个队列的作用，就是和 committedRequests 中的元素做对比，匹配上了才能提交。
    protected final LinkedBlockingQueue<Request> queuedWriteRequests = new LinkedBlockingQueue<>();

    /**
     * The number of read requests currently held in all session queues
     * （译：当前所有会话队列中保留的读请求数）
     */
    private AtomicInteger numReadQueuedRequests = new AtomicInteger(0);

    /**
     * The number of quorum requests currently held in all session queued
     * （译：当前在所有会话中排队的quorum请求数）
     */
    private AtomicInteger numWriteQueuedRequests = new AtomicInteger(0);

    /**
     * Requests that have been committed.
     * （译：已提交的请求。）
     */
    // commit 请求，通知该处理器，这些请求包含的 zxid 已经投票通过了，可以 commit 了
    protected final LinkedBlockingQueue<Request> committedRequests = new LinkedBlockingQueue<Request>();

    /**
     * Requests that we are holding until commit comes in. Keys represent
     * session ids, each value is a linked list of the session's requests.
     * （译：在 commit 之前，我们一直 holding 的请求。键代表会话ID，每个值都是会话请求的链接列表。）
     */
    protected final Map<Long, Deque<Request>> pendingRequests = new HashMap<>(10000);

    /** The number of requests currently being processed
     * （译：当前正在处理的请求数）
     */
    protected final AtomicInteger numRequestsProcessing = new AtomicInteger(0);

    RequestProcessor nextProcessor;

    /** For testing purposes, we use a separated stopping condition for the
     * outer loop.*/
    protected volatile boolean stoppedMainLoop = true;
    protected volatile boolean stopped = true;
    private long workerShutdownTimeoutMS;
    protected WorkerService workerPool;
    private Object emptyPoolSync = new Object();

    /**
     * Max number of reads to process from queuedRequests before switching to
     * processing commits. If the value is negative, we switch whenever we have
     * a local write, and pending commits.
     * A high read batch size will delay commit processing causing us to
     * serve stale data.
     */
    private static volatile int maxReadBatchSize;
    /**
     * Max number of commits to process before processing reads. We will try to
     * process as many remote/local commits as we can till we reach this
     * count.
     * A high commit batch size will delay reads while processing more commits.
     * A low commit batch size will favor reads.
     */
    private static volatile int maxCommitBatchSize;

    /**
     * This flag indicates whether we need to wait for a response to come back from the
     * leader or we just let the sync operation flow through like a read. The flag will
     * be false if the CommitProcessor is in a Leader pipeline.
     *
     * （译：此标志指示我们是否需要等待leader返回的响应，还是让同步操作像读取一样流过。如果CommitProcessor在Leader管道中，则该标志为false。）
     */
    boolean matchSyncs;

    public CommitProcessor(RequestProcessor nextProcessor, String id, boolean matchSyncs, ZooKeeperServerListener listener) {
        super("CommitProcessor:" + id, listener);
        this.nextProcessor = nextProcessor;
        this.matchSyncs = matchSyncs;
    }

    private boolean isProcessingRequest() {
        return numRequestsProcessing.get() != 0;
    }

    protected boolean needCommit(Request request) {
        switch (request.type) {
        case OpCode.create:
        case OpCode.create2:
        case OpCode.createTTL:
        case OpCode.createContainer:
        case OpCode.delete:
        case OpCode.deleteContainer:
        case OpCode.setData:
        case OpCode.reconfig:
        case OpCode.multi:
        case OpCode.setACL:
        case OpCode.check:
            return true;
        case OpCode.sync:
            // Leader创建的CommitProcessor为false，Follower创建的为true
            return matchSyncs;
        case OpCode.createSession:
        case OpCode.closeSession:
            return !request.isLocalSession();
        default:
            return false;
        }
    }

    @Override
    public void run() {
        try {
            /*
             * In each iteration of the following loop we process at most
             * requestsToProcess requests of queuedRequests. We have to limit
             * the number of request we poll from queuedRequests, since it is
             * possible to endlessly poll read requests from queuedRequests, and
             * that will lead to a starvation of non-local committed requests.
             *
             * 译：在以下循环的每次迭代中，我们最多处理 queueedRequests 的 requestsToProcess 个请求。
             *    我们必须限制从 queuedRequests 轮询的请求数，因为有可能无限轮询来自 queuedRequests 的读取请求，
             *    这将导致非本地提交请求的匮乏。
             */
            int requestsToProcess = 0;
            boolean commitIsWaiting = false;
            do {
                /*
                 * Since requests are placed in the queue before being sent to
                 * the leader, if commitIsWaiting = true, the commit belongs to
                 * the first update operation in the queuedRequests or to a
                 * request from a client on another server (i.e., the order of
                 * the following two lines is important!).
                 *
                 * 译：由于请求是在发送给 Leader 之前放入队列中，因此如果 commitIsWaiting = true，
                 *    则 commit 属于 queuedRequests 中的第一个更新操作，
                 *    或者属于另一台服务器上客户端的请求（即，以下两行的顺序很重要！））。
                 */
                commitIsWaiting = !committedRequests.isEmpty();  // 有正在等待的 commit 请求
                requestsToProcess = queuedRequests.size();  // 要处理的请求
                // Avoid sync if we have something to do  （译：如果我们有事情要做，请避免同步）
                if (requestsToProcess == 0 && !commitIsWaiting) {  // 如果没有待处理的请求，也没有等待中的 commit 请求（即 committedRequests 为空）
                    // Waiting for requests to process  （译：等待处理请求）
                    synchronized (this) {
                        while (!stopped && requestsToProcess == 0 && !commitIsWaiting) {
                            // 挂起，等待notify
                            wait();
                            // 被 notify 后，重新检查，看是否跳出循环
                            commitIsWaiting = !committedRequests.isEmpty();
                            requestsToProcess = queuedRequests.size();
                        }
                    }
                }

                // 执行到这里，说明committedRequests、queuedRequests其中之一不为空

                ServerMetrics.getMetrics().READS_QUEUED_IN_COMMIT_PROCESSOR.add(numReadQueuedRequests.get());
                ServerMetrics.getMetrics().WRITES_QUEUED_IN_COMMIT_PROCESSOR.add(numWriteQueuedRequests.get());
                ServerMetrics.getMetrics().COMMITS_QUEUED_IN_COMMIT_PROCESSOR.add(committedRequests.size());

                long time = Time.currentElapsedTime();

                /*
                 * Processing up to requestsToProcess requests from the incoming
                 * queue (queuedRequests). If maxReadBatchSize is set then no
                 * commits will be processed until maxReadBatchSize number of
                 * reads are processed (or no more reads remain in the queue).
                 * After the loop a single committed request is processed if
                 * one is waiting (or a batch of commits if maxCommitBatchSize
                 * is set).
                 *
                 * 译：处理来自传入队列（queuedRequests）的最多requestToProcess请求。
                 *    如果设置了maxReadBatchSize，则在处理maxReadBatchSize次读取之前（或队列中不再剩余读取），将不处理任何提交。
                 *    循环后，如果一个committed请求正在等待，则将处理一个committed的请求（如果设置了maxCommitBatchSize，则将处理一组 commit）。
                 */
                Request request;
                int readsProcessed = 0;
                /** 1、处理queuedRequests中的请求 */
                while (!stopped
                       && requestsToProcess > 0
                       && (maxReadBatchSize < 0 || readsProcessed <= maxReadBatchSize)
                       && (request = queuedRequests.poll()) != null) {
                    // 待处理的请求数 -1
                    requestsToProcess--;
                    if (needCommit(request) || pendingRequests.containsKey(request.sessionId)) {
                        // Add request to pending
                        // 将请求加入pending队列，每个sessionId对应一个队列
                        Deque<Request> requests = pendingRequests.computeIfAbsent(request.sessionId, sid -> new ArrayDeque<>());
                        requests.addLast(request);
                        ServerMetrics.getMetrics().REQUESTS_IN_SESSION_QUEUE.add(requests.size());
                    } else {
                        readsProcessed++;
                        numReadQueuedRequests.decrementAndGet();
                        // 包装成 CommitWorkRequest，然后丢到线程池里，task的主要功能是将请求转给下一个RequestProcessor
                        sendToNextProcessor(request);
                    }
                    /*
                     * Stop feeding the pool if there is a local pending update
                     * and a committed request that is ready. Once we have a
                     * pending request with a waiting committed request, we know
                     * we can process the committed one. This is because commits
                     * for local requests arrive in the order they appeared in
                     * the queue, so if we have a pending request and a
                     * committed request, the committed request must be for that
                     * pending write or for a write originating at a different
                     * server. We skip this if maxReadBatchSize is set.
                     *
                     * 译：如果存在本地暂挂更新和已提交的已提交请求，请停止向池中添加数据。
                     *    一旦我们有了一个待处理的请求和一个等待的提交请求，我们就知道可以处理提交的请求了。
                     *    这是因为对本地请求的提交以它们在队列中出现的顺序到达，因此，如果我们有一个挂起的请求和一个提交的请求，
                     *    则提交的请求必须是针对该挂起的写或源自其他服务器的写的。 如果设置了maxReadBatchSize，我们将跳过此步骤。
                     */
                    if (maxReadBatchSize < 0 && !pendingRequests.isEmpty() && !committedRequests.isEmpty()) {  // maxReadBatchSize默认-1
                        /*
                         * We set commitIsWaiting so that we won't check committedRequests again.
                         * （译：我们设置commitIsWaiting，这样我们就不会再次检查commitRequests。）
                         */
                        // 有等待中的pendingRequests或者committedRequests，则跳出循环，不继续处理queuedRequests了.....
                        commitIsWaiting = true;
                        break;
                    }
                }
                ServerMetrics.getMetrics().READS_ISSUED_IN_COMMIT_PROC.add(readsProcessed);

                if (!commitIsWaiting) {
                    commitIsWaiting = !committedRequests.isEmpty();
                }

                /*
                 * Handle commits, if any.
                 * （译：处理commits（如果有的话）。）
                 */
                /** 处理 committedRequests 中的请求*/
                if (commitIsWaiting && !stopped) {
                    /*
                     * Drain outstanding reads
                     */
                    // 阻塞到 numRequestsProcessing 消费完（上面的sendToNextProcessor丢到线程池前，会给这个值+1）
                    waitForEmptyPool();

                    if (stopped) {
                        return;
                    }

                    // 默认1
                    int commitsToProcess = maxCommitBatchSize;

                    /*
                     * Loop through all the commits, and try to drain them.
                     * （译：遍历所有commits，并尝试将其耗尽。）
                     */
                    Set<Long> queuesToDrain = new HashSet<>();
                    long startWriteTime = Time.currentElapsedTime();
                    int commitsProcessed = 0;
                    while (commitIsWaiting && !stopped && commitsToProcess > 0) {

                        // Process committed head
                        // 获取队列头，但是不会 remove 掉
                        request = committedRequests.peek();

                        /*
                         * Check if this is a local write request is pending,
                         * if so, update it with the committed info. If the commit matches
                         * the first write queued in the blockedRequestQueue, we know this is
                         * a commit for a local write, as commits are received in order. Else
                         * it must be a commit for a remote write.
                         *
                         * 译：检查这是否是本地写请求挂起，如果是，请使用提交的信息对其进行更新。
                         *    如果提交与 blockedRequestQueue 中排队的第一个写入匹配，
                         *    则我们知道这是本地写入的 commit ，因为按顺序接收到 commit。 否则，它必须是远程写的 commit 。
                         */
                        if (!queuedWriteRequests.isEmpty()
                            && queuedWriteRequests.peek().sessionId == request.sessionId
                            && queuedWriteRequests.peek().cxid == request.cxid) {  // committedRequests 的第一个元素，和 queuedWriteRequests 的第一个元素匹配
                            /*
                             * Commit matches the earliest write in our write queue.
                             * （译：commit 匹配到我们的写入队列中最早的写入。）
                             */
                            Deque<Request> sessionQueue = pendingRequests.get(request.sessionId);
                            ServerMetrics.getMetrics().PENDING_SESSION_QUEUE_SIZE.add(pendingRequests.size());
                            if (sessionQueue == null || sessionQueue.isEmpty() || !needCommit(sessionQueue.peek())) {
                                /*
                                 * Can't process this write yet.
                                 * Either there are reads pending in this session, or we
                                 * haven't gotten to this write yet.
                                 * （译：尚无法处理此写入。 在此会话中有待处理的读取，或者我们还没有写完。）
                                 */
                                break;
                            } else {
                                ServerMetrics.getMetrics().REQUESTS_IN_SESSION_QUEUE.add(sessionQueue.size());
                                // If session queue != null, then it is also not empty.
                                Request topPending = sessionQueue.poll();
                                /*
                                 * Generally, we want to send to the next processor our version of the request,
                                 * since it contains the session information that is needed for post update processing.
                                 * In more details, when a request is in the local queue, there is (or could be) a client
                                 * attached to this server waiting for a response, and there is other bookkeeping of
                                 * requests that are outstanding and have originated from this server
                                 * (e.g., for setting the max outstanding requests) - we need to update this info when an
                                 * outstanding request completes. Note that in the other case, the operation
                                 * originated from a different server and there is no local bookkeeping or a local client
                                 * session that needs to be notified.
                                 *
                                 * 译：
                                 * 通常，我们希望将请求的版本发送到下一个处理器，因为它包含更新后处理所需的会话信息。
                                 * 更详细地，当请求在本地队列中时，有一个（或可能有）一个连接到此服务器的客户端等待响应，
                                 * 并且存在其他其他未完成的请求的簿记（例如， （用于设置最大未完成请求）-我们需要在未完成请求完成时更新此信息。
                                 * 请注意，在另一种情况下，该操作源自其他服务器，并且没有本地簿记或本地客户端会话需要通知。
                                 */
                                topPending.setHdr(request.getHdr());
                                topPending.setTxn(request.getTxn());
                                topPending.setTxnDigest(request.getTxnDigest());
                                topPending.zxid = request.zxid;
                                topPending.commitRecvTime = request.commitRecvTime;
                                request = topPending;
                                // Only decrement if we take a request off the queue.
                                numWriteQueuedRequests.decrementAndGet();
                                queuedWriteRequests.poll();
                                queuesToDrain.add(request.sessionId);
                            }
                        }
                        /*
                         * Pull the request off the commit queue, now that we are going to process it.
                         * （译：现在，我们将处理请求，使其脱离提交队列。）
                         */
                        committedRequests.remove();
                        commitsToProcess--;
                        commitsProcessed++;

                        // Process the write inline.
                        // 调用下一个RequestProcessor
                        processWrite(request);

                        commitIsWaiting = !committedRequests.isEmpty();
                    }
                    ServerMetrics.getMetrics().WRITE_BATCH_TIME_IN_COMMIT_PROCESSOR
                        .add(Time.currentElapsedTime() - startWriteTime);
                    ServerMetrics.getMetrics().WRITES_ISSUED_IN_COMMIT_PROC.add(commitsProcessed);

                    /*
                     * Process following reads if any, remove session queue(s) if empty.
                     * （译：后续进程读取（如果有），如果为空，则删除会话队列。）
                     */
                    readsProcessed = 0;
                    for (Long sessionId : queuesToDrain) {
                        Deque<Request> sessionQueue = pendingRequests.get(sessionId);
                        int readsAfterWrite = 0;
                        while (!stopped && !sessionQueue.isEmpty() && !needCommit(sessionQueue.peek())) {
                            numReadQueuedRequests.decrementAndGet();
                            // 不需要提交，所以是读请求？
                            sendToNextProcessor(sessionQueue.poll());
                            readsAfterWrite++;
                        }
                        ServerMetrics.getMetrics().READS_AFTER_WRITE_IN_SESSION_QUEUE.add(readsAfterWrite);
                        readsProcessed += readsAfterWrite;

                        // Remove empty queues
                        if (sessionQueue.isEmpty()) {
                            pendingRequests.remove(sessionId);
                        }
                    }
                    ServerMetrics.getMetrics().SESSION_QUEUES_DRAINED.add(queuesToDrain.size());
                    ServerMetrics.getMetrics().READ_ISSUED_FROM_SESSION_QUEUE.add(readsProcessed);
                }

                ServerMetrics.getMetrics().COMMIT_PROCESS_TIME.add(Time.currentElapsedTime() - time);
                // 啥也没干
                endOfIteration();
            } while (!stoppedMainLoop);
        } catch (Throwable e) {
            handleException(this.getName(), e);
        }
        LOG.info("CommitProcessor exited loop!");
    }

    //for test only
    protected void endOfIteration() {

    }

    protected void waitForEmptyPool() throws InterruptedException {
        int numRequestsInProcess = numRequestsProcessing.get();
        if (numRequestsInProcess != 0) {
            ServerMetrics.getMetrics().CONCURRENT_REQUEST_PROCESSING_IN_COMMIT_PROCESSOR.add(numRequestsInProcess);
        }

        long startWaitTime = Time.currentElapsedTime();
        synchronized (emptyPoolSync) {
            while ((!stopped) && isProcessingRequest()) {
                emptyPoolSync.wait();
            }
        }
        ServerMetrics.getMetrics().TIME_WAITING_EMPTY_POOL_IN_COMMIT_PROCESSOR_READ
            .add(Time.currentElapsedTime() - startWaitTime);
    }

    @Override
    public void start() {
        int numCores = Runtime.getRuntime().availableProcessors();
        int numWorkerThreads = Integer.getInteger(ZOOKEEPER_COMMIT_PROC_NUM_WORKER_THREADS, numCores);
        workerShutdownTimeoutMS = Long.getLong(ZOOKEEPER_COMMIT_PROC_SHUTDOWN_TIMEOUT, 5000);

        initBatchSizes();

        LOG.info(
            "Configuring CommitProcessor with {} worker threads.",
            numWorkerThreads > 0 ? numWorkerThreads : "no");
        if (workerPool == null) {
            workerPool = new WorkerService("CommitProcWork", numWorkerThreads, true);
        }
        stopped = false;
        stoppedMainLoop = false;
        super.start();
    }

    /**
     * Schedule final request processing; if a worker thread pool is not being
     * used, processing is done directly by this thread.
     */
    private void sendToNextProcessor(Request request) {
        numRequestsProcessing.incrementAndGet();
        workerPool.schedule(new CommitWorkRequest(request), request.sessionId);
    }

    private void processWrite(Request request) throws RequestProcessorException {
        processCommitMetrics(request, true);

        long timeBeforeFinalProc = Time.currentElapsedTime();
        nextProcessor.processRequest(request);
        ServerMetrics.getMetrics().WRITE_FINAL_PROC_TIME.add(Time.currentElapsedTime() - timeBeforeFinalProc);
    }

    private static void initBatchSizes() {
        maxReadBatchSize = Integer.getInteger(ZOOKEEPER_COMMIT_PROC_MAX_READ_BATCH_SIZE, -1);
        maxCommitBatchSize = Integer.getInteger(ZOOKEEPER_COMMIT_PROC_MAX_COMMIT_BATCH_SIZE, 1);

        if (maxCommitBatchSize <= 0) {
            String errorMsg = "maxCommitBatchSize must be positive, was " + maxCommitBatchSize;
            throw new IllegalArgumentException(errorMsg);
        }

        LOG.info
            ("Configuring CommitProcessor with readBatchSize {} commitBatchSize {}",
             maxReadBatchSize,
             maxCommitBatchSize);
    }

    private static void processCommitMetrics(Request request, boolean isWrite) {
        if (isWrite) {
            if (request.commitProcQueueStartTime != -1 && request.commitRecvTime != -1) {
                // Locally issued writes.
                long currentTime = Time.currentElapsedTime();
                ServerMetrics.getMetrics().WRITE_COMMITPROC_TIME.add(currentTime - request.commitProcQueueStartTime);
                ServerMetrics.getMetrics().LOCAL_WRITE_COMMITTED_TIME.add(currentTime - request.commitRecvTime);
            } else if (request.commitRecvTime != -1) {
                // Writes issued by other servers.
                ServerMetrics.getMetrics().SERVER_WRITE_COMMITTED_TIME
                    .add(Time.currentElapsedTime() - request.commitRecvTime);
            }
        } else {
            if (request.commitProcQueueStartTime != -1) {
                ServerMetrics.getMetrics().READ_COMMITPROC_TIME
                    .add(Time.currentElapsedTime() - request.commitProcQueueStartTime);
            }
        }
    }

    public static int getMaxReadBatchSize() {
        return maxReadBatchSize;
    }

    public static int getMaxCommitBatchSize() {
        return maxCommitBatchSize;
    }

    public static void setMaxReadBatchSize(int size) {
        maxReadBatchSize = size;
        LOG.info("Configuring CommitProcessor with readBatchSize {}", maxReadBatchSize);
    }

    public static void setMaxCommitBatchSize(int size) {
        if (size > 0) {
            maxCommitBatchSize = size;
            LOG.info("Configuring CommitProcessor with commitBatchSize {}", maxCommitBatchSize);
        }
    }

    /**
     * CommitWorkRequest is a small wrapper class to allow
     * downstream processing to be run using the WorkerService
     *
     * （译：CommitWorkRequest是一个小型包装器类，允许使用WorkerService运行下游处理）
     */
    private class CommitWorkRequest extends WorkerService.WorkRequest {

        private final Request request;

        CommitWorkRequest(Request request) {
            this.request = request;
        }

        @Override
        public void cleanup() {
            if (!stopped) {
                LOG.error("Exception thrown by downstream processor, unable to continue.");
                CommitProcessor.this.halt();
            }
        }

        public void doWork() throws RequestProcessorException {
            try {
                processCommitMetrics(request, needCommit(request));

                long timeBeforeFinalProc = Time.currentElapsedTime();
                nextProcessor.processRequest(request);
                if (needCommit(request)) {
                    ServerMetrics.getMetrics().WRITE_FINAL_PROC_TIME
                        .add(Time.currentElapsedTime() - timeBeforeFinalProc);
                } else {
                    ServerMetrics.getMetrics().READ_FINAL_PROC_TIME
                        .add(Time.currentElapsedTime() - timeBeforeFinalProc);
                }

            } finally {

                if (numRequestsProcessing.decrementAndGet() == 0) {
                    wakeupOnEmpty();
                }
            }
        }

    }

    @SuppressFBWarnings("NN_NAKED_NOTIFY")
    private synchronized void wakeup() {
        notifyAll();
    }

    private void wakeupOnEmpty() {
        synchronized (emptyPoolSync) {
            emptyPoolSync.notifyAll();
        }
    }

    public void commit(Request request) {
        if (stopped || request == null) {
            return;
        }
        LOG.debug("Committing request:: {}", request);
        request.commitRecvTime = Time.currentElapsedTime();
        ServerMetrics.getMetrics().COMMITS_QUEUED.add(1);
        committedRequests.add(request);
        wakeup();
    }

    @Override
    public void processRequest(Request request) {
        if (stopped) {
            return;
        }
        LOG.debug("Processing request:: {}", request);
        request.commitProcQueueStartTime = Time.currentElapsedTime();
        queuedRequests.add(request);
        // If the request will block, add it to the queue of blocking requests  （译：如果请求将被阻塞，请将其添加到阻塞请求的队列中）
        if (needCommit(request)) {
            // 需要进行 commit 操作的进入这个队列
            queuedWriteRequests.add(request);
            numWriteQueuedRequests.incrementAndGet();
        } else {
            numReadQueuedRequests.incrementAndGet();
        }
        wakeup();
    }

    private void halt() {
        stoppedMainLoop = true;
        stopped = true;
        wakeupOnEmpty();
        wakeup();
        queuedRequests.clear();
        if (workerPool != null) {
            workerPool.stop();
        }
    }

    public void shutdown() {
        LOG.info("Shutting down");

        halt();

        if (workerPool != null) {
            workerPool.join(workerShutdownTimeoutMS);
        }

        if (nextProcessor != null) {
            nextProcessor.shutdown();
        }
    }

}
