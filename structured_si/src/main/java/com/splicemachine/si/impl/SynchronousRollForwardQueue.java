package com.splicemachine.si.impl;

import com.google.common.collect.Lists;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import com.splicemachine.si.api.RollForwardQueue;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.Pair;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.*;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

/**
 * Collect "roll-forward" requests for a given table. Accumulate these by transaction ID. At regular intervals
 * asynchronously execute roll-forwards on a per transaction basis. A roll-forward request is an update to the SI
 * metadata on a data row to reflect the final transaction status for a given transaction.
 * <p/>
 * The core asynchronous mechanism is implemented by scheduling future tasks on the scheduler.
 * <p/>
 * Roll-forward requests are generated both as rows are written within transactions and as rows are read. It is possible
 * that when the roll-forward action is executed, the transaction has not yet reached a final state. In these cases the
 * roll-forward request is ignored by the lower level code.
 * <p/>
 * The roll-forward actions are executed in a "best effort" manner. That is, it is acceptable for them to not be run.
 * The SI algorithms will still be correct in these cases. If a roll-forward request is ignored then it is expected that
 * a later request will be generated (e.g. the next time the row is read or compacted).
 * <p/>
 * This is closely related to the "roll-forward" that occurs at HBase compaction time, but that logic follows a
 * different code path.
 * <p/>
 * One of these is created for each HBase region. It needs to be thread-safe, since many concurrent threads can
 * generate roll-forward requests for the region. Therefore, pay particular attention to the synchronized blocks below.
 * They are written to provide crisp semantics, to avoid concurrent changes to the underlying queue, and to only have as
 * much code as required inside of the synchronized blocks. All of the mutable state is accessed from inside of
 * synchronized blocks.
 */
public class SynchronousRollForwardQueue implements RollForwardQueue {
    static final Logger LOG = Logger.getLogger(SynchronousRollForwardQueue.class);

    /**
     * The thread pool to use for running asynchronous roll-forward actions.
     */
    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5,
            new ThreadFactoryBuilder().setNameFormat("synchronous-rollforward-%d").build());


    /**
     * A plug-point to define what action to take at roll-forward time.
     */
    private final RollForwardAction action;

    /**
     * The maximum number of roll-forward requests that will be queued at a given time. This is to provide a safety
     * measure against consuming too many resources as massive writes/updates are performed.
     */
    private final int maxCount;

    /**
     * The amount of time to wait in milliseconds between the time that a roll-forward request is received for a
     * transaction and the time that a batch of requests will be executed for that transaction.
     */
    private final int rollForwardDelayMS;

    /**
     * The time interval in milliseconds at which the queue of roll-forward requests will be cleared. This is done as
     * a safety precaution to ensure that a failure in scheduling the requests does not cause the request queue to get
     * blocked.
     */
    private final int resetMS;

    /**
     * The queue of pending roll-forward requests. It is a map from transaction ID to a pair of boolean and a
     * set of row keys. The boolean indicates whether rows for the transaction ID are currently being enqueued.
     */
    private Map<Long, Pair<Boolean,Set<byte[]>>> transactionMap;

    /**
     * A reference to the task that is scheduled to reset the queue. As tasks run they check that this is still scheduled.
     * This is a safety measure against the possibility of the reset task failing and failing to re-schedule itself.
     */
    private ScheduledFuture<?> resetterHandle;

    /**
     * The current number of requests in queue.
     */
    private int count;

    private final String tableName;

    /**
     * It is expected that one queue will be created for each HBase region. All of the queues in a JVM will use a shared
     * thread pool.
     */
    public SynchronousRollForwardQueue(RollForwardAction action, int maxCount, int rollForwardDelayMS, int resetMS, String tableName) {
        this.action = action;
        this.maxCount = maxCount;
        this.rollForwardDelayMS = rollForwardDelayMS;
        this.resetMS = resetMS;
        this.tableName = tableName;
        transactionMap = new HashMap<Long, Pair<Boolean,Set<byte[]>>>(maxCount * 2);
        reset();
    }

    @Override
    public void start() {
        //no-op
    }

    @Override
    public void stop() {
        //no-op
    }

    /**
     * This is the main function for users of the queue. Callers notify the queue that the given row in the underlying
     * table should be updated to reflect the final status of the given transaction. If the caller knows
     * (through whatever context it has available) that the transaction has in fact committed, it passes along
     * that information.
     */
    @Override
    public void recordRow(long transactionId, byte[] rowKey, Boolean knownToBeCommitted) {
        synchronized (this) {
            forceResetIfNeeded();
            if (count < maxCount) {
                Pair<Boolean,Set<byte[]>> transPair = transactionMap.get(transactionId);
								boolean shouldSchedule = false;
                if (transPair == null) {
										//noinspection RedundantTypeArguments
										transPair = new Pair<Boolean, Set<byte[]>>(true, Sets.<byte[]>newTreeSet(Bytes.BYTES_COMPARATOR));
                    transactionMap.put(transactionId, transPair);
										shouldSchedule = true;
                }
                if (knownToBeCommitted){
                    transPair.setFirst(true);
                    if (transPair.getSecond().size() == 0){
												shouldSchedule = true;
                    }
                }
                if (transPair.getFirst()){
                    Set<byte[]> rowSet = transPair.getSecond();
										if(rowSet.add(rowKey))
                        count = count + 1;
                }
								if(shouldSchedule)
										scheduleRollForward(transactionId);
            }
        }
    }

    /**
     * For testing, expose the count of items currently in queue.
     */
    public int getCount() {
        synchronized (this) {
            return count;
        }
    }

    /**
     * Schedule a task to run roll-forward actions for the given transaction.
     */
    private void scheduleRollForward(final long transactionId) {
        final Runnable roller = new Runnable() {
            public void run() {
                List<byte[]> rowList = takeRowList(transactionId);
                try {
                    Boolean transactionFinished = action.rollForward(transactionId, rowList);
                    if (!transactionFinished){
                        synchronized (SynchronousRollForwardQueue.this){
                            // If we find that transaction not yet complete, stop collecting
                            // roll-forward requests for it
                            transactionMap.get(transactionId).setFirst(false);
                        }
                    }
                } catch (WrongRegionException e) {
                    LOG.debug("WrongRegionException while rolling forward", e);
                }  catch (IOException e) {
                    // Since this is running on a separate thread and the roll-forward is best effort there is no need
                    // to propagate this further up the call stack.
                    LOG.warn("Error while rolling forward", e);
                }
                // Ignore any rows that came in for the transaction after we started the roll-forward.
                clearRowList(transactionId);
            }
        };
        scheduler.schedule(roller, getRollForwardDelay(), TimeUnit.MILLISECONDS);
    }

    /**
     * Provide a hook for tests to reach in and change the roll-forward delay.
     */
    private int getRollForwardDelay() {
        Integer override = Tracer.rollForwardDelayOverride;
        if (override == null) {
            return rollForwardDelayMS;
        } else {
            return override;
        }
    }

    /**
     * Retrieve all of the queued rows for a given transaction. This causes the rows to be atomically removed from the
     * queue.
     */
    private List<byte[]> takeRowList(long transactionId) {
        Set<byte[]> rowSet;
        synchronized (this) {
            Pair<Boolean, Set<byte[]>> pair = transactionMap.get(transactionId);
            rowSet = pair.getSecond();
            if (rowSet.size() > 0) {
                count = count - rowSet.size();
                pair.setSecond(Sets.<byte[]>newTreeSet(Bytes.BYTES_COMPARATOR));
            }
        }
        return produceRowList(rowSet);
    }

    /**
     * Convert the internal set of wrapped row identifiers into a list of un-wrapped identifiers.
     */
    private List<byte[]> produceRowList(Set<byte[]> rowSet) {
        if (rowSet == null || rowSet.size() == 0) {
            return Collections.emptyList();
        } else {
            List<byte[]> result = Lists.newArrayListWithExpectedSize(rowSet.size());
            for (byte[] row : rowSet) {
                result.add(row);
            }
            return result;
        }
    }

    private void clearRowList(long transactionId) {
        synchronized (this) {
            final Set rowSet = transactionMap.get(transactionId).getSecond();
            if (rowSet != null) {
                count = count - rowSet.size();
                rowSet.clear();
            }
        }
    }

    /**
     * Clear out the queue of requests and schedule the next reset call. This is a safety measure to keep old, failed
     * requests from getting stuck in the queue.
     */
    private void reset() {
        synchronized (this) {
            for (Map.Entry<Long, Pair<Boolean, Set<byte[]>>> entry: transactionMap.entrySet()) {
                entry.getValue().getSecond().clear();
            }
            count = 0;
            scheduleReset();
        }
    }

    /**
     * Schedule a task to reset the queue.
     */
    private void scheduleReset() {
        final Runnable resetter = new Runnable() {
            public void run() {
                reset();
            }
        };
        synchronized (this) {
            resetterHandle = scheduler.schedule(resetter, resetMS, TimeUnit.MILLISECONDS);
        }
    }

    /**
     * This is not expected to be needed, but if there is not a pending reset task, then perform a reset() now.
     */
    private void forceResetIfNeeded() {
        synchronized (this) {
            if (resetterHandle.isDone()) {
                reset();
            }
        }
    }

}
