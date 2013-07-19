package com.splicemachine.si.impl;

import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
public class RollForwardQueue<Data, Hashable> {
    static final Logger LOG = Logger.getLogger(RollForwardQueue.class);

    /**
     * The thread pool to use for running asynchronous roll-forward actions.
     */
    public static ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(5);

    private final Hasher<Data, Hashable> hasher;

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
     * The queue of pending roll-forward requests. It is a map from transaction ID to a set of row keys.
     */
    private Map<Long, Set<Hashable>> transactionMap = new HashMap<Long, Set<Hashable>>();

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
    public RollForwardQueue(Hasher<Data, Hashable> hasher, RollForwardAction<Data> action, int maxCount, int rollForwardDelayMS, int resetMS, String tableName) {
        this.hasher = hasher;
        this.action = action;
        this.maxCount = maxCount;
        this.rollForwardDelayMS = rollForwardDelayMS;
        this.resetMS = resetMS;
        this.tableName = tableName;
        reset();
    }

    /**
     * This is the main function for users of the queue. Callers notify the queue that the given row in the underlying
     * table should be updated to reflect the final status of the given transaction.
     */
    public void recordRow(long transactionId, Data rowKey) {
        synchronized (this) {
            forceResetIfNeeded();
            if (count < maxCount) {
                Set<Hashable> rowSet = transactionMap.get(transactionId);
                if (rowSet == null) {
                    rowSet = new HashSet();
                    transactionMap.put(transactionId, rowSet);
                    scheduleRollForward(transactionId);
                }
                final Hashable newRow = hasher.toHashable(rowKey);
                if (!rowSet.contains(newRow)) {
                    rowSet.add(newRow);
                    count = count + 1;
                }
            }
        }
    }

    /**
     * For testing, expose the count of items currently in queue.
     */
    int getCount() {
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
                List<Data> rowList = takeRowList(transactionId);
                try {
                    action.rollForward(transactionId, rowList);
                } catch (IOException e) {
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
            return override.intValue();
        }
    }

    /**
     * Retrieve all of the queued rows for a given transaction. This causes the rows to be atomically removed from the
     * queue.
     */
    private List<Data> takeRowList(long transactionId) {
        Set rowSet;
        synchronized (this) {
            rowSet = transactionMap.get(transactionId);
            if (rowSet != null) {
                transactionMap.remove(transactionId);
                count = count - rowSet.size();
            }
        }
        return produceRowList(rowSet);
    }

    /**
     * Convert the internal set of wrapped row identifiers into a list of un-wrapped identifiers.
     */
    private List<Data> produceRowList(Set<Hashable> rowSet) {
        if (rowSet == null) {
            return Collections.emptyList();
        } else {
            List<Data> result = new ArrayList<Data>(rowSet.size());
            for (Hashable row : rowSet) {
                result.add(hasher.fromHashable(row));
            }
            return result;
        }
    }

    private void clearRowList(long transactionId) {
        synchronized (this) {
            final Set rowSet = transactionMap.get(transactionId);
            if (rowSet != null) {
                transactionMap.remove(transactionId);
                count = count - rowSet.size();
            }
        }
    }

    /**
     * Clear out the queue of requests and schedule the next reset call. This is a safety measure to keep old, failed
     * requests from getting stuck in the queue.
     */
    private void reset() {
        synchronized (this) {
            transactionMap.clear();
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
