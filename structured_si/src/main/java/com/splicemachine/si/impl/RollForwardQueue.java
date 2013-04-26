package com.splicemachine.si.impl;

import com.splicemachine.constants.bytes.HashableBytes;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;

public class RollForwardQueue {
    static final Logger LOG = Logger.getLogger(RollForwardQueue.class);

    private final ScheduledExecutorService scheduler = Executors.newScheduledThreadPool(1);
    private final RollForwardAction action;
    private final int maxCount;
    private final int rollForwardDelayMS;
    private final int resetMS;

    private Map<Long, Set> transactionMap;
    private ScheduledFuture<?> resetterHandle;
    private int count;

    public RollForwardQueue(RollForwardAction action, int maxCount, int rollForwardDelayMS, int resetMS) {
        this.action = action;
        this.maxCount = maxCount;
        this.rollForwardDelayMS = rollForwardDelayMS;
        this.resetMS = resetMS;
        reset();
    }

    public int getCount() {
        synchronized (this) {
            return count;
        }
    }

    public void reset() {
        synchronized (this) {
            transactionMap = new HashMap<Long, Set>();
            count = 0;
            scheduleReset();
        }
    }

    private void scheduleReset() {
        final Runnable resetter = new Runnable() {
            public void run() {
                reset();
            }
        };
        resetterHandle = scheduler.schedule(resetter, resetMS, TimeUnit.MILLISECONDS);
    }

    public void shutdown() {
        scheduler.shutdownNow();
    }

    public void recordRow(long transactionId, Object rowKey) {
        synchronized (this) {
            if (resetterHandle.isDone()) {
                reset();
            }
            if (count < maxCount) {
                Set rowSet = transactionMap.get(transactionId);
                if (rowSet == null) {
                    rowSet = new HashSet();
                    transactionMap.put(transactionId, rowSet);
                    scheduleRollForward(transactionId);
                }
                final Object newRow = toHashable(rowKey);
                if (!rowSet.contains(newRow)) {
                    rowSet.add(newRow);
                    count = count + 1;
                }
            }
        }
    }

    private void scheduleRollForward(final long transactionId) {
        final Runnable roller = new Runnable() {
            public void run() {
                List rowList = takeRowList(transactionId);
                try {
                    action.rollForward(transactionId, rowList);
                } catch (IOException e) {
                    LOG.warn("Error while rolling forward", e);
                }
                clearRowList(transactionId);
            }
        };
        scheduler.schedule(roller, getRollForwardDelay(), TimeUnit.MILLISECONDS);
    }

    private int getRollForwardDelay() {
        Integer override = Tracer.rollForwardDelayOverride;
        if (override == null) {
            return rollForwardDelayMS;
        } else {
            return override.intValue();
        }
    }

    public List takeRowList(long transactionId) {
        Set rowSet;
        synchronized (this) {
            rowSet = transactionMap.get(transactionId);
            if (rowSet != null) {
                transactionMap.remove(transactionId);
                count = count - rowSet.size();
            }
        }
        if (rowSet == null) {
            return new ArrayList();
        } else {
            List result = new ArrayList(rowSet.size());
            for (Object row : rowSet) {
                result.add(recoverUnhashable(row));
            }
            return result;
        }
    }

    private Object toHashable(Object row) {
        if (row.getClass().isArray() && row.getClass().getComponentType() == Byte.TYPE) {
            return new HashableBytes((byte[]) row);
        } else {
            return row;
        }
    }

    private Object recoverUnhashable(Object row) {
        if (row instanceof HashableBytes) {
            return ((HashableBytes) row).getBytes();
        } else {
            return row;
        }
    }

    public void clearRowList(long transactionId) {
        synchronized (this) {
            final Set rowSet = transactionMap.get(transactionId);
            transactionMap.remove(transactionId);
            count = count - rowSet.size();
        }
    }

}
