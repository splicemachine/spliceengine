package com.splicemachine.derby.stats;

import org.junit.Test;

import java.util.Arrays;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class TaskStatsTest {

    @Test
    public void merge() {

        TaskStats stats1 = new TaskStats(10L, 20L, 30L, new boolean[]{false, false, false, false});
        TaskStats stats2 = new TaskStats(100L, 200L, 300L, new boolean[]{false, true, false, false});
        TaskStats stats3 = new TaskStats(1000L, 2000L, 3000L, new boolean[]{false, false, true, false});

        TaskStats mergedStats = TaskStats.merge(Arrays.asList(stats1, stats2, stats3));

        assertEquals(1110L, mergedStats.getTotalTime());
        assertEquals(2220L, mergedStats.getTotalRowsProcessed());
        assertEquals(3330L, mergedStats.getTotalRowsWritten());
        assertTrue(Arrays.equals(new boolean[]{false, true, true, false}, mergedStats.getTempBuckets()));
    }

    @Test
    public void sumTotalRowsWritten() {
        TaskStats stats1 = new TaskStats(10L, 20L, 30L, new boolean[]{false, false, false, false});
        TaskStats stats2 = new TaskStats(100L, 200L, 300L, new boolean[]{false, true, false, false});
        TaskStats stats3 = new TaskStats(1000L, 2000L, 3000L, new boolean[]{false, false, true, false});

        long value = TaskStats.sumTotalRowsWritten(Arrays.asList(stats1, stats2, stats3));

        assertEquals(3330L, value);
    }

}