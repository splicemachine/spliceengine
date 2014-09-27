package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.api.RollForward;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.junit.Test;

import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * @author Scott Fines
 *         Date: 9/8/14
 */
public class SegmentedRollForwardTest {


    @Test
    public void testCanProperlySplitARollForward() throws Exception {
        HRegion region = mock(HRegion.class);
        when(region.getStartKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);
        when(region.getEndKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);

        ScheduledExecutorService noopService = mock(ScheduledExecutorService.class);
        when(noopService.schedule(any(Runnable.class),anyLong(),any(TimeUnit.class))).thenReturn(null);

        RollForwardStatus status = new RollForwardStatus();

        SegmentedRollForward rollForward = new SegmentedRollForward(region,noopService,4,1<<10,1<<10,SegmentedRollForward.NOOP_ACTION,status);
    }
}
