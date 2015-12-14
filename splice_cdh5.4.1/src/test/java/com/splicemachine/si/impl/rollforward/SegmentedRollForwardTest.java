package com.splicemachine.si.impl.rollforward;

import com.splicemachine.si.api.readresolve.RollForwardAction;
import com.splicemachine.si.data.hbase.HbRegion;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HRegionInfo;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.junit.Test;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
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
        HRegionInfo regionInfo = mock(HRegionInfo.class);
        when(regionInfo.getStartKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);
        when(regionInfo.getEndKey()).thenReturn(HConstants.EMPTY_BYTE_ARRAY);
        when(region.getRegionInfo()).thenReturn(regionInfo);
        ScheduledExecutorService noopService = mock(ScheduledExecutorService.class);
        when(noopService.schedule(any(Runnable.class),anyLong(),any(TimeUnit.class))).thenReturn(null);

        RollForwardStatus status = new RollForwardStatus();

        SegmentedRollForward rollForward = new SegmentedRollForward(new HbRegion(region),noopService,4,1<<10,1<<10,10, RollForwardAction.NOOP_ACTION,status);
    }
}
