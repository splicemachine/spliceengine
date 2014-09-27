package com.splicemachine.derby.hbase;

import com.splicemachine.si.impl.ActionFactory;
import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.log4j.Logger;

import java.util.concurrent.ExecutionException;

/**
 * @author Scott Fines
 * Date: 9/4/14
 */
public class RollForwardAction implements SegmentedRollForward.Action{
    private static final Logger LOG = Logger.getLogger(RollForwardAction.class);
    private static final RollForwardAction INSTANCE = new RollForwardAction();
    public static final ActionFactory FACTORY = new ActionFactory() {
        @Override
        public SegmentedRollForward.Action newAction(HRegion region) {
            return INSTANCE;
        }
    };

    @Override
    public void submitAction(HRegion region, byte[] startKey, byte[] stopKey, SegmentedRollForward.Context context) {
        RollForwardTask task = new RollForwardTask(region,startKey,stopKey,context);
        try {
            SpliceDriver.driver().getTaskScheduler().submit(task);
        } catch (ExecutionException e) {
            LOG.info("Unable to submit roll forward action on region "+ region.getRegionNameAsString(),e.getCause());
        }
    }

}
