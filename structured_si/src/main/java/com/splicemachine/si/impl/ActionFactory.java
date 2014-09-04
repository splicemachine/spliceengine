package com.splicemachine.si.impl;

import com.splicemachine.si.impl.rollforward.SegmentedRollForward;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * @author Scott Fines
 * Date: 9/4/14
 */
public interface ActionFactory {
    SegmentedRollForward.Action newAction(HRegion region);

    public static final ActionFactory NOOP_ACTION_FACTORY = new ActionFactory(){
        @Override
        public SegmentedRollForward.Action newAction(HRegion region) {
            return SegmentedRollForward.NOOP_ACTION;
        }
    };
}
