package com.splicemachine.si.impl.readresolve;

import com.splicemachine.si.impl.rollforward.RegionSegment;
import com.splicemachine.si.impl.rollforward.RollForwardStatus;

/**
 * Created by jleach on 12/11/15.
 */
public class RegionSegmentContext {
        private final RegionSegment segment;
        private final RollForwardStatus status;

        public RegionSegmentContext(RegionSegment segment, RollForwardStatus status){
            this.segment=segment;
            this.status=status;
        }

        public void rowResolved(){
            segment.rowResolved();
        }

        public void complete(){
            segment.markCompleted();
        }
}
