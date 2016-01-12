package com.splicemachine.si.impl.readresolve;

import com.splicemachine.si.impl.rollforward.RegionSegment;

/**
 *
 * Created by jleach on 12/11/15.
 */
public class RegionSegmentContext{
    private final RegionSegment segment;

    public RegionSegmentContext(RegionSegment segment){
        this.segment=segment;
    }

    public void rowResolved(){
        segment.rowResolved();
    }

    public void complete(){
        segment.markCompleted();
    }
}
