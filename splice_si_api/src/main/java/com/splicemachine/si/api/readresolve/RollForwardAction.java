package com.splicemachine.si.api.readresolve;

import com.splicemachine.si.api.data.IHTable;
import com.splicemachine.si.impl.readresolve.RegionSegmentContext;

/**
 * Created by jleach on 12/11/15.
 */
public interface RollForwardAction {
    void submitAction(IHTable region,byte[] startKey,byte[] stopKey,RegionSegmentContext context);
    public static final RollForwardAction NOOP_ACTION=new RollForwardAction(){
        @Override
        public void submitAction(IHTable region,byte[] startKey,byte[] stopKey,RegionSegmentContext context){
        }
    };
}
