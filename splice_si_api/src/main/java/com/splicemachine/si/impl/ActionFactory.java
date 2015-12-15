package com.splicemachine.si.impl;

import com.splicemachine.storage.Partition;
import com.splicemachine.si.api.readresolve.RollForwardAction;

/**
 * @author Scott Fines
 * Date: 9/4/14
 */
public interface ActionFactory {
    RollForwardAction newAction(Partition region);

    public static final ActionFactory NOOP_ACTION_FACTORY = new ActionFactory(){
        @Override
        public RollForwardAction newAction(Partition region) {
            return RollForwardAction.NOOP_ACTION;
        }
    };
}
