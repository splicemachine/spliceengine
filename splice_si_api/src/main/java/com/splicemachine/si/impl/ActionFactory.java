package com.splicemachine.si.impl;

import com.splicemachine.si.api.data.IHTable;
import com.splicemachine.si.api.readresolve.RollForwardAction;

/**
 * @author Scott Fines
 * Date: 9/4/14
 */
public interface ActionFactory {
    RollForwardAction newAction(IHTable region);

    public static final ActionFactory NOOP_ACTION_FACTORY = new ActionFactory(){
        @Override
        public RollForwardAction newAction(IHTable region) {
            return RollForwardAction.NOOP_ACTION;
        }
    };
}
