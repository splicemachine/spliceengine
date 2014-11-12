package com.splicemachine.si.impl.rollforward;

import javax.management.MXBean;

/**
 * @author Scott Fines
 *         Date: 9/4/14
 */
@MXBean(true)
public interface RollForwardManagement {

    long getTotalUpdates();

    long getTotalRowsToResolve();
}
