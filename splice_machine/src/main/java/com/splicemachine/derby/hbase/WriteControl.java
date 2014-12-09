package com.splicemachine.derby.hbase;

import com.splicemachine.utils.TrafficControl;

/**
 * @author Scott Fines
 *         Date: 11/25/14
 */
public interface WriteControl {

    TrafficControl independentControl();

    TrafficControl dependentControl();

    int getOccupiedThreads();

    int maxWriteThreads();

    int getAvailableDependentPermits();

    int getAvailableIndependentPermits();

    int getMaxDependentPermits();

    int getMaxIndependentPermits();

    void setMaxIndependentPermits(int newMaxIndependenThroughput);

    void setMaxDependentPermits(int newMaxDependentThroughput);
}
