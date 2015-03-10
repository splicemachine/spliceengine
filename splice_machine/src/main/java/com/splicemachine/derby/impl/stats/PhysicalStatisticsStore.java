package com.splicemachine.derby.impl.stats;

import com.splicemachine.derby.iapi.catalog.PhysicalStatsDescriptor;

import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public interface PhysicalStatisticsStore {

    List<PhysicalStatsDescriptor> allPhysicalStats();
}
