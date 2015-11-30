package com.splicemachine.derby.impl.stats;

import com.splicemachine.db.iapi.sql.dictionary.PhysicalStatsDescriptor;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 3/9/15
 */
public interface PhysicalStatisticsStore {

    void start();

    List<PhysicalStatsDescriptor> allPhysicalStats();
}
