package com.splicemachine.logicalstats.histogram;

import com.splicemachine.logicalstats.IntUpdateable;

/**
 * @author Scott Fines
 *         Date: 4/13/14
 */
public interface IntHistogramBuilder extends HistogramBuilder<Integer>,IntUpdateable {

		IntHistogram build();

}
