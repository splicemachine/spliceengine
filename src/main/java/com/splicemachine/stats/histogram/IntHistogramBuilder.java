package com.splicemachine.stats.histogram;

import com.splicemachine.stats.IntUpdateable;

/**
 * @author Scott Fines
 *         Date: 4/13/14
 */
public interface IntHistogramBuilder extends HistogramBuilder<Integer>,IntUpdateable {

		IntHistogram build();

}
