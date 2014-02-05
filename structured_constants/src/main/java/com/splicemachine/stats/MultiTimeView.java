package com.splicemachine.stats;

/**
 * @author Scott Fines
 * Date: 2/5/14
 */
public interface MultiTimeView extends TimeView {
		void update(TimeView timeView);


}
