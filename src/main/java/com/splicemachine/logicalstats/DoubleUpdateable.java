package com.splicemachine.logicalstats;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
public interface DoubleUpdateable extends Updateable<Double>{

		void update(double item);

		void update(double item, long count);
}
