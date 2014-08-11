package com.splicemachine.logicalstats;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
public interface ShortUpdateable extends Updateable<Short>{

		void update(short item);

		void update(short item, long count);
}
