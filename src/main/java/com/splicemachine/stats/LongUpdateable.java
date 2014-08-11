package com.splicemachine.stats;

/**
 * Represents a datastructure which stores longs in an incrementally updateable fashion.
 *
 * @author Scott Fines
 * Date: 3/26/14
 */
public interface LongUpdateable extends Updateable<Long>{

		/**
		 * Update the data structure.
		 *
		 * @param item the item to update
		 */
		void update(long item);

		void update(long item, long count);
}
