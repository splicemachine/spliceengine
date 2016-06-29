package com.splicemachine.stats;

/**
 * An integer-specific streaming data structure.
 *
 * This is functionally equivalent to {@link com.splicemachine.stats.Updateable<Integer>},
 * but supports additional methods to avoid extraneous object creation due to autoboxing.
 *
 * @author Scott Fines
 *         Date: 3/26/14
 */
public interface IntUpdateable extends Updateable<Integer>{

		/**
		 * Update the underlying data structure with the new item.
		 *
		 * This is functionally equivalent to {@link #update(Object)}, but avoids
		 * autoboxing object creation.
		 *
		 * @param item the new item to update
		 */
		void update(int item);

		/**
		 * Update the underlying data structure with the new item,
		 * assuming that the new item occurs {@code count} number of times.
		 *
		 * This is functionally equivalent to {@link #update(Object,long)}, but avoids
		 * autoboxing object creation.
		 *
		 * @param item the new item to update
		 * @param count the number of times the item occurs in the stream.
		 */
		void update(int item, long count);
}
