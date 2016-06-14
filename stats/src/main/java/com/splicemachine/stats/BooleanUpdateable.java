package com.splicemachine.stats;

/**
 * Boolean-specific implementation of the Updateable interface.
 *
 * This interface is functionaly equivalent to {@link com.splicemachine.stats.Updateable<Boolean>},
 * but supports additional methods so that autoboxing can be avoided whenever possible.
 *
 * @author Scott Fines
 * Date: 3/26/14
 */
public interface BooleanUpdateable extends Updateable<Boolean>{

		/**
		 * Update the underlying data structure with the new item.
		 *
		 * This is a non-autoboxed interface, that allows us to avoid extraneous
		 * object creation during updates.
		 *
		 * @param item the new item to update
		 */
		void update(boolean item);

		/**
		 * Update the underlying data structure with the new item,
		 * assuming that the new item occurs {@code count} number of times.
		 *
		 * This is a non-autoboxed version of {@link #update(Object, long)}, to
		 * avoid extraneous object creation during updates.
		 *
		 * @param item the new item to update
		 * @param count the number of times the item occurs in the stream.
		 */
		void update(boolean item, long count);
}
