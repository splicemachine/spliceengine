package com.splicemachine.stats;

/**
 * Defines an algorithm which can be updated.
 *
 * This interface is an object-representation of the elements--each individual
 * java primitive (int, short, etc.) has a subinterface which allows for efficient,
 * non-autoboxed versions of this interface.
 *
 * @author Scott Fines
 * Date: 6/5/14
 */
public interface Updateable<T> {

    /**
     * Update the underlying data structure with the new item.
     *
     * @param item the new item to update
     */
		public void update(T item);

    /**
     * Update the underlying data structure with the new item,
     * assuming that the new item occurs {@code count} number of times.
     *
     * @param item the new item to update
     * @param count the number of times the item occurs in the stream.
     */
		public void update(T item, long count);
}
