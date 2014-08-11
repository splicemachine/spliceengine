package com.splicemachine.logicalstats;

/**
 * @author Scott Fines
 * Date: 6/5/14
 */
public interface Updateable<T> {

		public void update(T item);

		public void update(T item, long count);
}
