package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
public interface IntUpdateable extends Updateable<Integer>{

		void update(int item);

		void update(int item, long count);
}
