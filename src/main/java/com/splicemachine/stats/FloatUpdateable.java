package com.splicemachine.stats;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
public interface FloatUpdateable extends Updateable<Float>{

		void update(float item);

		void update(float item, long count);
}
