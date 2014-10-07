package com.splicemachine.stats;

/**
 * Boolean-specific implementation of the Updateable interface.
 *
 *
 * @author Scott Fines
 * Date: 3/26/14
 */
public interface BooleanUpdateable extends Updateable<Boolean>{

		void update(boolean item);
}
