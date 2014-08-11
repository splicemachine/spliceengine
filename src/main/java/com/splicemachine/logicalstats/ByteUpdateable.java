package com.splicemachine.logicalstats;

/**
 * @author Scott Fines
 *         Date: 3/26/14
 */
public interface ByteUpdateable extends Updateable<Byte>{

		void update(byte item);

		void update(byte item, long count);
}
