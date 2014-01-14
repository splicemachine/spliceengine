package com.splicemachine.derby.impl.sql.execute.operations.framework;

import com.splicemachine.utils.hash.ByteHash32;
import com.splicemachine.utils.hash.HashFunctions;
/**
 * Baseline constants for AbstractAggregateBuffer
 *
 */
public class AbstractAggregateBufferConstants {
	/**
	 * Default Hash strategies: we only use HashFunctions.murmur3(0)
	 * 
	 */
	protected static final ByteHash32[] DEFAULT_HASHES = new ByteHash32[]{
		HashFunctions.murmur3(0),
		HashFunctions.murmur3(5),
		HashFunctions.murmur3(7)
	};
}
