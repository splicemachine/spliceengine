package com.splicemachine.derby.impl.sql.execute.operations.framework;

import com.splicemachine.hash.Hash32;
import com.splicemachine.hash.HashFunctions;

/**
 * Baseline constants for AbstractAggregateBuffer
 *
 */
public class AbstractAggregateBufferConstants {
	/**
	 * Default Hash strategies: we only use HashFunctions.murmur3(0)
	 * 
	 */
	protected static final Hash32[] DEFAULT_HASHES = new Hash32[]{
		HashFunctions.murmur3(0),
		HashFunctions.murmur3(5),
		HashFunctions.murmur3(7)
	};
}
