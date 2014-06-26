package com.splicemachine.si.impl.rollforward;

import org.cliffc.high_scale_lib.Counter;

/**
 *
 * Represents a Segment of a Region. When Statistics are integrated, we can
 * replace many of the components of this class with cleaner implementations. In
 * the meantime, we steal the ConcurrentHyperLogLogCounter and use that.
 *
 * @author Scott Fines
 * Date: 6/26/14
 */
class RegionSegment {
		private final byte[] startKey;
		private final byte[] endKey;
		private final Counter toResolve = new Counter();
		private final ConcurrentHyperLogLogCounter txnCounter =
}
