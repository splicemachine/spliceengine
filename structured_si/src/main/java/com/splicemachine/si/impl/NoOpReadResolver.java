package com.splicemachine.si.impl;

import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 *         Date: 6/26/14
 */
public class NoOpReadResolver implements ReadResolver {
		public static final ReadResolver INSTANCE = new NoOpReadResolver();

		@Override public void resolveCommitted(ByteSlice rowKey, long txnId, long commitTimestamp) {  }
		@Override public void resolveRolledback(ByteSlice rowKey, long txnId) {  }
}
