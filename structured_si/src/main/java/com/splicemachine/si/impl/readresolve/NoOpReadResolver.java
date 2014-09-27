package com.splicemachine.si.impl.readresolve;

import com.splicemachine.si.api.ReadResolver;
import com.splicemachine.utils.ByteSlice;

/**
 * @author Scott Fines
 *         Date: 6/26/14
 */
public class NoOpReadResolver implements ReadResolver {
		public static final ReadResolver INSTANCE = new NoOpReadResolver();

    @Override public void resolve(ByteSlice rowKey, long txnId) {  }
}
