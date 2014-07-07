package com.splicemachine.si.api;

import com.splicemachine.utils.ByteSlice;

/**
 * Interface for notifying other systems that a read was resolved.
 *
 * This is somewhat similar to the RollForward structure, in that
 * it is allowed to asynchronously resolve data as written, but is
 * different in its expected usage. In this case, it is allowed
 * to directly modify data--it is not required to wait any period
 * for an unknown transaction state to resolve itself externally--
 * the state is known directly ahead of time.
 *
 * @author Scott Fines
 * Date: 6/25/14
 */
public interface ReadResolver {

    void resolve(ByteSlice rowKey, long txnId);

}
