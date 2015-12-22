package com.splicemachine.pipeline.callbuffer;

import com.splicemachine.kvpair.KVPair;
import java.util.Collection;

/**
 * Before flush, this hook will modify the buffer.  Since some operations will be local, it is 
 * important that a new ObjectArrayList be created.
 *
 */
public interface PreFlushHook{
    Collection<KVPair> transform(Collection<KVPair> buffer) throws Exception;
}

