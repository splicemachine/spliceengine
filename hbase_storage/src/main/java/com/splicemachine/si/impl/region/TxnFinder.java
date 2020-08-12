package com.splicemachine.si.impl.region;

import com.splicemachine.utils.Pair;

import java.io.IOException;

public interface TxnFinder {
    Pair<Long, Long> find(byte bucket, byte[] begin, boolean reverse) throws IOException;
}
