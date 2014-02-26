package com.splicemachine.hbase.batch;

import com.splicemachine.hbase.KVPair;

import java.io.IOException;
import java.util.List;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteHandler {

    void next(KVPair mutation, WriteContext ctx);
    
    void next(List<KVPair> mutations, WriteContext ctx);

    void finishWrites(WriteContext ctx) throws IOException;
}
