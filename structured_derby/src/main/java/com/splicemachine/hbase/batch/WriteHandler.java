package com.splicemachine.hbase.batch;

import com.splicemachine.hbase.writer.KVPair;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteHandler {

    void next(KVPair mutation, WriteContext ctx);

    void finishWrites(WriteContext ctx) throws IOException;
}
