package com.splicemachine.hbase.batch;

import org.apache.hadoop.hbase.client.Mutation;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteHandler {

    void next(Mutation mutation, WriteContext ctx);

    void finishWrites(WriteContext ctx) throws IOException;
}
