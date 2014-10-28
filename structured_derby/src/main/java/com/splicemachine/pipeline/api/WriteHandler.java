package com.splicemachine.pipeline.api;

import com.splicemachine.hbase.KVPair;

import java.io.IOException;
import java.util.List;

/**
 * 
 * Simple Interface for Handling Writes for a giving write pipeline (WriteContext).
 * 
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteHandler {
	/**
	 * Process the mutation with the given handler
	 * 
	 * @param mutation
	 * @param ctx
	 */
    void next(KVPair mutation, WriteContext ctx);
    /**
     * 
     * Process a list of mutations with the given handler
     * 
     * @param mutations
     * @param ctx
     */
    void next(List<KVPair> mutations, WriteContext ctx);

    /**
     * 
     * Flush the writes with the given handler.  This method assumes possible asynchrnous underlying calls.
     * 
     * @param ctx
     * @throws IOException
     */
    void flush(WriteContext ctx) throws IOException;
    /**
     * 
     * This closes the writes with the given handler.  It will need to wait for all underlying calls to finish or throw exceptions.
     * 
     * @param ctx
     * @throws IOException
     */
    void close(WriteContext ctx) throws IOException;

}
