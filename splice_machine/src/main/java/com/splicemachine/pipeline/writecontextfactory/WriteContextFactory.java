package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.writehandler.IndexCallBufferFactory;
import com.splicemachine.si.api.txn.TxnView;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 4/30/13
 */
public interface WriteContextFactory<T> {

    WriteContext create(IndexCallBufferFactory indexSharedCallBuffer,
                        TxnView txn,
                        T key,
                        RegionCoprocessorEnvironment env) throws IOException, InterruptedException;

    WriteContext create(IndexCallBufferFactory indexSharedCallBuffer,
                        TxnView txn,
                        T key,
                        int expectedWrites,
                        boolean skipIndexWrites,
                        RegionCoprocessorEnvironment env) throws IOException, InterruptedException;

    /**
     * Creates a context that only updates side effects.
     */
    WriteContext createPassThrough(IndexCallBufferFactory indexSharedCallBuffer,
                                   TxnView txn,
                                   T key,
                                   int expectedWrites,
                                   RegionCoprocessorEnvironment env) throws IOException, InterruptedException;

    void addDDLChange(DDLMessage.DDLChange ddlChange);

    void close();

    void prepare();

    boolean hasDependentWrite(TxnView txn) throws IOException, InterruptedException;

}
