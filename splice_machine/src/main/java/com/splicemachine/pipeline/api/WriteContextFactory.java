package com.splicemachine.pipeline.api;

import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import com.splicemachine.pipeline.ddl.DDLChange;
import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteContextFactory<T> {

    WriteContext create(SharedCallBuffer indexSharedCallBuffer, TxnView txn, T key, RegionCoprocessorEnvironment env) throws IOException, InterruptedException;

    WriteContext create(SharedCallBuffer indexSharedCallBuffer, TxnView txn, T key,
                        int expectedWrites, RegionCoprocessorEnvironment env) throws IOException, InterruptedException;

    /**
     * Creates a context that only updates side effects.
     *
     * @param key
     * @param env
     * @return
     * @throws IOException
     * @throws InterruptedException
     */
    WriteContext createPassThrough(SharedCallBuffer indexSharedCallBuffer, TxnView txn, T key, int expectedWrites, RegionCoprocessorEnvironment env) throws IOException,InterruptedException;

    void dropIndex(long indexConglomId,TxnView txn);

    void addIndex(DDLChange ddlChange, int[] columnOrdring, int[] typeIds);

    void addDDLChange(DDLChange ddlChange);

}
