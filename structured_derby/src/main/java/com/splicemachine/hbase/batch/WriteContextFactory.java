package com.splicemachine.hbase.batch;

import com.splicemachine.derby.ddl.DDLChange;
import com.splicemachine.si.api.RollForward;
import com.splicemachine.si.api.Txn;
import com.splicemachine.si.api.TxnView;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public interface WriteContextFactory<T> {

    WriteContext create(TxnView txn, T key, RegionCoprocessorEnvironment env) throws IOException, InterruptedException;

    WriteContext create(TxnView txn, T key,
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
    WriteContext createPassThrough(TxnView txn, T key, int expectedWrites, RegionCoprocessorEnvironment env) throws IOException,InterruptedException;

    void dropIndex(long indexConglomId);

    void addIndex(DDLChange ddlChange, int[] columnOrdring, int[] typeIds);

    void addDDLChange(DDLChange ddlChange);
}
