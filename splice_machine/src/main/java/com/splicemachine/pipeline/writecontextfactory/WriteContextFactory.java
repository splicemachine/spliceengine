package com.splicemachine.pipeline.writecontextfactory;

import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.pipeline.writehandler.IndexCallBufferFactory;
import com.splicemachine.si.api.TxnView;
import com.splicemachine.db.iapi.sql.dictionary.ConstraintDescriptorList;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.List;

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
                        RegionCoprocessorEnvironment env) throws IOException, InterruptedException;

    /**
     * Creates a context that only updates side effects.
     */
    WriteContext createPassThrough(IndexCallBufferFactory indexSharedCallBuffer,
                                   TxnView txn,
                                   T key,
                                   int expectedWrites,
                                   RegionCoprocessorEnvironment env) throws IOException, InterruptedException;

    void dropIndex(long indexConglomId, TxnView txn);

    void addIndex(DDLChange ddlChange, int[] columnOrdering, int[] typeIds);

    void addForeignKeyParentCheckWriteFactory(int[] backingIndexFormatIds);

    void addForeignKeyParentInterceptWriteFactory(String parentTableName, List<Long> backingIndexConglomIds);

    void addDDLChange(DDLChange ddlChange);

    void close();

    void prepare();

    boolean hasDependentWrite(TxnView txn) throws IOException, InterruptedException;

}
