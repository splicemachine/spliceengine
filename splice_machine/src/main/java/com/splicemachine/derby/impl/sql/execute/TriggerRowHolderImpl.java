/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.impl.sql.execute;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.PublicAPI;
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.ResubmitDistributedException;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.TemporaryRowHolder;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.store.access.conglomerate.TransactionManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLRef;
import com.splicemachine.db.iapi.types.SQLLongint;
import com.splicemachine.db.impl.jdbc.EmbedResultSet40;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
import com.splicemachine.db.impl.sql.execute.TemporaryRowHolderResultSet;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import java.io.Closeable;
import java.io.IOException;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.UnsafeWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;


/**
* This is a class that is used to temporarily
* (non-persistently) hold rows that are used in
* language execution.  It will store them in an
* array, or a temporary conglomerate, depending
* on the number of rows.  
* <p>
* It is used for deferred DML processing.
*
*/
public class TriggerRowHolderImpl implements TemporaryRowHolder
{
    public static final int DEFAULT_OVERFLOWTHRESHOLD = 5;

    protected static final int STATE_UNINIT = 0;
    public static final int STATE_INSERT = 1;
    public static final int STATE_DRAIN = 2;


    protected ExecRow[]         rowArray;
    private int                 numRowsIn = 0;
    private int                 lastArraySlot = -1;
    public int                  state = STATE_UNINIT;

    private long                CID;
    private boolean             conglomCreated;
    private ConglomerateController	cc;
    private Properties				properties;

    private	ResultDescription		resultDescription;
    /** Activation object with local state information. */
    public Activation						activation;

    int 			overflowToConglomThreshold;
    boolean                     isSpark;  // Is the query executing on spark?
    private ExecRow execRowDefinition;

    protected WriteCoordinator writeCoordinator;
    protected RecordingCallBuffer<KVPair> triggerTempTableWriteBuffer;
    protected Callable<Void> triggerTempTableflushCallback;
    protected Partition triggerTempPartition;
    private TxnView txn;
    private byte[] token;

    /**
     * Create a temporary row holder with the defined overflow to conglom
     *
     * @param activation the activation
     * @param properties the properties of the original table.  Used
     *		to help the store use optimal page size, etc.
     * @param resultDescription the result description.  Relevant for the getResultDescription
     * 		call on the result set returned by getResultSet.  May be null
     * @param overflowToConglomThreshold on an attempt to insert
     * 		this number of rows, the rows will be put
     *		into a temporary conglomerate.
     */
    public TriggerRowHolderImpl
    (
            Activation              activation,
            Properties              properties,
            ResultDescription       resultDescription,
            int                     overflowToConglomThreshold,
            ExecRow                 execRowDefinition,
            boolean                 isSpark,
            TxnView                 txn,
            byte[]                  token
    )
    {
        if (SanityManager.DEBUG)
        {
                if (overflowToConglomThreshold < 0)
                {
                        SanityManager.THROWASSERT("It is assumed that "+
                                "the overflow threshold is >= 0.  "+
                                "If you you need to change this you have to recode some of "+
                                "this class.");
                }
        }

        this.activation = activation;
        this.properties = properties;
        this.resultDescription = resultDescription;
        this.txn = txn;
        this.token = token;

        rowArray = new ExecRow[overflowToConglomThreshold < 1 ? 1 : overflowToConglomThreshold];
        lastArraySlot = -1;
        this.execRowDefinition = execRowDefinition;
        rowArray[0] = execRowDefinition.getClone();
        this.overflowToConglomThreshold = overflowToConglomThreshold;
        if (overflowToConglomThreshold == 0 && execRowDefinition != null) {
            try {
                createConglomerate(execRowDefinition);
            }
            catch (StandardException e) {
                throw new RuntimeException(e);
            }
        }
    }

    public void setTxn(TxnView txn) {
        this.txn = txn;
    }

    public Callable<Void> getTriggerTempTableflushCallback() { return triggerTempTableflushCallback; }
    public Activation getActivation() { return activation; }
    public long getConglomerateId() { return CID; }

    private ExecRow cloneRow(ExecRow inputRow)
    {
        DataValueDescriptor[] cols = inputRow.getRowArray();
        int ncols = cols.length;
        ExecRow cloned = ((ValueRow) inputRow).cloneMe();
        for (int i = 0; i < ncols; i++)
        {
            if (cols[i] != null)
            {
                /* Rows are 1-based, cols[] is 0-based */
                cloned.setColumn(i + 1, cols[i].cloneHolder());
            }
        }
        if (inputRow instanceof IndexValueRow)
            return new IndexValueRow(cloned);
        else
            return cloned;
    }

    private void createConglomerate(ExecRow templateRow) throws StandardException{
        if (!conglomCreated)
        {
            TransactionController tc = activation.getTransactionController();

            /*
            ** Create the conglomerate with the template row.
            */
            CID =
                tc.createConglomerate(false,
                 "heap",
                 templateRow.getRowArray(),
                 null, //column sort order - not required for heap
                 null, //collation_ids
                 properties,
                 TransactionController.IS_TEMPORARY |
                 TransactionController.IS_KEPT);

            conglomCreated = true;

            cc = tc.openConglomerate(CID,
                 false,
                 TransactionController.OPENMODE_FORUPDATE,
                 TransactionController.MODE_TABLE,
                 TransactionController.ISOLATION_SERIALIZABLE);
        }

    }

    class InMemoryTriggerRowsIterator implements Iterator<ExecRow>, Closeable {
        private TriggerRowHolderImpl holder;
        private int position = 0;
        public InMemoryTriggerRowsIterator(TriggerRowHolderImpl holder)
        {
            this.holder = holder;
        }

        @Override
        public boolean hasNext() {
            if (position < overflowToConglomThreshold)
                return true;
            return false;
        }

        @Override
        public ExecRow next() {
            if (position < overflowToConglomThreshold)
                return rowArray[position++];
            return null;
        }

        @Override
        public void close() throws IOException {
            position = 0;
        }
    }
    public InMemoryTriggerRowsIterator getCachedRowsIterator() {
        return new InMemoryTriggerRowsIterator(this);
    }

    // Must use the version of insert that provides the KVPair.
    public void insert(ExecRow inputRow)
            throws StandardException
    {
        throw new UnsupportedOperationException();
    }

    protected void rollbackTxn() throws StandardException {
        if (txn instanceof Txn) {
            Txn transaction = (Txn)txn;
            try {
                transaction.rollback();
            }
            catch (UnsupportedOperationException | IOException e) {
                if (e instanceof IOException)
                    throw StandardException.plainWrapException(e);
            }
        }

    }

    /**
     * Insert a row
     *
     * @param inputRow the row to insert
     *
     * @exception StandardException on error
     */
    public void insert(ExecRow inputRow, KVPair encode)
            throws StandardException
    {

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(state != STATE_DRAIN, "you cannot insert rows after starting to drain");

        state = STATE_INSERT;

        if (numRowsIn < overflowToConglomThreshold)
        {
            rowArray[numRowsIn++] = cloneRow(inputRow);
            lastArraySlot++;
            return;
        }
        if (numRowsIn == overflowToConglomThreshold) {

            writeCoordinator = PipelineDriver.driver().writeCoordinator();

            // If running on HBase, don't use a conglomerate if there are many rows
            // to insert, just switch to using spark.
            if (PipelineDriver.driver().exceptionFactory().isHBase()) {
                //rollbackTxn();  msirek-temp
                throw new ResubmitDistributedException();
            }
        }
        numRowsIn++;
        if (!conglomCreated) {
            createConglomerate(inputRow);
            try {
                writeCoordinator = PipelineDriver.driver().writeCoordinator();  // msirek-temp
                triggerTempPartition = SIDriver.driver().getTableFactory().getTable(Long.toString(CID));
                WriteConfiguration writeConfiguration = writeCoordinator.defaultWriteConfiguration();
                writeConfiguration = new UnsafeWriteConfiguration(writeConfiguration, false, true);
                triggerTempTableWriteBuffer = writeCoordinator.writeBuffer(triggerTempPartition, txn, token, writeConfiguration);
                triggerTempTableflushCallback = TriggerHandler.flushCallback(triggerTempTableWriteBuffer);
            }
            catch (Exception e) {
                if (e instanceof StandardException)
                    throw (StandardException)e;
                throw StandardException.plainWrapException(e);
            }
        }

        try {
            triggerTempTableWriteBuffer.add(encode);
        }
        catch (Exception e) {
            if (e instanceof StandardException)
                throw (StandardException)e;
            throw StandardException.plainWrapException(e);
        }
//
//
//        int status = 0;
//        status = cc.insert(inputRow);   msirek-temp
//
//        if (SanityManager.DEBUG)
//        {
//            if (status != 0)
//            {
//                    SanityManager.THROWASSERT("got funky status ("+status+") back from "+
//                                    "ConglomerateConstroller.insert()");
//            }
//        }
    }


    /**
     * Maintain an unique index based on the input row's row location in the
     * base table, this index make sures that we don't insert duplicate rows
     * into the temporary heap.
     * @param inputRow  the row we are inserting to temporary row holder
     * @exception StandardException on error
     */



    /**
     * Get a result set for scanning what has been inserted
     * so far.
     *
     * @return a result set to use
     */
    public CursorResultSet getResultSet()
    {
            state = STATE_DRAIN;
            TransactionController tc = activation.getTransactionController();
            return new TemporaryRowHolderResultSet(tc, rowArray, resultDescription, this);
    }   // msirek-temp

    /**
     * Purge the row holder of all its rows.
     * Resets the row holder so that it can
     * accept new inserts.  A cheap way to
     * recycle a row holder.
     *
     * @exception StandardException on error
     */
    public void truncate() throws StandardException
    {
            close();
    if (SanityManager.DEBUG) {
     SanityManager.ASSERT(lastArraySlot == -1);
     SanityManager.ASSERT(state == STATE_UNINIT);
     SanityManager.ASSERT(!conglomCreated);
     SanityManager.ASSERT(CID == 0);
    }
            for (int i = 0; i < rowArray.length; i++)
            {
                    rowArray[i] = null;
            }

            numRowsIn = 0;
    }

    public long getTemporaryConglomId()
    {
        return getConglomerateId();
    }


    private Properties makeIndexProperties(DataValueDescriptor[] indexRowArray, long conglomId ) throws StandardException {
            int nCols = indexRowArray.length;
            Properties props = new Properties();
            props.put("allowDuplicates", "false");
            // all columns form the key, (currently) required
            props.put("nKeyFields", String.valueOf(nCols));
            props.put("nUniqueColumns", String.valueOf(nCols-1));
            props.put("rowLocationColumn", String.valueOf(nCols-1));
            props.put("baseConglomerateId", String.valueOf(conglomId));
            return props;
    }


    private void dropTable(long conglomID) throws StandardException {
        try {
            SIDriver driver = SIDriver.driver();
            PartitionFactory partitionFactory = driver.getTableFactory();
            PartitionAdmin partitionAdmin = partitionFactory.getAdmin();
            partitionAdmin.deleteTable(Long.toString(conglomID));
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }
    }
    /**
     * Clean up
     *
     * @exception StandardException on error
     */
    public void close() throws StandardException
    {

        if (cc != null)
        {
                cc.close();
                cc = null;
        }

        TransactionController tc = activation.getTransactionController();

        if(triggerTempPartition!=null){
            try{
                triggerTempPartition.close();
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
        }

        if (conglomCreated)
        {
                tc.dropConglomerate(CID);
                dropTable(CID);

                conglomCreated = false;
                CID = 0;
        }
        else
        {
             if (SanityManager.DEBUG) {
                 SanityManager.ASSERT(CID == 0, "CID(" + CID + ")==0");
        }
     }
     state = STATE_UNINIT;
     lastArraySlot = -1;
     numRowsIn = 0;
    }

    public int getLastArraySlot() { return lastArraySlot; }
    public void decrementLastArraySlot() { lastArraySlot--; }
    public int getState() { return state; }
    public void setState(int state) { this.state = state; }
}


