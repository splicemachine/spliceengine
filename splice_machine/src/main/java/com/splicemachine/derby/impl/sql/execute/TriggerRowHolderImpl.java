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
import com.splicemachine.db.iapi.services.sanity.SanityManager;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.ResubmitDistributedException;
import com.splicemachine.db.iapi.sql.execute.CursorResultSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.sql.execute.TemporaryRowHolder;
import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.ResultDescription;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.TransactionController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.impl.sql.execute.IndexValueRow;
import com.splicemachine.db.impl.sql.execute.TemporaryRowHolderResultSet;
import com.splicemachine.db.impl.sql.execute.TriggerExecutionContext;
import com.splicemachine.db.impl.sql.execute.ValueRow;

import java.io.*;
import java.util.Iterator;
import java.util.Properties;
import java.util.concurrent.Callable;

import com.splicemachine.derby.impl.sql.execute.operations.DMLWriteOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.UnsafeWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import org.apache.log4j.Logger;

import static java.lang.String.format;


/**
* This is a class that is used to temporarily
* (non-persistently) hold rows of the DML result
* set for statement triggers.  It will store them in an
* array, plus a temporary conglomerate, if there is overflow.
* But upon overflow of the array, if running true Splice with HBase, the
* statement is aborted and rerouted to run on Spark.
* <p>
* It is used for deferred DML processing.
* This class is a modified version of TemporaryRowHolderImpl.
*
*/
public class TriggerRowHolderImpl implements TemporaryRowHolder, Externalizable
{
     private static final Logger LOG = Logger.getLogger(TriggerRowHolderImpl.class);

    protected static final int STATE_UNINIT = 0;
    public static final int STATE_INSERT = 1;
    public static final int STATE_DRAIN = 2;


    protected ExecRow[]         rowArray;
    private int                 numRowsIn = 0;
    private int                 lastArraySlot = -1;
    public int                  state = STATE_UNINIT;

    private long                CID;
    private boolean             conglomCreated;
    private boolean             pipelineBufferCreated;
    private ConglomerateController	cc;
    private Properties				properties;

    private	ResultDescription		resultDescription;
    /** Activation object with local state information. */
    public Activation						activation;

    int 			overflowToConglomThreshold;
    int 			switchToSparkThreshold;
    boolean                     isSpark;  // Is the query executing on spark?
    private ExecRow execRowDefinition;
    private String  tableVersion;

    protected WriteCoordinator writeCoordinator;
    protected RecordingCallBuffer<KVPair> triggerTempTableWriteBuffer;
    protected Callable<Void> triggerTempTableflushCallback;
    protected Partition triggerTempPartition;
    private TxnView txn;
    private byte[] token;
    private TriggerExecutionContext tec;

    public TriggerRowHolderImpl() {

    }

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
            int                     switchToSparkThreshold,
            ExecRow                 execRowDefinition,
            String                  tableVersion,
            boolean                 isSpark,
            TxnView                 txn,
            byte[]                  token,
            long                    ConglomID,
            TriggerExecutionContext tec
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
        this.tec = tec;
        this.isSpark = isSpark;

        int initialArraySize = overflowToConglomThreshold < 1 ? 1 : overflowToConglomThreshold;
        if (initialArraySize > 2000)
            initialArraySize = 2000;
        rowArray = new ExecRow[initialArraySize];
        lastArraySlot = -1;
        this.execRowDefinition = execRowDefinition;
        this.tableVersion = tableVersion;
        rowArray[0] = execRowDefinition.getClone();
        this.overflowToConglomThreshold = overflowToConglomThreshold;
        this.switchToSparkThreshold = switchToSparkThreshold;

        if (ConglomID != 0) {
            // If we already created a conglomerate, that means we can't use
            // an in-memory cache for whatever reason, so set
            // overflowToConglomThreshold accordingly.
            this.overflowToConglomThreshold = 0;
            conglomCreated = true;
            this.CID = ConglomID;
        }
        else if (overflowToConglomThreshold == 0 && execRowDefinition != null) {
            try {
                createConglomerate(execRowDefinition);
                conglomCreated = true;
                tec.setExecRowDefinition(execRowDefinition);
                tec.setTableVersion(tableVersion);
                tec.setConglomId(this.CID);
            }
            catch (StandardException e) {
                throw new RuntimeException();
            }
        }
    }

    public boolean isSpark() { return isSpark; }

    public ExecRow getExecRowDefinition() {
        return execRowDefinition;
    }

    public String getTableVersion() {
        return tableVersion;
    }

    public void setActivation(Activation activation) {
        this.activation = activation;
    }

    public DataSet<ExecRow> getSourceSet() {
        if (!(getActivation().getResultSet() instanceof DMLWriteOperation))
            return null;
        DataSet<ExecRow> dataSet =
                   ((DMLWriteOperation) getActivation().getResultSet()).getSourceSet();
        return dataSet;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // Version number
        out.writeInt(1);

        out.writeBoolean(isSpark);
        out.writeObject(execRowDefinition);
        out.writeUTF(tableVersion);
        out.writeInt(lastArraySlot);
        int numElements = lastArraySlot+1;

        if (lastArraySlot > -1)
            for (int i = 0; i < numElements; i++)
                out.writeObject(rowArray[i]);

        out.writeInt(numRowsIn);

        out.writeInt(state);
        out.writeInt(overflowToConglomThreshold);
        out.writeInt(switchToSparkThreshold);
        out.writeLong(CID);
        out.writeBoolean(conglomCreated);
        out.writeObject(resultDescription);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Version number
        in.readInt();

        isSpark = in.readBoolean();
        execRowDefinition = (ExecRow) in.readObject();
        tableVersion = in.readUTF();
        lastArraySlot = in.readInt();
        int numElements = lastArraySlot <= -1 ? 1 : lastArraySlot+1;
        rowArray = new ValueRow[numElements];
        if (lastArraySlot <= -1)
            rowArray[0] = execRowDefinition.getClone();
        else
            for (int i = 0; i < rowArray.length; i++)
                rowArray[i] = (ExecRow) in.readObject();

        numRowsIn = in.readInt();
        state = in.readInt();
        overflowToConglomThreshold = in.readInt();
        switchToSparkThreshold = in.readInt();
        CID = in.readLong();
        conglomCreated = in.readBoolean();
        resultDescription = (ResultDescription) in.readObject();
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

            LOG.trace(format("Created temporary conglomerate splice:%d", CID));

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
            if (position <= lastArraySlot)
                return true;
            return false;
        }

        @Override
        public ExecRow next() {
            if (position <= lastArraySlot)
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

    /**
     * Insert a row
     *
     * @param sourceRow the original source row of the DML statement, to insert,
     *                  modified to extract only the relevant columns.
     *
     * @exception StandardException on error
     */
    public void insert(ExecRow sourceRow, KVPair encode)
            throws StandardException
    {

        if (SanityManager.DEBUG)
            SanityManager.ASSERT(state != STATE_DRAIN, "you cannot insert rows after starting to drain");

        state = STATE_INSERT;

        ExecRow inputRow = sourceRow;

        if (numRowsIn == switchToSparkThreshold && !isSpark) {
            // If running on HBase, and we exceeded the spark rows threshold,
            // throw ResubmitDistributedException so we switch to using spark.
            if (PipelineDriver.driver().exceptionFactory().isHBase()) {
                close();
                throw new ResubmitDistributedException();
            }
        }
        if (numRowsIn < overflowToConglomThreshold)
        {
            if (numRowsIn == rowArray.length) {
                int lengthToCopy = rowArray.length;
                // The following logic can grow the array twice if the initial array size is less than
                // a fourth of the overflowToConglomThreshold.
                int newSize = numRowsIn < overflowToConglomThreshold / 4 ? overflowToConglomThreshold / 4
                              : overflowToConglomThreshold;
                ExecRow [] newRowArray = new ExecRow[newSize];
                for (int i = 0; i < lengthToCopy; i++)
                    newRowArray[i] = rowArray[i];
                rowArray = newRowArray;
            }
            rowArray[numRowsIn++] = cloneRow(inputRow);
            lastArraySlot++;
            return;
        }

        numRowsIn++;
        if (!conglomCreated) {
            createConglomerate(inputRow);
        }
        if (!pipelineBufferCreated) {
            pipelineBufferCreated = true;
            try {
                writeCoordinator = PipelineDriver.driver().writeCoordinator();
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
    }

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
    }

    public long getTemporaryConglomId()
    {
        return getConglomerateId();
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

    private void dropConglomerate() throws StandardException {
        TransactionController tc = activation.getTransactionController();
        LOG.trace(format("Dropping temporary conglomerate splice:%d", CID));
        tc.dropConglomerate(CID);
        dropTable(CID);
        conglomCreated = false;
        CID = 0;
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

        if(triggerTempPartition!=null){
            try{
                triggerTempPartition.close();
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
        }

        if (conglomCreated)
            dropConglomerate();
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


