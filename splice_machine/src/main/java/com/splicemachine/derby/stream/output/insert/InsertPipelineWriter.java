/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.output.insert;

import com.splicemachine.SpliceKryoRegistry;
import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.operations.InsertOperation;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.impl.sql.execute.sequence.SpliceSequence;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.PermissiveInsertWriteConfiguration;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.config.RollforwardWriteConfiguration;
import com.splicemachine.pipeline.config.UnsafeWriteConfiguration;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.Partition;
import com.splicemachine.utils.IntArrays;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.Iterator;

/**
 * Created by jleach on 5/5/15.
 */
public class InsertPipelineWriter extends AbstractPipelineWriter<ExecRow>{
    protected int[] pkCols;

    protected RowLocation[] autoIncrementRowLocationArray;
    protected KVPair.Type dataType;
    protected SpliceSequence[] spliceSequences;
    protected PairEncoder encoder;
    protected InsertOperation insertOperation;
    protected boolean isUpsert;
    private Partition table;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public InsertPipelineWriter(int[] pkCols,
                                String tableVersion,
                                ExecRow execRowDefinition,
                                RowLocation[] autoIncrementRowLocationArray,
                                SpliceSequence[] spliceSequences,
                                long heapConglom,
                                long tempConglomID,
                                TxnView txn,
                                byte[] token, OperationContext operationContext,
                                boolean isUpsert) {
        super(txn,token,heapConglom,tempConglomID,tableVersion, execRowDefinition, operationContext);
        assert txn !=null:"txn not supplied";
        this.pkCols = pkCols;
        this.autoIncrementRowLocationArray = autoIncrementRowLocationArray;
        this.spliceSequences = spliceSequences;
        this.destinationTable = Bytes.toBytes(Long.toString(heapConglom));
        this.isUpsert = isUpsert;
        this.dataType = isUpsert?KVPair.Type.UPSERT:KVPair.Type.INSERT;
        if (operationContext!=null) {
            this.insertOperation = (InsertOperation) operationContext.getOperation();
        }
    }

    public void open() throws StandardException {
          open(insertOperation==null?null:insertOperation.getTriggerHandler(),insertOperation);
    }

    public void open(TriggerHandler triggerHandler, SpliceOperation operation) throws StandardException {
        super.open(triggerHandler, operation);
        try {
            encoder = new PairEncoder(getKeyEncoder(), getRowHash(), dataType);
            WriteConfiguration writeConfiguration = writeCoordinator.defaultWriteConfiguration();
            if(insertOperation!=null && operationContext.isPermissive())
                    writeConfiguration = new PermissiveInsertWriteConfiguration(writeConfiguration,
                            operationContext,
                            encoder, execRowDefinition);
            if(insertOperation.skipConflictDetection() || insertOperation.skipWAL()) {
                writeConfiguration = new UnsafeWriteConfiguration(writeConfiguration, insertOperation.skipConflictDetection(), insertOperation.skipWAL());
            }
            if(rollforward)
                writeConfiguration = new RollforwardWriteConfiguration(writeConfiguration);

            writeConfiguration.setRecordingContext(operationContext);
            this.table =SIDriver.driver().getTableFactory().getTable(Long.toString(heapConglom));

            writeBuffer = writeCoordinator.writeBuffer(table,txn,token,writeConfiguration);
            if (insertOperation != null)
                insertOperation.tableWriter = this;
            flushCallback = triggerHandler == null ? null : TriggerHandler.flushCallback(writeBuffer);

        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    public void insert(ExecRow execRow) throws StandardException {
        try {
            if (operationContext!=null && operationContext.isFailed())
                return;
            beforeRow(execRow);
            KVPair encode = encoder.encode(execRow);
            writeBuffer.add(encode);
            addRowToTriggeringResultSet(execRow, encode);
            TriggerHandler.fireAfterRowTriggers(triggerHandler, execRow, flushCallback);
        } catch (Exception e) {
            if (operationContext!=null && operationContext.isPermissive()) {
                    operationContext.recordBadRecord(e.getLocalizedMessage() + execRow.toString(), e);
                return;
            }
            throw Exceptions.parseException(e);
        }
    }

    public void insert(Iterator<ExecRow> execRows) throws StandardException {
        while (execRows.hasNext())
            insert(execRows.next());
    }

    public void write(ExecRow execRow) throws StandardException {
        insert(execRow);
    }

    public void write(Iterator<ExecRow> execRows) throws StandardException {
        insert(execRows);
    }


    public KeyEncoder getKeyEncoder() throws StandardException {
        HashPrefix prefix;
        DataHash dataHash;
        KeyPostfix postfix = NoOpPostfix.INSTANCE;
        if(pkCols==null){
            prefix = new SaltedPrefix(EngineDriver.driver().newUUIDGenerator(100));
            dataHash = NoOpDataHash.INSTANCE;
        }else{
            int[] keyColumns = new int[pkCols.length];
            for(int i=0;i<keyColumns.length;i++){
                keyColumns[i] = pkCols[i] -1;
            }
            prefix = NoOpPrefix.INSTANCE;
            DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(execRowDefinition);
            dataHash = BareKeyHash.encoder(keyColumns,null, SpliceKryoRegistry.getInstance(),serializers);
        }
        return new KeyEncoder(prefix,dataHash,postfix);
    }

    public DataHash getRowHash() throws StandardException {
        //get all columns that are being set
        int[] columns = getEncodingColumns(execRowDefinition.nColumns(),pkCols);
        DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(execRowDefinition);
        return new EntryDataHash(columns,null,serializers);
    }

    @Override
    public void close() throws StandardException{
        super.close();
        if(table!=null){
            try{
                table.close();
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
        }
    }

    public static int[] getEncodingColumns(int n, int[] pkCols) {
        int[] columns = IntArrays.count(n);
        // Skip primary key columns to save space
        if (pkCols != null) {
            for(int pkCol:pkCols) {
                columns[pkCol-1] = -1;
            }
        }
        return columns;
    }
}
