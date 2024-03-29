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

package com.splicemachine.derby.stream.output.update;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.impl.sql.execute.LazyDataValueFactory;
import com.splicemachine.derby.impl.sql.execute.operations.TriggerHandler;
import com.splicemachine.derby.impl.sql.execute.operations.UpdateOperation;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.stream.output.AbstractPipelineWriter;
import com.splicemachine.derby.utils.marshall.*;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.callbuffer.ForwardRecordingCallBuffer;
import com.splicemachine.pipeline.callbuffer.PreFlushHook;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.pipeline.config.RollforwardWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.pipeline.utils.PipelineUtils;
import com.splicemachine.storage.util.UpdateUtils;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by jleach on 5/5/15.
 */
public class UpdatePipelineWriter extends AbstractPipelineWriter<ExecRow>{
    protected static final KVPair.Type dataType=KVPair.Type.UPDATE;
    protected int[] formatIds;
    protected int[] columnOrdering;
    protected int[] pkCols;
    protected FormatableBitSet pkColumns;
    protected DataValueDescriptor[] kdvds;
    protected int[] colPositionMap;
    protected FormatableBitSet heapList;
    protected FormatableBitSet valuesList;
    protected boolean modifiedPrimaryKeys=false;
    protected int[] finalPkColumns;

    protected PairEncoder encoder;

    protected ExecRow currentRow;
    public int rowsUpdated=0;
    protected UpdateOperation updateOperation;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public UpdatePipelineWriter(long heapConglom,long tempConglomID,int[] formatIds,int[] columnOrdering,
                                int[] pkCols,FormatableBitSet pkColumns,String tableVersion,TxnView txn,byte[] token,
                                ExecRow execRowDefinition,FormatableBitSet heapList,OperationContext operationContext) throws StandardException{
        super(txn, token, heapConglom, tempConglomID, tableVersion, execRowDefinition, operationContext, false);
        assert pkCols!=null && columnOrdering!=null:"Primary Key Information is null";
        this.formatIds=formatIds;
        this.columnOrdering=columnOrdering;
        this.pkCols=pkCols;
        this.pkColumns=pkColumns;
        this.heapList=heapList;
        if (operationContext != null) {
            updateOperation = (UpdateOperation)operationContext.getOperation();
        }
    }

    public void open() throws StandardException{
        open(updateOperation != null ? updateOperation.getTriggerHandler() : null, updateOperation, false);
    }

    public void open(TriggerHandler triggerHandler, SpliceOperation operation, boolean loadReplaceMode) throws StandardException{
        super.open(triggerHandler,operation, loadReplaceMode);
        kdvds=new DataValueDescriptor[columnOrdering.length];
        // Get the DVDS for the primary keys...
        for(int i=0;i<columnOrdering.length;++i)
            kdvds[i]=LazyDataValueFactory.getLazyNull(formatIds[columnOrdering[i]]);

        setupColumnMapping();

        // Grab the final PK Columns
        if(pkCols!=null){
            finalPkColumns=new int[pkCols.length];
            int count=0;
            for(int i : pkCols){
                finalPkColumns[count]=colPositionMap[i];
                count++;
            }
        }else{
            finalPkColumns=new int[0];
        }

        WriteConfiguration writeConfiguration = writeCoordinator.newDefaultWriteConfiguration();
        if(rollforward)
            writeConfiguration = new RollforwardWriteConfiguration(writeConfiguration);

        RecordingCallBuffer<KVPair> bufferToTransform=null;
        try{
            bufferToTransform=writeCoordinator.writeBuffer(destinationTable,txn,null, PipelineUtils.noOpFlushHook,writeConfiguration,Metrics.noOpMetricFactory());
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        writeBuffer=transformWriteBuffer(bufferToTransform);
        encoder=new PairEncoder(getKeyEncoder(),getRowHash(),dataType);
        flushCallback=triggerHandler==null?null:TriggerHandler.flushCallback(writeBuffer);
    }

    /*  colPositionMap and heapList control how we map values coming into the Update operation to the
    actual write going to the WritePipeline
        heapList determines the number of columns of the written value and references colPositionMap
    for the absolute position of those values on the row
        Initially colPositionMap has as many elements as columns in the targetTable and heapList has
    as many bits set as columns are being modified. Since we are going to include the old values too
    we have to duplicate their size and rewire some mappings
        If we have a table with 4 columns and we are updating columns {0, 2} we'd have something like
            inputRow = { oldValue0, oldValue1, newValue0, newValue1, ROWID }
            colPositionMap (1-based) = { 0, 2 (newValue0), 0, 3 (newValue1), 0 }
            heapList = { 1, 3 } (referencing colPositionMap[1]=newValue0 and colPositionMap[3]=newValue1
            outputRow = { newValue0, newValue1 }
        Since we want to include the oldValues we rewire them to

                                                  newValue1      oldValue1
                                                  v              v
            colPositionMap (1-based) = { 0, 2, 0, 3, 0, 0, 0, 0, 1, 0 }
                                            ^              ^
                                            newValue0      oldValue0

            heapList = { 1, 3, 6, 8 } (referencing colPositionMap[1,3,6,8] respectively
            outputRow = { newValue0, newValue1, oldValue0, oldValue1 }
    */
    private void setupColumnMapping() {
        // We are including the new and the old values on the write, so we need twice the space
        // See example in UpdateUtils.extractWriteFromUpdate()
        colPositionMap=new int[heapList.size()*2];
        // Map Column Positions for encoding
        for(int i=heapList.anySetBit(), pos=heapList.getNumBitsSet();i!=-1;i=heapList.anySetBit(i),pos++) {
            colPositionMap[i] = pos;
            colPositionMap[i+heapList.size()] = pos - heapList.getNumBitsSet();
        }
        // Check for PK Modifications...
        if(pkColumns!=null){
            for(int pkCol=pkColumns.anySetBit();pkCol!=-1;pkCol=pkColumns.anySetBit(pkCol)){
                if(heapList.isSet(pkCol+1)){
                    modifiedPrimaryKeys=true;
                    break;
                }
            }

            // unset any pk columns, they shouldn't be included in the value (they go in the key)
            for(int pkCol=pkColumns.anySetBit();pkCol!=-1;pkCol=pkColumns.anySetBit(pkCol)){
                heapList.clear(pkCol+1);
            }
        }
        // grow heapList
        valuesList = (FormatableBitSet) heapList.clone();
        valuesList.grow(heapList.size()*2);
        for (int i=heapList.anySetBit();i!=-1;i=heapList.anySetBit(i)) {
            valuesList.set(i + heapList.size());
        }
    }

    public KeyEncoder getKeyEncoder() throws StandardException{
        DataHash hash;
        if(!modifiedPrimaryKeys){
            hash=new DataHash<ExecRow>(){
                private ExecRow currentRow;

                @Override
                public void setRow(ExecRow rowToEncode){
                    this.currentRow=rowToEncode;
                }

                @Override
                public byte[] encode() throws StandardException {
                    return ((RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
                }

                @Override
                public void close() throws IOException{
                }

                @Override
                public KeyHashDecoder getDecoder(){
                    return NoOpKeyHashDecoder.INSTANCE;
                }
            };
        }else{
            //TODO -sf- we need a sort order here for descending columns, don't we?
            //hash = BareKeyHash.encoder(getFinalPkColumns(getColumnPositionMap(heapList)),null);
            hash=new PkDataHash(finalPkColumns,kdvds,tableVersion);
        }
        return new KeyEncoder(NoOpPrefix.INSTANCE,hash,NoOpPostfix.INSTANCE);
    }

    public DataHash getRowHash() throws StandardException{
        DescriptorSerializer[] serializers=VersionedSerializers.forVersion(tableVersion,false).getSerializers(execRowDefinition);
        return new RowHash(colPositionMap,null,serializers,valuesList);
    }

    public RecordingCallBuffer<KVPair> transformWriteBuffer(final RecordingCallBuffer<KVPair> bufferToTransform) throws StandardException{
        if(modifiedPrimaryKeys){
            PreFlushHook preFlushHook=new PreFlushHook(){
                @Override
                public Collection<KVPair> transform(Collection<KVPair> buffer) throws Exception{
                    bufferToTransform.flushBufferAndWait();
                    return new ArrayList<>(buffer); // Remove this but here to match no op...
                }
            };
            try{
                return new ForwardRecordingCallBuffer<KVPair>(writeCoordinator.writeBuffer(getDestinationTable(),txn,null,preFlushHook)){
                    @Override
                    public void add(KVPair element) throws Exception{
                        byte[] oldLocation=((RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
                        if(!Bytes.equals(oldLocation,element.getRowKey())){
                            bufferToTransform.add(new KVPair(oldLocation,SIConstants.EMPTY_BYTE_ARRAY,KVPair.Type.DELETE));
                            element = UpdateUtils.updateFromWrite(element);
                            element.setType(KVPair.Type.INSERT);
                        }else{
                            element.setType(KVPair.Type.UPDATE);
                        }
                        delegate.add(element);
                    }

                    @Override
                    public KVPair lastElement(){
                        return delegate.lastElement();
                    }
                };
            }catch(IOException e){
                throw Exceptions.parseException(e);
            }
        }else
            return bufferToTransform;
    }

    public void update(ExecRow execRow) throws StandardException{
        try{
            beforeRow(execRow);
            currentRow=execRow;
            rowsUpdated++;
            KVPair encode=encoder.encode(execRow);
            assert encode.getRowKey()!=null && encode.getRowKey().length>0:"Tried to buffer incorrect row key";
            writeBuffer.add(encode);
            if (triggerRowsEncoder != null) {
                KVPair encodeTriggerRow = triggerRowsEncoder.encode(execRow);
                addRowToTriggeringResultSet(execRow, encodeTriggerRow);
            }
            TriggerHandler.fireAfterRowTriggers(triggerHandler,execRow,flushCallback);
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    public void update(Iterator<ExecRow> execRows) throws StandardException{
        while(execRows.hasNext())
            update(execRows.next());
    }

    public void write(Iterator<ExecRow> execRows) throws StandardException{
        update(execRows);
    }

    public void write(ExecRow execRow) throws StandardException{
        update(execRow);
    }

}
