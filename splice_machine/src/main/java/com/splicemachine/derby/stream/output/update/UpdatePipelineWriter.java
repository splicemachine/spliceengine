/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.output.update;

import com.carrotsearch.hppc.BitSet;
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
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.pipeline.callbuffer.ForwardRecordingCallBuffer;
import com.splicemachine.pipeline.callbuffer.PreFlushHook;
import com.splicemachine.pipeline.callbuffer.RecordingCallBuffer;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.Txn;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.Record;
import com.splicemachine.storage.RecordType;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Iterator;

/**
 * Created by jleach on 5/5/15.
 */
public class UpdatePipelineWriter extends AbstractPipelineWriter<ExecRow>{
    protected static final RecordType dataType= RecordType.UPDATE;
    protected int[] formatIds;
    protected int[] columnOrdering;
    protected int[] pkCols;
    protected FormatableBitSet pkColumns;
    protected DataValueDescriptor[] kdvds;
    protected int[] colPositionMap;
    protected FormatableBitSet heapList;
    protected boolean modifiedPrimaryKeys=false;
    protected int[] finalPkColumns;
    protected String tableVersion;
    protected ExecRow execRowDefinition;
    protected ExecRow currentRow;
    public int rowsUpdated=0;
    protected UpdateOperation updateOperation;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public UpdatePipelineWriter(long heapConglom,int[] formatIds,int[] columnOrdering,
                                int[] pkCols,FormatableBitSet pkColumns,String tableVersion,Txn txn,
                                ExecRow execRowDefinition,FormatableBitSet heapList,OperationContext operationContext) throws StandardException{
        super(txn,heapConglom,operationContext);
        assert pkCols!=null && columnOrdering!=null:"Primary Key Information is null";
        this.formatIds=formatIds;
        this.columnOrdering=columnOrdering;
        this.pkCols=pkCols;
        this.pkColumns=pkColumns;
        this.tableVersion=tableVersion;
        this.execRowDefinition=execRowDefinition;
        this.heapList=heapList;
        if (operationContext != null) {
            updateOperation = (UpdateOperation)operationContext.getOperation();
        }
    }

    public void open() throws StandardException{
        open(updateOperation != null ? updateOperation.getTriggerHandler() : null, updateOperation);
    }

    public void open(TriggerHandler triggerHandler,SpliceOperation operation) throws StandardException{
        super.open(triggerHandler,operation);
        kdvds=new DataValueDescriptor[columnOrdering.length];
        // Get the DVDS for the primary keys...
        for(int i=0;i<columnOrdering.length;++i)
            kdvds[i]=LazyDataValueFactory.getLazyNull(formatIds[columnOrdering[i]]);
        colPositionMap=new int[heapList.size()];
        // Map Column Positions for encoding
        for(int i=heapList.anySetBit(), pos=heapList.getNumBitsSet();i!=-1;i=heapList.anySetBit(i),pos++)
            colPositionMap[i]=pos;
        // Check for PK Modifications...
        if(pkColumns!=null){
            for(int pkCol=pkColumns.anySetBit();pkCol!=-1;pkCol=pkColumns.anySetBit(pkCol)){
                if(heapList.isSet(pkCol+1)){
                    modifiedPrimaryKeys=true;
                    break;
                }
            }
        }
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


        RecordingCallBuffer<Record> bufferToTransform=null;
        try{
            bufferToTransform=writeCoordinator.writeBuffer(destinationTable,txn,Metrics.noOpMetricFactory());
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        writeBuffer=transformWriteBuffer(bufferToTransform);
        encoder=new PairEncoder(getKeyEncoder(),getRowHash(),dataType);
        flushCallback=triggerHandler==null?null:TriggerHandler.flushCallback(writeBuffer);
    }

    public DataHash getRowHash() throws StandardException{
        //if we haven't modified any of our primary keys, then we can just change it directly
        DescriptorSerializer[] serializers=VersionedSerializers.forVersion(tableVersion,false).getSerializers(execRowDefinition);
        if(!modifiedPrimaryKeys){
            return new NonPkRowHash(colPositionMap,null,serializers,heapList);
        }
        ResultSupplier resultSupplier=new ResultSupplier(new BitSet(),txn,heapConglom);
        return new PkRowHash(finalPkColumns,null,heapList,colPositionMap,resultSupplier,serializers);
    }

    public RecordingCallBuffer<Record> transformWriteBuffer(final RecordingCallBuffer<Record> bufferToTransform) throws StandardException{
        if(modifiedPrimaryKeys){
            PreFlushHook preFlushHook=new PreFlushHook(){
                @Override
                public Collection<Record> transform(Collection<Record> buffer) throws Exception{
                    bufferToTransform.flushBufferAndWait();
                    return new ArrayList<>(buffer); // Remove this but here to match no op...
                }
            };
            try{
                return new ForwardRecordingCallBuffer<Record>(writeCoordinator.writeBuffer(getDestinationTable(),txn,preFlushHook)){
                    @Override
                    public void add(Record element) throws Exception{
                        byte[] oldLocation=((RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
                        if(!Bytes.equals(oldLocation,element.getKey())){
                            bufferToTransform.add(new KVPair(oldLocation,SIConstants.EMPTY_BYTE_ARRAY,RecordType.DELETE));
                            element.setRecordType(RecordType.INSERT);
                        }else{
                            element.setRecordType(RecordType.UPDATE);
                        }
                        delegate.add(element);
                    }

                    @Override
                    public Record lastElement(){
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
            assert encode.getKey()!=null && encode.getRowKey().length>0:"Tried to buffer incorrect row key";
            writeBuffer.add(encode);
            TriggerHandler.fireAfterRowTriggers(triggerHandler,execRow,flushCallback);
            operationContext.recordWrite();
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
