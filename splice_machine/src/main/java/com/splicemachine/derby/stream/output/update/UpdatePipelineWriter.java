/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import com.carrotsearch.hppc.BitSet;
import com.splicemachine.access.impl.data.UnsafeRecord;
import com.splicemachine.access.impl.data.UnsafeRecordUtils;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
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
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.DataCell;
import com.splicemachine.storage.Record;
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
    protected int[] columnOrdering;
    protected int[] pkCols;
    protected FormatableBitSet pkColumns;
    protected DataValueDescriptor[] kdvds;
    protected int[] colPositionMap;
    protected FormatableBitSet heapList;
    protected boolean modifiedPrimaryKeys=false;
    protected int[] finalPkColumns;
    protected PairEncoder encoder;
    protected ExecRow currentRow;
    public int rowsUpdated=0;
    protected UpdateOperation updateOperation;
    protected KeyEncoder keyEncoder;
    protected UnsafeRecord unsafeRecord;
    protected ExecRow fullRow;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public UpdatePipelineWriter(long heapConglom, int[] columnOrdering,
                                int[] pkCols,FormatableBitSet pkColumns,String tableVersion,TxnView txn,byte[] token,
                                ExecRow execRowDefinition,FormatableBitSet heapList,OperationContext operationContext) throws StandardException{
        super(tableVersion,txn,token,heapConglom,operationContext,execRowDefinition);
        assert pkCols!=null && columnOrdering!=null:"Primary Key Information is null";
        this.columnOrdering=columnOrdering;
        this.pkCols=pkCols;
        this.pkColumns=pkColumns;
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
            kdvds[i]=execRow.getColumn(columnOrdering[i]+1);
        fullRow = new ValueRow(execRow.nColumns());
        for(int i=0;i<fullRow.size();++i)
            fullRow.setColumn(i+1,execRow.getColumn(i+1)); // This is wrong...  TODO JL

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


        RecordingCallBuffer<KVPair> bufferToTransform=null;
        try{
            bufferToTransform=writeCoordinator.writeBuffer(destinationTable,txn,null,Metrics.noOpMetricFactory(),execRow);
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
        writeBuffer=transformWriteBuffer(bufferToTransform);
        encoder=new PairEncoder(getKeyEncoder(),getRowHash(),dataType);
        flushCallback=triggerHandler==null?null:TriggerHandler.flushCallback(writeBuffer);
    }

    public KeyEncoder getKeyEncoder() throws StandardException{
        if (keyEncoder == null) {
            DataHash hash;
            if (!modifiedPrimaryKeys) {
                hash = new DataHash<ExecRow>() {
                    private ExecRow currentRow;

                    @Override
                    public void setRow(ExecRow rowToEncode) {
                        this.currentRow = rowToEncode;
                    }

                    @Override
                    public byte[] encode() throws StandardException, IOException {
                        return ((RowLocation) currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
                    }

                    @Override
                    public void close() throws IOException {
                    }

                    @Override
                    public KeyHashDecoder getDecoder() {
                        return NoOpKeyHashDecoder.INSTANCE;
                    }
                };
            } else {
                //TODO -sf- we need a sort order here for descending columns, don't we?
                //hash = BareKeyHash.encoder(getFinalPkColumns(getColumnPositionMap(heapList)),null);
                hash = new PkDataHash(finalPkColumns, kdvds, tableVersion);
            }
            keyEncoder = new KeyEncoder(NoOpPrefix.INSTANCE, hash, NoOpPostfix.INSTANCE);
        }
        return keyEncoder;
    }

    public DataHash getRowHash() throws StandardException{
        //if we haven't modified any of our primary keys, then we can just change it directly
        DescriptorSerializer[] serializers=VersionedSerializers.forVersion(tableVersion,false).getSerializers(execRow);
        if(!modifiedPrimaryKeys){
            return new NonPkRowHash(colPositionMap,null,serializers,heapList);
        }
        ResultSupplier resultSupplier=new ResultSupplier(new BitSet(),txn,heapConglom);
        return new PkRowHash(finalPkColumns,null,heapList,colPositionMap,resultSupplier,serializers);
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
                return new ForwardRecordingCallBuffer<KVPair>(writeCoordinator.writeBuffer(getDestinationTable(),txn,null,preFlushHook,execRow)){
                    @Override
                    public void add(KVPair element) throws Exception{
                        byte[] oldLocation=((RowLocation)currentRow.getColumn(currentRow.nColumns()).getObject()).getBytes();
                        if(!Bytes.equals(oldLocation,element.getRowKey())){
                            if (tableVersion.equals("3.0")) {
                                RedoResultSupplier redoResultSupplier =  new RedoResultSupplier(new BitSet(),txn,heapConglom, fullRow);
                                redoResultSupplier.setLocation(oldLocation);
                                UnsafeRecord unsafeRecord = new UnsafeRecord();
                                unsafeRecord.wrap(redoResultSupplier.getResult());
                                UnsafeRecord update = new UnsafeRecord();
                                update.wrap(element,true,unsafeRecord.getVersion());
                                Record[] updatedRecords = unsafeRecord.updateRecord(update,fullRow);
                                updatedRecords[0].setTxnId1(txn.getTxnId());
                                updatedRecords[0].setKey(element.getRowKey());

                                UnsafeRecord deleteRecord = new UnsafeRecord(
                                        oldLocation,
                                        1,
                                        new byte[UnsafeRecordUtils.calculateFixedRecordSize(1)],
                                        0l, true);
                                deleteRecord.setNumberOfColumns(0);
                                deleteRecord.setTxnId1(txn.getTxnId());
                                deleteRecord.setHasTombstone(true);
                                KVPair encode = deleteRecord.getKVPair(); // Do I need this?
                                encode.setType(KVPair.Type.DELETE);
                                bufferToTransform.add(encode);
                                element = ((UnsafeRecord)updatedRecords[0]).getKVPair();
                                redoResultSupplier.close();
                            } else {
                                bufferToTransform.add(new KVPair(oldLocation, SIConstants.EMPTY_BYTE_ARRAY, KVPair.Type.DELETE));
                            }
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
            KVPair encode;
            if (tableVersion.equals("3.0")) {
                unsafeRecord = new UnsafeRecord(
                        keyEncoder.getKey(execRow),
                        1,
                        new byte[UnsafeRecordUtils.calculateFixedRecordSize(execRow.nColumns())],
                        0l, false);
                unsafeRecord.setNumberOfColumns(execRow.nColumns());
                unsafeRecord.setTxnId1(txn.getTxnId());
                unsafeRecord.setData(colPositionMap,heapList,execRow.getRowArray());
                encode = unsafeRecord.getKVPair(); // Do I need this?
                encode.setType(KVPair.Type.UPDATE);
            } else {
                encode=encoder.encode(execRow);
            }
            assert encode.getRowKey()!=null && encode.getRowKey().length>0:"Tried to buffer incorrect row key";
            writeBuffer.add(encode);
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
