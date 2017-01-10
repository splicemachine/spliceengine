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

package com.splicemachine.derby.impl.store.access.base;

import com.carrotsearch.hppc.BitSet;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.IsolationLevel;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import org.apache.log4j.Logger;
import org.apache.parquet.Closeables;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public abstract class SpliceController implements ConglomerateController{
    protected static Logger LOG=Logger.getLogger(SpliceController.class);
    protected OpenSpliceConglomerate openSpliceConglomerate;
    private PartitionFactory partitionFactory;
    protected BaseSpliceTransaction trans;
    private String tableVersion;
    private Partition table;
    protected TxnOperationFactory opFactory;
    private static Function ROWLOCATION_TO_BYTES = new Function<RowLocation, byte[]>() {
        @Override
        public byte[] apply(@Nullable RowLocation rowLocation) {
            try {
                return rowLocation.getBytes();
            } catch (Exception e) {
                throw new RuntimeException(e);
            }
        }
    };


    public SpliceController(){
    }

    public SpliceController(OpenSpliceConglomerate openSpliceConglomerate,
                            Transaction trans,
                            PartitionFactory partitionFactory,
                            TxnOperationFactory operationFactory){
        this.openSpliceConglomerate=openSpliceConglomerate;
        this.partitionFactory=partitionFactory;
        this.opFactory = operationFactory;
        this.trans=(BaseSpliceTransaction)trans;
        try{
            this.trans.setActiveState(false,false,null);
        }catch(Exception e){
            throw new RuntimeException(e);
        }
        this.tableVersion="1.0";    //TODO -sf- move this to non-1.0
    }

    public void close() throws StandardException{
        if(table!=null){
            try{ table.close(); }catch(IOException ignored){ }
        }
        try{
            if((openSpliceConglomerate!=null) && (openSpliceConglomerate.getTransactionManager()!=null))
                openSpliceConglomerate.getTransactionManager().closeMe(this);
        }catch(Exception e){
            throw StandardException.newException("error on close"+e);
        }
    }

    public void getTableProperties(Properties prop) throws StandardException{
    }

    public Properties getInternalTablePropertySet(Properties prop) throws StandardException{
        return prop;
    }

    public boolean closeForEndTransaction(boolean closeHeldScan) throws StandardException{
        return false;
    }

    public void checkConsistency() throws StandardException{
        //nothing to do here
    }

    public boolean lockRow(RowLocation loc,int lock_oper,boolean wait,int lock_duration) throws StandardException{
        throw new UnsupportedOperationException("Unable to lock rows in SpliceMachine");
    }


    public boolean lockRow(long page_num,int record_id,int lock_oper,boolean wait,int lock_duration) throws StandardException{
        throw new UnsupportedOperationException("Unable to lock rows in SpliceMachine");
    }


    public void unlockRowAfterRead(RowLocation loc,boolean forUpdate,boolean row_qualified) throws StandardException{
    }


    public RowLocation newRowLocationTemplate() throws StandardException{
        return new HBaseRowLocation();
    }

    public void debugConglomerate() throws StandardException{
        //no-op
    }

    public boolean isKeyed(){
        return false;
    }

    public boolean delete(RowLocation loc) throws StandardException{
        Partition htable = getTable();
        try{
            htable.delete(loc.getBytes(),((SpliceTransaction)trans).getTxn());
            return true;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    public boolean fetch(RowLocation loc,DataValueDescriptor[] destRow,FormatableBitSet validColumns) throws StandardException{
        return fetch(loc,destRow,validColumns,false);
    }

    public boolean fetch(RowLocation loc,DataValueDescriptor[] destRow,FormatableBitSet validColumns,boolean waitForLock) throws StandardException{
        Partition htable = getTable();
        try{
            DataGet baseGet=opFactory.newDataGet(trans.getTxnInformation(),loc.getBytes(),null);
            DataGet get=createGet(baseGet,destRow,validColumns);//loc,destRow,validColumns,trans.getTxnInformation());
            Record result=htable.get(loc.getBytes(),trans.getTxnInformation(), IsolationLevel.SNAPSHOT_ISOLATION);
            validColumns

            if(result==null || result.size()<=0) return false;

            int[] cols=FormatableBitSetUtils.toIntArray(validColumns);
            DescriptorSerializer[] serializers=VersionedSerializers.forVersion(tableVersion,true).getSerializers(destRow);
            try(KeyHashDecoder rowDecoder=new EntryDataDecoder(cols,null,serializers)){
                ExecRow row=new ValueRow(destRow.length);
                row.setRowArray(destRow);
                row.resetRowArray();
                DataCell keyValue=result.userData();
                rowDecoder.set(keyValue.valueArray(),keyValue.valueOffset(),keyValue.valueLength());
                rowDecoder.decode(row);
            }
            return true;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
    @Override
    public boolean batchFetch(List<RowLocation> locations, List<ExecRow> destRows, FormatableBitSet validColumns) throws StandardException{
        return batchFetch(locations,destRows,validColumns,false);
    }
    @Override
    public boolean batchFetch(List<RowLocation> locations, List<ExecRow> destRows,FormatableBitSet validColumns,boolean waitForLock) throws StandardException{
        if (locations.size() == 0)
            return false;
        Partition htable = getTable();
        KeyHashDecoder rowDecoder = null;
        try{
            DataGet baseGet=opFactory.newDataGet(trans.getTxnInformation(),locations.get(0).getBytes(),null);
            baseGet.returnAllVersions();
            DataGet get = createGet(baseGet,destRows.get(0).getRowArray(),validColumns);//loc,destRow,validColumns,trans.getTxnInformation());
            Iterator<DataResult> results = htable.batchGet(get, Lists.transform(locations, ROWLOCATION_TO_BYTES));
            assert results != null:"Results Returned are Null";
            int i = 0;
            DescriptorSerializer[] serializers = null;
            int[] cols=FormatableBitSetUtils.toIntArray(validColumns);
            while (results.hasNext()) {
                DataResult result = results.next();
                DataValueDescriptor[] destRow = destRows.get(i).getRowArray();
                if (serializers ==null) {
                    serializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(destRow);
                    rowDecoder=new EntryDataDecoder(cols,null,serializers);
                }
                ExecRow row=new ValueRow(destRow.length);
                row.setRowArray(destRow);
                row.resetRowArray();
                DataCell keyValue=result.userData();
                rowDecoder.set(keyValue.valueArray(),keyValue.valueOffset(),keyValue.valueLength());
                rowDecoder.decode(row);
                i++;
            }
            return true;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        } finally {
            if (rowDecoder !=null)
                Closeables.closeAndSwallowIOExceptions(rowDecoder);
        }

    }


    @Override
    public String toString(){
        return "SpliceController {conglomId="+openSpliceConglomerate.getConglomerate().getContainerid()+"}";
    }

    protected Partition getTable(){
        if(table==null){
            try{
                table=partitionFactory.getTable(Long.toString(openSpliceConglomerate.getConglomerate().getContainerid()));
            }catch(IOException e){
                throw new RuntimeException(e);
            }
        }
        return table;
    }


    protected void encodeRow(DataValueDescriptor[] row,DataPut put,int[] columns,FormatableBitSet validColumns) throws StandardException, IOException{
        if(entryEncoder==null){
            int[] validCols=EngineUtils.bitSetToMap(validColumns);
            DescriptorSerializer[] serializers=VersionedSerializers.forVersion(tableVersion,true).getSerializers(row);
            entryEncoder=new EntryDataHash(validCols,null,serializers);
        }
        ValueRow rowToEncode=new ValueRow(row.length);
        rowToEncode.setRowArray(row);
        entryEncoder.setRow(rowToEncode);
        byte[] data=entryEncoder.encode();
        put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,data);
    }

    protected void elevateTransaction() throws StandardException{
        ((SpliceTransaction)trans).elevate(Bytes.toBytes(Long.toString(openSpliceConglomerate.getConglomerate().getContainerid())));
    }

    public SpliceConglomerate getConglomerate(){
        return (SpliceConglomerate)openSpliceConglomerate.getConglomerate();
    }


    protected static DataGet createGet(DataGet baseGet,
                                    DataValueDescriptor[] destRow,
                                    FormatableBitSet validColumns) throws StandardException {
        try {
//            Get get = createGet(txn, loc.getBytes());
            BitSet fieldsToReturn;
            if(validColumns!=null){
                fieldsToReturn = new BitSet(validColumns.size());
                for(int i=validColumns.anySetBit();i>=0;i=validColumns.anySetBit(i)){
                    fieldsToReturn.set(i);
                }
            }else{
                fieldsToReturn = new BitSet(destRow.length);
                fieldsToReturn.set(0,destRow.length);
            }
            EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fieldsToReturn);
            baseGet.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
            return baseGet;
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }
}
