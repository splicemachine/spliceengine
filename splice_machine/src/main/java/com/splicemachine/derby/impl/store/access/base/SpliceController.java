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

package com.splicemachine.derby.impl.store.access.base;

import com.carrotsearch.hppc.BitSet;
import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.impl.data.UnsafeRecord;
import com.splicemachine.access.impl.data.UnsafeRecordUtils;
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
import com.splicemachine.db.iapi.types.HBaseRowLocation;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.api.txn.TxnView;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import org.apache.commons.lang.SerializationUtils;
import org.apache.log4j.Logger;
import org.apache.parquet.Closeables;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Properties;

public abstract class SpliceController implements ConglomerateController{
    protected static Logger LOG=Logger.getLogger(SpliceController.class);
    protected OpenSpliceConglomerate openSpliceConglomerate;
    private PartitionFactory partitionFactory;
    protected BaseSpliceTransaction trans;
    protected EntryDataHash entryEncoder;
    private String tableVersion;
    private Partition table;
    protected TxnOperationFactory opFactory;
    protected ExecRow rowTemplate;
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
                            TxnOperationFactory operationFactory) throws StandardException {
        this.openSpliceConglomerate=openSpliceConglomerate;
        rowTemplate = openSpliceConglomerate.cloneExecRowTemplate();
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
        if(entryEncoder!=null)
            try{ entryEncoder.close();}catch(IOException ignored){}
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
            DataMutation dataMutation=opFactory.newDataDelete(((SpliceTransaction)trans).getTxn(),loc.getBytes());
            htable.mutate(dataMutation);
            return true;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }
    @Override
    public boolean fetch(RowLocation loc,ExecRow destRow,FormatableBitSet validColumns) throws StandardException{
        return fetch(loc,destRow,validColumns,false);
    }
    @Override
    public boolean fetch(RowLocation loc,ExecRow destRow,FormatableBitSet validColumns,boolean waitForLock) throws StandardException{
        return batchFetch(Collections.singletonList(loc),Collections.singletonList(destRow),validColumns,waitForLock);
    }
    @Override
    public boolean batchFetch(List<RowLocation> locations, List<ExecRow> destRows, FormatableBitSet validColumns) throws StandardException{
        return batchFetch(locations,destRows,validColumns,false);
    }
    @Override
    public boolean batchFetch(List<RowLocation> locations, List<ExecRow> destRows,FormatableBitSet validColumns,boolean waitForLock) throws StandardException{
        if (locations.isEmpty())
            return false;
        Partition htable = getTable();
        KeyHashDecoder rowDecoder = null;
        try{
            DataGet baseGet=opFactory.newDataGet(trans.getTxnInformation(),locations.get(0).getBytes(),null);
            if (htable.isRedoPartition()) {
                baseGet.addColumn(SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES,SIConstants.PACKED_COLUMN_BYTES);
            } else {
                baseGet.returnAllVersions();
            }

            DataGet get = createGet(baseGet,destRows.get(0).getRowArray(),validColumns);//loc,destRow,validColumns,trans.getTxnInformation());
            Iterator<DataResult> results = htable.batchGet(get, Lists.transform(locations, ROWLOCATION_TO_BYTES));
            assert results != null:"Results Returned are Null";

            if (htable.isRedoPartition()) {
                int i = 0;
                UnsafeRecord record = new UnsafeRecord();
                while (results.hasNext()) {
                    DataResult result = results.next();
                    DataValueDescriptor[] destRow = destRows.get(i).getRowArray();
                    ExecRow row = new ValueRow(destRow.length);
                    row.setRowArray(destRow);
                    row.resetRowArray();
                    DataCell keyValue = result.activeData();
                    record.wrap(keyValue);
                    record.getData(validColumns,row);
                    i++;
                }
            } else {
                int i = 0;
                DescriptorSerializer[] serializers = null;
                int[] cols = FormatableBitSetUtils.toIntArray(validColumns);
                while (results.hasNext()) {
                    DataResult result = results.next();
                    DataValueDescriptor[] destRow = destRows.get(i).getRowArray();
                    if (serializers == null) {
                        serializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(destRow);
                        rowDecoder = new EntryDataDecoder(cols, null, serializers);
                    }
                    ExecRow row = new ValueRow(destRow.length);
                    row.setRowArray(destRow);
                    row.resetRowArray();
                    DataCell keyValue = result.userData();
                    rowDecoder.set(keyValue.valueArray(), keyValue.valueOffset(), keyValue.valueLength());
                    rowDecoder.decode(row);
                    i++;
                }
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


    protected void encodeRow(Partition table, TxnView txnView, DataValueDescriptor[] row, DataPut put, FormatableBitSet validColumns) throws StandardException, IOException {
        try {
            if (table.isRedoPartition()) {
                UnsafeRecord record = new UnsafeRecord(
                        put.key(),
                        1,
                        new byte[UnsafeRecordUtils.calculateFixedRecordSize(row.length)],
                        0l, true);
                record.setNumberOfColumns(row.length);
                record.setData(validColumns, row);
                record.setTxnId1(txnView.getTxnId());
                put.addAttribute(SIConstants.SI_EXEC_ROW, SerializationUtils.serialize(rowTemplate));
                put.addCell(SIConstants.DEFAULT_FAMILY_ACTIVE_BYTES, SIConstants.PACKED_COLUMN_BYTES, 1, record.getValue());
            } else {
                if (entryEncoder == null) {
                    int[] validCols = EngineUtils.bitSetToMap(validColumns);
                    DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion, true).getSerializers(row);
                    entryEncoder = new EntryDataHash(validCols, null, serializers);
                }
                ValueRow rowToEncode = new ValueRow(row.length);
                rowToEncode.setRowArray(row);
                entryEncoder.setRow(rowToEncode);
                byte[] data = entryEncoder.encode();
                put.addCell(SIConstants.DEFAULT_FAMILY_BYTES, SIConstants.PACKED_COLUMN_BYTES, data);
            }
        } catch (StandardException | IOException | NullPointerException e) {
            throw e;
        }
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
