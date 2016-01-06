package com.splicemachine.derby.impl.store.access.base;

import com.carrotsearch.hppc.BitSet;
import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.ConglomerateController;
import com.splicemachine.db.iapi.store.access.SpaceInfo;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.db.impl.store.raw.data.SpaceInformation;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.SpliceTransaction;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.FormatableBitSetUtils;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.KeyHashDecoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.storage.*;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Properties;

public abstract class SpliceController implements ConglomerateController{
    protected static final SDataLib dataLib=SIDriver.driver().getDataLib();
    protected static Logger LOG=Logger.getLogger(SpliceController.class);
    protected OpenSpliceConglomerate openSpliceConglomerate;
    private PartitionFactory partitionFactory;
    protected BaseSpliceTransaction trans;
    protected EntryDataHash entryEncoder;
    private String tableVersion;
    private Partition table;
    protected TxnOperationFactory opFactory;

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
            try{
                table.close();
            }catch(IOException ignored){ }
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
        throw new UnsupportedOperationException("checkConsistency not enabled");
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

    public SpaceInfo getSpaceInfo() throws StandardException{
        return new SpaceInformation(0l,0l,0l);
    }


    public void debugConglomerate() throws StandardException{
        throw new UnsupportedOperationException("DebugConglomerate Not enabled");
    }

    public boolean isKeyed(){
        return false;
    }

    public boolean delete(RowLocation loc) throws StandardException{
        try(Partition htable = getTable()){
            opFactory.newDataDelete(((SpliceTransaction)trans).getTxn(),loc.getBytes());
//            SpliceUtils.doDelete(htable,((SpliceTransaction)trans).getTxn(),loc.getBytes());
            return true;
        }catch(Exception e){
            throw Exceptions.parseException(e);
        }
    }

    public boolean fetch(RowLocation loc,DataValueDescriptor[] destRow,FormatableBitSet validColumns) throws StandardException{
        return fetch(loc,destRow,validColumns,false);
    }

    public boolean fetch(RowLocation loc,DataValueDescriptor[] destRow,FormatableBitSet validColumns,boolean waitForLock) throws StandardException{
        try(Partition htable = getTable()){
            DataGet baseGet=opFactory.newDataGet(trans.getTxnInformation(),loc.getBytes(),null);
            DataGet get=createGet(baseGet,destRow,validColumns);//loc,destRow,validColumns,trans.getTxnInformation());
            DataResult result=htable.get(get,null);
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
            EntryPredicateFilter predicateFilter = new EntryPredicateFilter(fieldsToReturn, new ObjectArrayList<Predicate>());
            baseGet.addAttribute(SIConstants.ENTRY_PREDICATE_LABEL,predicateFilter.toBytes());
            return baseGet;
        } catch (Exception e) {
            throw Exceptions.parseException(e);
        }
    }
}
