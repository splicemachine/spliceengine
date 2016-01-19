package com.splicemachine.derby.impl.store.access.base;

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.FormatableBitSet;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.store.access.BackingStoreHashtable;
import com.splicemachine.db.iapi.store.access.Qualifier;
import com.splicemachine.db.iapi.store.access.RowUtil;
import com.splicemachine.db.iapi.store.access.ScanInfo;
import com.splicemachine.db.iapi.store.access.conglomerate.ScanManager;
import com.splicemachine.db.iapi.store.raw.Transaction;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.db.impl.sql.execute.ValueRow;
import com.splicemachine.derby.impl.sql.execute.LazyScan;
import com.splicemachine.derby.impl.store.access.BaseSpliceTransaction;
import com.splicemachine.derby.impl.store.access.hbase.HBaseRowLocation;
import com.splicemachine.derby.utils.EngineUtils;
import com.splicemachine.derby.utils.Scans;
import com.splicemachine.derby.utils.marshall.EntryDataDecoder;
import com.splicemachine.derby.utils.marshall.EntryDataHash;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.TxnOperationFactory;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;

public class SpliceScan implements ScanManager, LazyScan{
    protected static Logger LOG=Logger.getLogger(SpliceScan.class);
    protected OpenSpliceConglomerate spliceConglomerate;
    private BaseSpliceTransaction trans;
    protected DataScan scan;
    protected FormatableBitSet scanColumnList;
    protected DataValueDescriptor[] startKeyValue;
    protected int startSearchOperator;
    protected Qualifier[][] qualifier;
    protected DataValueDescriptor[] stopKeyValue;
    protected int stopSearchOperator;
    protected DataResultScanner scanner;
    protected Partition table;
    protected boolean currentRowDeleted=false;
    protected HBaseRowLocation currentRowLocation;
    protected DataValueDescriptor[] currentRow;
    protected DataResult currentResult;
    protected long estimatedRowCount=0;
    protected boolean isKeyed;
    protected boolean scannerInitialized=false;
    protected String tableName;
    private EntryDecoder entryDecoder;

    private TxnOperationFactory opFactory;
    private PartitionFactory partitionFactory;

    public SpliceScan(){
        if(LOG.isTraceEnabled())
            LOG.trace("Instantiate Splice Scan for conglomerate ");
    }

    public SpliceScan(OpenSpliceConglomerate spliceConglomerate,
                      FormatableBitSet scanColumnList,
                      DataValueDescriptor[] startKeyValue,
                      int startSearchOperator,
                      Qualifier[][] qualifier,
                      DataValueDescriptor[] stopKeyValue,
                      int stopSearchOperator,
                      Transaction trans,
                      boolean isKeyed,
                      TxnOperationFactory operationFactory,
                      PartitionFactory partitionFactory){
        this.spliceConglomerate=spliceConglomerate;
        this.isKeyed=isKeyed;
        this.scanColumnList=scanColumnList;
        this.startKeyValue=startKeyValue;
        this.startSearchOperator=startSearchOperator;
        this.qualifier=qualifier;
        this.stopKeyValue=stopKeyValue;
        this.stopSearchOperator=stopSearchOperator;
        this.trans=(BaseSpliceTransaction)trans;
        this.opFactory = operationFactory;
        this.partitionFactory = partitionFactory;
        setupScan();
        attachFilter();
        tableName=Long.toString(spliceConglomerate.getConglomerate().getContainerid());
        if(LOG.isTraceEnabled()){
            SpliceLogUtils.trace(LOG,"scanning with start key %s and stop key %s and transaction %s",Arrays.toString(startKeyValue),Arrays.toString(stopKeyValue),trans);
        }
//				setupRowColumns();
    }

    public void close() throws StandardException{
        if(entryDecoder!=null)
            entryDecoder.close();

        try{
            if(scanner!=null) scanner.close();
        }catch(IOException ignored){ }

        try{
            if(scanner!=null) scanner.close();
        }catch(IOException ignored){ }
    }

    protected void attachFilter(){
        try{
            Scans.buildPredicateFilter(
                    qualifier,
                    null,
                    spliceConglomerate.getColumnOrdering(),
                    spliceConglomerate.getFormatIds(),
                    scan,"1.0");
        }catch(Exception e){
            throw new RuntimeException("error attaching Filter",e);
        }
    }

    public void setupScan(){
        try{
            assert spliceConglomerate!=null;
            boolean[] sortOrder=((SpliceConglomerate)this.spliceConglomerate.getConglomerate()).getAscDescInfo();
            boolean sameStartStop=isSameStartStop(startKeyValue,startSearchOperator,stopKeyValue,stopSearchOperator);
            scan=Scans.setupScan(startKeyValue,startSearchOperator,stopKeyValue,stopSearchOperator,qualifier,
                    sortOrder,scanColumnList,trans.getActiveStateTxn(),sameStartStop,
                    ((SpliceConglomerate)this.spliceConglomerate.getConglomerate()).format_ids,
                    ((SpliceConglomerate)this.spliceConglomerate.getConglomerate()).columnOrdering,
                    ((SpliceConglomerate)this.spliceConglomerate.getConglomerate()).columnOrdering,
                    trans.getDataValueFactory(),"1.0",false);
        }catch(Exception e){
            LOG.error("Exception creating start key");
            throw new RuntimeException(e);
        }
    }

    private boolean isSameStartStop(DataValueDescriptor[] startKeyValue,int startSearchOperator,DataValueDescriptor[] stopKeyValue,int stopSearchOperator) throws StandardException{
                /*
                 * Determine if the start and stop operators are actually, in fact the same.
				 *
				 * This assumes that the start and stop key operators are actually of the same type. While
				 * I don't think that this is a bad assumption, I suppose it could be in some circumstances.
				 */
        if(startSearchOperator!=stopSearchOperator) return false;

        if(startKeyValue==null){
            return stopKeyValue==null;
        }else if(stopKeyValue==null) return false;
        for(int i=0;i<startKeyValue.length;i++){
            if(i>=stopKeyValue.length) return false;
            DataValueDescriptor startDvd=startKeyValue[i];
            DataValueDescriptor stopDvd=stopKeyValue[i];
            if(startDvd.getTypeFormatId()!=stopDvd.getTypeFormatId()) return false;
            if(startDvd.compare(stopDvd)!=0) return false;
        }
        return true;
    }

    public boolean delete() throws StandardException{
        if(currentResult==null)
            throw StandardException.newException("Attempting to delete with a null current result");
        try(Partition table = partitionFactory.getTable(tableName)){
            DataMutation dataMutation=opFactory.newDataDelete(trans.getActiveStateTxn(),currentResult.key());
            table.mutate(dataMutation);
            currentRowDeleted=true;
            return true;
        }catch(Exception e){
            LOG.error(e.getMessage(),e);
            throw Exceptions.parseException(e);
        }
    }

    public boolean next() throws StandardException{
        initialize();
        currentRowDeleted=false;
        try{
            currentResult=scanner.next();
            if(currentResult!=null)
                this.currentRowLocation=new HBaseRowLocation(currentResult.key());
            return currentResult!=null;
        }catch(IOException e){
            throw Exceptions.parseException(e);
        }
    }

    public void fetch(DataValueDescriptor[] destRow) throws StandardException{
        if(this.currentResult==null)
            return;
        fetchWithoutQualify(destRow);
    }

    public void didNotQualify() throws StandardException{
    }

    public boolean doesCurrentPositionQualify() throws StandardException{
        throw new RuntimeException("Not Implemented");
    }

    public boolean isHeldAfterCommit() throws StandardException{
        // TODO Auto-generated method stub
        return false;
    }

    public boolean closeForEndTransaction(boolean closeHeldScan) throws StandardException{
        return false;
    }

    public boolean fetchNext(DataValueDescriptor[] destRow) throws StandardException{
        next();
        if(currentResult!=null){
            fetch(destRow);
            return true;
        }else
            return false;
    }

    public boolean isKeyed(){
        if(LOG.isTraceEnabled())
            LOG.trace("isKeyed");
        return isKeyed;
    }

    public boolean isTableLocked(){
        if(LOG.isTraceEnabled())
            LOG.trace("isTableLocked");
        return false;
    }

    public ScanInfo getScanInfo() throws StandardException{
        return new SpliceScanInfo(this);
    }

    public RowLocation newRowLocationTemplate() throws StandardException{
        if(LOG.isTraceEnabled())
            LOG.trace("newRowLocationTemplate");
        return new HBaseRowLocation();
    }

    public boolean isCurrentPositionDeleted() throws StandardException{
        if(LOG.isTraceEnabled())
            LOG.trace("isCurrentPositionDeleted");
        return currentRowDeleted;
    }

    public void fetchLocation(RowLocation destRowLocation) throws StandardException{
        if(currentResult==null)
            throw StandardException.newException("currentResult is null ");
        if(LOG.isTraceEnabled())
            SpliceLogUtils.trace(LOG,"fetchLocation %s",Bytes.toString(currentResult.key()));
        destRowLocation.setValue(this.currentResult.key());
    }

    public void fetchWithoutQualify(DataValueDescriptor[] destRow) throws StandardException{
        try{
            if(destRow!=null){
                ExecRow row=new ValueRow(destRow.length);
                row.setRowArray(destRow);
                DescriptorSerializer[] serializers=VersionedSerializers.forVersion("1.0",true).getSerializers(destRow);

                try(EntryDataDecoder decoder=new EntryDataDecoder(null,null,serializers)){
                    DataCell kv=currentResult.userData();//dataLib.matchDataColumn(currentResult);
                    decoder.set(kv.valueArray(),kv.valueOffset(),kv.valueLength());//dataLib.getDataValueBuffer(kv),dataLib.getDataValueOffset(kv),dataLib.getDataValuelength(kv));
                    decoder.decode(row);
                    this.currentRow=destRow;
                }
            }
            this.currentRowLocation=new HBaseRowLocation(currentResult.key());
        }catch(Exception e){
            throw StandardException.newException("Error occurred during fetch",e);
        }
    }

    public long getEstimatedRowCount() throws StandardException{
        return estimatedRowCount;
    }

    public void setEstimatedRowCount(long estimatedRowCount) throws StandardException{
        this.estimatedRowCount=estimatedRowCount;
    }

    public void fetchSet(long max_rowcnt,int[] key_column_numbers,BackingStoreHashtable hashTable) throws StandardException{
        SpliceLogUtils.trace(LOG,"IndexScan fetchSet for number of rows %d",max_rowcnt);
        initialize();
        if(max_rowcnt==0)
            return;
        if(max_rowcnt==-1)
            max_rowcnt=Long.MAX_VALUE;
        int rowCount=0;
        DataValueDescriptor[] fetchedRow=null;
        try{
            while((currentResult=scanner.next())!=null){
                SpliceLogUtils.trace(LOG,"fetch set iterator %s",currentResult);
                if(entryDecoder==null)
                    entryDecoder=new EntryDecoder();

                fetchedRow=RowUtil.newTemplate(
                        spliceConglomerate.getTransaction().getDataValueFactory(),
                        null,spliceConglomerate.getFormatIds(),spliceConglomerate.getCollationIds());
                DescriptorSerializer[] serializers=VersionedSerializers.forVersion("1.0",true).getSerializers(fetchedRow);

                try(EntryDataDecoder decoder=new EntryDataDecoder(null,null,serializers)){
                    ExecRow row=new ValueRow(fetchedRow.length);
                    row.setRowArray(fetchedRow);
                    DataCell kv=currentResult.userData();//dataLib.matchDataColumn(currentResult);
                    decoder.set(kv.valueArray(),kv.valueOffset(),kv.valueLength());//edataLib.getDataValueBuffer(kv),dataLib.getDataValueOffset(kv),dataLib.getDataValuelength(kv));
                    decoder.decode(row);
                }
                hashTable.putRow(false,fetchedRow);
                this.currentRowLocation=new HBaseRowLocation(currentResult.key());
                rowCount++;
                if(rowCount==max_rowcnt)
                    break;
            }
            this.currentRow=fetchedRow;
        }catch(Exception e){
            LOG.error(e.getMessage(),e);
            throw StandardException.newException("Error during fetchSet "+e);
        }
    }

    public int fetchNextGroup(DataValueDescriptor[][] row_array,RowLocation[] oldrowloc_array,RowLocation[] newrowloc_array) throws StandardException{
        throw new RuntimeException("Not Implemented");
        //	return 0;
    }

    public void reopenScan(DataValueDescriptor[] startKeyValue,int startSearchOperator,
                           Qualifier[][] qualifier,
                           DataValueDescriptor[] stopKeyValue,int stopSearchOperator) throws StandardException{
        this.startKeyValue=startKeyValue;
        this.startSearchOperator=startSearchOperator;
        this.qualifier=qualifier;
        this.stopKeyValue=stopKeyValue;
        this.stopSearchOperator=stopSearchOperator;
        setupScan();
        attachFilter();
        try{
            if(table==null)
                table=partitionFactory.getTable(Long.toString(spliceConglomerate.getConglomerate().getContainerid()));
            scanner=table.openResultScanner(scan);
        }catch(IOException e){
            throw Exceptions.parseException(e); //TODO -sf- replace this with an exceptionFactory
        }
    }

    public void reopenScanByRowLocation(RowLocation startRowLocation,Qualifier[][] qualifier) throws StandardException{
        SpliceLogUtils.trace(LOG,"reopenScanByRowLocation %s  for qualifier ",startRowLocation,qualifier);
        this.qualifier=qualifier;
        setupScan();
        scan.startKey(startRowLocation.getBytes());
        attachFilter();
        try{
            scanner=table.openResultScanner(scan);
        }catch(IOException e){
            throw Exceptions.parseException(e); //TODO -sf- replace this with an exceptionFactory
        }
    }

    public boolean positionAtRowLocation(RowLocation rl) throws StandardException{
        SpliceLogUtils.trace(LOG,"positionAtRowLocation %s",rl);
        return this.currentRowLocation!=null && this.currentRowLocation.equals(rl);
    }

    public int fetchNextGroup(DataValueDescriptor[][] row_array,RowLocation[] rowloc_array) throws StandardException{
        try{
            initialize();
            if(scanner==null)
                return 0;
            if(row_array==null || row_array.length==0)
                return 0;

            throw new UnsupportedOperationException("IMPLEMENT");
//            DataResult[] results=scanner.next(row_array.length);
//            // Have To generate template
//            if(results!=null && results.length>0){
//                SpliceLogUtils.trace(LOG,"HBaseScan fetchNextGroup total number of results=%d",results.length);
//                for(int i=0;i<results.length;i++){
//                    DataResult result = results[i];
//                    DataValueDescriptor[] kdvds=row_array[i];
//                    ExecRow row=new ValueRow(kdvds.length);
//                    row.setRowArray(kdvds);
//                    DescriptorSerializer[] serializers=VersionedSerializers.forVersion("1.0",true).getSerializers(kdvds);
//                    KeyHashDecoder decoder=new EntryDataDecoder(null,null,serializers);
//                    try{
//                        if(results[i]!=null){
//                            DataCell kv=result.userData();//dataLib.matchDataColumn(results[i]);
//                            decoder.set(kv.valueArray(),kv.valueOffset(),kv.valueLength());//dataLib.getDataValueBuffer(kv),
////                                    dataLib.getDataValueOffset(kv),dataLib.getDataValuelength(kv));
//                            decoder.decode(row);
//                        }
//                    }finally{
//                        Closeables.closeQuietly(decoder);
//                    }
//                }
//                this.currentRowLocation=new HBaseRowLocation(results[results.length-1].key());
//                this.currentRow=row_array[results.length-1];
//                this.currentResult=results[results.length-1];
//                return results.length;
//            }
//            return 0;
        }catch(Exception e){
            LOG.error(e.getMessage(),e);
            throw StandardException.newException("Error during fetchNextGroup "+e);
        }
    }

    public boolean replace(DataValueDescriptor[] row,FormatableBitSet validColumns) throws StandardException{
        SpliceLogUtils.trace(LOG,"replace values for these valid Columns %s",validColumns);
        try{
            int[] validCols=EngineUtils.bitSetToMap(validColumns);
            DataPut put=opFactory.newDataPut(trans.getActiveStateTxn(),currentRowLocation.getBytes());//SpliceUtils.createPut(currentRowLocation.getBytes(),trans.getActiveStateTxn());

            DescriptorSerializer[] serializers=VersionedSerializers.forVersion("1.0",true).getSerializers(row);
            EntryDataHash entryEncoder=new EntryDataHash(validCols,null,serializers);
            ExecRow execRow=new ValueRow(row.length);
            execRow.setRowArray(row);
            entryEncoder.setRow(execRow);
            byte[] data=entryEncoder.encode();
            put.addCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES,data);

            table.put(put);

//			table.put(Puts.buildInsert(currentRowLocation.getByteCopy(), row, validColumns, transID));
            return true;
        }catch(Exception e){
            throw StandardException.newException("Error during replace "+e);
        }
    }

    public DataScan getScan(){
        if(LOG.isTraceEnabled())
            LOG.trace("getScan called from ParallelScan Interface");
        return scan;
    }

    public String getTableName(){
        return this.tableName;
    }

    @Override
    public void initialize(){
        if(scannerInitialized) return;
        try{
            if(table==null)
                table=partitionFactory.getTable(Long.toString(spliceConglomerate.getConglomerate().getContainerid()));
            scanner=table.openResultScanner(scan);
            this.scannerInitialized=true;
        }catch(IOException e){
            LOG.error("Initializing scanner failed",e);
            throw new RuntimeException(e);
        }
    }


}
