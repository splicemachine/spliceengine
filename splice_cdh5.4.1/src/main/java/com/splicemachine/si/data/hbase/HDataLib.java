package com.splicemachine.si.data.hbase;

import com.google.common.base.Function;
import com.google.common.collect.Lists;
import com.splicemachine.hbase.CellUtils;
import com.splicemachine.hbase.SICompactionScanner;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.impl.region.ActiveTxnFilter;
import com.splicemachine.storage.*;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.impl.server.SICompactionState;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.regionserver.InternalScanner;
import org.apache.hadoop.hbase.regionserver.RegionScanner;
import org.apache.hadoop.hbase.util.Pair;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;

import static com.splicemachine.si.constants.SIConstants.*;

/**
 * Implementation of SDataLib that is specific to the HBase operation and result types.
 */
public class HDataLib implements SDataLib<OperationWithAttributes,
        Cell,
        Delete,
        Filter,
        Get,
        Put,
        RegionScanner,
        Result,
        Scan
        >{
    private static final HDataLib INSTANCE= new HDataLib();

    @Override
    public byte[] newRowKey(Object... args){
        List<byte[]> bytes=new ArrayList<>();
        for(Object a : args){
            bytes.add(convertToBytes(a,a.getClass()));
        }
        return Bytes.concat(bytes);
    }

    @Override
    public List<DataCell> listResult(Result result){
        final HCell newCellWrapper=new HCell();
        return Lists.transform(result.listCells(),new Function<Cell, DataCell>(){
            @Override
            public DataCell apply(Cell input){
                newCellWrapper.set(input);
                return newCellWrapper;
            }
        });
    }

    @Override
    public byte[] encode(Object value){
        return convertToBytes(value,value.getClass());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(byte[] value,Class<T> type){
        if(value==null){
            return null;
        }
        if(type.equals(Boolean.class)){
            return (T)(Boolean)Bytes.toBoolean(value);
        }else if(type.equals(Short.class)){
            return (T)(Short)Bytes.toShort(value);
        }else if(type.equals(Integer.class)){
            if(value.length<4) return (T)new Integer(-1);
            return (T)(Integer)Bytes.toInt(value);
        }else if(type.equals(Long.class)){
            return (T)(Long)Bytes.toLong(value);
        }else if(type.equals(Byte.class)){
            if(value.length>0){
                return (T)(Byte)value[0];
            }else{
                return null;
            }
        }else if(type.equals(String.class)){
            return (T)Bytes.toString(value);
        }
        throw new RuntimeException("unsupported type conversion: "+type.getName());
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T> T decode(byte[] value,int offset,int length,Class<T> type){
        if(value==null){
            return null;
        }
        if(type.equals(Boolean.class)){
            return (T)(Boolean)Bytes.toBoolean(value,offset);
        }else if(type.equals(Short.class)){
            return (T)(Short)Bytes.toShort(value,offset);
        }else if(type.equals(Integer.class)){
            if(length<4) return (T)new Integer(-1);
            return (T)(Integer)Bytes.toInt(value,offset);
        }else if(type.equals(Long.class)){
            return (T)(Long)Bytes.toLong(value,offset);
        }else if(type.equals(Byte.class)){
            if(length>0){
                return (T)(Byte)value[offset];
            }else{
                return null;
            }
        }else if(type.equals(String.class)){
            return (T)Bytes.toString(value,offset,length);
        }
        throw new RuntimeException("unsupported type conversion: "+type.getName());
    }

    @Override
    public void addKeyValueToPut(Put put,byte[] family,byte[] qualifier,long timestamp,byte[] value){
        if(timestamp<0){
            put.addColumn(family,qualifier,value);
        }else{
            put.addColumn(family,qualifier,timestamp,value);
        }
    }

    @Override
    public Put newPut(byte[] key){
        return new Put(key);
    }

    public static HDataLib instance(){
       return INSTANCE;
    }

    private Put newPut(ByteSlice key){
        return new Put(key.array(),key.offset(),key.length());
    }

    @Override
    public byte[] getGetRow(Get get){
        return get.getRow();
    }

    @Override
    public void setGetTimeRange(Get get,long minTimestamp,long maxTimestamp){
        try{
            get.setTimeRange(minTimestamp,maxTimestamp);
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setGetMaxVersions(Get get){
        get.setMaxVersions();
    }

    @Override
    public void setScanTimeRange(Scan scan,long minTimestamp,long maxTimestamp){
        try{
            scan.setTimeRange(minTimestamp,maxTimestamp);
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void setScanMaxVersions(Scan scan){
        scan.setMaxVersions();
    }

    @Override
    public Delete newDelete(byte[] rowKey){
        return new Delete(rowKey);
    }

    static byte[] convertToBytes(Object value,Class clazz){
        if(clazz==String.class){
            return Bytes.toBytes((String)value);
        }else if(clazz==Integer.class){
            return Bytes.toBytes((Integer)value);
        }else if(clazz==Short.class){
            return Bytes.toBytes((Short)value);
        }else if(clazz==Long.class){
            return Bytes.toBytes((Long)value);
        }else if(clazz==Boolean.class){
            return Bytes.toBytes((Boolean)value);
        }else if(clazz==byte[].class){
            return (byte[])value;
        }else if(clazz==Byte.class){
            return new byte[]{(Byte)value};
        }
        throw new RuntimeException("Unsupported class "+clazz.getName()+" for "+value);
    }

    @Override
    public boolean singleMatchingColumn(Cell element,byte[] family,
                                        byte[] qualifier){
        return CellUtils.singleMatchingColumn(element,family,qualifier);
    }

    @Override
    public boolean singleMatchingFamily(Cell element,byte[] family){
        return CellUtils.singleMatchingFamily(element,family);
    }

    @Override
    public boolean singleMatchingQualifier(Cell element,byte[] qualifier){
        return CellUtils.singleMatchingQualifier(element,qualifier);
    }

    @Override
    public boolean matchingValue(Cell element,byte[] value){
        return CellUtils.matchingValue(element,value);
    }

    @Override
    public boolean matchingRowKeyValue(Cell element,Cell other){
        return CellUtils.matchingRowKeyValue(element,other);
    }

    @Override
    public Cell newValue(Cell element,byte[] value){
        return CellUtils.newKeyValue(element,value);
    }

    @Override
    public Comparator getComparator(){
        return KeyValue.COMPARATOR;
    }

    @Override
    public long getTimestamp(Cell element){
        return element.getTimestamp();
    }

    @Override
    public String getFamilyAsString(Cell element){
        return Bytes.toString(CellUtil.cloneFamily(element));
    }

    @Override
    public String getQualifierAsString(Cell element){
        return Bytes.toString(CellUtil.cloneQualifier(element));
    }

    @Override
    public void setRowInSlice(Cell element,ByteSlice slice){
        slice.set(element.getRowArray(),element.getRowOffset(),element.getRowLength());
    }

    @Override
    public boolean isFailedCommitTimestamp(Cell element){
        return element.getValueLength()==1 && element.getValueArray()[element.getValueOffset()]==SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0];
    }

    @Override
    public Cell newTransactionTimeStampKeyValue(Cell element,byte[] value){
        return new KeyValue(element.getRowArray(),element.getRowOffset(),element.getRowLength(),DEFAULT_FAMILY_BYTES,0,1,SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,0,1,element.getTimestamp(),KeyValue.Type.Put,value,0,value==null?0:value.length);
    }

    @Override
    public long getValueLength(Cell element){
        return element.getValueLength();
    }

    @Override
    public long getValueToLong(Cell element){
        return Bytes.toLong(element.getValueArray(),element.getValueOffset(),element.getValueLength());
    }

    @Override
    public byte[] getDataValue(Cell element){
        return CellUtil.cloneValue(element);
    }

    @Override
    public Cell[] getDataFromResult(Result result){
        return result.rawCells();
    }

    @Override
    public byte[] getDataRow(Cell element){
        return CellUtil.cloneRow(element);
    }

    @Override
    public DataCell getColumnLatest(Result result,byte[] family,
                                byte[] qualifier){
        return new HCell(result.getColumnLatestCell(family,qualifier));
    }

    @Override
    public byte[] getDataValueBuffer(Cell element){
        return element.getValueArray();
    }

    @Override
    public int getDataValueOffset(Cell element){
        return element.getValueOffset();
    }

    @Override
    public int getDataValuelength(Cell element){
        return element.getValueLength();
    }

    @Override
    public int getLength(Cell element){
        return element.getQualifierLength()+element.getFamilyLength()+element.getRowLength()+element.getValueLength()+element.getTagsLength();
    }

    @Override
    public byte[] getDataRowBuffer(Cell element){
        return element.getRowArray();
    }

    @Override
    public int getDataRowOffset(Cell element){
        return element.getRowOffset();
    }

    @Override
    public int getDataRowlength(Cell element){
        return element.getRowLength();
    }

    @Override
    public boolean regionScannerNext(RegionScanner regionScanner,
                                     List<Cell> data) throws IOException{
        return regionScanner.next(data);
    }

    @Override
    public boolean regionScannerNextRaw(RegionScanner regionScanner,
                                        List<Cell> data) throws IOException{
        return regionScanner.nextRaw(data);
    }

    @Override
    public Filter getActiveTransactionFilter(long beforeTs,long afterTs,
                                             byte[] destinationTable){
        return new ActiveTxnFilter(this,beforeTs,afterTs,destinationTable);
    }

//    @Override
    public InternalScanner getCompactionScanner(InternalScanner scanner,
                                                SICompactionState state){
        return new SICompactionScanner(state,scanner);
    }

//    @Override
    public boolean internalScannerNext(InternalScanner internalScanner,
                                       List<Cell> data) throws IOException{
        return internalScanner.next(data);
    }

//    @Override
    public boolean isDataInRange(Cell data,Pair<byte[], byte[]> range){
        return CellUtils.isKeyValueInRange(data,range);
    }

    @Override
    public Cell matchKeyValue(Cell[] kvs,byte[] columnFamily,
                              byte[] qualifier){
        int size=kvs!=null?kvs.length:0;
        for(int i=0;i<size;i++){
            Cell kv=kvs[i];
            if(CellUtils.matchingColumn(kv,columnFamily,qualifier))
                return kv;
        }
        return null;
    }

    @Override
    public Cell matchDataColumn(Cell[] kvs){
        return matchKeyValue(kvs,DEFAULT_FAMILY_BYTES,
                PACKED_COLUMN_BYTES);
    }

    @Override
    public DataResult newResult(List<DataCell> visibleColumns){
        List<Cell> actualCells = new ArrayList<>(visibleColumns.size());
        for(DataCell dc:visibleColumns){
            actualCells.add(((HCell)dc).unwrapDelegate());
        }
        Result r = Result.create(actualCells);
        return new HResult(r);
    }

    @Override
    public void addKeyValueToPut(Put put,byte[] family,byte[] qualifier,byte[] value){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Get newGet(byte[] key){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void addFamilyQualifierToGet(Get read,byte[] family,byte[] column){

        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Scan newScan(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public Scan newScan(byte[] startRowKey,byte[] endRowKey){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataScan newDataScan(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void setScanMaxVersions(Scan get,int maxVersions){
        throw new UnsupportedOperationException("IMPLEMENT");

    }

    @Override
    public void setAttribute(OperationWithAttributes operation,String name,byte[] value){
        throw new UnsupportedOperationException("IMPLEMENT");

    }

    @Override
    public byte[] getAttribute(OperationWithAttributes operation,String attributeName){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean noResult(Result result){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void setFilterOnScan(Scan scan,Filter filter){

        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataPut newDataPut(ByteSlice key){
        return new HPut(key);
    }

    @Override
    public DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp){
        ByteSlice rowKey=kvPair.rowKeySlice();
        ByteSlice val=kvPair.valueSlice();
        Put put=newPut(rowKey);
        Cell kv=new KeyValue(rowKey.array(),rowKey.offset(),rowKey.length(),
                family,0,family.length,
                column,0,column.length,
                timestamp,
                KeyValue.Type.Put,val.array(),val.offset(),val.length());
        try{
            put.add(kv);
        }catch(IOException ignored){
                        /*
						 * This exception only appears to occur if the row in the Cell does not match
						 * the row that's set in the Put. This is definitionally not the case for the above
						 * code block, so we shouldn't have to worry about this error. As a result, throwing
						 * a RuntimeException here is legitimate
						 */
            throw new RuntimeException(ignored);
        }
        return new HPut(put);
    }

    @Override
    public DataDelete newDataDelete(byte[] key){
        return new HDelete(key);
    }

}
