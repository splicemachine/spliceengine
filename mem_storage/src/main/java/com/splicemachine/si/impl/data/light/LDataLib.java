package com.splicemachine.si.impl.data.light;

import com.google.common.collect.Lists;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.primitives.Bytes;
import com.splicemachine.si.api.data.SDataLib;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.storage.*;
import com.splicemachine.utils.ByteSlice;
import com.splicemachine.utils.ComparableComparator;

import java.io.IOException;
import java.util.*;

public class LDataLib implements SDataLib<
        LOperationWithAttributes,
        DataCell,
        LTuple,
        Void,
        LGet,
        LTuple,
        LScan,
        MResult,
        LGet>{


    @Override
    public void addKeyValueToPut(LTuple dataCells,byte[] family,byte[] qualifier,byte[] value){
        dataCells.values.add(new MCell(dataCells.key,family,qualifier,1l,value,CellType.USER_DATA));
    }

    @Override
    public LGet newGet(byte[] key){
        return new LGet(key,key,null,null,null);
    }

    @Override
    public void addFamilyQualifierToGet(LGet read,byte[] family,byte[] column){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public LGet newScan(){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public LGet newScan(byte[] startRowKey,byte[] endRowKey){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public DataScan newDataScan(){
        return new MScan();
    }

    @Override
    public void setScanMaxVersions(LGet get,int maxVersions){
        throw new UnsupportedOperationException("IMPLEMENT");

    }

    @Override
    public boolean regionScannerNext(LScan lScan,List<DataCell> data) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean regionScannerNextRaw(LScan lScan,List<DataCell> data) throws IOException{
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void setAttribute(LOperationWithAttributes operation,String name,byte[] value){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public byte[] getAttribute(LOperationWithAttributes operation,String attributeName){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public boolean noResult(MResult dataCells){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void setFilterOnScan(LGet lGet,Void aVoid){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public byte[] newRowKey(Object... args){
        StringBuilder builder=new StringBuilder();
        for(Object a : args){
            Object toAppend=a;
            if(a instanceof Short){
                toAppend=String.format("%1$06d",a);
            }else if(a instanceof Long){
                toAppend=String.format("%1$020d",a);
            }else if(a instanceof Byte){
                toAppend=String.format("%1$02d",a);
            }
            builder.append(toAppend);
        }
        return builder.toString().getBytes();
    }

    private boolean nullSafeComparison(Object o1,Object o2){
        if(o1==null){
            return o2==null;
        }else if(o2==null) return false;

        if(o1 instanceof byte[] && o2 instanceof byte[])
            return Arrays.equals((byte[])o1,(byte[])o2);
        else
            return o1.equals(o2);

//        return (o1 == null && o2 == null) || ((o1 != null) && o1.equals(o2));
    }

    @Override
    public byte[] encode(Object value){
        if(value instanceof String){
            return ((String)value).getBytes();
        }else if(value instanceof Boolean)
            return new byte[]{((Boolean)value)?(byte)-1:(byte)0};
        else if(value instanceof Integer)
            return Bytes.toBytes((Integer)value);
        else if(value instanceof Long)
            return Bytes.toBytes((Long)value);
        else if(value instanceof Byte)
            return new byte[]{(Byte)value};
        else if(value instanceof Short)
            return Bytes.toBytes((Short)value);
        else
            return (byte[])value;
    }


    @SuppressWarnings("unchecked")
    public <T> T decode(byte[] value,Class<T> type){
        if(!(value instanceof byte[])){
            return (T)value;
        }

        if(byte[].class.equals(type))
            return (T)value;
        if(String.class.equals(type))
            return (T)Bytes.toString(value);
        else if(Long.class.equals(type))
            return (T)(Long)Bytes.toLong(value);
        else if(Integer.class.equals(type)){
            if(value.length<4)
                return (T)new Integer(-1);
            return (T)(Integer)Bytes.toInt(value);
        }else if(Boolean.class.equals(type))
            return (T)(Boolean)Bytes.toBoolean(value);
        else if(Byte.class.equals(type))
            return (T)(Byte)value[0];
        else
            throw new RuntimeException("types don't match "+value.getClass().getName()+" "+type.getName()+" "+value);
    }

    public <T> T decode(byte[] value,int offset,int length,Class<T> type){
        if(!(value instanceof byte[])){
            return (T)value;
        }

        if(byte[].class.equals(type))
            return (T)value;
        if(String.class.equals(type))
            return (T)Bytes.toString(value,offset,length);
        else if(Long.class.equals(type))
            return (T)(Long)Bytes.toLong(value,offset);
        else if(Integer.class.equals(type)){
            if(length<4)
                return (T)new Integer(-1);
            return (T)(Integer)Bytes.toInt(value,offset);
        }else if(Boolean.class.equals(type))
            return (T)(Boolean)Bytes.toBoolean(value,offset);
        else if(Byte.class.equals(type))
            return (T)(Byte)value[offset];
        else
            throw new RuntimeException("types don't match "+value.getClass().getName()+" "+type.getName()+" "+value);
    }

    @Override
    public void addKeyValueToPut(LTuple put,byte[] family,byte[] qualifier,long timestamp,byte[] value){
        addKeyValueToTuple(put,family,qualifier,timestamp,value);
    }

    private void addKeyValueToTuple(LTuple tuple,Object family,Object qualifier,long timestamp,byte[] value){
        //TODO -sf- set the correct cell type
        DataCell newCell=new MCell(tuple.key,(byte[])family,(byte[])qualifier,timestamp,value,CellType.USER_DATA);
        tuple.values.add(newCell);
    }

    @Override
    public LTuple newPut(byte[] key){
        return newPut(key,null);
    }

    private LTuple newPut(byte[] key,Integer lock){
        return new LTuple(key,new ArrayList<DataCell>(),lock);
    }

    @Override
    public byte[] getGetRow(LGet get){
        return get.startTupleKey;
    }

    @Override
    public void setGetTimeRange(LGet get,long minTimestamp,long maxTimestamp){
        assert minTimestamp==0L;
        get.effectiveTimestamp=maxTimestamp-1;
    }

    @Override
    public void setGetMaxVersions(LGet get){
    }

    @Override
    public void setScanTimeRange(LGet get,long minTimestamp,long maxTimestamp){
        assert minTimestamp==0L;
        get.effectiveTimestamp=maxTimestamp-1;
    }

    @Override
    public void setScanMaxVersions(LGet get){
    }

    private void ensureFamilyDirect(LGet lGet,byte[] family){
        if(lGet.families.isEmpty() && (lGet.columns==null || lGet.columns.isEmpty())){
        }else{
            if(lGet.families.contains(family)){
            }else{
                lGet.families.add(family);
            }
        }
    }

    private byte[] getTupleKey(Object result){
        return ((LTuple)result).key;
    }

    @Override
    public List<DataCell> listResult(MResult result){
        List<DataCell> values=Lists.newArrayList(result);
        Collections.sort(values);
        return values;
    }


    @Override
    public LTuple newDelete(byte[] rowKey){
        return newPut(rowKey,null);
    }

    @Override
    public boolean singleMatchingColumn(DataCell element,byte[] family,
                                        byte[] qualifier){
        return element.matchesQualifier(family,qualifier);
    }

    @Override
    public boolean singleMatchingFamily(DataCell element,byte[] family){
        return element.matchesFamily(family);
    }

    @Override
    public boolean singleMatchingQualifier(DataCell element,byte[] qualifier){
        return element.matchesQualifier(element.family(),qualifier);
    }

    @Override
    public boolean matchingValue(DataCell element,byte[] value){
        return Bytes.equals(element.valueArray(),element.valueOffset(),element.valueLength(),value,0,value.length);
    }

    @Override
    public boolean matchingRowKeyValue(DataCell element,DataCell other){
        return Bytes.equals(element.valueArray(),element.valueOffset(),element.valueLength(),
                other.valueArray(),other.valueOffset(),other.valueLength());
    }

    @Override
    public DataCell newValue(DataCell element,byte[] value){
        return element.copyValue(value);
    }

    @Override
    public Comparator getComparator(){
        return ComparableComparator.newComparator();
    }

    @Override
    public long getTimestamp(DataCell element){
        return element.version();
    }

    @Override
    public String getFamilyAsString(DataCell element){
        return Bytes.toString(element.family());
    }

    @Override
    public String getQualifierAsString(DataCell element){
        return Bytes.toString(element.qualifier());
    }

    @Override
    public void setRowInSlice(DataCell element,ByteSlice slice){
        slice.set(element.keyArray(),element.keyOffset(),element.keyLength());
    }

    @Override
    public boolean isFailedCommitTimestamp(DataCell element){
        return element.valueLength()==1 && element.valueArray()[element.valueOffset()]==SIConstants.SNAPSHOT_ISOLATION_FAILED_TIMESTAMP[0];
    }

    @Override
    public DataCell newTransactionTimeStampKeyValue(DataCell element,
                                                    byte[] value){
        return new MCell(element.keyArray(),element.keyOffset(),element.keyLength(),
                SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES,
                element.version(),value,0,value==null?0:value.length,CellType.COMMIT_TIMESTAMP);
    }

    @Override
    public long getValueLength(DataCell element){
        return element.valueLength();
    }

    @Override
    public long getValueToLong(DataCell element){
        return element.valueAsLong();
    }

    @Override
    public byte[] getDataValue(DataCell element){
        return element.value();
    }

    @Override
    public MResult newResult(List<DataCell> values){
        return new MResult(values);
    }

    @Override
    public DataCell[] getDataFromResult(MResult result){
        if(result.size()==0) return new DataCell[]{};
        DataCell[] dc = new DataCell[result.size()];
        Iterator<DataCell> dcIter = result.iterator();
        for(int i=0;i<dc.length;i++){
            if(!dcIter.hasNext()) throw new IllegalStateException("Programmer error: result.size() does not match iterator!");
            dc[i] = dcIter.next();
        }
        return dc;
    }

    @Override
    public byte[] getDataRow(DataCell element){
        return element.key();
    }

    @Override
    public DataCell getColumnLatest(MResult result,byte[] family,
                                    byte[] qualifier){
        return result.latestCell(family,qualifier);
    }

    @Override
    public byte[] getDataValueBuffer(DataCell element){
        return element.valueArray();
    }

    @Override
    public int getDataValueOffset(DataCell element){
        return element.valueOffset();
    }

    @Override
    public int getDataValuelength(DataCell element){
        return element.valueLength();
    }

    @Override
    public int getLength(DataCell element){
        return element.keyLength(); //TODO -sf- is this right?
    }

    @Override
    public byte[] getDataRowBuffer(DataCell element){
        return element.keyArray();
    }

    @Override
    public int getDataRowOffset(DataCell element){
        return element.keyOffset();
    }

    @Override
    public int getDataRowlength(DataCell element){
        return element.keyLength();
    }

    @Override
    public Void getActiveTransactionFilter(long beforeTs,long afterTs,
                                             byte[] destinationTable){
        throw new RuntimeException("Not Implemented");
    }

    private static boolean matchingColumn(DataCell c,byte[] family,byte[] qualifier){
        return c.matchesQualifier(family,qualifier);
    }

    @Override
    public DataCell matchKeyValue(DataCell[] kvs,byte[] columnFamily,
                                  byte[] qualifier){
        for(DataCell kv : kvs){
            if(matchingColumn(kv,columnFamily,qualifier))
                return kv;
        }
        return null;
    }

    @Override
    public DataCell matchDataColumn(DataCell[] kvs){
        for(DataCell dc : kvs){
            if(dc.dataType()==CellType.USER_DATA) return dc;
        }
        return null;
    }

    @Override
    public DataPut newDataPut(ByteSlice key){
        return new MPut(key);
    }

    @Override
    public DataPut toDataPut(KVPair kvPair,byte[] family,byte[] column,long timestamp){
        ByteSlice rowKey=kvPair.rowKeySlice();
        ByteSlice val=kvPair.valueSlice();
        MPut put=new MPut(rowKey);
        DataCell kv=new MCell(rowKey.array(),rowKey.offset(),rowKey.length(),
                family,
                column,
                timestamp,
                val.array(),val.offset(),val.length(),CellType.USER_DATA);
        put.addCell(kv);
        return put;
    }

    @Override
    public DataDelete newDataDelete(byte[] key){
        return new MDelete(key);
    }

}