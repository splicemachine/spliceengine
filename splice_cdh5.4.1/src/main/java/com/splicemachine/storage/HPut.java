package com.splicemachine.storage;

import com.google.common.collect.Iterables;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class HPut implements DataPut{
    private Put put;

    public HPut(byte[] rowKey){
        this.put = new Put(rowKey);
    }

    public HPut(ByteSlice key){
        this.put = new Put(key.array(),key.offset(),key.length());
    }

    public HPut(Put put){
        this.put = put;
    }

    @Override
    public void tombstone(long txnIdLong){
        put.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                txnIdLong,SIConstants.EMPTY_BYTE_ARRAY);
    }

    @Override
    public void antiTombstone(long txnIdLong){
        put.addColumn(SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                txnIdLong,
                SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES);
    }

    @Override
    public void addCell(byte[] family,byte[] qualifier,long timestamp,byte[] value){
        put.addColumn(family,qualifier,timestamp,value);
    }

    @Override
    public byte[] key(){
        return put.getRow();
    }

    @Override
    public Iterable<DataCell> cells(){
        return new CellIterable(Iterables.concat(put.getFamilyCellMap().values()));
    }

    @Override
    public void addCell(DataCell kv){
        assert kv instanceof HCell: "Improper type for cell!";
        try{
            put.add(((HCell)kv).unwrapDelegate());
        }catch(IOException e){
            throw new RuntimeException(e); //should never happen
        }
    }

    @Override
    public void addAttribute(String key,byte[] value){
        put.setAttribute(key,value);
    }

    @Override
    public byte[] getAttribute(String key){
        return put.getAttribute(key);
    }

    public Put unwrapDelegate(){
        return put;
    }
}
