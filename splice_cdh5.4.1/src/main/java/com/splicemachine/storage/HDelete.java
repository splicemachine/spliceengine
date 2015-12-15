package com.splicemachine.storage;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class HDelete implements DataDelete{
    private final Delete delete;

    public HDelete(byte[] rowKey){
        this.delete = new Delete(rowKey);
    }

    @Override
    public void deleteColumn(DataCell dc){
        assert dc instanceof HCell: "Programmer error: attempting to delete a non-hbase cell";
        Cell c = ((HCell)dc).unwrapDelegate();
        try{
            delete.addDeleteMarker(c);
        }catch(IOException e){
            throw new RuntimeException(e);
        }
    }

    @Override
    public void deleteColumn(byte[] family,byte[] qualifier,long version){
        delete.addColumn(family,qualifier,version);
    }

    @Override
    public void addAttribute(String key,byte[] value){
        delete.setAttribute(key,value);
    }

    @Override
    public byte[] getAttribute(String key){
        return delete.getAttribute(key);
    }

    public Delete unwrapDelegate(){
       return delete;
    }
}
