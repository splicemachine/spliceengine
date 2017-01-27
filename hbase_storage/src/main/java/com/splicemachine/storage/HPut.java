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

package com.splicemachine.storage;

import org.spark_project.guava.collect.Iterables;
import com.splicemachine.si.constants.SIConstants;
import com.splicemachine.utils.ByteSlice;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.client.Put;
import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class HPut implements HMutation,DataPut{
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
        put.add(SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                txnIdLong,SIConstants.EMPTY_BYTE_ARRAY);
    }

    @Override
    public void antiTombstone(long txnIdLong){
        put.add(SIConstants.DEFAULT_FAMILY_BYTES,
                SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES,
                txnIdLong,
                SIConstants.SNAPSHOT_ISOLATION_ANTI_TOMBSTONE_VALUE_BYTES);
    }

    @Override
    public void addCell(byte[] family,byte[] qualifier,long timestamp,byte[] value){
        put.add(family,qualifier,timestamp,value);
    }

    @Override
    public void addCell(byte[] family,byte[] qualifier,byte[] value){
        put.add(family,qualifier,value);
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

    @Override
    public Map<String, byte[]> allAttributes(){
        return put.getAttributesMap();
    }

    @Override
    public void setAllAttributes(Map<String, byte[]> attrMap){
        for(Map.Entry<String,byte[]> me:attrMap.entrySet()){
            put.setAttribute(me.getKey(),me.getValue());
        }
    }

    public Put unwrapDelegate(){
        return put;
    }

    @Override
    public Mutation unwrapHbaseMutation(){
        return put;
    }
}
