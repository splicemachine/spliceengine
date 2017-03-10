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

package com.splicemachine.storage;

import org.apache.hadoop.hbase.client.Durability;
import org.sparkproject.guava.collect.Iterables;
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
    public void skipWAL() {
        put.setDurability(Durability.SKIP_WAL);
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
