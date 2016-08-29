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

import org.spark_project.guava.collect.Iterables;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import java.io.IOException;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class HDelete implements DataDelete,HMutation{
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
    public DataDelete deleteColumn(byte[] family,byte[] qualifier,long version){
        delete.deleteColumn(family,qualifier,version);
        return this;
    }

    @Override
    public void addAttribute(String key,byte[] value){
        delete.setAttribute(key,value);
    }

    @Override
    public byte[] getAttribute(String key){
        return delete.getAttribute(key);
    }

    @Override
    public byte[] key(){
        return delete.getRow();
    }

    @Override
    public Iterable<DataCell> cells(){
        return new CellIterable(Iterables.concat(delete.getFamilyCellMap().values()));
    }

    @Override
    public Map<String, byte[]> allAttributes(){
        return delete.getAttributesMap();
    }

    @Override
    public void setAllAttributes(Map<String, byte[]> attrMap){
        for(Map.Entry<String,byte[]> me:attrMap.entrySet()){
            delete.setAttribute(me.getKey(),me.getValue());
        }
    }

    @Override
    public Mutation unwrapHbaseMutation(){
        return delete;
    }

    public Delete unwrapDelegate(){
       return delete;
    }
}
