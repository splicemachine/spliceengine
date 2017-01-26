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
