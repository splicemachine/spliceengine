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

import com.splicemachine.access.util.ByteComparisons;
import org.spark_project.guava.base.Predicate;
import org.spark_project.guava.collect.Iterables;
import com.splicemachine.primitives.Bytes;

import java.util.*;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public class MResult implements DataResult{
    private List<DataCell> dataCells;

    public MResult(){ }

    public MResult(List<DataCell> dataCells){
        set(dataCells);
    }

    public void set(List<DataCell> cells){
        this.dataCells = cells;
    }

    @Override
    public DataCell commitTimestamp(){
        if(dataCells==null) return null;
        for(DataCell mc:dataCells){
            if(mc.dataType()==CellType.COMMIT_TIMESTAMP) return mc;
        }
        return null;
    }

    @Override
    public DataCell tombstone(){
        if(dataCells==null) return null;
        for(DataCell mc:dataCells){
            if(mc.dataType()==CellType.TOMBSTONE||mc.dataType()==CellType.ANTI_TOMBSTONE) return mc;
        }
        return null;
    }

    @Override
    public DataCell userData(){
        if(dataCells==null) return null;
        for(DataCell mc:dataCells){
            if(mc.dataType()==CellType.USER_DATA) return mc;
        }
        return null;
    }

    @Override
    public DataCell fkCounter(){
        if(dataCells==null) return null;
        for(DataCell mc:dataCells){
            if(mc.dataType()==CellType.FOREIGN_KEY_COUNTER) return mc;
        }
        return null;
    }

    @Override
    public int size(){
        return dataCells==null? 0: dataCells.size();
    }

    @Override
    public DataCell latestCell(byte[] family,byte[] qualifier){
        if(dataCells==null) return null;
        for(DataCell dc:dataCells){
            if(dc.matchesQualifier(family,qualifier)) return dc;
        }
        return null;
    }

    @Override
    public Iterator<DataCell> iterator(){
        if(dataCells==null)return Collections.emptyIterator();
        return dataCells.iterator();
    }

    @Override
    public Iterable<DataCell> columnCells(final byte[] family,final byte[] qualifier){
        return Iterables.filter(this,new Predicate<DataCell>(){
            @Override
            public boolean apply(DataCell input){
                return input.matchesQualifier(family,qualifier);
            }
        });
    }

    @Override
    public byte[] key(){
        if(dataCells==null||dataCells.size()<=0) return null;
        return dataCells.get(0).key();
    }

    @Override
    public Map<byte[], byte[]> familyCellMap(byte[] userColumnFamily){
        if(dataCells==null||dataCells.size()<=0) return Collections.emptyMap();
        Map<byte[],byte[]> familyCellMap = new TreeMap<>(ByteComparisons.comparator());
        for(DataCell dc:dataCells){
            familyCellMap.put(dc.family(),dc.qualifier());
        }
        return familyCellMap;
    }

    @Override
    public DataResult getClone(){
        return new MResult(new ArrayList<>(dataCells));
    }
}
