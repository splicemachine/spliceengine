/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
