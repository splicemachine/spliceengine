/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

import splice.com.google.common.base.Function;
import splice.com.google.common.collect.Iterables;
import splice.com.google.common.collect.Iterators;
import com.splicemachine.si.constants.SIConstants;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.client.Result;
import java.util.Collections;
import java.util.Iterator;
import java.util.Map;

/**
 * @author Scott Fines
 *         Date: 12/15/15
 */
public class HResult implements DataResult{
    private Result result;

    private HCell wrapper = new HCell();
    private Function<? super Cell,? extends DataCell> transform = new Function<Cell, DataCell>(){
        @Override
        public DataCell apply(Cell input){
            wrapper.set(input);
            return wrapper;
        }
    };

    public HResult(){ }

    public HResult(Result result){
        this.result=result;
    }

    public void set(Result result){
        this.result = result;
    }

    @Override
    public DataCell commitTimestamp(){
        if(result==null) return null;
        Cell columnLatestCell=result.getColumnLatestCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.COMMIT_TIMESTAMP_COLUMN_BYTES);
        if(columnLatestCell==null) return null;
        wrapper.set(columnLatestCell);
        return wrapper;
    }

    @Override
    public DataCell tombstoneOrAntiTombstone(){
        if(result==null) return null;
        Cell columnLatestCell=result.getColumnLatestCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.TOMBSTONE_COLUMN_BYTES);
        if(columnLatestCell==null) return null;
        wrapper.set(columnLatestCell);
        return wrapper;
    }

    @Override
    public DataCell userData(){
        if(result==null) return null;
        Cell columnLatestCell=result.getColumnLatestCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.PACKED_COLUMN_BYTES);
        if(columnLatestCell==null) return null;
        wrapper.set(columnLatestCell);
        return wrapper;
    }

    @Override
    public DataCell fkCounter(){
        if(result==null) return null;
        Cell columnLatestCell=result.getColumnLatestCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.FK_COUNTER_COLUMN_BYTES);
        if(columnLatestCell==null) return null;
        wrapper.set(columnLatestCell);
        return wrapper;
    }

    @Override
    public DataCell firstOccurrenceToken() {
        if(result==null) return null;
        Cell columnLatestCell=result.getColumnLatestCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.FIRST_OCCURRENCE_TOKEN_COLUMN_BYTES);
        if(columnLatestCell==null) return null;
        wrapper.set(columnLatestCell);
        return wrapper;
    }

    @Override
    public DataCell firstWriteToken() {
        DataCell cell = firstOccurrenceToken();
        if (cell == null)
            return null;
        if (cell.dataType() == CellType.FIRST_WRITE_TOKEN)
            return cell;
        return null;
    }

    @Override
    public int size(){
        if(result==null) return 0;
        return result.size();
    }

    @Override
    public boolean isEmpty() {
        return result == null || result.isEmpty();
    }

    @Override
    public DataCell latestCell(byte[] family,byte[] qualifier){
        if(result==null) return null;
        Cell columnLatestCell=result.getColumnLatestCell(family,qualifier);
        if(columnLatestCell==null) return null;
        wrapper.set(columnLatestCell);
        return wrapper;
    }

    @Override
    public Iterator<DataCell> iterator(){
        if(result==null||result.isEmpty()) return Collections.emptyIterator();
        return Iterators.transform(result.listCells().iterator(),transform);
    }

    @Override
    public Iterable<DataCell> columnCells(byte[] family,byte[] qualifier){
        return Iterables.transform(result.getColumnCells(family,qualifier),transform);
    }

    @Override
    public byte[] key(){
        return result.getRow();
    }

    @Override
    public Map<byte[], byte[]> familyCellMap(byte[] userColumnFamily){
        return result.getFamilyMap(userColumnFamily);
    }

    @Override
    public DataResult getClone(){
        return new HResult(Result.create(result.rawCells(),result.getExists(),result.isStale()));
    }
}
