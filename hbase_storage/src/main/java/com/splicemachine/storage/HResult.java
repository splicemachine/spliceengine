package com.splicemachine.storage;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.google.common.collect.Iterators;
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
        Cell columnLatestCell=result.getColumnLatestCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_COMMIT_TIMESTAMP_COLUMN_BYTES);
        if(columnLatestCell==null) return null;
        wrapper.set(columnLatestCell);
        return wrapper;
    }

    @Override
    public DataCell tombstone(){
        if(result==null) return null;
        Cell columnLatestCell=result.getColumnLatestCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_TOMBSTONE_COLUMN_BYTES);
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
        Cell columnLatestCell=result.getColumnLatestCell(SIConstants.DEFAULT_FAMILY_BYTES,SIConstants.SNAPSHOT_ISOLATION_FK_COUNTER_COLUMN_BYTES);
        if(columnLatestCell==null) return null;
        wrapper.set(columnLatestCell);
        return wrapper;
    }

    @Override
    public int size(){
        if(result==null) return 0;
        return result.size();
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
