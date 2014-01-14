package com.splicemachine.derby.impl.sql.execute.deprecate;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import com.splicemachine.derby.impl.sql.execute.operations.HashCodeGenerator;
import com.splicemachine.derby.impl.sql.execute.operations.framework.AggregateFinisher;

import java.util.AbstractMap;
import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 * Created on: 9/13/13
 */
@Deprecated
public class ArrayHashBuffer<K,T extends ExecRow> implements HashBuffer<K,T>{
    private final Map.Entry<K,T>[] buffer;
    private final HashCodeGenerator<K> hasher;
    private int size;

    public ArrayHashBuffer(int capacity,HashCodeGenerator<K> hasher) {
        this.buffer = new Map.Entry[capacity];
        this.hasher = hasher;
    }

    @Override
    public Map.Entry<K, T> add(K key, T element) {
        int position = hash(key);
        Map.Entry<K,T> oldEntry = buffer[position];
        buffer[position] = new AbstractMap.SimpleEntry<K, T>(key,element);

        return oldEntry;
    }

    private int hash(K key) {
        //stolen from java.util.HashCode as a way to improve hash spreads
        int h = hasher.hash(key);

        h ^= (h>>>20) ^ ( 2>>>12);
        return h ^ (h>>> 7 ) ^ (h>>>4);
    }

    @Override
    public boolean merge(K key, T element, HashMerger<K, T> merger) {
        T matchedRow = merger.shouldMerge(this,key);
        if(matchedRow==null){
            return false;
        }
        merger.merge(this,matchedRow,element);
        return true;
    }

    @Override
    public HashBuffer<K, T> finishAggregates(AggregateFinisher<K, T> aggregateFinisher) throws StandardException {
        ArrayHashBuffer<K,T> finalizedBuffer = new ArrayHashBuffer<K, T>(buffer.length,hasher);
        for(int i=0;i<buffer.length;i++){
            Map.Entry<K,T> value = buffer[i];
            finalizedBuffer.buffer[i] = new AbstractMap.SimpleEntry<K, T>(value.getKey(),aggregateFinisher.finishAggregation(value.getValue()));
            buffer[i] = null;
        }
        return finalizedBuffer;
    }

    @Override
    public T get(K key) {
        int position = hasher.hash(key);
        return buffer[position].getValue();
    }

    @Override
    public int size() {
        return size;
    }

    @Override
    public Set<K> keySet() {

        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public T remove(K key) {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isEmpty() {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Collection<T> values() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public Set<Map.Entry<K, T>> entrySet() {
        return null;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public void clear() {
        //To change body of implemented methods use File | Settings | File Templates.
    }
}
