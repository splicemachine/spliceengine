package com.splicemachine.derby.impl.sql.execute.deprecate;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.sql.execute.ExecRow;

import com.splicemachine.derby.impl.sql.execute.operations.framework.AggregateFinisher;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

/**
 * @author Scott Fines
 *         Created on: 9/13/13
 */
@Deprecated
public interface HashBuffer<K, T extends ExecRow> {
    @SuppressWarnings("LoopStatementThatDoesntLoop")
    Map.Entry<K,T> add(K key, T element);
    boolean merge(K key, T element, HashMerger<K, T> merger);
    HashBuffer<K, T> finishAggregates(AggregateFinisher<K, T> aggregateFinisher) throws StandardException;
    @Override
    String toString();

    T get(K key);

    int size();

    Set<K> keySet();

    T remove(K key);

    boolean isEmpty();

    Collection<T> values();

    Set<Map.Entry<K,T>> entrySet();

    void clear();
}
