package com.splicemachine.derby.impl.stats;

import com.google.common.base.Function;
import com.google.common.collect.Iterators;

import java.util.AbstractSet;
import java.util.Iterator;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 2/27/15
 */
public class ConvertingSetView<T,E> extends AbstractSet<E> {
    private final Set<? extends T> baseValue;
    private final Function<T,E> conversionFunction;

    public ConvertingSetView(Set<? extends T> baseValue, Function<T, E> conversionFunction) {
        this.baseValue = baseValue;
        this.conversionFunction = conversionFunction;
    }

    @Override
    public Iterator<E> iterator() {
        return Iterators.transform(baseValue.iterator(),conversionFunction);
    }

    @Override public int size() { return baseValue.size(); }
}
