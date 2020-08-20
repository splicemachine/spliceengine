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

package com.splicemachine.collections;

import com.google.common.base.Function;
import com.google.common.collect.Collections2;
import com.google.common.collect.Iterators;
import java.util.*;

/**
 * A Navigable Set which casts to and from two types which are the same (usually, T is a subtype of V), but which
 * is prevented from pretty access because of Java Generics Rules.
 *
 * @author Scott Fines
 *         Date: 5/11/15
 */
public class NavigableCastingSet<T,V> implements NavigableSet<V>{
    private final NavigableSet<T> kvset;
    private final Comparator<? super V> castingComparator;
    private final Function<T,V> castFunction = new Function<T, V>(){
        @Override public V apply(T input){ return (V)input; }
    };
    private final Function<V,T> reverseCastFunction = new Function<V, T>(){
        @Override public T apply(V input){ return (T)input; }
    };

    public static <T,V> NavigableCastingSet<T,V> create(NavigableSet<T> kvSet,final Comparator<T> baseComparator){
       return new NavigableCastingSet<>(kvSet,new Comparator<V>(){
            @Override
            public int compare(V o1,V o2){
                return baseComparator.compare((T)o1,(T)o2);
            }
        });
    }

    public NavigableCastingSet(NavigableSet<T> kvset,final Comparator<? super V> comparator){
        this.kvset=kvset;
        this.castingComparator =comparator;
    }

    @Override
    public V lower(V cell){
//        assert cell instanceof T: "Programmer error: incorrect type!";
        return (V)kvset.lower((T)cell);
    }

    @Override
    public V floor(V cell){
//        assert cell instanceof T: "Programmer error: incorrect type!";
        return (V)kvset.floor((T)cell);
    }

    @Override
    public V ceiling(V cell){
//        assert cell instanceof T: "Programmer error: incorrect type!";
        return (V)kvset.ceiling((T)cell);
    }

    @Override
    public V higher(V cell){
//        assert cell instanceof T: "Programmer error: incorrect type!";
        return (V)kvset.higher((T)cell);
    }

    @Override public V pollFirst(){ return (V)kvset.pollFirst(); }
    @Override public V pollLast(){ return (V)kvset.pollLast(); }
    @Override public NavigableSet<V> descendingSet(){ return new NavigableCastingSet<>(kvset.descendingSet(),castingComparator); }

    @Override
    public Iterator<V> descendingIterator(){
        return Iterators.transform(kvset.descendingIterator(),castFunction);
    }

    @Override
    public NavigableSet<V> subSet(V fromElement,boolean fromInclusive,V toElement,boolean toInclusive){
//        assert fromElement instanceof T: "Programmer error: incorrect type for fromElement!";
//        assert toElement instanceof T: "Programmer error: incorrect type for toElement!";
        return new NavigableCastingSet<>(kvset.subSet((T)fromElement,fromInclusive,(T)toElement,toInclusive),castingComparator);
    }

    @Override
    public NavigableSet<V> headSet(V toElement,boolean inclusive){
//        assert toElement instanceof T: "Programmer error: incorrect type for toElement!";
        return new NavigableCastingSet<>(kvset.headSet((T)toElement,inclusive),castingComparator);
    }

    @Override
    public NavigableSet<V> tailSet(V fromElement,boolean inclusive){
//        assert fromElement instanceof T: "Programmer error: incorrect type for fromElement!";
        return new NavigableCastingSet<>(kvset.tailSet((T)fromElement,inclusive),castingComparator);
    }

    @Override public Iterator<V> iterator(){ return Iterators.transform(kvset.iterator(),castFunction); }

    @Override
    public SortedSet<V> subSet(V fromElement,V toElement){
        return subSet(fromElement,true,toElement,false);
    }

    @Override public SortedSet<V> headSet(V toElement){ return headSet(toElement,false); }
    @Override public SortedSet<V> tailSet(V fromElement){ return tailSet(fromElement,true); }
    @Override public Comparator<? super V> comparator(){ return castingComparator; }
    @Override public V first(){ return (V)kvset.first(); }
    @Override public V last(){ return (V)kvset.last(); }
    @Override public int size(){ return kvset.size(); }
    @Override public boolean isEmpty(){ return kvset.isEmpty(); }
    @Override public boolean contains(Object o){ return kvset.contains(o); }
    @Override public Object[] toArray(){ return kvset.toArray(); }
    @Override public <T> T[] toArray(T[] a){ return kvset.toArray(a); }

    @Override
    public boolean add(V cell){
//        assert cell instanceof T: "Programmer error: incorrect type!";
        return kvset.add((T)cell);
    }

    @Override public boolean remove(Object o){ return kvset.remove(o); }
    @Override public boolean containsAll(Collection<?> c){ return kvset.containsAll(c); }

    @Override
    public boolean addAll(Collection<? extends V> c){
        return kvset.addAll(Collections2.transform(c,reverseCastFunction));
    }

    @Override public boolean retainAll(Collection<?> c){ return kvset.retainAll(c); }
    @Override public boolean removeAll(Collection<?> c){ return kvset.removeAll(c); }
    @Override public void clear(){ kvset.clear(); }
}
