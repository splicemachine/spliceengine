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

import javax.annotation.Nonnull;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 1/19/16
 */
public class EmptyNavigableSet<E> extends AbstractSet<E> implements NavigableSet<E>{
    private static final EmptyNavigableSet INSTANCE = new EmptyNavigableSet();

    private EmptyNavigableSet(){}

    @SuppressWarnings("unchecked")
    public static <T> NavigableSet<T> instance(){
        return (NavigableSet<T>)INSTANCE;
    }

    @Override public E lower(E e){ return null; }
    @Override public E floor(E e){ return null; }
    @Override public E ceiling(E e){ return null; }
    @Override public E higher(E e){ return null; }
    @Override public E pollFirst(){ return null; }
    @Override public E pollLast(){ return null; }

    @Override
    public @Nonnull Iterator<E> iterator(){
        return Collections.emptyIterator();
    }

    @Override
    public @Nonnull NavigableSet<E> descendingSet(){
        return this;
    }

    @Override
    public @Nonnull Iterator<E> descendingIterator(){
        return iterator();
    }

    @Override
    public @Nonnull NavigableSet<E> subSet(E fromElement,boolean fromInclusive,E toElement,boolean toInclusive){
        return this;
    }

    @Override
    public @Nonnull NavigableSet<E> headSet(E toElement,boolean inclusive){
        return this;
    }

    @Override
    public @Nonnull NavigableSet<E> tailSet(E fromElement,boolean inclusive){
        return this;
    }

    @Override
    public Comparator<? super E> comparator(){
        return null;
    }

    @Override
    public @Nonnull SortedSet<E> subSet(E fromElement,E toElement){
        return this;
    }

    @Override
    public @Nonnull SortedSet<E> headSet(E toElement){
        return this;
    }

    @Override
    public @Nonnull SortedSet<E> tailSet(E fromElement){
        return this;
    }

    @Override
    public E first(){
        throw new NoSuchElementException();
    }

    @Override
    public E last(){
        throw new NoSuchElementException();
    }

    @Override public int size(){ return 0; }
}
