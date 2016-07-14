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
