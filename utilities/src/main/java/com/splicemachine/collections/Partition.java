/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

import java.util.*;

/**
 * @author P Trolard
 *         Date: 25/10/2013
 */
public class Partition {

    private static int needsPadding(int seqLength, int partitionSize, int stepSize){
        return (seqLength % stepSize) + (partitionSize - stepSize);
    }

    /**
     * Partition coll into sub-collection of given size, advancing by given number of steps
     * in coll between partitions and optionally padding any empty space with null. (Specifying
     * a step size smaller than the partition size allows overlapping partitions.)
     */
    public static <T> Iterable<List<T>> partition(Collection<T> coll,
                                                  final int size,
                                                  final int steps,
                                                  final boolean pad){
        final List<T> c = new ArrayList<T>(coll);
        final int collSize = c.size();

        int padding = Math.max(needsPadding(collSize, size, steps), 0);
        if (pad && padding > 0) {
            for (int i = 0; i < padding; i++){
                c.add(null);
            }
        }

        return new Iterable<List<T>>() {
            @Override
            public Iterator<List<T>> iterator() {
                return new Iterator<List<T>>() {
                    private int cursor = 0;
                    @Override
                    public boolean hasNext() {
                        return cursor < collSize;
                    }

                    @Override
                    public List<T> next() {
                        if (!hasNext()){
                            throw new NoSuchElementException();
                        }
                        List<T> partition = c.subList(cursor, cursor + size < c.size() ?
                                                                cursor + size : c.size());
                        cursor = cursor + steps;
                        return Collections.unmodifiableList(partition);
                    }

                    @Override
                    public void remove() {
                        throw new UnsupportedOperationException();
                    }
                };
            }
        };
    }

    public static <T> Iterable<List<T>> partition(List<T> coll, int size, int steps){
        return partition(coll, size, steps, false);
    }

    public static <T> Iterable<List<T>> partition(List<T> coll, int size){
        return partition(coll, size, size, false);
    }

    public static <T> Iterable<List<T>> partition(List<T> coll, int size, boolean pad){
        return partition(coll, size, size, pad);
    }
}
