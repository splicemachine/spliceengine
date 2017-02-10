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

package com.splicemachine.concurrent;

import org.spark_project.guava.base.Supplier;
import org.spark_project.guava.primitives.Longs;
import java.util.concurrent.Semaphore;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReadWriteLock;
import java.util.concurrent.locks.ReentrantLock;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Represents a Lock Striping scheme similar to that of
 * ConcurrentHashMap. A concurrency item (semaphore, lock, etc)
 * is chosen by selecting the first {@code k} bits of the element's
 * hash code.
 *
 * @author Scott Fines
 * Date: 6/19/14
 */
public class LongStripedSynchronizer<T> {
    Object[] syncs;

    private LongStripedSynchronizer(int size,Supplier<T> supplier){
        syncs = new Object[size];
        for(int i=0;i<size;i++){
            syncs[i] = supplier.get();
        }
    }

    public static LongStripedSynchronizer<Lock> stripedLock(int stripes){
        int s = getNearestPowerOf2(stripes);
        return new LongStripedSynchronizer<Lock>(s,new Supplier<Lock>() {
            @Override
            public Lock get() {
                return new PaddedLock();
            }
        });
    }

    public static LongStripedSynchronizer<ReadWriteLock> stripedReadWriteLock(int stripes, final boolean fair){
        int s = getNearestPowerOf2(stripes);
        return new LongStripedSynchronizer<ReadWriteLock>(s,new Supplier<ReadWriteLock>() {
            @Override
            public ReadWriteLock get() {
                return new ReentrantReadWriteLock(fair);
            }
        });
    }

    public static LongStripedSynchronizer<Semaphore> stripedSemaphor(int stripes, final int permitSize){
        return new LongStripedSynchronizer<Semaphore>(getNearestPowerOf2(stripes),new Supplier<Semaphore>() {
            @Override
            public Semaphore get() {
                return new PaddedSemaphore(permitSize);
            }
        });
    }

    private static int getNearestPowerOf2(int stripes) {
        int s =1;
        while(s<stripes){
            s<<=1;
        }
        return s;
    }
    @SuppressWarnings("unchecked")
    public T get(long key){
        int elementPos = smear(Longs.hashCode(key)) & (syncs.length-1);
        return (T)syncs[elementPos]; //can supress because we fill the elements ourselves
    }

    /*
   * This method was written by Doug Lea with assistance from members of JCP
   * JSR-166 Expert Group and released to the public domain, as explained at
   * http://creativecommons.org/licenses/publicdomain
   *
   * As of 2010/06/11, this method is identical to the (package private) hash
   * method in OpenJDK 7's java.util.HashMap class.
   */
    // Copied from java/com/google/common/collect/Hashing.java
    private static int smear(int hashCode) {
        hashCode ^= (hashCode >>> 20) ^ (hashCode >>> 12);
        return hashCode ^ (hashCode >>> 7) ^ (hashCode >>> 4);
    }

    private static class PaddedLock extends ReentrantLock {
        /*
         * Padding from 40 into 64 bytes, same size as cache line. Might be beneficial to add
         * a fourth long here, to minimize chance of interference between consecutive locks,
         * but I couldn't observe any benefit from that.
         */
        @SuppressWarnings("unused")
        long q1, q2, q3;

        PaddedLock() {
            super(false);
        }
    }

    private static class PaddedSemaphore extends Semaphore {
        // See PaddedReentrantLock comment
        @SuppressWarnings("unused")
        long q1, q2, q3;

        PaddedSemaphore(int permits) {
            super(permits, false);
        }
    }
}