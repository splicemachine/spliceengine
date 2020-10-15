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
package com.splicemachine.db.impl.sql.catalog;


import splice.com.google.common.cache.Cache;

import java.beans.ConstructorProperties;
import java.io.Serializable;
import java.util.List;

public class TotalManagedCache<K, V> implements ManagedCacheMBean, GenericManagedCacheIFace<K, V> {

    private final List<ManagedCache<K,V>> managedCache;

    @ConstructorProperties({"managedCache"})
    public TotalManagedCache(List<ManagedCache<K, V>> managedCache){
        this.managedCache = managedCache;
    }
    @Override public long getSize(){
        long size = 0;
        for(ManagedCache<K, V> mc : managedCache){
            size += mc.getSize();
        }
        return size;
    }
    @Override public long getHitCount(){
        long hitCount = 0;
        for(ManagedCache<K, V> mc : managedCache){
            hitCount += mc.getHitCount();
        }
        return hitCount;
    }
    @Override public long getMissCount(){
        long missCount = 0;
        for(ManagedCache<K, V> mc : managedCache){
            missCount += mc.getMissCount();
        }
        return missCount;
    }
    @Override public double getHitRate(){
        return getRequestCount()>0?((getHitCount()*1.0)/getRequestCount()):0;
    }
    @Override public double getMissRate(){
        return getRequestCount()>0?((getMissCount()*1.0)/getRequestCount()):0;
    }
    @Override public long getRequestCount(){
        long requestCount = 0;
        for(ManagedCache<K, V> mc : managedCache){
            requestCount += mc.getRequestCount();
        }
        return requestCount;
    }
    @Override public void invalidateAll(){
        for(ManagedCache<K, V> mc : managedCache){
            mc.invalidateAll();
        }
    }
    @Override public void put(K var1, V var2){
        throw new UnsupportedOperationException();
    }
    @Override public V getIfPresent(K k) {
        throw new UnsupportedOperationException();
    }
    @Override public void invalidate(K k) {
        throw new UnsupportedOperationException();
    }

}
