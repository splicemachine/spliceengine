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
package com.splicemachine.db.impl.sql.catalog;


import com.google.common.cache.LoadingCache;
import org.spark_project.guava.cache.CacheBuilder;
import org.spark_project.guava.cache.CacheLoader;

import java.beans.ConstructorProperties;

public class ManagedCache<K, V> implements ManagedCacheMBean, GenericManagedCacheIFace<K, V>{

    private final org.spark_project.guava.cache.Cache<K,V> managedCache;


    @ConstructorProperties({"managedCache"})
    public ManagedCache(org.spark_project.guava.cache.Cache<K, V> managedCache){
        this.managedCache = managedCache;
    }
    @Override public long getSize(){ return managedCache.size(); }
    @Override public long getHitCount(){ return managedCache.stats().hitCount(); }
    @Override public long getMissCount(){ return managedCache.stats().missCount(); }
    @Override public double getHitRate(){ return managedCache.stats().hitRate(); }
    @Override public double getMissRate(){ return managedCache.stats().missRate(); }
    @Override public long getRequestCount(){ return managedCache.stats().requestCount(); }
    @Override public void invalidateAll(){ managedCache.invalidateAll(); }
    @Override public void put(K var1, V var2){ managedCache.put(var1, var2); }
    @Override public V getIfPresent(K k) { return managedCache.getIfPresent(k);}
    @Override public void invalidate(K k) { managedCache.invalidate(k);}

}
