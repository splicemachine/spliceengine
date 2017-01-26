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

package com.splicemachine.tools;

import org.spark_project.guava.base.Preconditions;
import org.spark_project.guava.cache.*;

import java.util.concurrent.Callable;
import java.util.concurrent.TimeUnit;

/**
 * @author Scott Fines
 *         Created on: 5/28/13
 */
public class CachedResourcePool<E,K extends ResourcePool.Key> implements ResourcePool<E,K> {
    private final Generator<E,K> generator;

    private final Cache<K,E> cache;

    public CachedResourcePool(Generator<E, K> generator, Cache<K, E> cache) {
        this.generator = generator;
        this.cache = cache;
    }

    @Override
    public E get(final K key) throws Exception {
        return cache.get(key, new Callable<E>() {
            @Override
            public E call() throws Exception {
                return generator.makeNew(key);
            }
        });
    }

    @Override
    public void release(K key) throws Exception {
        //no-op, rely on timeout to remove it
    }

    public static class Builder<E,K extends Key> {
        private CacheBuilder<K,E> cacheBuilder;
        private Generator<E,K> generator;

        private Builder(CacheBuilder cacheBuilder) {
            this.cacheBuilder = cacheBuilder.removalListener(new RemovalListener<K, E>() {
                @Override
                public void onRemoval(RemovalNotification<K, E> notification) {
                    try {
                        generator.close(notification.getValue());
                    } catch (Exception e) {
                        throw new RuntimeException(e);
                    }
                }
            });
        }

        public static <E, K extends Key> Builder<E,K> newBuilder(){
            CacheBuilder builder = CacheBuilder.newBuilder();
           return new Builder<E,K>(builder);
        }

        public Builder<E,K> generator(Generator<E, K> generator){
            this.generator = generator;
            return this;
        }

        public Builder<E,K> expireAfterAccess(long time, TimeUnit unit){
            cacheBuilder = cacheBuilder.expireAfterAccess(time,unit);
            return this;
        }

        public CachedResourcePool<E,K> build(){
            Preconditions.checkNotNull(generator,"No Generator specified!");
            return new CachedResourcePool<E, K>(generator,cacheBuilder.build(new CacheLoader<K, E>() {
                @Override
                public E load(K key) throws Exception {
                    return generator.makeNew(key);
                }
            }));
        }
    }
}
