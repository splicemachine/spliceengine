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

package com.splicemachine.si.data.hbase.coprocessor;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.storage.PartitionInfoCache;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/29/15
 */
public class PartitionCacheService{
    private static volatile PartitionInfoCache INSTANCE;

    public static PartitionInfoCache loadPartitionCache(SConfiguration configuration){
        PartitionInfoCache cache = INSTANCE;
        if(cache==null){
            synchronized(PartitionCacheService.class){
                cache = INSTANCE;
                if(cache==null){
                    cache = INSTANCE = loadCache(configuration);
                }
            }
        }
        return cache;
    }

    private static PartitionInfoCache loadCache(SConfiguration config) {
        ServiceLoader<PartitionInfoCache> serviceLoader = ServiceLoader.load(PartitionInfoCache.class);
        Iterator<PartitionInfoCache> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No PartitionCache found!");

        PartitionInfoCache next=iter.next();
        next.configure(config);
        return next;
    }
}
