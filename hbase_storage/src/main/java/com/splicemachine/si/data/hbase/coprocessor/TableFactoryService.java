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

import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.si.impl.TxnNetworkLayerFactory;
import com.splicemachine.storage.PartitionInfoCache;
import org.apache.hadoop.hbase.TableName;

import java.io.IOException;
import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 12/22/15
 */
public class TableFactoryService{
    @SuppressWarnings("unchecked")
    public static PartitionFactory<TableName> loadTableFactory(Clock clock, SConfiguration configuration, PartitionInfoCache partitionCache) throws IOException{
        ServiceLoader<PartitionFactory> serviceLoader = ServiceLoader.load(PartitionFactory.class);
        Iterator<PartitionFactory> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No TableFactory found!");

        PartitionFactory stf = iter.next();
        stf.initialize(clock,configuration,partitionCache);
        return (PartitionFactory<TableName>)stf;
    }

    public static TxnNetworkLayerFactory loadTxnNetworkLayer(SConfiguration config) throws IOException{
        ServiceLoader<TxnNetworkLayerFactory> serviceLoader = ServiceLoader.load(TxnNetworkLayerFactory.class);
        Iterator<TxnNetworkLayerFactory> iter = serviceLoader.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No TableFactory found!");

        TxnNetworkLayerFactory next=iter.next();
        next.configure(config);
        return next;
    }
}
