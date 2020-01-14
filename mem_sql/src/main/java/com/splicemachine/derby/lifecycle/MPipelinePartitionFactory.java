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

package com.splicemachine.derby.lifecycle;

import com.splicemachine.access.api.PartitionAdmin;
import com.splicemachine.access.api.PartitionFactory;
import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.concurrent.Clock;
import com.splicemachine.storage.Partition;
import com.splicemachine.storage.PartitionInfoCache;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public class MPipelinePartitionFactory implements PartitionFactory<Object>{
    private final PartitionFactory<Object> baseFactory;

    public MPipelinePartitionFactory(PartitionFactory<Object> baseFactory){
        this.baseFactory=baseFactory;
    }

    public void initialize(Clock clock,SConfiguration configuration,PartitionInfoCache partitionInfoCache) throws IOException{
        baseFactory.initialize(clock,configuration,partitionInfoCache);
    }

    public Partition getTable(String name) throws IOException{
        return baseFactory.getTable(name);
    }

    public PartitionAdmin getAdmin() throws IOException{
        return new MEnginePartitionAdmin(baseFactory.getAdmin());
    }

    public Partition getTable(Object tableName) throws IOException{
        return baseFactory.getTable(tableName);
    }

    public Partition getTable(byte[] name) throws IOException{
        return baseFactory.getTable(name);
    }
}
