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

package com.splicemachine.pipeline;

import com.splicemachine.pipeline.api.WritePipelineFactory;
import org.apache.log4j.Logger;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MappedPipelineFactory implements WritePipelineFactory{
    private static final Logger LOG=Logger.getLogger(MappedPipelineFactory.class);
    private final ConcurrentMap<String,PartitionWritePipeline> map = new ConcurrentHashMap<>();

    @Override
    public PartitionWritePipeline getPipeline(String partitionName){
        return map.get(partitionName);
    }

    @Override
    public void registerPipeline(String name,PartitionWritePipeline pipelineWriter){
        if(LOG.isDebugEnabled()){
            LOG.debug("Registering pipeline for region "+ name);
        }
        PartitionWritePipeline partitionWritePipeline=map.putIfAbsent(name,pipelineWriter);
        if(LOG.isDebugEnabled() && partitionWritePipeline!=null){
            LOG.debug("Region "+ name+" was already registered");
        }
    }

    @Override
    public void deregisterPipeline(String name){
        if(LOG.isDebugEnabled())
            LOG.debug("De-registering region "+ name);
        map.remove(name);
    }
}
