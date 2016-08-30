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

package com.splicemachine.pipeline;

import akka.event.NoLogging$;
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
