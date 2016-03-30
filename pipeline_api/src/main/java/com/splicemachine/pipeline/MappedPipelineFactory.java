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

    public PartitionWritePipeline registerPipeline(String name,PartitionWritePipeline pipelineWriter){
        if(LOG.isDebugEnabled()){
            LOG.debug("Registering pipeline for region "+ name);
        }
        PartitionWritePipeline partitionWritePipeline=map.putIfAbsent(name,pipelineWriter);
        if(LOG.isDebugEnabled() && partitionWritePipeline!=null){
            LOG.debug("Region "+ name+" was already registered");
        }
        return partitionWritePipeline;
    }

    public void deregisterPipeline(String name){
        if(LOG.isDebugEnabled())
            LOG.debug("De-registering region "+ name);
        map.remove(name);
    }
}
