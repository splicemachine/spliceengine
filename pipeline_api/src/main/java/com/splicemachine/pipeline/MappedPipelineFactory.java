package com.splicemachine.pipeline;

import com.splicemachine.pipeline.api.WritePipelineFactory;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class MappedPipelineFactory implements WritePipelineFactory{
    private final ConcurrentMap<String,PartitionWritePipeline> map = new ConcurrentHashMap<>();

    @Override
    public PartitionWritePipeline getPipeline(String partitionName){
        return map.get(partitionName);
    }

    public PartitionWritePipeline registerPipeline(String name,PartitionWritePipeline pipelineWriter){
        return map.putIfAbsent(name,pipelineWriter);
    }

    public void deregisterPipeline(String name){
        map.remove(name);
    }
}
