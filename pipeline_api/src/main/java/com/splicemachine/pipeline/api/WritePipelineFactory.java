package com.splicemachine.pipeline.api;

import com.splicemachine.pipeline.PartitionWritePipeline;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public interface WritePipelineFactory{

    PartitionWritePipeline getPipeline(String partitionName);
}
