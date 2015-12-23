package com.splicemacine.pipeline;

import com.splicemachine.pipeline.api.PipelineTooBusy;
import org.apache.hadoop.hbase.RegionTooBusyException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class HTooBusy extends RegionTooBusyException implements PipelineTooBusy{
}
