package com.splicemacine.pipeline;

import com.splicemachine.access.api.NotServingPartitionException;
import org.apache.hadoop.hbase.NotServingRegionException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class HNotServingRegion extends NotServingRegionException implements NotServingPartitionException{
}
