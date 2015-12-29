package com.splicemachine.pipeline;

import com.splicemachine.access.api.WrongPartitionException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class HWrongRegion extends WrongRegionException implements WrongPartitionException{
}
