package com.splicemachine.si.impl;

import com.splicemachine.si.api.RollForward;
import org.apache.hadoop.hbase.regionserver.HRegion;

/**
 * Builds RollForward instances for a specified region.
 *
 * @author Scott Fines
 * Date: 6/27/14
 */
public class RollForwardFactory {

		public RollForwardFactory() {
		}

		public RollForward newRollForward(HRegion region){
				throw new UnsupportedOperationException("IMPLEMENT!");
		}
}
