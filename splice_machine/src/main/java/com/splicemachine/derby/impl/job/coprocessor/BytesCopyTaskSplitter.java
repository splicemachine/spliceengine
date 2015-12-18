package com.splicemachine.derby.impl.job.coprocessor;

import com.splicemachine.constants.SpliceConstants;
import org.apache.hadoop.hbase.regionserver.HRegion;
import org.apache.hadoop.hbase.regionserver.HRegionUtil;
import org.apache.hadoop.hbase.regionserver.Store;
import org.apache.hadoop.hbase.util.Bytes;
import java.io.IOException;
import java.util.*;

/**
 * @author Scott Fines
 *         Date: 4/16/14
 */
public class BytesCopyTaskSplitter {

		public static List<byte[]> getCutPoints(HRegion region, byte[] start, byte[] end) throws IOException {
			Store store = null;
			try {
				store = region.getStore(SpliceConstants.DEFAULT_FAMILY_BYTES);
				HRegionUtil.lockStore(store);
				return HRegionUtil.getCutpoints(store, start, end);				
			}finally{
				HRegionUtil.unlockStore(store);
			}
		}

}
