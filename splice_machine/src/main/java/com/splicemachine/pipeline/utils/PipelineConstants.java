package com.splicemachine.pipeline.utils;

import java.lang.reflect.Method;
import java.util.Comparator;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;

import com.carrotsearch.hppc.ObjectArrayList;
import com.google.common.base.Predicate;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.utils.SpliceLogUtils;

public class PipelineConstants extends SIConstants {
    private static final Logger LOG = Logger.getLogger(PipelineConstants.class);
	public static final String WRITE_COORDINATOR_OBJECT_LOCATION = "com.splicemachine.writer:type=WriteCoordinatorStatus";
	public static final String WRITER_STATUS_OBJECT_LOCATION = "com.splicemachine.writer.async:type=WriterStatus";
	public static final String THREAD_POOL_STATUS_LOCATION = "com.splicemachine.writer.async:type=ThreadPoolStatus";
	public static final SnappyCodec snappy;
	
	
    public static boolean supportsNative;

	static {
		snappy = new org.apache.hadoop.io.compress.SnappyCodec();
		snappy.setConf(SpliceConstants.config);
		Method method;
		try{
    		// Cloudera Path
			method = SnappyCodec.class.getMethod("isNativeCodeLoaded", null);
    		supportsNative = (Boolean) method.invoke(snappy, null);
    	} catch (Exception e) {
    		SpliceLogUtils.error(LOG, "basic snappy codec not supported, checking alternative method signature");
				try {
					method = SnappyCodec.class.getMethod("isNativeSnappyLoaded", Configuration.class);
					supportsNative = (Boolean) method.invoke(snappy, SpliceConstants.config);
				} catch (Exception ioe) {
					SpliceLogUtils.error(LOG, "Alternative signature did not work, No Snappy Codec Support",ioe);
					supportsNative = false;
				}
		}
    	if (!supportsNative)
			SpliceLogUtils.error(LOG, "No Native Snappy Installed: Splice Machine's Write Pipeline will not compress data over the wire.");
    	else
			SpliceLogUtils.info(LOG, "Snappy Installed: Splice Machine's Write Pipeline will compress data over the wire.");
    	supportsNative = false;
    }
    
	public static Comparator<BulkWrite> writeComparator = new Comparator<BulkWrite>() {
        @Override
        public int compare(BulkWrite o1, BulkWrite o2) {
            if(o1==null) {
                if(o2==null) return 1;
                else return -1;
            }else if(o2==null)
                return 1;

            else return Bytes.compareTo(o1.getRegionKey(),o2.getRegionKey());
        }
    };
    
    public static PreFlushHook noOpFlushHook = new PreFlushHook() {
        @Override
        public ObjectArrayList<KVPair> transform(ObjectArrayList<KVPair> buffer) throws Exception {
            return buffer.clone();
        }
    };

    
	public static final Predicate<BulkWrite> nonEmptyPredicate = new Predicate<BulkWrite>() {
		@Override
		public boolean apply(@Nullable BulkWrite input) {
				return input!=null && input.getSize()>0;
		}
	};

	public static final ObjectArrayList<KVPair> emptyList = new ObjectArrayList<KVPair>();

		
}
