package com.splicemachine.pipeline.utils;

import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;

import javax.annotation.Nullable;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;

import com.carrotsearch.hppc.ObjectArrayList;
import com.splicemachine.constants.SIConstants;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.PreFlushHook;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.compress.SnappyCodec;
import org.apache.log4j.Logger;

import java.lang.reflect.Method;

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

		public static PreFlushHook noOpFlushHook = new PreFlushHook() {
				@Override
				public Collection<KVPair> transform(Collection<KVPair> buffer) throws Exception {
						return new ArrayList<>(buffer);
				}
		};


		public static final ObjectArrayList<KVPair> emptyList = new ObjectArrayList<KVPair>();


}
