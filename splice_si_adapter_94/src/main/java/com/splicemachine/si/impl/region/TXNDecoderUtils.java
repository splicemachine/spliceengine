package com.splicemachine.si.impl.region;

import org.apache.hadoop.hbase.KeyValue;
import com.splicemachine.si.api.Txn.IsolationLevel;
import com.splicemachine.si.api.Txn.State;
import com.splicemachine.si.data.api.SDataLib;
import com.splicemachine.si.impl.DenseTxn;
import com.splicemachine.si.impl.SIFactoryDriver;
import com.splicemachine.utils.ByteSlice;

public class TXNDecoderUtils {
	   public static final SDataLib dataLib = SIFactoryDriver.siFactory.getDataLib();

		public static DenseTxn composeValue(KeyValue destinationTables,
				IsolationLevel level, long txnId, long beginTs, long parentTs,
				boolean hasAdditive, boolean additive, long commitTs,
				long globalCommitTs, State state, long kaTime) {
		    ByteSlice destTableBuffer = null;
		    if(destinationTables!=null){
		        destTableBuffer = new ByteSlice();
		        destTableBuffer.set(dataLib.getDataValueBuffer(destinationTables),
		        		dataLib.getDataValueOffset(destinationTables),
		        		dataLib.getDataValuelength(destinationTables));
		    }
		    return new DenseTxn(txnId,beginTs,parentTs,
		            commitTs,globalCommitTs, hasAdditive,additive,level,state,destTableBuffer,kaTime);
		}
}
