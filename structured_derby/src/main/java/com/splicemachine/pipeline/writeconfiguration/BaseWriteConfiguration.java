package com.splicemachine.pipeline.writeconfiguration;

import java.net.ConnectException;
import java.util.concurrent.ExecutionException;

import org.apache.hadoop.hbase.NotServingRegionException;
import org.apache.hadoop.hbase.RegionTooBusyException;
import org.apache.hadoop.hbase.regionserver.WrongRegionException;
import org.apache.log4j.Logger;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.ObjectObjectOpenHashMap;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.pipeline.exception.IndexNotSetUpException;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;

public abstract class BaseWriteConfiguration implements WriteConfiguration {
    private static final Logger LOG = Logger.getLogger(BaseWriteConfiguration.class);

    @Override
    public WriteResponse globalError(Throwable t) throws ExecutionException {
					if(t instanceof RegionTooBusyException){
							return WriteResponse.RETRY;
					}
					else if(t instanceof InterruptedException){
							Thread.currentThread().interrupt();
							return WriteResponse.IGNORE; //
					}else if(t instanceof ConnectException
                || t instanceof WrongRegionException
                || t instanceof IndexNotSetUpException
                || t instanceof NotServingRegionException )
            return WriteResponse.RETRY;
        else
            return WriteResponse.THROW_ERROR;
    }
    
	@Override
	public WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult) throws Throwable {
		WriteResult writeResult = bulkWriteResult.getGlobalResult();
		if (writeResult.isSuccess())
			return WriteResponse.SUCCESS;
		else if (writeResult.isPartial()) {
			IntObjectOpenHashMap<WriteResult> failedRows = bulkWriteResult.getFailedRows();
			if(failedRows!=null && failedRows.size()>0){
				return WriteResponse.PARTIAL;
			}
			return WriteResponse.IGNORE;
		}	
		else if (!writeResult.canRetry())
				throw Exceptions.fromString(writeResult);
		else if (writeResult.canRetry()) {
			return WriteResponse.RETRY;
		}
		else {
			return WriteResponse.IGNORE;
		}
	}
	
	@Override
	public void registerContext(WriteContext context,
			ObjectObjectOpenHashMap<KVPair, KVPair> indexToMainMutationMap) {
		SpliceLogUtils.warn(LOG, "registering Context with a base class");
	}
	
}
