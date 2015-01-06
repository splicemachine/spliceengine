package com.splicemachine.pipeline.writeconfiguration;

import java.util.concurrent.ExecutionException;

import org.apache.log4j.Logger;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.derby.impl.load.FailAlwaysReporter;
import com.splicemachine.derby.impl.load.ImportErrorReporter;
import com.splicemachine.derby.impl.load.SequentialImporter;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.exception.ErrorState;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.utils.SpliceLogUtils;

public class SequentialImporterWriteConfiguration extends ForwardingWriteConfiguration {
    private static final Logger LOG = Logger.getLogger(SequentialImporterWriteConfiguration.class);
	protected SequentialImporter sequentialImporter;
	protected MetricFactory metricFactory;
	protected ImportErrorReporter errorReporter;
	
	public SequentialImporterWriteConfiguration(WriteConfiguration delegate, SequentialImporter sequentialImporter, MetricFactory metricFactory, ImportErrorReporter errorReporter) {
		super(delegate);
		this.sequentialImporter = sequentialImporter;
		this.metricFactory = metricFactory;
		this.errorReporter = errorReporter;
	}
	
	@Override
	public WriteResponse globalError(Throwable t) throws ExecutionException {
			if(sequentialImporter.isFailed()) return WriteResponse.IGNORE;
			return super.globalError(t);
	}

    @Override
    public WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult) throws Throwable {
        if(sequentialImporter.isFailed()) return WriteResponse.IGNORE;
        return super.processGlobalResult(bulkWriteResult);
    }

	@Override
	public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
			if (LOG.isDebugEnabled())
				SpliceLogUtils.debug(LOG, "partialFailure result=%s", result);
			if(sequentialImporter.isFailed()) return WriteResponse.IGNORE;
			//filter out and report bad records
			IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
			@SuppressWarnings("MismatchedReadAndWriteOfArray") Object[] fRows = failedRows.values;
			boolean ignore = result.getNotRunRows().size()<=0;
			for(IntObjectCursor<WriteResult> resultCursor:failedRows){
					WriteResult value = resultCursor.value;
					int rowNum = resultCursor.key;
					if(!value.canRetry()){
							if(!errorReporter.reportError((KVPair)request.getBuffer()[rowNum],value)){
									if(errorReporter ==FailAlwaysReporter.INSTANCE)
											return WriteResponse.THROW_ERROR;
									else
											throw new ExecutionException(ErrorState.LANG_IMPORT_TOO_MANY_BAD_RECORDS.newException());
							}
							failedRows.allocated[resultCursor.index] = false;
							fRows[resultCursor.index] = null;
					}else
						ignore = false;
			}
			//can only ignore if we don't need to retry notRunRows
			if(ignore)
					return WriteResponse.IGNORE;
			else
					return WriteResponse.RETRY;
	}
	@Override public MetricFactory getMetricFactory() {
			return metricFactory;
	}
	
	@Override
	public String toString() {
		return String.format("SequentialImporterWriteConfiguration{delegate=%s}",delegate);
	}

}