package com.splicemachine.derby.stream.output;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.hbase.KVPair;
import com.splicemachine.metrics.MetricFactory;
import com.splicemachine.metrics.Metrics;
import com.splicemachine.pipeline.api.WriteConfiguration;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.impl.BulkWrite;
import com.splicemachine.pipeline.impl.BulkWriteResult;
import com.splicemachine.pipeline.impl.WriteResult;
import com.splicemachine.pipeline.writeconfiguration.ForwardingWriteConfiguration;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class PermissiveInsertWriteConfiguration extends ForwardingWriteConfiguration {
    private static final Logger LOG = Logger.getLogger(PermissiveInsertWriteConfiguration.class);
    protected OperationContext operationContext;

    public PermissiveInsertWriteConfiguration(WriteConfiguration delegate, OperationContext operationContext) {
        super(delegate);
        assert operationContext!=null;
        this.operationContext = operationContext;
    }

    @Override
    public WriteResponse globalError(Throwable t) throws ExecutionException {
        if(operationContext.isFailed()) return WriteResponse.IGNORE;
        return super.globalError(t);
    }

    @Override
    public WriteResponse processGlobalResult(BulkWriteResult bulkWriteResult) throws Throwable {
        if(operationContext.isFailed()) return WriteResponse.IGNORE;
        return super.processGlobalResult(bulkWriteResult);
    }

    @Override
    public WriteResponse partialFailure(BulkWriteResult result, BulkWrite request) throws ExecutionException {
        if (LOG.isDebugEnabled())
            SpliceLogUtils.debug(LOG, "partialFailure result=%s", result);
        if(operationContext.isFailed()) return WriteResponse.IGNORE;
        //filter out and report bad records
        IntObjectOpenHashMap<WriteResult> failedRows = result.getFailedRows();
        @SuppressWarnings("MismatchedReadAndWriteOfArray") Object[] fRows = failedRows.values;
        boolean ignore = result.getNotRunRows().size()<=0;
        List<KVPair> kvPairList = request.mutationsList();
        for(IntObjectCursor<WriteResult> resultCursor:failedRows) {
            WriteResult value = resultCursor.value;
            int rowNum = resultCursor.key;
            if (!value.canRetry()) {
                if (operationContext.isFailed())
                    ignore = true;
                operationContext.recordBadRecord(errorRow(kvPairList.get(rowNum).toString(),value));
                if (operationContext.isFailed())
                    ignore = true;
            }
         }
        if(ignore)
            return WriteResponse.IGNORE;
        else
            return WriteResponse.RETRY;
    }
    @Override public MetricFactory getMetricFactory() {
        return Metrics.noOpMetricFactory();
    }

    @Override
    public String toString() {
        return String.format("PermissiveImporterWriteConfiguration{delegate=%s}",delegate);
    }



    private static String errorRow(String row, WriteResult result) {
        StringBuilder sb = new StringBuilder();
        switch (result.getCode()) {
            case NOT_NULL:
                sb = sb.append("NOTNULL(").append(result.getErrorMessage()).append(")");
                break;
            case FAILED:
                sb = sb.append("ERROR(").append(result.getErrorMessage()).append(")");
                break;
            case WRITE_CONFLICT:
                sb = sb.append("WRITECONFLICT(").append(result.getErrorMessage()).append(")");
                break;
            case PRIMARY_KEY_VIOLATION:
                sb = sb.append("PRIMARYKEY");
                break;
            case UNIQUE_VIOLATION:
                sb = sb.append("UNIQUE");
                break;
            case FOREIGN_KEY_VIOLATION:
                sb = sb.append("FOREIGNKEY");
                break;
            case CHECK_VIOLATION:
                sb = sb.append("CHECK");
                break;
            case NOT_SERVING_REGION:
            case WRONG_REGION:
            case REGION_TOO_BUSY:
                sb = sb.append("ENVIRONMENT(").append(result.getCode()).append(")");
                break;
        }
        return sb.toString();
    }
}

