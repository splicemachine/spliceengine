/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.output;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.config.ForwardingWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
import com.splicemachine.storage.Record;
import com.splicemachine.utils.SpliceLogUtils;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.log4j.Logger;

import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 *
 */
public class PermissiveInsertWriteConfiguration extends ForwardingWriteConfiguration {
    private static final Logger LOG = Logger.getLogger(PermissiveInsertWriteConfiguration.class);
    protected OperationContext operationContext;
    protected ExecRow execRow;

    public PermissiveInsertWriteConfiguration(WriteConfiguration delegate, OperationContext operationContext,
                                              ExecRow execRow) {
        super(delegate);
        assert operationContext!=null:"Passed in null values to PermissiveInsert";
        this.operationContext = operationContext;
        this.execRow = execRow;
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
        boolean ignore = result.getNotRunRows().size()<=0 && result.getFailedRows().size()<=0;
        List<Record> kvPairList = request.mutationsList();
        for(IntObjectCursor<WriteResult> resultCursor:failedRows) {
            WriteResult value = resultCursor.value;
            int rowNum = resultCursor.key;
            if (!value.canRetry()) {
                if (operationContext.isFailed())
                    ignore = true;
                try {
                    throw new UnsupportedOperationException("not implemented");
//                    operationContext.recordBadRecord(errorRow(pairDecoder.decode(kvPairList.get(rowNum)).toString(), value), null);
                } catch (Exception e) {
                    ignore = true;
                }

                if (operationContext.isFailed())
                    ignore = true;
            }
         }
        if(ignore)
            return WriteResponse.IGNORE;
        else
            return WriteResponse.RETRY;
    }

    @Override
    public String toString() {
        return String.format("PermissiveImporterWriteConfiguration{delegate=%s}",delegate);
    }



    @SuppressFBWarnings(value = "SF_SWITCH_NO_DEFAULT",justification = "Intentional")
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
                String[] ctxMsgs = result.getConstraintContext().getMessages();
                sb = sb.append(result.getCode()).append("(").append(ctxMsgs != null && ctxMsgs.length > 0 ? ctxMsgs[0] : "")
                       .append(" ").append(ctxMsgs != null && ctxMsgs.length > 1 ? ctxMsgs[1] : "").append(")");
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
        sb.append(": " + row);
        return sb.toString();
    }
}

