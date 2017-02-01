/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.stream.output;

import com.carrotsearch.hppc.IntObjectOpenHashMap;
import com.carrotsearch.hppc.cursors.IntObjectCursor;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.OperationContext;
import com.splicemachine.derby.utils.marshall.PairDecoder;
import com.splicemachine.derby.utils.marshall.PairEncoder;
import com.splicemachine.kvpair.KVPair;
import com.splicemachine.pipeline.api.WriteResponse;
import com.splicemachine.pipeline.client.BulkWrite;
import com.splicemachine.pipeline.client.BulkWriteResult;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.config.ForwardingWriteConfiguration;
import com.splicemachine.pipeline.config.WriteConfiguration;
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
    protected ThreadLocal<PairDecoder> pairDecoder;

    public PermissiveInsertWriteConfiguration(WriteConfiguration delegate, OperationContext operationContext, PairEncoder encoder, ExecRow execRowDefinition) {
        super(delegate);
        assert operationContext!=null && encoder != null:"Passed in null values to PermissiveInsert";
        this.operationContext = operationContext;
        this.pairDecoder = new ThreadLocal<PairDecoder>() {
            @Override
            protected PairDecoder initialValue() {
                return encoder.getDecoder(execRowDefinition.getClone());
            }
        };
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
        List<KVPair> kvPairList = request.mutationsList();
        for(IntObjectCursor<WriteResult> resultCursor:failedRows) {
            WriteResult value = resultCursor.value;
            int rowNum = resultCursor.key;
            if (!value.canRetry()) {
                if (operationContext.isFailed())
                    ignore = true;
                try {
                    operationContext.recordBadRecord(errorRow(pairDecoder.get().decode(kvPairList.get(rowNum).shallowClone()).toString(), value), null);
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

