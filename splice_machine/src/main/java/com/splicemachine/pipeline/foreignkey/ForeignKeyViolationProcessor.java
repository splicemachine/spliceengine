/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
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

package com.splicemachine.pipeline.foreignkey;

import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.client.WriteResult;
import com.splicemachine.pipeline.constraint.ForeignKeyViolation;
import com.splicemachine.pipeline.context.WriteContext;
import com.splicemachine.primitives.Bytes;

import java.util.Map;

/**
 * We intercept writes on either the parent or child table and check for the existence of referenced or referring
 * rows on the child or parent table(s).  When those writes fail the remote *CheckWriteHandler returns a failure
 * to us in the form of an exception.  This class is used by the intercept write handlers (*InterceptWriteHandler) to
 * take the remote exception and translate it into a user-friendly foreign key violation error message.
 */
public class ForeignKeyViolationProcessor {

    private final PipelineExceptionFactory exceptionFactory;

    ForeignKeyViolationProcessor(PipelineExceptionFactory exceptionFactory) {
        this.exceptionFactory = exceptionFactory;
    }

    /**
     * This code looks fragile but it is validated by every single FK IT test method. Breakages in this method would
     * result in all FK ITs failing. Still, it would be nice if would could simplify this.  DB-2952 is for simplifying
     * how error details are passed between FK CheckWriteHandlers and FK InterceptWriteHandlers.
     */
    @SuppressWarnings("ThrowableResultOfMethodCallIgnored")
    public void failWrite(Exception originalException, WriteContext ctx, Map<String, byte[]> originators) {
        Throwable t =exceptionFactory.processPipelineException(originalException);
        if(t instanceof ForeignKeyViolation){
            doFail(ctx,(ForeignKeyViolation)t, originators);
        }
    }

    /**
     *
     * @param ctx The write context
     * @param cause The cause, if the cause if coming from another pipeline, then we peek its messages looking for an originator
     *              which is a rowKey of a local mutation caused a failure on a (remote) pipeline.
     * @param originators childTableBaseRow -> parentTableBaseRow mapping for also failing any originating row in the current
     *                    pipeline.
     */
    private void doFail(WriteContext ctx, ForeignKeyViolation cause, Map<String, byte[]> originators) {
        String hexEncodedFailedRowKey;
        if(cause.getContext().getMessages().length == 5) {
            hexEncodedFailedRowKey = cause.getContext().getMessages()[4];
        } else {
            hexEncodedFailedRowKey = cause.getContext().getMessages()[0];
        }
        byte[] failedRowKey = Bytes.fromHex(hexEncodedFailedRowKey);
        ConstraintContext constraintContext = cause.getContext();
        ctx.result(failedRowKey, new WriteResult(Code.FOREIGN_KEY_VIOLATION, constraintContext));
        if(originators != null) {
            byte[] originator = originators.remove(hexEncodedFailedRowKey);
            if (originator != null) {
                ctx.result(originator, new WriteResult(Code.FOREIGN_KEY_VIOLATION, constraintContext));
            }
        }
    }
}
