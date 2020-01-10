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

package com.splicemachine.pipeline.client;

import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.api.Constraint;
import com.splicemachine.pipeline.constraint.ConstraintContext;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Immutable class representing the result of one KV write.
 *
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class WriteResult implements Externalizable {

    private static final long serialVersionUID = 1l;

    private static final WriteResult NOT_RUN_RESULT = new WriteResult(Code.NOT_RUN);
    private static final WriteResult SUCCESS_RESULT = new WriteResult(Code.SUCCESS);
    private static final WriteResult PARTIAL_RESULT = new WriteResult(Code.PARTIAL);
    private static final WriteResult INTERRUPTED = new WriteResult(Code.INTERRUPTED_EXCEPTION);
    private static final WriteResult INDEX_NOT_SETUP = new WriteResult(Code.INDEX_NOT_SETUP_EXCEPTION);
    private static final WriteResult WRONG_REGION = new WriteResult(Code.WRONG_REGION);
    private static final WriteResult NOT_SERVING_REGION = new WriteResult(Code.NOT_SERVING_REGION);
    private static final WriteResult REGION_TO_BUSY = new WriteResult(Code.REGION_TOO_BUSY);

    private Code code;
    private String errorMessage;
    private ConstraintContext constraintContext;

    public WriteResult() {
    }

    public WriteResult(Code code, String errorMessage) {
        this.code = code;
        this.errorMessage = errorMessage;
    }

    public WriteResult(Code code, ConstraintContext constraintContext) {
        this.code = code;
        this.constraintContext = constraintContext;
    }

    public WriteResult(Code code) {
        this.code = code;
    }

    public WriteResult(Code code, String errorMessage, ConstraintContext context) {
        this.code = code;
        this.constraintContext = context;
        this.errorMessage = errorMessage;
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Code getCode() {
        return code;
    }

    public ConstraintContext getConstraintContext() {
        return constraintContext;
    }

    public boolean shouldCancel() {
        return code.shouldCancel();
    }
    public boolean canRetry() {

        return code.canRetry() ||
                ((errorMessage!= null) &&
                        (errorMessage.contains("NoRouteToHostException") ||
                                errorMessage.contains(("FailedServerException")) ||
                                errorMessage.contains("ServerNotRunningYetException") ||
                                errorMessage.contains("ConnectTimeoutException")));
    }

    public boolean isPartial() {
        return code.isPartial();
    }

    public boolean isSuccess() {
        return code.isSuccess();
    }

    public boolean refreshCache() {
        return code.refreshCache();
    }

    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
    // static factory methods
    // - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

    public static WriteResult failed(String message) {
        return new WriteResult(Code.FAILED, message);
    }

    public static WriteResult pipelineTooBusy(String regionNameAsString) {
        return new WriteResult(Code.PIPELINE_TOO_BUSY, "pipeline for regionserver owning region " + regionNameAsString + " is too busy");
    }

    public static WriteResult notRun() {
        return NOT_RUN_RESULT;
    }

    public static WriteResult success() {
        return SUCCESS_RESULT;
    }

    public static WriteResult partial() {
        return PARTIAL_RESULT;
    }

    public static WriteResult interrupted() {
        return INTERRUPTED;
    }

    public static WriteResult indexNotSetup() {
        return INDEX_NOT_SETUP;
    }

    public static WriteResult wrongRegion() {
        return WRONG_REGION;
    }

    public static WriteResult notServingRegion() {
        return NOT_SERVING_REGION;
    }

    public static WriteResult notServingRegion(String contextMessage) {
        if(contextMessage==null) return notServingRegion();
        else return new WriteResult(Code.NOT_SERVING_REGION,contextMessage);
    }

    public static WriteResult regionTooBusy() {
        return REGION_TO_BUSY;
    }

    public static Code convertType(Constraint.Type type) {
        switch (type) {
            case PRIMARY_KEY:
                return Code.PRIMARY_KEY_VIOLATION;
            case UNIQUE:
                return Code.UNIQUE_VIOLATION;
            case FOREIGN_KEY:
                return Code.FOREIGN_KEY_VIOLATION;
            case CHECK:
                return Code.CHECK_VIOLATION;
            default:
                return Code.SUCCESS;
        }
    }

    @Override
    public String toString() {
        return "WriteResult{ " +
                "code=" + code +
                ", errorMessage=" + errorMessage +
                " }";
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(code.name());
        out.writeBoolean(errorMessage != null);
        if (errorMessage != null) {
            out.writeUTF(errorMessage);
        }
        out.writeBoolean(constraintContext != null);
        if (constraintContext != null) {
            out.writeObject(constraintContext);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        code = Code.valueOf(in.readUTF());
        if (in.readBoolean())
            errorMessage = in.readUTF();
        if (in.readBoolean())
            constraintContext = (ConstraintContext) in.readObject();
    }

}
