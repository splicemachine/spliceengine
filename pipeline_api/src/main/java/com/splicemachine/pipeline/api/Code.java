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

package com.splicemachine.pipeline.api;

/**
 * Class that enumerates the error code.
 */
public enum Code {
    /*Logical behavior codes*/
    SUCCESS,
    /*Indicates that it failed for any non-specific reason*/
    FAILED,
    /*Indicates that a row encountered a Write/Write conflict*/
    WRITE_CONFLICT,
    /**
     * Indicates that some <em>but not all</em>rows in the BulkWrite
     * failed for some reason. Those reasons are specified on a row-by-row basis
     * and each row should be consulted to determine the proper response.
     *
     * This can happen when a row is sent to the wrong region (i.e. immediately
     * after a split occurred), or when a row violates a unique constraint, or
     * any other variation on the same theme.
     */
    PARTIAL{
        @Override public boolean canRetry() { return true; }
        @Override public boolean refreshCache(){ return true; }
    },
    /**
     * Indicates that the given write was not run. This can happen for several reasons:
     *
     * 1. The pipeline decided for any reason not to try this write
     * 2. The row was unable to acquire the internal HBase lock (indicating that someone
     * else already has it).
     *
     */
    NOT_RUN{                   @Override public boolean canRetry() { return true; }},
    /*Constraint violation codes*/
    PRIMARY_KEY_VIOLATION,
    UNIQUE_VIOLATION,
    FOREIGN_KEY_VIOLATION,
    CHECK_VIOLATION,
    NOT_NULL,
    /*RegionServer environment codes*/
    NOT_SERVING_REGION{
        @Override public boolean canRetry() { return true; }
        @Override public boolean refreshCache(){ return true; }
    },
    /**
     * Indicates that the write was sent to the wrong region. In this case,
     * one should back off for a bit, refresh the cache, and then try again.
     */
    WRONG_REGION{
        @Override public boolean canRetry() { return true; }
        @Override public boolean refreshCache(){ return true; }
    },

    /*pipeline behavior codes*/
    /**
     * Indicates that the write was interrupted. This usually means that a region
     * server is shutting down, but is not required to. In this case, the pipeline
     * should back off for a little bit, then try again to the same server just to see
     */
    INTERRUPTED_EXCEPTION {      @Override public boolean canRetry() { return true; }},
    REGION_TOO_BUSY{           @Override public boolean canRetry() { return true; }},
    /**
     * Indicates that this write was unable to proceed because the rate limiter
     * on the server did not allow the write within the time desired. In this case,
     * the pipeline needs to back off for a little bit before trying again, but
     * can try again to the same server.
     */
    PIPELINE_TOO_BUSY{         @Override public boolean canRetry() { return true; }},
    /**
     * Indicates that the initial setup of the write pipeline is taking longer
     * than expected. In this case, back off for a bit and try again to the same
     * server.
     */
    INDEX_NOT_SETUP_EXCEPTION{ @Override public boolean canRetry() { return true; }},
    ;

    public boolean shouldCancel() {
        return this == UNIQUE_VIOLATION;
    }
    public boolean canRetry(){ return false; }
    public boolean isSuccess(){ return this == SUCCESS; }
    public boolean isPartial(){ return this == PARTIAL; }
    public boolean refreshCache(){ return false; }
}
