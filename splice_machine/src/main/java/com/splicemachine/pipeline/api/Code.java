package com.splicemachine.pipeline.api;
/**
 * Class that enumerates the error code.
 *
 *
 */
public enum Code {
    /*Logical behavior codes*/
    SUCCESS((byte)0x00),
    /*Indicates that it failed for any non-specific reason*/
    FAILED((byte)0x01),
    /*Indicates that a row encountered a Write/Write conflict*/
    WRITE_CONFLICT((byte)0x02),
    /**
     * Indicates that some <em>but not all</em>rows in the BulkWrite
     * failed for some reason. Those reasons are specified on a row-by-row basis
     * and each row should be consulted to determine the proper response.
     *
     * This can happen when a row is sent to the wrong region (i.e. immediately
     * after a split occurred), or when a row violates a unique constraint, or
     * any other variation on the same theme.
     */
    PARTIAL((byte)0x03){
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
    NOT_RUN((byte)0x04){                   @Override public boolean canRetry() { return true; }},
    /*Constraint violation codes*/
    PRIMARY_KEY_VIOLATION((byte)0x05),
    UNIQUE_VIOLATION((byte)0x06),
    FOREIGN_KEY_VIOLATION((byte)0x07),
    CHECK_VIOLATION((byte)0x08),
    NOT_NULL((byte)0x09),
    /*RegionServer environment codes*/
    NOT_SERVING_REGION((byte)0x0A){
        @Override public boolean canRetry() { return true; }
        @Override public boolean refreshCache(){ return true; }
    },
    /**
     * Indicates that the write was sent to the wrong region. In this case,
     * one should back off for a bit, refresh the cache, and then try again.
     */
    WRONG_REGION((byte)0x0B){
        @Override public boolean canRetry() { return true; }
        @Override public boolean refreshCache(){ return true; }
    },

    /*pipeline behavior codes*/
    /**
     * Indicates that the write was interrupted. This usually means that a region
     * server is shutting down, but is not required to. In this case, the pipeline
     * should back off for a little bit, then try again to the same server just to see
     */
    INTERRUPTED_EXCEPTON((byte)0x0C){      @Override public boolean canRetry() { return true; }},
    REGION_TOO_BUSY((byte)0x0D){           @Override public boolean canRetry() { return true; }},
    /**
     * Indicates that this write was unable to proceed because the rate limiter
     * on the server did not allow the write within the time desired. In this case,
     * the pipeline needs to back off for a little bit before trying again, but
     * can try again to the same server.
     */
    PIPELINE_TOO_BUSY((byte)0x0E){         @Override public boolean canRetry() { return true; }},
    /**
     * Indicates that the initial setup of the write pipeline is taking longer
     * than expected. In this case, back off for a bit and try again to the same
     * server.
     */
    INDEX_NOT_SETUP_EXCEPTION((byte)0x0F){ @Override public boolean canRetry() { return true; }},
    ;

    private final byte encodedValue;

    Code(byte encodedValue) {
        this.encodedValue = encodedValue;
    }

    public boolean canRetry(){ return false; }
    public boolean isSuccess(){ return this == SUCCESS; }
    public boolean isPartial(){ return this == PARTIAL; }
    public boolean refreshCache(){ return false; }

    public byte encode(){
        return encodedValue;
    }

    public static Code forByte(byte b){
        if(SUCCESS.encodedValue==b) return SUCCESS;
        else if(FAILED.encodedValue==b) return FAILED;
        else if(WRITE_CONFLICT.encodedValue==b) return WRITE_CONFLICT;
        else if(PARTIAL.encodedValue==b) return PARTIAL;
        else if(NOT_RUN.encodedValue==b) return NOT_RUN;
        else if(PRIMARY_KEY_VIOLATION.encodedValue==b) return PRIMARY_KEY_VIOLATION;
        else if(UNIQUE_VIOLATION.encodedValue==b) return UNIQUE_VIOLATION;
        else if(FOREIGN_KEY_VIOLATION.encodedValue==b) return FOREIGN_KEY_VIOLATION;
        else if(CHECK_VIOLATION.encodedValue==b) return CHECK_VIOLATION;
        else if(NOT_NULL.encodedValue==b) return NOT_NULL;
        else if(NOT_SERVING_REGION.encodedValue==b) return NOT_SERVING_REGION;
        else if(WRONG_REGION.encodedValue==b) return WRONG_REGION;
        else if(INTERRUPTED_EXCEPTON.encodedValue==b) return INTERRUPTED_EXCEPTON;
        else if(REGION_TOO_BUSY.encodedValue==b) return REGION_TOO_BUSY;
        else if(PIPELINE_TOO_BUSY.encodedValue==b) return PIPELINE_TOO_BUSY;
        else if(INDEX_NOT_SETUP_EXCEPTION.encodedValue==b) return INDEX_NOT_SETUP_EXCEPTION;
        else
            throw new IllegalArgumentException("No Code for byte value "+ b);
    }

    public static Code decode(byte[] bytes, int offset,long[] lengthHolder){
        Code code = forByte(bytes[offset]);
        lengthHolder[0] = 1;
        return code;
    }
}
