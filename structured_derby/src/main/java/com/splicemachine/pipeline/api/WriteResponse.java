package com.splicemachine.pipeline.api;
/**
 * WriteResponse enumeration
 * 
 *
 */
public enum WriteResponse{
    THROW_ERROR,
    RETRY,
    SUCCESS,
    IGNORE,
    PARTIAL
}
