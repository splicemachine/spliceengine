package com.splicemachine.pipeline.ddl;

import java.io.Externalizable;

/**
 * Interface common to our DDL descriptions.
 */
public interface TentativeDDLDesc {

    long getBaseConglomerateNumber();

    long getConglomerateNumber();
}
