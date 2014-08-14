package com.splicemachine.derby.ddl;

/**
 * Interface common to our DDL descriptions.
 */
public interface TentativeDDLDesc {

    long getBaseConglomerateNumber();

    long getConglomerateNumber();
}
