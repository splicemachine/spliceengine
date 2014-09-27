package com.splicemachine.derby.ddl;

import java.io.Externalizable;

/**
 * Interface common to our DDL descriptions.
 */
public interface TentativeDDLDesc extends Externalizable{

    long getBaseConglomerateNumber();

    long getConglomerateNumber();
}
