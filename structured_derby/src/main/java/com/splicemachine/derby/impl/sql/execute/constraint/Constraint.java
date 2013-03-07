package com.splicemachine.derby.impl.sql.execute.constraint;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public interface Constraint {


    public static enum Type{
        PRIMARY_KEY,
        UNIQUE,
        FOREIGN_KEY,
        CHECK,
        NONE //used for NoConstraint
    }

    Type getType();

    boolean validate(Put put,RegionCoprocessorEnvironment rce) throws IOException;

    boolean validate(Delete delete,RegionCoprocessorEnvironment rce) throws IOException;

}

