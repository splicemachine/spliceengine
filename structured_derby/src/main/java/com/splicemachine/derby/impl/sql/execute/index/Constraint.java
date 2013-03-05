package com.splicemachine.derby.impl.sql.execute.index;

import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Put;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Created on: 2/28/13
 */
public interface Constraint {

    boolean validate(Put put) throws IOException;

    boolean validate(Delete delete) throws IOException;


    public static Constraint NO_CONSTRAINTS = new Constraint(){

        @Override
        public boolean validate(Put put) throws IOException {
            return true; //no-op
        }

        @Override
        public boolean validate(Delete delete) throws IOException {
            return true;
        }
    };
}

