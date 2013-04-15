package com.splicemachine.derby.impl.sql.execute.constraint;

import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import java.io.IOException;
import java.util.Collection;

/**
 * Utilities relating to Constraints
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class Constraints {
    private Constraints(){} //can't make me!
    /*
     * an Empty Constraint which doesn't check anything. A Nice default
     */
    private static final Constraint EMPTY_CONSTRAINT = new Constraint() {
        @Override
        public Type getType() {
            return Type.NONE;
        }

        @Override
        public boolean validate(Mutation mutation, RegionCoprocessorEnvironment rce) throws IOException {
            return true;
        }

        @Override
        public boolean validate(Collection<Mutation> mutations, RegionCoprocessorEnvironment rce) throws IOException {
            return true;
        }
    };


    /**
     * @return an empty constraint which passes everything
     */
    public static Constraint noConstraint(){
        return EMPTY_CONSTRAINT;
    }

}
