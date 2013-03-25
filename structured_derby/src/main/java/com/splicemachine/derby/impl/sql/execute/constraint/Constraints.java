package com.splicemachine.derby.impl.sql.execute.constraint;

import com.google.common.base.Predicate;
import com.google.common.collect.Collections2;
import com.splicemachine.derby.utils.Mutations;
import com.splicemachine.derby.utils.Puts;
import com.splicemachine.si2.data.hbase.TransactorFactory;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Mutation;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;

import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
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
