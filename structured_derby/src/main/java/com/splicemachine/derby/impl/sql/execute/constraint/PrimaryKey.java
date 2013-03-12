package com.splicemachine.derby.impl.sql.execute.constraint;

import org.apache.log4j.Logger;

/**
 * Indicates a Primary Key Constraint.
 *
 * @author Scott Fines
 * Created on: 2/28/13
 */
public class PrimaryKey extends UniqueConstraint {
    private static final Logger logger = Logger.getLogger(PrimaryKey.class);

    public PrimaryKey(){}

    @Override
    public Type getType() {
        return Type.PRIMARY_KEY;
    }

    //TODO -sf- validate Foreign Key Constraints here?

    @Override
    public String toString() {
        return "PrimaryKey";
    }
}
