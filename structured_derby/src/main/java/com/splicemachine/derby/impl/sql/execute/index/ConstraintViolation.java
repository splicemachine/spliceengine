package com.splicemachine.derby.impl.sql.execute.index;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * @author Scott Fines
 *         Created on: 3/1/13
 */
public class ConstraintViolation extends DoNotRetryIOException{
    public static enum Type{
        PRIMARY_KEY,
        FOREIGN_KEY,
        FAILED_SETUP,
        SHUTDOWN
    }

    private final Type type;

    private ConstraintViolation(Type type){
        super();
       this.type = type;
    }

    private ConstraintViolation(Type type, String message) {
        super(message);
        this.type = type;
    }

    private ConstraintViolation(Type type, String message, Throwable cause ) {
        super(message, cause);
        this.type = type;
    }

    public Type getType() {return type;}

    public static ConstraintViolation failedSetup(String tableName){
        return new ConstraintViolation(Type.FAILED_SETUP,
                "Failed to set up index management for table "+ tableName);
    }

    public static ConstraintViolation shutdown(String tableName){
        return new ConstraintViolation(Type.SHUTDOWN,
                "Index management for table "+ tableName+ " has been shutdown");
    }

    public static DoNotRetryIOException duplicatePrimaryKey(){
        return new PrimaryKeyViolation("Duplicate Primary Key");
    }

    public static class PrimaryKeyViolation extends DoNotRetryIOException{
        public PrimaryKeyViolation() { super(); }
        public PrimaryKeyViolation(String message) { super(message); }
        public PrimaryKeyViolation(String message, Throwable cause) { super(message, cause);}
    }
}
