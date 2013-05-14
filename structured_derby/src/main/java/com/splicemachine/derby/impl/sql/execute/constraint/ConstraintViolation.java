package com.splicemachine.derby.impl.sql.execute.constraint;

import org.apache.hadoop.hbase.DoNotRetryIOException;

import java.io.IOException;

/**
 * Indicates a Constraint Violation
 *
 * TODO -sf- this isn't a very clean implementation
 * @author Scott Fines
 * Created on: 3/1/13
 */
public class ConstraintViolation extends DoNotRetryIOException{

    private final Constraint.Type type;

    private ConstraintViolation(Constraint.Type type){
        super();
       this.type = type;
    }

    private ConstraintViolation(Constraint.Type type, String message) {
        super(message);
        this.type = type;
    }

    private ConstraintViolation(Constraint.Type type, String message, Throwable cause ) {
        super(message, cause);
        this.type = type;
    }

    public Constraint.Type getType() {return type;}

    public static ConstraintViolation failedSetup(String tableName){
        return new ConstraintViolation(Constraint.Type.NONE,
                "Failed to set up index management for table "+ tableName);
    }

    public static ConstraintViolation shutdown(String tableName){
        return new ConstraintViolation(Constraint.Type.NONE,
                "Index management for table "+ tableName+ " has been shutdown");
    }

    public static DoNotRetryIOException create(Constraint.Type type) {
        switch (type) {
            case PRIMARY_KEY:
                return new PrimaryKeyViolation("Duplicate Primary Key");
            case UNIQUE:
                return new UniqueConstraintViolation("Violated Unique Constraint");
            case NOT_NULL:
                return new NotNullConstraintViolation("Non Null Constraint Violated");
            default:
                return null; //TODO -sf- implement foreign and check constraints
        }
    }

    public static class PrimaryKeyViolation extends DoNotRetryIOException{
        /**
         * Used for serialization, DO NOT USE
         */
        @Deprecated
        public PrimaryKeyViolation() { super(); }
        public PrimaryKeyViolation(String message) { super(message); }
        public PrimaryKeyViolation(String message, Throwable cause) { super(message, cause);}

        @Override
        public String toString(){
            return "PrimaryKeyViolation["+super.toString()+"]";
        }
    }

    public static class UniqueConstraintViolation extends DoNotRetryIOException{
        /**
         * Used for serialization, DO NOT USE
         */
        @Deprecated
        public UniqueConstraintViolation() {super(); }
        public UniqueConstraintViolation(String message) { super(message); }
        public UniqueConstraintViolation(String message, Throwable cause) { super(message, cause); }

        @Override
        public String toString(){
            String superStr = super.toString();
            return "UniqueConstraintViolation["+superStr+"]";
        }
    }

    public static class NotNullConstraintViolation extends DoNotRetryIOException{
        @Deprecated
        public NotNullConstraintViolation() { }
        public NotNullConstraintViolation(String message) { super(message); }
        public NotNullConstraintViolation(String message, Throwable cause) { super(message, cause); }

        @Override
        public String toString(){
            return "NotNullConstraintViolation["+super.toString()+"]";
        }
    }
}
