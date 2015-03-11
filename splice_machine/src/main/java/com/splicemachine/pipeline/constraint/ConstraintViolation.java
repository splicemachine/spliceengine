package com.splicemachine.pipeline.constraint;

import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * Indicates a Constraint Violation
 *
 * @author Scott Fines
 * Created on: 3/1/13
 */
public final class ConstraintViolation {

    private ConstraintViolation() {}

    public static DoNotRetryIOException create(Constraint.Type type, ConstraintContext constraintContext) {
        switch (type) {
            case PRIMARY_KEY:
                return new PrimaryKeyViolation("Duplicate Primary Key", constraintContext);
            case FOREIGN_KEY:
                return new ForeignKeyConstraintViolation(constraintContext);
            case UNIQUE:
                return new UniqueConstraintViolation("Violated Unique Constraint", constraintContext);
            case NOT_NULL:
                return new NotNullConstraintViolation("Non Null Constraint Violated", constraintContext);
            default:
                throw new IllegalStateException("Programmer error, unexpected constraint type = " + type);
        }
    }

    public static class ConstraintViolationException extends DoNotRetryIOException {

        private ConstraintContext constraintContext = null;

        public ConstraintViolationException() { super(); }
        public ConstraintViolationException(String message) { super(message); }
        public ConstraintViolationException(String message, ConstraintContext constraintContext) {
            super(message);
            this.constraintContext = constraintContext;
        }
        public ConstraintViolationException(String message, Throwable cause) { super(message, cause);}

        public ConstraintContext getConstraintContext(){
            return constraintContext;
        }
    }

    public static class PrimaryKeyViolation extends ConstraintViolationException{
        /* Used for serialization, DO NOT USE */
        @Deprecated
        public PrimaryKeyViolation() { super(); }
        public PrimaryKeyViolation(String message) { super(message); }
        public PrimaryKeyViolation(String message, ConstraintContext constraintContext) { super(message, constraintContext); }
        public PrimaryKeyViolation(String message, Throwable cause) { super(message, cause);}

        @Override
        public String toString(){
            return "PrimaryKeyViolation["+super.toString()+"]";
        }
    }

    public static class UniqueConstraintViolation extends ConstraintViolationException{
        /* Used for serialization, DO NOT USE */
        @Deprecated
        public UniqueConstraintViolation() {super(); }
        public UniqueConstraintViolation(String message) { super(message); }
        public UniqueConstraintViolation(String message, ConstraintContext constraintContext) { super(message, constraintContext); }
        public UniqueConstraintViolation(String message, Throwable cause) { super(message, cause); }

        @Override
        public String toString(){
            String superStr = super.toString();
            return "UniqueConstraintViolation["+superStr+"]";
        }
    }

    public static class NotNullConstraintViolation extends ConstraintViolationException{
        @Deprecated
        public NotNullConstraintViolation() { }
        public NotNullConstraintViolation(String message) { super(message); }
        public NotNullConstraintViolation(String message, ConstraintContext cc) { super(message, cc); }
        public NotNullConstraintViolation(String message, Throwable cause) { super(message, cause); }

        @Override
        public String toString(){
            return "NotNullConstraintViolation["+super.toString()+"]";
        }
    }

    public static class ForeignKeyConstraintViolation extends ConstraintViolationException{
        @Deprecated
        public ForeignKeyConstraintViolation() { }
        public ForeignKeyConstraintViolation(ConstraintContext cc) { super("", cc); }
        public ForeignKeyConstraintViolation(String message) {
            super(message);
        }

        @Override
        public String toString(){
            return "ForeignKeyConstraintViolation["+super.toString()+"]";
        }
    }

}
