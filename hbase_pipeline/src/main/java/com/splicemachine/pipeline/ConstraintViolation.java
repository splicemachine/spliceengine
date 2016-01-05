package com.splicemachine.pipeline;

import com.splicemachine.pipeline.constraint.ConstraintContext;
import com.splicemachine.pipeline.constraint.ForeignKeyViolation;
import com.splicemachine.pipeline.constraint.UniqueConstraintViolation;
import org.apache.hadoop.hbase.DoNotRetryIOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class ConstraintViolation extends DoNotRetryIOException{
    protected ConstraintContext cc;


    public ConstraintViolation(){ }

    public ConstraintViolation(String message){
        super(message);
    }

    public ConstraintViolation(String message,Throwable cause){
        super(message,cause);
    }

    public ConstraintViolation(String message,ConstraintContext context){
        super(message);
        this.cc = context;
    }

    public ConstraintContext getConstraintContext(){
        return cc;
    }

    public static class PkViolation extends ConstraintViolation implements UniqueConstraintViolation{
        public PkViolation(){ }

        public PkViolation(String message){
            super(message);
        }

        public PkViolation(String message,Throwable cause){
            super(message,cause);
        }

        public PkViolation(String message,ConstraintContext context){
            super(message,context);
        }
    }

    public static class FkViolation extends ConstraintViolation implements ForeignKeyViolation{
        public FkViolation(){ }

        public FkViolation(String message){
            super(message);
        }

        public FkViolation(String message,Throwable cause){
            super(message,cause);
        }

        public FkViolation(String message,ConstraintContext context){
            super(message,context);
        }
    }

    public static class UniqueViolation extends ConstraintViolation implements UniqueConstraintViolation{
        public UniqueViolation(){ }

        public UniqueViolation(String message){
            super(message);
        }

        public UniqueViolation(String message,Throwable cause){
            super(message,cause);
        }

        public UniqueViolation(String message,ConstraintContext context){
            super(message,context);
        }
    }

    public static class NotNullViolation extends ConstraintViolation{
        public NotNullViolation(){ }

        public NotNullViolation(String message){
            super(message);
        }

        public NotNullViolation(String message,Throwable cause){
            super(message,cause);
        }

        public NotNullViolation(String message,ConstraintContext context){
            super(message,context);
        }
    }
}
