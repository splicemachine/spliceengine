/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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

    public ConstraintContext getContext(){
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
