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

package com.splicemachine.storage;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public abstract class MOperationStatus implements MutationStatus{
    private static final MOperationStatus SUCCESS = new MOperationStatus(){
        @Override public boolean isSuccess(){ return true; }
        @Override public boolean isFailed(){ return false; }
        @Override public boolean isNotRun(){ return false; }
        @Override public boolean hasException() { return false; }
        @Override public IOException getException() { return null; }
    };

    private static final MOperationStatus NOT_RUN = new MOperationStatus(){
        @Override public boolean isSuccess(){ return false; }
        @Override public boolean isFailed(){ return false; }
        @Override public boolean isNotRun(){ return true; }
        @Override public boolean hasException() { return false; }
        @Override public IOException getException() { return null; }
    };

    @Override
    public String errorMessage(){
        return null;
    }

    @Override
    public MutationStatus getClone(){
        return this;
    }

    public static MOperationStatus success(){ return SUCCESS;}
    public static MOperationStatus notRun(){ return NOT_RUN;}
    public static MOperationStatus failure(String message){ return new Failure(message);}
    public static MOperationStatus failure(IOException ex){ return new Failure(ex);}

    private static class Failure extends MOperationStatus{
        private final String errorMsg;
        private IOException ex = null;

        public Failure(String errorMsg){
            this.errorMsg = errorMsg;
        }

        public Failure(IOException ex) {
            this(ex.getMessage());
            this.ex = ex;
        }

        @Override public boolean isSuccess(){ return false; }
        @Override public boolean isNotRun(){ return false; }
        @Override public boolean isFailed(){ return true; }

        @Override
        public String errorMessage(){
            return errorMsg;
        }

        @Override
        public MutationStatus getClone(){
            return new Failure(errorMsg);
        }

        @Override
        public boolean hasException() {
            return ex != null;
        }

        @Override
        public IOException getException() {
            return ex;
        }
    }
}
