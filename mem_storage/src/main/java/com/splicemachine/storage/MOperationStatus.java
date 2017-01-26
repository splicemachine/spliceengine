/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
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
