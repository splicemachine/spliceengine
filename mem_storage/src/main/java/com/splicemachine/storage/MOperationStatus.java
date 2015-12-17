package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public abstract class MOperationStatus implements MutationStatus{
    private static final MOperationStatus SUCCESS = new MOperationStatus(){
        @Override public boolean isSuccess(){ return true; }
        @Override public boolean isFailed(){ return false; }
        @Override public boolean isNotRun(){ return false; }
    };

    private static final MOperationStatus NOT_RUN = new MOperationStatus(){
        @Override public boolean isSuccess(){ return false; }
        @Override public boolean isFailed(){ return false; }
        @Override public boolean isNotRun(){ return true; }
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

    private static class Failure extends MOperationStatus{
        private final String errorMsg;

        public Failure(String errorMsg){
            this.errorMsg = errorMsg;
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
    }
}
