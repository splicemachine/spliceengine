package com.splicemachine.pipeline.api;
/**
 * Class that enumerates the error code.
 * 
 *
 */
public enum Code {
    FAILED,
    PARTIAL{
        @Override
        public boolean canRetry() {
            return true;
        }
        @Override
        public boolean isPartial() {
            return true;
        }
        @Override
        public boolean refreshCache(){
            return true;
        }
    },
    WRITE_CONFLICT,
    SUCCESS {         
    	@Override
    	public boolean isSuccess() {
    		return true;
    	}
    },
    PRIMARY_KEY_VIOLATION,
    UNIQUE_VIOLATION,
    FOREIGN_KEY_VIOLATION,
    CHECK_VIOLATION,
    INDEX_NOT_SETUP_EXCEPTION{
        @Override
        public boolean canRetry() {
            return true;
        }    	
    },
    NOT_SERVING_REGION{
        @Override
        public boolean canRetry() {
            return true;
        }
        @Override
        public boolean refreshCache(){
            return true;
        }
    },
    WRONG_REGION{
        @Override
        public boolean canRetry() {
            return true;
        }
        @Override
        public boolean refreshCache(){
            return true;
        }
    },
    INTERRUPTED_EXCEPTON{
        @Override
        public boolean canRetry() {
            return true;
        }
    },
    REGION_TOO_BUSY{
        @Override
        public boolean canRetry() {
            return true;
        }
    },
    PIPELINE_TOO_BUSY{
        @Override
        public boolean canRetry() {
            return true;
        }
    },
    NOT_RUN{
        @Override
        public boolean canRetry() {
            return true;
        }
    },
			NOT_NULL;

    public boolean canRetry(){
        return false;
    }

    public boolean isSuccess(){
        return false;
    }

    public boolean isPartial(){
        return false;
    }

    public boolean refreshCache(){
        return false;
    }

			public static Code fromOrdinal(int ordinal){
					for(Code code:values()){
							if(code.ordinal()==ordinal)
									return code;
					}
					return null;
			}
	}
