package com.splicemachine.pipeline.impl;

import com.splicemachine.pipeline.api.Code;
import com.splicemachine.pipeline.constraint.Constraint;
import com.splicemachine.pipeline.constraint.ConstraintContext;
import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 *         Created on: 8/8/13
 */
public class WriteResult implements Externalizable{
    private static final long serialVersionUID = 1l;
    private Code code;
    private String errorMessage;
    private ConstraintContext constraintContext;

    public WriteResult() { }

    public WriteResult(Code code, String errorMessage) {
        this.code = code;
        this.errorMessage = errorMessage;
    }

    public WriteResult(Code code, ConstraintContext constraintContext){
        this.code = code;
        this.constraintContext = constraintContext;
    }

    public WriteResult(Code code) {
        this.code = code;
    }

		public WriteResult(Code code, String errorMessage, ConstraintContext context) {
				this.code = code;
				this.constraintContext = context;
				this.errorMessage = errorMessage;
		}

		@Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(code.name());
        out.writeBoolean(errorMessage!=null);
        if(errorMessage != null){
            out.writeUTF(errorMessage);
        }
        out.writeBoolean(constraintContext!=null);
        if(constraintContext!=null){
            out.writeObject(constraintContext);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        code = Code.valueOf(in.readUTF());
        if(in.readBoolean())
            errorMessage = in.readUTF();
        if(in.readBoolean())
            constraintContext = (ConstraintContext)in.readObject();
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public Code getCode() {
        return code;
    }

    public ConstraintContext getConstraintContext() {
        return constraintContext;
    }

    public boolean canRetry() {
        return code.canRetry();
    }
    public boolean isPartial() {
        return code.isPartial();
    }
    public boolean isSuccess() {
        return code.isSuccess();
    }

    public boolean refreshCache() {
        return code.refreshCache();
    }


		private static final WriteResult NOT_RUN_RESULT = new WriteResult(Code.NOT_RUN);
    public static WriteResult notRun() {
				return NOT_RUN_RESULT;
    }

		private static final WriteResult SUCCESS_RESULT = new WriteResult(Code.SUCCESS);
    public static WriteResult success() {
				return SUCCESS_RESULT;
    }

	private static final WriteResult PARTIAL_RESULT = new WriteResult(Code.PARTIAL);
    public static WriteResult partial() {
				return PARTIAL_RESULT;
    }

    
    public static WriteResult failed(String message) {
        return new WriteResult(Code.FAILED,message);
    }

    public static Code convertType(Constraint.Type type) {
        switch(type){
            case PRIMARY_KEY:
                return Code.PRIMARY_KEY_VIOLATION;
            case UNIQUE:
                return Code.UNIQUE_VIOLATION;
            case FOREIGN_KEY:
                return Code.FOREIGN_KEY_VIOLATION;
            case CHECK:
                return Code.CHECK_VIOLATION;
            default:
                return Code.SUCCESS;
        }
    }

    public static WriteResult wrongRegion() {
        return new WriteResult(Code.WRONG_REGION);
    }

    public static WriteResult notServingRegion() {
        return new WriteResult(Code.NOT_SERVING_REGION);
    }

    public static WriteResult regionTooBusy() {
        return new WriteResult(Code.REGION_TOO_BUSY);
    }

	@Override
	public String toString() {
		return "WriteResult{ "+
				"code=" + code + 
				", errorMessage=" + errorMessage + 
				" }";
		
	}

}
