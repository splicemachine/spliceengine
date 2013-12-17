package com.splicemachine.hbase.writer;

import com.esotericsoftware.kryo.io.Input;
import com.esotericsoftware.kryo.io.Output;
import com.splicemachine.derby.impl.sql.execute.constraint.Constraint;
import com.splicemachine.derby.impl.sql.execute.constraint.ConstraintContext;

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
        if(code == Code.FAILED){
            out.writeUTF(errorMessage);
        }
        out.writeBoolean(constraintContext!=null);
        if(constraintContext!=null){
            out.writeObject(constraintContext);
        }
    }

		public void write(Output output) throws IOException{
			output.writeInt(code.ordinal());
				if(code==Code.FAILED){
						output.writeString(errorMessage);
				}
				output.writeBoolean(constraintContext!=null);
				if(constraintContext!=null)
						constraintContext.write(output);
		}

		public static WriteResult fromBytes(Input input) throws IOException{
				Code code = Code.fromOrdinal(input.readInt());
				String errorMessage = code==Code.FAILED?input.readString():null;
				ConstraintContext context = input.readBoolean()? ConstraintContext.fromBytes(input):null;
				return new WriteResult(code,errorMessage,context);
		}

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        code = Code.valueOf(in.readUTF());
        if(code == Code.FAILED)
            errorMessage = in.readUTF();

        if(in.readBoolean())
            constraintContext = (ConstraintContext)in.readObject();
    }

    public String getErrorMessage() {
        return errorMessage;
    }

    public WriteResult.Code getCode() {
        return code;
    }

    public ConstraintContext getConstraintContext() {
        return constraintContext;
    }

    public boolean canRetry() {
        return code.canRetry();
    }

    public static WriteResult notRun() {
        return new WriteResult(Code.NOT_RUN);
    }

    public static WriteResult success() {
        return new WriteResult(Code.SUCCESS);
    }

    public static WriteResult failed(String message) {
        return new WriteResult(Code.FAILED,message);
    }

    public static Code convertType(Constraint.Type type) {
        switch(type){
            case PRIMARY_KEY:
                return WriteResult.Code.PRIMARY_KEY_VIOLATION;
            case UNIQUE:
                return WriteResult.Code.UNIQUE_VIOLATION;
            case FOREIGN_KEY:
                return WriteResult.Code.FOREIGN_KEY_VIOLATION;
            case CHECK:
                return WriteResult.Code.CHECK_VIOLATION;
            default:
                return WriteResult.Code.SUCCESS;
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

    public enum Code {
        FAILED,
        WRITE_CONFLICT,
        SUCCESS,
        PRIMARY_KEY_VIOLATION,
        UNIQUE_VIOLATION,
        FOREIGN_KEY_VIOLATION,
        CHECK_VIOLATION,
        NOT_SERVING_REGION{
            @Override
            public boolean canRetry() {
                return true;
            }
        },
        WRONG_REGION{
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
        NOT_RUN{
            @Override
            public boolean canRetry() {
                return true;
            }
        };

        public boolean canRetry(){
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
}
