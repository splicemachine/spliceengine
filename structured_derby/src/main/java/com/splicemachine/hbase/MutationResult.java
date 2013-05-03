package com.splicemachine.hbase;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * @author Scott Fines
 * Created on: 4/30/13
 */
public class MutationResult implements Externalizable{
    private static final MutationResult SUCCESS = new MutationResult(Code.SUCCESS);
    private static final MutationResult NOT_RUN = new MutationResult(Code.NOT_RUN);

    public static MutationResult success() {
        return SUCCESS;
    }

    public static MutationResult notRun() {
        return NOT_RUN;
    }

    public Code getCode(){
        return code;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(code.name());
        if(code == Code.FAILED){
            out.writeUTF(errorMsg);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        code = Code.valueOf(in.readUTF());
        if(code == Code.FAILED)
            errorMsg = in.readUTF();
    }

    public String getErrorMsg() {
        return errorMsg;
    }

    public static enum Code{
        SUCCESS,
        FAILED,
        NOT_RUN,
        PRIMARY_KEY_VIOLATION,
        UNIQUE_VIOLATION,
        FOREIGN_KEY_VIOLATION,
        CHECK_VIOLATION,
        WRITE_CONFLICT;

        public static Code parse(String s) {
            for(Code code:values()){
                if(code.name().equals(s))
                    return code;
            }
            return null;
        }
    }
    private String errorMsg;
    private Code code;

    @Deprecated
    public MutationResult(){  }

    public MutationResult(Code code) {
        this(code,null);
    }

    public MutationResult(Code code,String errorMsg) {
        this.errorMsg = errorMsg;
        this.code = code;
    }
}
