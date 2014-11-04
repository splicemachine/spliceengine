package com.splicemachine.hbase;

import com.splicemachine.constants.bytes.BytesUtil;

/**
 * @author Scott Fines
 * Created on: 8/30/13
 */
public class MissingRowException extends RuntimeException {
    private static final long serialVersionUID = 1l;

    private byte[] missingRow;

    public MissingRowException() { }

    public MissingRowException(byte[] missingRow) {
        this.missingRow = missingRow;
    }

    public byte[] getMissingRow(){
        return missingRow;
    }

    @Override
    public String getMessage() {
        return "Missing Row: "+ BytesUtil.toHex(missingRow);
    }
}
