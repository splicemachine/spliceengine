package com.splicemachine.derby.utils.marshall;

import com.splicemachine.db.iapi.error.StandardException;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 11/18/13
 */
public class FixedDataHash<T> implements DataHash<T>{
    private final byte[] bytes;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public FixedDataHash(byte[] bytes){
        this.bytes=bytes;
    }

    @Override
    public void setRow(T rowToEncode){
    }

    @Override
    public KeyHashDecoder getDecoder(){
        return null;
    }

    @Override
    public byte[] encode() throws StandardException, IOException{
        return bytes;
    }

    @Override
    public void close() throws IOException{

    }
}
