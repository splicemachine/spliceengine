package com.splicemachine.derby.utils.marshall;

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;

/**
 * Prefix which always attaches the same byte[] to the front
 * of the hash.
 *
 * @author Scott Fines
 *         Date: 11/15/13
 */
public class FixedPrefix implements HashPrefix{
    private final byte[] prefix;

    @SuppressFBWarnings(value="EI_EXPOSE_REP2", justification="Intentional")
    public FixedPrefix(byte[] prefix){
        this.prefix=prefix;
    }

    @Override
    public int getPrefixLength(){
        return prefix.length;
    }

    @Override
    public void encode(byte[] bytes,int offset,byte[] hashBytes){
        System.arraycopy(prefix,0,bytes,offset,prefix.length);
    }

    @Override
    public void close() throws IOException{
    }
}
