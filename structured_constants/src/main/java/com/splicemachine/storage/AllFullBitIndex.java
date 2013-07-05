package com.splicemachine.storage;

/**
 * BitIndex representing an all-full bitmap.
 *
 * @author Scott Fines
 * Created on: 7/5/13
 */
public class AllFullBitIndex implements BitIndex {
    @Override
    public int length() {
        return 0;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public boolean isSet(int pos) {
        return false;  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public byte[] encode() {
        return new byte[0];  //To change body of implemented methods use File | Settings | File Templates.
    }

    @Override
    public int cardinality() {
        return length();
    }
}
