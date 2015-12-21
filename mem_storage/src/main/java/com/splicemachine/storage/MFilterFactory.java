package com.splicemachine.storage;

/**
 * @author Scott Fines
 *         Date: 12/16/15
 */
public class MFilterFactory implements DataFilterFactory{
    public static final DataFilterFactory INSTANCE= new MFilterFactory();

    private MFilterFactory(){}

    @Override
    public DataFilter singleColumnEqualsValueFilter(byte[] family,byte[] qualifier,byte[] value){
        throw new UnsupportedOperationException("IMPLEMENT");
    }
}
