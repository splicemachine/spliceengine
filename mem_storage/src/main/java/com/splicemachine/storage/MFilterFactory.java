package com.splicemachine.storage;

import java.io.IOException;

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

    @Override
    public DataFilter allocatedFilter(byte[] localAddress){
        //TODO -sf- implement?
        return new DataFilter(){
            @Override
            public ReturnCode filterCell(DataCell keyValue) throws IOException{
                return ReturnCode.INCLUDE;
            }

            @Override
            public boolean filterRow() throws IOException{
                return false;
            }

            @Override
            public void reset() throws IOException{

            }
        };
    }
}
