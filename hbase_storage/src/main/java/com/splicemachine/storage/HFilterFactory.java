package com.splicemachine.storage;

import com.splicemachine.derby.hbase.AllocatedFilter;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HFilterFactory implements DataFilterFactory{

    public static final DataFilterFactory INSTANCE=new HFilterFactory();

    private HFilterFactory(){}

    @Override
    public DataFilter singleColumnEqualsValueFilter(byte[] family,byte[] qualifier,byte[] value){
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(family,qualifier,CompareFilter.CompareOp.EQUAL,value);
        return new HFilterWrapper(scvf);
    }

    @Override
    public DataFilter allocatedFilter(byte[] localAddress){
        return new HFilterWrapper(new AllocatedFilter(localAddress));
    }
}
