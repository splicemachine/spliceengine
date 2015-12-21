package com.splicemachine.storage;

import org.apache.hadoop.hbase.filter.Filter;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/17/15
 */
public class HFilterWrapper implements DataFilter{
    private Filter hbaseFilter;

    public HFilterWrapper(Filter hbaseFilter){
        this.hbaseFilter=hbaseFilter;
    }

    @Override
    public ReturnCode filterKeyValue(DataCell keyValue) throws IOException{
        /*
         * This is generally not used. The FilterWrapper class is mainly present to provide API
         * compatibility. Still, just in case it IS used, we implement it.
         */
        assert keyValue instanceof HCell: "Programmer error: incorrect type for filter!";
        org.apache.hadoop.hbase.filter.Filter.ReturnCode rc = hbaseFilter.filterKeyValue(((HCell)keyValue).unwrapDelegate());
        switch(rc){
            case NEXT_ROW:
                return ReturnCode.NEXT_ROW;
            case INCLUDE:
                return ReturnCode.INCLUDE;
            case INCLUDE_AND_NEXT_COL:
                return ReturnCode.INCLUDE_AND_NEXT_COL;
            case NEXT_COL:
                return ReturnCode.NEXT_COL;
            case SEEK_NEXT_USING_HINT:
                return ReturnCode.SEEK;
            case SKIP:
                return ReturnCode.SKIP;
            default:
                throw new IllegalStateException("Unexpected ReturnCode "+ rc+"!");
        }
    }

    @Override
    public boolean filterRow() throws IOException{
        return hbaseFilter.filterRow();
    }

    @Override
    public void reset() throws IOException{
        hbaseFilter.reset();
    }

    public Filter unwrapDelegate(){
        return hbaseFilter;
    }
}
