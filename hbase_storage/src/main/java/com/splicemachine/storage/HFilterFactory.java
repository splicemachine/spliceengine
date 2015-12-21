package com.splicemachine.storage;

import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.filter.CompareFilter;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterList;
import org.apache.hadoop.hbase.filter.SingleColumnValueFilter;

/**
 * @author Scott Fines
 *         Date: 12/18/15
 */
public class HFilterFactory implements DataFilterFactory{

    public static final DataFilterFactory INSTANCE=new HFilterFactory();

    private HFilterFactory(){}

    @Override
    public void addSingleColumnEqualsValueFilter(DataScan scan,byte[] family,byte[] qualifier,byte[] value){
        SingleColumnValueFilter scvf = new SingleColumnValueFilter(family,qualifier,CompareFilter.CompareOp.EQUAL,value);
        Scan hScan=((HScan)scan).unwrapDelegate();
        Filter toAdd = combineFilters(hScan.getFilter(),scvf);
        hScan.setFilter(toAdd);
    }

    /* ***************************************************************************************************************/
    /*private helper methods*/
    private Filter combineFilters(Filter filter,Filter scvf){
        if(filter!=null){
            FilterList fl;
            if(filter instanceof FilterList){
                fl = (FilterList)filter;
            }else{
                fl = new FilterList(filter);
            }
            fl.addFilter(scvf);
            return fl;
        }
        return scvf;
    }
}
