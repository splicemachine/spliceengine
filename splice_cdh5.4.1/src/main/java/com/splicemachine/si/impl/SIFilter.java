package com.splicemachine.si.impl;

import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.si.impl.filter.BaseSIFilter;
import com.sun.corba.se.spi.ior.Writeable;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.omg.CORBA_2_3.portable.OutputStream;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class SIFilter extends FilterBase implements Writeable {
    protected BaseSIFilter<Cell,ReturnCode> baseSIFilter;
    public SIFilter() {
    	super();
    }

    public SIFilter(TxnFilter txnFilter){
        baseSIFilter = new BaseSIFilter(txnFilter);
    }


    public boolean filterRow() {
        return baseSIFilter.filterRow();
    }

    public boolean hasFilterRow() {
        return baseSIFilter.hasFilterRow();
    }


    public void reset() {
        baseSIFilter.reset();
    }

    @Override
		@SuppressWarnings("unchecked")
    public ReturnCode filterKeyValue(Cell keyValue) {
    	return internalFilter(keyValue);
    }


    @Override
    public void write(OutputStream arg0) {
        throw new RuntimeException("should not be serialized");
    }
}