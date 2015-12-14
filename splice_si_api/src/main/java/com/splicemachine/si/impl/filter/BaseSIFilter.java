package com.splicemachine.si.impl.filter;

import com.splicemachine.si.api.filter.TxnFilter;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;
import java.io.IOException;

/**
 * An HBase filter that applies SI logic when reading data values.
 */
public class BaseSIFilter<Data,ReturnCode> {
    private static Logger LOG = Logger.getLogger(BaseSIFilter.class);
    private TxnFilter<Data,ReturnCode> filterState = null;

    public BaseSIFilter() {
    }

    public BaseSIFilter(TxnFilter<Data,ReturnCode> txnFilter) {
        this.filterState = txnFilter;
    }

    public ReturnCode internalFilter(Data keyValue) {
        if (LOG.isTraceEnabled()) {
            SpliceLogUtils.trace(LOG, "filterKeyValue %s", keyValue);
        }
        try {
            return filterState.filterKeyValue(keyValue);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }


    public boolean filterRow() {
        return filterState.getExcludeRow();
    }

    public boolean hasFilterRow() {
        return true;
    }


    public void reset() {
        if (filterState != null) {
            filterState.nextRow();
        }
    }

}
