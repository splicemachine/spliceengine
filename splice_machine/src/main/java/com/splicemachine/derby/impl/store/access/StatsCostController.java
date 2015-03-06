package com.splicemachine.derby.impl.store.access;

import org.apache.derby.iapi.error.StandardException;
import org.apache.derby.iapi.services.io.FormatableBitSet;
import org.apache.derby.iapi.sql.compile.CostEstimate;
import org.apache.derby.iapi.store.access.StoreCostController;
import org.apache.derby.iapi.store.access.StoreCostResult;
import org.apache.derby.iapi.types.DataValueDescriptor;
import org.apache.derby.iapi.types.RowLocation;
import org.apache.derby.impl.store.access.conglomerate.GenericController;

/**
 * A Cost Controller which uses underlying Statistics information to estimate the cost of a scan.
 *
 * @author Scott Fines
 *         Date: 3/4/15
 */
public class StatsCostController extends GenericController implements StoreCostController{

    @Override
    public void close() throws StandardException {

    }

    @Override
    public void getFetchFromRowLocationCost(FormatableBitSet validColumns, int access_type, CostEstimate cost) throws StandardException {

    }

    @Override
    public void getFetchFromFullKeyCost(FormatableBitSet validColumns, int access_type, CostEstimate cost) throws StandardException {

    }

    @Override
    public void getScanCost(int scan_type, long row_count, int group_size, boolean forUpdate, FormatableBitSet scanColumnList, DataValueDescriptor[] template, DataValueDescriptor[] startKeyValue, int startSearchOperator, DataValueDescriptor[] stopKeyValue, int stopSearchOperator, boolean reopen_scan, int access_type, StoreCostResult cost_result) throws StandardException {

    }

    @Override
    public void extraQualifierSelectivity(CostEstimate costEstimate) throws StandardException {

    }

    @Override
    public long getEstimatedRowCount() throws StandardException {
        return 0;
    }

    @Override
    public void setEstimatedRowCount(long count) throws StandardException {

    }
    @Override public RowLocation newRowLocationTemplate() throws StandardException { return null; }
}
