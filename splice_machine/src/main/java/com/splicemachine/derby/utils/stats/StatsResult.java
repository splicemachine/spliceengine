package com.splicemachine.derby.utils.stats;

import com.splicemachine.derby.iapi.sql.olap.AbstractOlapResult;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;

import java.util.List;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class StatsResult extends AbstractOlapResult {
    List<LocatedRow> rowList;

    public StatsResult(List<LocatedRow> rowList) {
        this.rowList = rowList;
    }

    public List<LocatedRow> getRowList() {
        return rowList;
    }

    @Override
    public boolean isSuccess() {
        return true;
    }
}
