package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.iapi.PairDataSet;

/**
 * Created by jleach on 4/15/15.
 */
public class ControlDataSetTest extends AbstractDataSetTest {

    @Override
    protected DataSet<ExecRow> getTenRowsTwoDuplicateRecordsDataSet() {
        return new ControlDataSet<>(tenRowsTwoDuplicateRecords);
    }

}