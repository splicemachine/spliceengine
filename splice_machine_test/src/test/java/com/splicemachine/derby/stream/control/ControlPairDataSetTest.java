package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import org.junit.Ignore;

/**
 * Created by jleach on 4/15/15.
 */

public class ControlPairDataSetTest extends AbstractPairDataSetTest {

    @Override
    protected PairDataSet<ExecRow, ExecRow> getTenRows() {
        return new ControlPairDataSet<>(tenRows);
    }

    @Override
    protected PairDataSet<ExecRow, ExecRow> getEvenRows() {
        return new ControlPairDataSet<ExecRow, ExecRow>(evenRows);
    }
}