package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.stream.AbstractDataSetTest;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.si.testenv.ArchitectureIndependent;
import org.junit.experimental.categories.Category;

/**
 * Created by jleach on 4/15/15.
 */
@Category(ArchitectureIndependent.class)
public class ControlDataSetTest extends AbstractDataSetTest{

    @Override
    protected DataSet<ExecRow> getTenRowsTwoDuplicateRecordsDataSet() {
        return new ControlDataSet<>(tenRowsTwoDuplicateRecords);
    }

}