package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.iapi.DataSet;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import org.junit.Ignore;

/**
 * Created by jleach on 4/15/15.
 */
@Ignore("Broken now that we moved to yarn, need to revisit")
public class SparkDataSetTest extends AbstractDataSetTest {

    public SparkDataSetTest() {
        super();
    }

    @Override
    protected DataSet<ExecRow> getTenRowsTwoDuplicateRecordsDataSet() {
        return new SparkDataSet<ExecRow>(SpliceSpark.getContext().parallelize(tenRowsTwoDuplicateRecords));
    }

}