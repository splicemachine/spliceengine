package com.splicemachine.derby.stream.spark;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.spark.SparkPairDataSet;

/**
 * Created by jleach on 4/15/15.
 */

public class SparkPairDataSetTest extends AbstractPairDataSetTest {

    @Override
    protected PairDataSet<ExecRow, ExecRow> getTenRows() {
        return new SparkPairDataSet(SpliceSpark.getContext().parallelizePairs(tenRows));
    }

    @Override
    protected PairDataSet<ExecRow, ExecRow> getEvenRows() {
        return new SparkPairDataSet(SpliceSpark.getContext().parallelizePairs(evenRows));
    }
}