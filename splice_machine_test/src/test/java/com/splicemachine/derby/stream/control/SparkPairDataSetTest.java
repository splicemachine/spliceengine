package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.stream.spark.SparkPairDataSet;
import org.junit.Ignore;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jleach on 4/15/15.
 */

@Ignore("DB-4354")
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