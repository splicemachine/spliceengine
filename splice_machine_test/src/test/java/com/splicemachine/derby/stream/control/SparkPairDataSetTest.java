package com.splicemachine.derby.stream.control;

import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.spark.SpliceSpark;
import com.splicemachine.derby.stream.iapi.PairDataSet;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.stream.spark.SparkPairDataSet;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by jleach on 4/15/15.
 */
public class SparkPairDataSetTest extends AbstractPairDataSetTest {

    @Override
    protected PairDataSet<ExecRow, ExecRow> getTenRows() {
        List<Tuple2<ExecRow,ExecRow>> pairs = new ArrayList<Tuple2<ExecRow,ExecRow>>();
        for (ExecRow key: tenRows.keySet()) {
            for (ExecRow value: tenRows.get(key)) {
                pairs.add(new Tuple2<ExecRow, ExecRow>(key,value));
            }
        }
        return new SparkPairDataSet(SpliceSpark.getContext().parallelizePairs(pairs));
    }

    @Override
    protected PairDataSet<ExecRow, ExecRow> getEvenRows() {
        List<Tuple2<ExecRow,ExecRow>> pairs = new ArrayList<Tuple2<ExecRow,ExecRow>>();
        for (ExecRow key: evenRows.keySet()) {
            for (ExecRow value: evenRows.get(key)) {
                pairs.add(new Tuple2<ExecRow, ExecRow>(key,value));
            }
        }
        return new SparkPairDataSet(SpliceSpark.getContext().parallelizePairs(pairs));
    }
}