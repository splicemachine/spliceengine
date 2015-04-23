package com.splicemachine.derby.stream;

import com.splicemachine.db.iapi.sql.Activation;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.RowLocation;
import com.splicemachine.derby.iapi.sql.execute.SpliceOperation;
import com.splicemachine.derby.stream.control.ControlDataSetProcessor;
import com.splicemachine.derby.stream.spark.SparkDataSetProcessor;

/**
 * Created by jleach on 4/16/15.
 */
public class StreamUtils {

    public static <Op extends SpliceOperation> DataSetProcessor<Op,RowLocation,ExecRow> getDataSetProcessorFromActivation(Activation activation) {
        return new ControlDataSetProcessor<>(); //TODO JLEACH : Split
//        return new SparkDataSetProcessor<>(); //TODO JLEACH : Split
    }


}
