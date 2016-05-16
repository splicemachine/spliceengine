package com.splicemachine.example;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import com.splicemachine.derby.stream.utils.StreamUtils;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;

import com.splicemachine.db.iapi.sql.ResultSet;

/**
 * Created by jleach on 12/15/15.
 */
public class SparkMLibUtils {

    public static JavaRDD<LocatedRow> resultSetToRDD(ResultSet rs) throws StandardException {
        return ((SparkDataSet) ( (SpliceBaseOperation) rs).getDataSet(EngineDriver.driver().processorFactory().distributedProcessor())).rdd;
    }

    public static JavaRDD<Vector> locatedRowRDDToVectorRDD(JavaRDD<LocatedRow> locatedRowJavaRDD, int[] fieldsToConvert) throws StandardException {
        return locatedRowJavaRDD.map(new ExecRowToVectorFunction(fieldsToConvert));
    }

    /**
     *
     * ExecRow is one-based as far as the elements
     *
     * @param execRow
     * @param fieldsToConvert
     * @return
     */
    public static Vector convertExecRowToVector(ExecRow execRow,int[] fieldsToConvert) throws StandardException {
        double[] vectorValues = new double[fieldsToConvert.length];
        for (int i=0;i<fieldsToConvert.length;i++) {
            vectorValues[i] = execRow.getColumn(fieldsToConvert[i]).getDouble();
        }
        return new DenseVector(vectorValues);
    }

    /**
     *
     * ExecRow is one-based as far as the elements
     *
     * @param execRow
     * @return
     */
    public static Vector convertExecRowToVector(ExecRow execRow) throws StandardException {
        int length = execRow.nColumns();
        double[] vectorValues = new double[length];
        for (int i=1;i<=length;i++) {
            vectorValues[i] = execRow.getColumn(i).getDouble();
        }
        return new DenseVector(vectorValues);
    }


}
