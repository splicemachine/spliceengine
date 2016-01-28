package com.splicemachine.example;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;

/**
 * Created by jleach on 12/15/15.
 */
public class ExecRowUtils {

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

}