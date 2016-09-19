/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

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
