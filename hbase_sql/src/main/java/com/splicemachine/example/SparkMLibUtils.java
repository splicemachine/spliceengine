/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.example;

import com.splicemachine.EngineDriver;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.stream.spark.SparkDataSet;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.DenseVector;
import org.apache.spark.mllib.linalg.Vector;
import com.splicemachine.db.iapi.sql.ResultSet;

/**
 *
 * Utils for converting to and from MLib structures.
 *
 */
public class SparkMLibUtils {

    public static JavaRDD<ExecRow> resultSetToRDD(ResultSet rs) throws StandardException {
        return ((SparkDataSet) ( (SpliceBaseOperation) rs).getDataSet(EngineDriver.driver().processorFactory().distributedProcessor())).rdd;
    }

    public static JavaRDD<Vector> locatedRowRDDToVectorRDD(JavaRDD<ExecRow> locatedRowJavaRDD, int[] fieldsToConvert) throws StandardException {
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
