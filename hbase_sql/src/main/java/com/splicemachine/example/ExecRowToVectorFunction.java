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

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;
import java.io.*;

/**
 *
 * Convert an ExecRow to a vector.  Vector is a common input for MLib libraries.
 *
 */
public class ExecRowToVectorFunction implements Function<ExecRow,Vector>, Externalizable{
    private int[] fieldsToConvert;

    public ExecRowToVectorFunction() {
    }

    public ExecRowToVectorFunction(int[] fieldsToConvert) {
        this.fieldsToConvert = fieldsToConvert;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        ArrayUtil.writeIntArray(out, fieldsToConvert);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fieldsToConvert = ArrayUtil.readIntArray(in);
    }

    @Override
    public Vector call(ExecRow locatedRow) throws Exception {
        return SparkMLibUtils.convertExecRowToVector(locatedRow,fieldsToConvert);
    }
}
