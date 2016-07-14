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

import com.splicemachine.db.iapi.services.io.ArrayUtil;
import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;

import java.io.*;

/**
 * Created by jleach on 12/15/15.
 */
public class ExecRowToVectorFunction implements Function<LocatedRow,Vector>, Externalizable{
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
    public Vector call(LocatedRow locatedRow) throws Exception {
        return SparkMLibUtils.convertExecRowToVector(locatedRow.getRow(),fieldsToConvert);
    }
}
