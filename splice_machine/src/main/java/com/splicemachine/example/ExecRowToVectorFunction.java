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
