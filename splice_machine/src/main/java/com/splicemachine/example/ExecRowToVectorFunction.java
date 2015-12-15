package com.splicemachine.example;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.mllib.linalg.Vector;

import java.io.*;

/**
 * Created by jleach on 12/15/15.
 */
public class ExecRowToVectorFunction implements Function<LocatedRow,Vector>, Externalizable{
    private int[] fieldsToConvert;
    public ExecRowToVectorFunction(int[] fieldsToConvert) {
        this.fieldsToConvert = fieldsToConvert;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // TODO Jun
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // TODO Jun
    }

    @Override
    public Vector call(LocatedRow locatedRow) throws Exception {
        return SparkMLibUtils.convertExecRowToVector(locatedRow.getRow(),fieldsToConvert);
    }
}
