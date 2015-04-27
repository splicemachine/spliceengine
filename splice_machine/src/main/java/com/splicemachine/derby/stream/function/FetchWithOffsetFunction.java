package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.impl.sql.execute.operations.LocatedRow;
import org.apache.spark.api.java.function.Function;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

/**
 * Created by jleach on 4/27/15.
 */
public class FetchWithOffsetFunction implements Function<LocatedRow,Boolean>, Externalizable {
    protected int fetch;
    protected int offset;
    public FetchWithOffsetFunction(int fetch, int offset) {
        this.fetch = fetch;
        this.offset = offset;
    }


    @Override
    public Boolean call(LocatedRow locatedRow) throws Exception {

        return null;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(fetch);
        out.writeInt(offset);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        fetch = in.readInt();
        offset = in.readInt();
    }
}
