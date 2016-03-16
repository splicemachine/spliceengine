package com.splicemachine.compactions;

import com.splicemachine.derby.iapi.sql.olap.OlapResult;
import com.splicemachine.olap.AbstractOlapResult;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.List;

/**
 * Created by dgomezferro on 3/16/16.
 */
public class CompactionResult extends AbstractOlapResult {
    private List<String> paths;

    public CompactionResult() {
    }

    public CompactionResult(List<String> paths) {
        this.paths = paths;
    }

    public List<String> getPaths() {
        return paths;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(paths);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        paths = (List<String>) in.readObject();
    }
}
