package com.splicemachine.compactions;

import com.splicemachine.olap.AbstractOlapResult;

import java.util.List;

/**
 * Created by dgomezferro on 3/16/16.
 */
public class CompactionResult extends AbstractOlapResult {
    private List<String> paths;
    private int id;

    public CompactionResult() {
    }

    public CompactionResult(List<String> paths, int id) {
        this.paths = paths;
        this.id = id;
    }

    public List<String> getPaths() {
        return paths;
    }
    public int getId() { return id; }

    @Override
    public boolean isSuccess(){
        return true;
    }
}
