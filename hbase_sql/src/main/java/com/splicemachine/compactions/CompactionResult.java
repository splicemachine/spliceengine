package com.splicemachine.compactions;

import com.splicemachine.olap.AbstractOlapResult;

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
    public boolean isSuccess(){
        return true;
    }
}
