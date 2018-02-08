package com.splicemachine.derby.impl.storage;

import com.splicemachine.derby.iapi.sql.olap.AbstractOlapResult;
import com.splicemachine.derby.iapi.sql.olap.OlapResult;

import java.util.List;
import java.util.Map;

/**
 * Created by jyuan on 2/7/18.
 */
public class CheckTableResult extends AbstractOlapResult {

    private Map<String, List<String>> results;
    private boolean isSuccess = true;

    public CheckTableResult() {
    }

    public CheckTableResult(Map<String, List<String>> results) {
        this.results = results;
    }

    public void setSuccess(boolean isSuccess) {
        this.isSuccess = isSuccess;
    }

    @Override
    public boolean isSuccess(){
        return isSuccess;
    }

    public void setResults(Map<String, List<String>> results) {
        this.results = results;
    }

    public Map<String, List<String>> getResults() {
        return results;
    }
}
