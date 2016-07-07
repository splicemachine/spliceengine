package com.splicemachine.derby.iapi.sql.olap;

/**
 * Created by dgomezferro on 6/15/16.
 */
public class SuccessfulOlapResult extends AbstractOlapResult {
    @Override
    public boolean isSuccess() {
        return true;
    }
}
