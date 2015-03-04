package com.splicemachine.derby.ddl;

import com.splicemachine.db.iapi.error.StandardException;

import com.splicemachine.pipeline.ddl.DDLChange;

public interface DDLController {

    /**
     * Notify remote nodes that a DDL change is in progress (but not yet committed).
     */
    public String notifyMetadataChange(DDLChange change) throws StandardException;

    /**
     * Notify remote nodes that the DDL change has been committed.
     */
    public void finishMetadataChange(String changeId) throws StandardException;

}
