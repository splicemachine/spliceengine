package com.splicemachine.derby.ddl;

import org.apache.derby.iapi.error.StandardException;

public interface DDLController {

    /**
     * Notify remote nodes that a DDL change is in progress (by not yet committed).
     */
    public String notifyMetadataChange(DDLChange change) throws StandardException;

    /**
     * Notify remote nodes that the DDL change has been committed.
     */
    public void finishMetadataChange(String identifier) throws StandardException;


}
