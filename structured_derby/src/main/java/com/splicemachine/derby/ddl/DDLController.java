package com.splicemachine.derby.ddl;

import org.apache.derby.iapi.error.StandardException;

public interface DDLController {
    /**
     * @param change DDLChange to notify
     * @return change identifier for calling finishMetadataChange()
     * @throws StandardException
     */
    public String notifyMetadataChange(DDLChange change) throws StandardException;
    public void finishMetadataChange(String identifier) throws StandardException;
}
