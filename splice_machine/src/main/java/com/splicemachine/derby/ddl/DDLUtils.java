package com.splicemachine.derby.ddl;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.ddl.DDLChange;
import com.splicemachine.si.api.Txn;

/**
 * Created by jleach on 11/12/15.
 */
public class DDLUtils {

    public static DDLChange performMetadataChange(Txn tentativeTransaction, TentativeIndexDesc tentativeIndexDesc) throws StandardException {
        DDLChange ddlChange = new DDLChange(tentativeTransaction,
                DDLChangeType.CREATE_INDEX);
        ddlChange.setTentativeDDLDesc(tentativeIndexDesc);
        notifyMetadataChangeAndWait(ddlChange);
        return ddlChange;
    }

    public static String notifyMetadataChange(DDLChange change) throws StandardException {
        return DDLCoordinationFactory.getController().notifyMetadataChange(change);
    }

    public static void notifyMetadataChangeAndWait(DDLChange change) throws StandardException{
        String changeId = notifyMetadataChange(change);
        DDLCoordinationFactory.getController().finishMetadataChange(changeId);
    }


}
