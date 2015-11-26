package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;
//import com.splicemachine.pipeline.writecontextfactory.WriteContextFactory;
//import com.splicemachine.pipeline.writecontextfactory.WriteContextFactoryManager;

/**
 * Created by dgomezferro on 11/24/15.
 */
public class AddIndexToPipeline implements DDLAction {
    @Override
    public void accept(DDLMessage.DDLChange change) {
        // TODO (wjk) - uncomment this or delete it. Part of manual integration to k2_refactor
        if (true) return;

//        if (change.getDdlChangeType() != DDLMessage.DDLChangeType.CREATE_INDEX)
//            return;
//
//        long conglomerateId = change.getTentativeIndex().getTable().getConglomerate();
//        WriteContextFactory contextFactory = WriteContextFactoryManager.getWriteContext(conglomerateId);
//        try {
//            contextFactory.addDDLChange(change);
//        }finally{
//            contextFactory.close();
//        }

    }
}
