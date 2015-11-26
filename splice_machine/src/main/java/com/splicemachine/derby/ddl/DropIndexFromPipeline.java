package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;
import com.splicemachine.pipeline.contextfactory.WriteContextFactoryManager;
//import com.splicemachine.si.api.TransactionalRegion;
//import com.splicemachine.si.api.TxnView;
//import com.splicemachine.si.impl.LazyTxnView;
//import com.splicemachine.si.impl.TransactionStorage;

/**
 * Created by dgomezferro on 11/24/15.
 */
public class DropIndexFromPipeline implements DDLAction {

    @Override
    public void accept(DDLMessage.DDLChange change) {
        // TODO (wjk) - uncomment this or delete it. Part of manual integration to k2_refactor
        if (true) return;

//        if (change.getDdlChangeType() != DDLMessage.DDLChangeType.DROP_INDEX_TRIGGER)
//            return;
//
//        long baseConglomId = change.getDropIndex().getBaseConglomerate();
//
//        WriteContextFactory<TransactionalRegion> writeContext = WriteContextFactoryManager.getWriteContext(baseConglomId);
//        try {
//            writeContext.addDDLChange(change);
//        } finally {
//            writeContext.close();
//        }
    }
}
