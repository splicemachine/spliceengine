package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.WriteContextFactory;

/**
 * Created by dgomezferro on 11/24/15.
 */
public class AddIndexToPipeline implements DDLAction {
    @Override
    public void accept(DDLMessage.DDLChange change) {
        if (change.getDdlChangeType() != DDLMessage.DDLChangeType.CREATE_INDEX)
            return;

        long conglomerateId = change.getTentativeIndex().getTable().getConglomerate();
        ContextFactoryLoader cfl = PipelineDriver.driver().getContextFactoryLoader(conglomerateId);
        try {
            cfl.ddlChange(change);
        } finally {
            cfl.close();
        }

    }
}
