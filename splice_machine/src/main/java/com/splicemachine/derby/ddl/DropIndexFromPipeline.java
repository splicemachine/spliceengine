package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;

/**
 * Created by dgomezferro on 11/24/15.
 */
public class DropIndexFromPipeline implements DDLAction {

    @Override
    public void accept(DDLMessage.DDLChange change) {
        if (change.getDdlChangeType() != DDLMessage.DDLChangeType.DROP_INDEX)
            return;

        long baseConglomId = change.getDropIndex().getBaseConglomerate();
        ContextFactoryLoader cfl = PipelineDriver.driver().getContextFactoryLoader(baseConglomId);
        try {
            cfl.ddlChange(change);
        } finally {
            cfl.close();
        }
    }
}
