package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;

/**
 * Created by jleach on 2/2/16.
 */
public class AddUniqueConstraintToPipeline implements DDLAction {
    @Override
    public void accept(DDLMessage.DDLChange change) {
        if (change.getDdlChangeType() != DDLMessage.DDLChangeType.ADD_UNIQUE_CONSTRAINT)
            return;
        long conglomerateId = change.getTentativeIndex().getTable().getConglomerate();
        try (ContextFactoryLoader cfl = PipelineDriver.driver().getContextFactoryLoader(conglomerateId)) {
            cfl.ddlChange(change);
        }

    }
}