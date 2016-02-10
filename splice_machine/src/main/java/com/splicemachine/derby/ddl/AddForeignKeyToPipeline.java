package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;

/**
 * Created by jleach on 2/2/16.
 */
public class AddForeignKeyToPipeline implements DDLAction {
    @Override
    public void accept(DDLMessage.DDLChange change) {
        if (change.getDdlChangeType() != DDLMessage.DDLChangeType.ADD_FOREIGN_KEY)
            return;

        long referencedConglomerateId = change.getTentativeFK().getReferencedConglomerateNumber();
        ContextFactoryLoader referencedFactory = PipelineDriver.driver().getContextFactoryLoader(referencedConglomerateId);

        long referencingConglomerateId = change.getTentativeFK().getReferencingConglomerateNumber();
        ContextFactoryLoader referencingFactory = PipelineDriver.driver().getContextFactoryLoader(referencingConglomerateId);

        try {
            referencedFactory.ddlChange(change);
            referencingFactory.ddlChange(change);
        } finally {
            referencedFactory.close();
            referencingFactory.close();
        }
    }
}