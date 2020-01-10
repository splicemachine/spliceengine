/*
 * Copyright (c) 2012 - 2020 Splice Machine, Inc.
 *
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 */

package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;

/**
 * Created by jleach on 2/2/16.
 */
public class DropForeignKeyFromPipeline implements DDLAction {
    @Override
    public void accept(DDLMessage.DDLChange change) {
        if (change.getDdlChangeType() != DDLMessage.DDLChangeType.DROP_FOREIGN_KEY)
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
