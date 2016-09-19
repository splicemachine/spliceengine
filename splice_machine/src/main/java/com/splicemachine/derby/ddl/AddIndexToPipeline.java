/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.ddl;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.PipelineDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;

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
