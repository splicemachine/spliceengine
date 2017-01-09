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

package com.splicemachine.pipeline;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.pipeline.contextfactory.*;
import com.splicemachine.si.api.txn.Txn;
import java.io.IOException;
import java.util.Collections;
import java.util.Set;
import java.util.concurrent.CopyOnWriteArraySet;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class ManualContextFactoryLoader implements ContextFactoryLoader{
    private final Set<ConstraintFactory> constraints = new CopyOnWriteArraySet<>();
    private final WriteFactoryGroup indices = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final WriteFactoryGroup fk = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final WriteFactoryGroup ddl = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());

    @Override
    public void load(Txn txn) throws IOException, InterruptedException{
        //no-op, because we expect to manually add them
    }

    @Override
    public WriteFactoryGroup getForeignKeyFactories(){
        return fk;
    }

    @Override
    public WriteFactoryGroup getIndexFactories(){
        return indices;
    }

    @Override
    public WriteFactoryGroup getDDLFactories(){
        return ddl;
    }

    @Override
    public Set<ConstraintFactory> getConstraintFactories(){
        return constraints;
    }

    @Override
    public void ddlChange(DDLMessage.DDLChange ddlChange){
        throw new UnsupportedOperationException("IMPLEMENT");
    }

    @Override
    public void close(){
        //no-op
    }
}
