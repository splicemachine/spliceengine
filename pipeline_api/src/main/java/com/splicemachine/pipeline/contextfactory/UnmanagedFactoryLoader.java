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

package com.splicemachine.pipeline.contextfactory;

import com.splicemachine.ddl.DDLMessage;
import com.splicemachine.si.api.txn.TxnView;

import java.io.IOException;
import java.util.Collections;
import java.util.Set;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class UnmanagedFactoryLoader implements ContextFactoryLoader{
    public static final UnmanagedFactoryLoader INSTANCE = new UnmanagedFactoryLoader();

    private final WriteFactoryGroup fk = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final WriteFactoryGroup ddl = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final WriteFactoryGroup indices = new ListWriteFactoryGroup(Collections.<LocalWriteFactory>emptyList());
    private final Set<ConstraintFactory> constraints = Collections.emptySet();


    private UnmanagedFactoryLoader(){}
    @Override
    public void ddlChange(DDLMessage.DDLChange ddlChange){
        //no-op
    }

    @Override
    public void load(TxnView txn) throws IOException, InterruptedException{
        //no-op
    }

    @Override
    public void close(){

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
}
