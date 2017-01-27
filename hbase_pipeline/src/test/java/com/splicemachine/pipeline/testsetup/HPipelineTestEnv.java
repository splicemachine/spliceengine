/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.pipeline.testsetup;

import com.splicemachine.access.api.PartitionCreator;
import com.splicemachine.lifecycle.DatabaseLifecycleManager;
import com.splicemachine.pipeline.ContextFactoryDriverService;
import com.splicemachine.pipeline.ManualContextFactoryLoader;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.pipeline.client.WriteCoordinator;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;
import com.splicemachine.pipeline.contextfactory.ContextFactoryLoader;
import com.splicemachine.pipeline.contextfactory.ReferenceCountingFactoryDriver;
import com.splicemachine.si.testsetup.HBaseSITestEnv;
import com.splicemachine.derby.hbase.HBasePipelineEnvironment;
import com.splicemachine.derby.hbase.SpliceIndexEndpoint;
import com.splicemachine.derby.hbase.SpliceIndexObserver;
import org.apache.log4j.Level;

import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 12/23/15
 */
public class HPipelineTestEnv extends HBaseSITestEnv implements PipelineTestEnv{
    private final HBasePipelineEnvironment env;

    public HPipelineTestEnv() throws IOException{
        super(Level.WARN); //don't create SI tables, we'll manually add them once the driver is setup
        ContextFactoryDriver ctxFactoryLoader=new ReferenceCountingFactoryDriver(){
            @Override
            public ContextFactoryLoader newDelegate(long conglomerateId){
                return new ManualContextFactoryLoader();
            }
        };
        ContextFactoryDriverService.setDriver(ctxFactoryLoader);
        //pass in rsServices = null because we don't need the extra safety for these tests
        this.env = HBasePipelineEnvironment.loadEnvironment(super.getClock(),ctxFactoryLoader);
        DatabaseLifecycleManager.manager().start(); //start the database
    }

    @Override
    public WriteCoordinator writeCoordinator(){
        return env.getPipelineDriver().writeCoordinator();
    }

    @Override
    public ContextFactoryLoader contextFactoryLoader(long conglomerateId){
        return env.contextFactoryDriver().getLoader(conglomerateId);
    }

    @Override
    public PipelineExceptionFactory pipelineExceptionFactory(){
        return env.pipelineExceptionFactory();
    }

    @Override
    public void initialize() throws IOException{

    }

    @Override
    protected void addCoprocessors(PartitionCreator creator) throws IOException{
        creator.withCoprocessor(SpliceIndexEndpoint.class.getName())
                .withCoprocessor(SpliceIndexObserver.class.getName());
    }
}
