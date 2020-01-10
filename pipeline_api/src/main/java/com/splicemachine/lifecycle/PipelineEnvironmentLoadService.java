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
 *
 */

package com.splicemachine.lifecycle;

import com.splicemachine.pipeline.ContextFactoryDriverService;
import com.splicemachine.pipeline.PipelineEnvironment;
import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;

import javax.management.MBeanServer;
import java.io.IOException;

/**
 * @author Scott Fines
 *         Date: 1/12/16
 */
public abstract class PipelineEnvironmentLoadService implements DatabaseLifecycleService{

    protected PipelineEnvironment pipelineEnv;

    public PipelineEnvironmentLoadService(){
    }

    @Override
    public void start() throws Exception {
        ContextFactoryDriver cfDriver = ContextFactoryDriverService.loadDriver();
        pipelineEnv = loadPipelineEnvironment(cfDriver);
    }

    @Override
    public void shutdown() throws Exception{
    }

    protected abstract PipelineEnvironment loadPipelineEnvironment(ContextFactoryDriver cfDriver) throws IOException;

    @Override
    public void registerJMX(MBeanServer mbs) throws Exception{
        if(pipelineEnv!=null)
            pipelineEnv.getPipelineDriver().registerJMX(mbs);
    }
}
