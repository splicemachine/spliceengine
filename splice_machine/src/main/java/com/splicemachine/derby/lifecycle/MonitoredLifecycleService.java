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

package com.splicemachine.derby.lifecycle;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.utils.DatabasePropertyManagementImpl;
import com.yammer.metrics.core.MetricsRegistry;
import com.yammer.metrics.reporting.JmxReporter;

import javax.management.MBeanServer;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class MonitoredLifecycleService extends EngineLifecycleService{

    private JmxReporter metricsReporter;
    private MetricsRegistry metricsRegistry = new MetricsRegistry();

    public MonitoredLifecycleService(DistributedDerbyStartup startup,
                                     SConfiguration configuration){
        super(startup, configuration);
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws Exception{
        super.registerJMX(mbs);
        metricsReporter = new JmxReporter(metricsRegistry);
        metricsReporter.start();

        DatabasePropertyManagementImpl.registerJMX(mbs);
    }


    @Override
    public void shutdown() throws Exception{
        if(metricsReporter!=null)
            metricsReporter.shutdown();
        super.shutdown();
    }
}
