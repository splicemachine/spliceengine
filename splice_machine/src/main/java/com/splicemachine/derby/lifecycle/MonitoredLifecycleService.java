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

package com.splicemachine.derby.lifecycle;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.utils.DatabasePropertyManagementImpl;
import com.splicemachine.si.api.txn.TxnRegistry;
import com.splicemachine.si.impl.driver.SIDriver;
import com.splicemachine.si.impl.txn.ConcurrentTxnRegistry;
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

        TxnRegistry txnRegistry=SIDriver.driver().getTxnRegistry();
        if(txnRegistry instanceof ConcurrentTxnRegistry)
            ((ConcurrentTxnRegistry)txnRegistry).registerJmx(mbs);

        DatabasePropertyManagementImpl.registerJMX(mbs);
    }


    @Override
    public void shutdown() throws Exception{
        if(metricsReporter!=null)
            metricsReporter.shutdown();
        super.shutdown();
    }
}
