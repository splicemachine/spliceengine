package com.splicemachine.derby.lifecycle;

import com.splicemachine.access.api.SConfiguration;
import com.splicemachine.derby.utils.DatabasePropertyManagementImpl;
import com.splicemachine.utils.logging.LogManager;
import com.splicemachine.utils.logging.Logging;
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
    private Logging logging;

    public MonitoredLifecycleService(DistributedDerbyStartup startup,
                                     SConfiguration configuration){
        super(startup, configuration);
    }

    @Override
    public void registerJMX(MBeanServer mbs) throws Exception{
        super.registerJMX(mbs);
        metricsReporter = new JmxReporter(metricsRegistry);
        metricsReporter.start();

        logging = new LogManager();
        DatabasePropertyManagementImpl.registerJMX(mbs);
    }


    @Override
    public void shutdown() throws Exception{
        if(metricsReporter!=null)
            metricsReporter.shutdown();
        super.shutdown();
    }
}
