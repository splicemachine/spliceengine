package com.splicemachine.derby.stream.spark;

import com.codahale.metrics.MetricRegistry;
import org.apache.spark.SparkEnv;
import org.apache.spark.metrics.source.Source;
import org.eclipse.jetty.servlet.ServletContextHandler;

/**
 * Created by jleach on 4/20/15.
 */
public class SpliceMachineSource implements Source {
    public static final String SPLICE_MACHINE = "com.splicemachine.derby.stream.spark.SpliceMachineSource";
    public static SpliceMachineSource instance;
    MetricRegistry registry;
    private SpliceMachineSource() {
        registry = new MetricRegistry();
        registry.meter("rows read");
        registry.meter("bytes read");
        instance = this;
    }

    @Override
    public String sourceName() {
        return SPLICE_MACHINE;
    }

    @Override
    public MetricRegistry metricRegistry() {
        return registry;
    }

    public static void register() {
        SparkEnv.get().metricsSystem().registerSource(new SpliceMachineSource());
    }

    public static SpliceMachineSource getMetrics() {
        return instance;
    }

}