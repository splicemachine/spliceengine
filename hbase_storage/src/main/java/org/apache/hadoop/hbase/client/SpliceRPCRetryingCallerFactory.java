package org.apache.hadoop.hbase.client;

import org.apache.hadoop.conf.Configuration;

/**
 * Created by jleach on 8/17/16.
 */
public class SpliceRPCRetryingCallerFactory extends RpcRetryingCallerFactory {

    public SpliceRPCRetryingCallerFactory(Configuration conf) {
        super(conf,new SpliceFailFastInterceptor());
    }

    public SpliceRPCRetryingCallerFactory(Configuration conf, RetryingCallerInterceptor interceptor) {
        super(conf, interceptor);
    }

    @Override
    public void setStatisticTracker(ServerStatisticTracker statisticTracker) {
        super.setStatisticTracker(statisticTracker);
    }

    @Override
    public <T> RpcRetryingCaller<T> newCaller() {
        return super.newCaller();
    }
}
