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
