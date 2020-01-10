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
