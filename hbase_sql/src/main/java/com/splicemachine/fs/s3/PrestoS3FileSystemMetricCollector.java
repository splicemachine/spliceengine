/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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
package com.splicemachine.fs.s3;

import splice.aws.com.amazonaws.Request;
import splice.aws.com.amazonaws.Response;
import splice.aws.com.amazonaws.metrics.RequestMetricCollector;
import splice.aws.com.amazonaws.util.AWSRequestMetrics;
import splice.aws.com.amazonaws.util.TimingInfo;
import io.airlift.units.Duration;

import static splice.aws.com.amazonaws.util.AWSRequestMetrics.Field.*;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.TimeUnit.MILLISECONDS;

public class PrestoS3FileSystemMetricCollector
        extends RequestMetricCollector
{
    private final PrestoS3FileSystemStats stats;

    public PrestoS3FileSystemMetricCollector(PrestoS3FileSystemStats stats)
    {
        this.stats = requireNonNull(stats, "stats is null");
    }

    @Override
    public void collectMetrics(Request<?> request, Response<?> response)
    {
        AWSRequestMetrics metrics = request.getAWSRequestMetrics();

        TimingInfo timingInfo = metrics.getTimingInfo();
        Number requestCounts = timingInfo.getCounter(RequestCount.name());
        Number retryCounts = timingInfo.getCounter(HttpClientRetryCount.name());
        Number throttleExceptions = timingInfo.getCounter(ThrottleException.name());
        TimingInfo requestTime = timingInfo.getSubMeasurement(HttpRequestTime.name());
        TimingInfo clientExecuteTime = timingInfo.getSubMeasurement(ClientExecuteTime.name());

        if (requestCounts != null) {
            stats.updateAwsRequestCount(requestCounts.longValue());
        }

        if (retryCounts != null) {
            stats.updateAwsRetryCount(retryCounts.longValue());
        }

        if (throttleExceptions != null) {
            stats.updateAwsThrottleExceptionsCount(throttleExceptions.longValue());
        }

        if (requestTime != null && requestTime.getTimeTakenMillisIfKnown() != null) {
            stats.addAwsRequestTime(new Duration(requestTime.getTimeTakenMillisIfKnown(), MILLISECONDS));
        }

        if (clientExecuteTime != null && clientExecuteTime.getTimeTakenMillisIfKnown() != null) {
            stats.addAwsClientExecuteTime(new Duration(clientExecuteTime.getTimeTakenMillisIfKnown(), MILLISECONDS));
        }
    }
}

