/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splicemachine.fs.s3;

import splice.aws.com.amazonaws.Request;
import splice.aws.com.amazonaws.Response;
import splice.aws.com.amazonaws.metrics.RequestMetricCollector;
import splice.aws.com.amazonaws.util.AWSRequestMetrics;
import splice.aws.com.amazonaws.util.TimingInfo;
import io.airlift.units.Duration;
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
        Number requestCounts = timingInfo.getCounter(AWSRequestMetrics.Field.RequestCount.name());
        Number retryCounts = timingInfo.getCounter(AWSRequestMetrics.Field.HttpClientRetryCount.name());
        Number throttleExceptions = timingInfo.getCounter(AWSRequestMetrics.Field.ThrottleException.name());
        TimingInfo requestTime = timingInfo.getSubMeasurement(AWSRequestMetrics.Field.HttpRequestTime.name());
        TimingInfo clientExecuteTime = timingInfo.getSubMeasurement(AWSRequestMetrics.Field.ClientExecuteTime.name());

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
