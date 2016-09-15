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

package com.splicemachine.pipeline.api;

/**
 * Record write activity for accumulators.
 */
public interface RecordingContext {
    void recordRead();
    void recordRead(long w);
    void recordFilter();
    void recordFilter(long w);
    void recordWrite();
    void recordPipelineWrites(long w);
    void recordThrownErrorRows(long w);
    void recordRetriedRows(long w);
    void recordPartialRows(long w);
    void recordPartialThrownErrorRows(long w);
    void recordPartialRetriedRows(long w);
    void recordPartialIgnoredRows(long w);
    void recordPartialWrite(long w);
    void recordIgnoredRows(long w);
    void recordCatchThrownRows(long w);
    void recordCatchRetriedRows(long w);

    void recordRetry(long w);

    void recordProduced();

    void recordBadRecord(String badRecord, Exception exception);

    void recordRegionTooBusy(long w);
}
