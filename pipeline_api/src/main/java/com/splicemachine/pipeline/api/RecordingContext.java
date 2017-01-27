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
