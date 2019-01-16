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
package com.splicemachine.orc.writer;

import com.splicemachine.orc.block.ColumnBlock;
import com.splicemachine.orc.metadata.ColumnEncoding;
import com.splicemachine.orc.metadata.MetadataWriter;
import com.splicemachine.orc.metadata.Stream;
import com.splicemachine.orc.metadata.statistics.ColumnStatistics;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.SliceOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

public interface ColumnWriter
{
    default List<ColumnWriter> getNestedColumnWriters()
    {
        return ImmutableList.of();
    }

    Map<Integer, ColumnEncoding> getColumnEncodings();

    void beginRowGroup();

    void writeBlock(ColumnBlock block);

    Map<Integer, ColumnStatistics> finishRowGroup();

    void close();

    Map<Integer, ColumnStatistics> getColumnStripeStatistics();

    /**
     * Write index streams to the output and return the streams in the
     * order in which they were written.  The ordering is critical because
     * the stream only contain a length with no offset.
     */
    List<Stream> writeIndexStreams(SliceOutput outputStream, MetadataWriter metadataWriter)
            throws IOException;

    /**
     * Write data streams to the output and return the streams in the
     * order in which they were written.  The ordering is critical because
     * the stream only contain a length with no offset.
     */
    List<Stream> writeDataStreams(SliceOutput outputStream)
            throws IOException;

    long getBufferedBytes();

    long getRetainedBytes();

    void reset();
}

