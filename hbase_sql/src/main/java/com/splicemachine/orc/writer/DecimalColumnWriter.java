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
import com.splicemachine.orc.checkpoint.BooleanStreamCheckpoint;
import com.splicemachine.orc.checkpoint.DecimalStreamCheckpoint;
import com.splicemachine.orc.checkpoint.LongStreamCheckpoint;
import com.splicemachine.orc.metadata.*;
import com.splicemachine.orc.metadata.Stream.StreamKind;
import com.splicemachine.orc.metadata.statistics.ColumnStatistics;
import com.splicemachine.orc.metadata.statistics.LongDecimalStatisticsBuilder;
import com.splicemachine.orc.metadata.statistics.ShortDecimalStatisticsBuilder;
import com.splicemachine.orc.reader.DecimalStreamReader;
import com.splicemachine.orc.stream.DecimalOutputStream;
import com.splicemachine.orc.stream.LongOutputStream;
import com.splicemachine.orc.stream.LongOutputStreamV2;
import com.splicemachine.orc.stream.PresentOutputStream;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.splicemachine.orc.types.Decimals;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.Decimal;
import org.apache.spark.sql.types.DecimalType;
import org.openjdk.jol.info.ClassLayout;
import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind.DIRECT_V2;
import static com.splicemachine.orc.metadata.CompressionKind.NONE;
import static com.splicemachine.orc.metadata.Stream.StreamKind.SECONDARY;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public class DecimalColumnWriter
        implements ColumnWriter
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(DecimalColumnWriter.class).instanceSize();
    private final int column;
    private final DecimalType type;
    private final ColumnEncoding columnEncoding;
    private final boolean compressed;
    private final DecimalOutputStream dataStream;
    private final LongOutputStream scaleStream;
    private final PresentOutputStream presentStream;

    private final List<ColumnStatistics> rowGroupColumnStatistics = new ArrayList<>();

    private ShortDecimalStatisticsBuilder shortDecimalStatisticsBuilder;
    private LongDecimalStatisticsBuilder longDecimalStatisticsBuilder;

    private boolean closed;

    public DecimalColumnWriter(int column, DataType type, CompressionKind compression, int bufferSize, boolean isDwrf)
    {
        checkArgument(column >= 0, "column is negative");
        checkArgument(!isDwrf, "DWRF does not support %s type", type);
        this.column = column;
        this.type = (DecimalType) requireNonNull(type, "type is null");
        this.compressed = requireNonNull(compression, "compression is null") != NONE;
        this.columnEncoding = new ColumnEncoding(DIRECT_V2, 0);
        this.dataStream = new DecimalOutputStream(compression, bufferSize);
        this.scaleStream = new LongOutputStreamV2(compression, bufferSize, true, SECONDARY);
        this.presentStream = new PresentOutputStream(compression, bufferSize);
        if (DecimalStreamReader.inLong(this.type)) {
            shortDecimalStatisticsBuilder = new ShortDecimalStatisticsBuilder(this.type.scale());
        }
        else {
            longDecimalStatisticsBuilder = new LongDecimalStatisticsBuilder();
        }
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        return ImmutableMap.of(column, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        presentStream.recordCheckpoint();
        dataStream.recordCheckpoint();
        scaleStream.recordCheckpoint();
    }

    @Override
    public void writeBlock(ColumnBlock block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        // record nulls
        for (int position = 0; position < block.getPositionCount(); position++) {
            presentStream.writeBoolean(!block.isNull(position));
        }

        // record values
        if (DecimalStreamReader.inLong(type)) {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    long value = block.getLong(position);
                    dataStream.writeUnscaledValue(value);
                    shortDecimalStatisticsBuilder.addValue(value);
                }
            }
        }
        else {
            for (int position = 0; position < block.getPositionCount(); position++) {
                if (!block.isNull(position)) {
                    Decimal decimal = block.getDecimal(position,type.precision(),type.scale());
                    Slice value = Decimals.encodeScaledValue(decimal.toJavaBigDecimal()); // Slow? TODO JL
                    dataStream.writeUnscaledValue(value);
                    longDecimalStatisticsBuilder.addValue(new BigDecimal(Decimals.decodeUnscaledValue(value), type.scale()));
                }
            }
        }
        for (int position = 0; position < block.getPositionCount(); position++) {
            if (!block.isNull(position)) {
                scaleStream.writeLong(type.scale());
            }
        }
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        ColumnStatistics statistics;
        if (DecimalStreamReader.inLong(type)) {
            statistics = shortDecimalStatisticsBuilder.buildColumnStatistics();
            shortDecimalStatisticsBuilder = new ShortDecimalStatisticsBuilder(type.scale());
        }
        else {
            statistics = longDecimalStatisticsBuilder.buildColumnStatistics();
            longDecimalStatisticsBuilder = new LongDecimalStatisticsBuilder();
        }
        rowGroupColumnStatistics.add(statistics);

        return ImmutableMap.of(column, statistics);
    }

    @Override
    public void close()
    {
        closed = true;
        dataStream.close();
        scaleStream.close();
        presentStream.close();
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        return ImmutableMap.of(column, ColumnStatistics.mergeColumnStatistics(rowGroupColumnStatistics));
    }

    @Override
    public List<Stream> writeIndexStreams(SliceOutput outputStream, MetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<DecimalStreamCheckpoint> dataCheckpoints = dataStream.getCheckpoints();
        List<LongStreamCheckpoint> scaleCheckpoints = scaleStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroupColumnStatistics.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroupColumnStatistics.get(groupId);
            DecimalStreamCheckpoint dataCheckpoint = dataCheckpoints.get(groupId);
            LongStreamCheckpoint scaleCheckpoint = scaleCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createDecimalColumnPositionList(compressed, dataCheckpoint, scaleCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        int length = metadataWriter.writeRowIndexes(outputStream, rowGroupIndexes.build());
        return ImmutableList.of(new Stream(column, StreamKind.ROW_INDEX, length, false));
    }

    private static List<Integer> createDecimalColumnPositionList(
            boolean compressed,
            DecimalStreamCheckpoint dataCheckpoint,
            LongStreamCheckpoint scaleCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(dataCheckpoint.toPositionList(compressed));
        positionList.addAll(scaleCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<Stream> writeDataStreams(SliceOutput outputStream)
            throws IOException
    {
        checkState(closed);

        ImmutableList.Builder<Stream> dataStreams = ImmutableList.builder();
        presentStream.writeDataStreams(column, outputStream).ifPresent(dataStreams::add);
        dataStream.writeDataStreams(column, outputStream).ifPresent(dataStreams::add);
        scaleStream.writeDataStreams(column, outputStream).ifPresent(dataStreams::add);
        return dataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        return dataStream.getBufferedBytes() + scaleStream.getBufferedBytes() + presentStream.getBufferedBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include stats because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE + dataStream.getRetainedBytes() + scaleStream.getRetainedBytes() + presentStream.getRetainedBytes();
    }

    @Override
    public void reset()
    {
        closed = false;
        dataStream.reset();
        scaleStream.reset();
        presentStream.reset();
        rowGroupColumnStatistics.clear();
        shortDecimalStatisticsBuilder = new ShortDecimalStatisticsBuilder(this.type.scale());
        longDecimalStatisticsBuilder = new LongDecimalStatisticsBuilder();
    }
}

