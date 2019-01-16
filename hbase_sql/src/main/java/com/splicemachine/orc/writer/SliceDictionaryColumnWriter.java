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

import com.splicemachine.art.SimpleART;
import com.splicemachine.art.tree.ART;
import com.splicemachine.orc.DictionaryCompressionOptimizer.DictionaryColumn;
import com.splicemachine.orc.array.IntBigArray;
import com.splicemachine.orc.block.ColumnBlock;
import com.splicemachine.orc.checkpoint.BooleanStreamCheckpoint;
import com.splicemachine.orc.checkpoint.LongStreamCheckpoint;
import com.splicemachine.orc.metadata.*;
import com.splicemachine.orc.metadata.Stream.StreamKind;
import com.splicemachine.orc.metadata.statistics.ColumnStatistics;
import com.splicemachine.orc.metadata.statistics.StringStatisticsBuilder;
import com.splicemachine.orc.stream.*;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.splicemachine.primitives.Bytes;
import io.airlift.slice.SliceOutput;
import io.airlift.slice.Slices;
import org.apache.spark.sql.types.DataType;
import org.openjdk.jol.info.ClassLayout;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import static com.splicemachine.orc.DictionaryCompressionOptimizer.estimateIndexBytesPerValue;
import static com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY;
import static com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind.DICTIONARY_V2;
import static com.splicemachine.orc.metadata.CompressionKind.NONE;
import static com.splicemachine.orc.metadata.Stream.StreamKind.DATA;
import static com.splicemachine.orc.stream.LongOutputStream.createLengthOutputStream;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkState;
import static java.lang.Math.toIntExact;
import static java.util.stream.Collectors.toList;
import static java.util.Objects.requireNonNull;

public class SliceDictionaryColumnWriter
        implements ColumnWriter, DictionaryColumn
{
    private static final int INSTANCE_SIZE = ClassLayout.parseClass(SliceDictionaryColumnWriter.class).instanceSize();
    private final int column;
    private final DataType type;
    private final CompressionKind compression;
    private final int bufferSize;
    private final boolean isDwrf;

    private final LongOutputStream dataStream;
    private final PresentOutputStream presentStream;
    private final ByteArrayOutputStream dictionaryDataStream;
    private final LongOutputStream dictionaryLengthStream;

    private final List<DictionaryRowGroup> rowGroups = new ArrayList<>();

    private IntBigArray values;
    private int valueCount;
    private StringStatisticsBuilder statisticsBuilder = new StringStatisticsBuilder();

    private long rawBytes;

    private boolean closed;
    private boolean inRowGroup;
    private ColumnEncoding columnEncoding;
    private boolean directEncoded;
    private SliceDirectColumnWriter directColumnWriter;
    private ART art;
    private int dictionaryIndex = 0;

    public SliceDictionaryColumnWriter(int column, DataType type, CompressionKind compression, int bufferSize, boolean isDwrf)
    {
        checkArgument(column >= 0, "column is negative");
        this.column = column;
        this.type = requireNonNull(type, "type is null");
        this.compression = requireNonNull(compression, "compression is null");
        this.bufferSize = bufferSize;
        this.isDwrf = isDwrf;
        LongOutputStream result;
        if (isDwrf) {
            result = new LongOutputStreamV1(compression, bufferSize, false, DATA);
        }
        else {
            result = new LongOutputStreamV2(compression, bufferSize, false, DATA);
        }
        this.dataStream = result;
        this.presentStream = new PresentOutputStream(compression, bufferSize);
        this.dictionaryDataStream = new ByteArrayOutputStream(compression, bufferSize, StreamKind.DICTIONARY_DATA);
        this.dictionaryLengthStream = createLengthOutputStream(compression, bufferSize, isDwrf);
        values = new IntBigArray();
        art = new SimpleART();
    }

    @Override
    public long getRawBytes()
    {
        checkState(!directEncoded);
        return rawBytes;
    }

    @Override
    public int getDictionaryBytes()
    {
        checkState(!directEncoded);
        return toIntExact(bufferSize);
    }

    @Override
    public int getValueCount()
    {
        return valueCount;
    }

    @Override
    public int getNonNullValueCount()
    {
        return toIntExact(statisticsBuilder.getNonNullValueCount());
    }

    @Override
    public int getDictionaryEntries() {
        return dictionaryIndex;
    }

    @Override
    public void convertToDirect()
    {
        checkState(!closed);
        checkState(!directEncoded);
        if (directColumnWriter == null) {
            directColumnWriter = new SliceDirectColumnWriter(column, type, compression, bufferSize, isDwrf, StringStatisticsBuilder::new);
        }
        for (DictionaryRowGroup rowGroup : rowGroups) {
            directColumnWriter.beginRowGroup();
            // todo we should be able to pass the stats down to avoid recalculating min and max
            writeDictionaryRowGroup(rowGroup.getValueCount(), rowGroup.getDictionaryIndexes());
            directColumnWriter.finishRowGroup();
        }
        if (inRowGroup) {
            directColumnWriter.beginRowGroup();
            writeDictionaryRowGroup(valueCount, values);
        }
        else {
            checkState(valueCount == 0);
        }

        rowGroups.clear();
        rawBytes = 0;
        valueCount = 0;
        statisticsBuilder = new StringStatisticsBuilder();

        directEncoded = true;
    }

    private void writeDictionaryRowGroup(int valueCount, IntBigArray dictionaryIndexes)
    {
        /*
        int[][] segments = dictionaryIndexes.getSegments();
        for (int i = 0; valueCount > 0 && i < segments.length; i++) {
            int[] segment = segments[i];
            int positionCount = Math.min(valueCount, segment.length);
            DictionaryBlock dictionaryBlock = new DictionaryBlock(positionCount, dictionary, segment);
            directColumnWriter.writeBlock(dictionaryBlock);
            valueCount -= positionCount;
        }
        checkState(valueCount == 0);
        */
    }

    @Override
    public Map<Integer, ColumnEncoding> getColumnEncodings()
    {
        checkState(closed);
        if (directEncoded) {
            return directColumnWriter.getColumnEncodings();
        }
        return ImmutableMap.of(column, columnEncoding);
    }

    @Override
    public void beginRowGroup()
    {
        checkState(!inRowGroup);
        inRowGroup = true;

        if (directEncoded) {
            directColumnWriter.beginRowGroup();
        }
    }

    @Override
    public void writeBlock(ColumnBlock block)
    {
        checkState(!closed);
        checkArgument(block.getPositionCount() > 0, "Block is empty");

        if (directEncoded) {
            directColumnWriter.writeBlock(block);
            return;
        }

        // record values
        values.ensureCapacity(valueCount + block.getPositionCount());
        for (int position = 0; position < block.getPositionCount(); position++) {
            byte[] value = block.getUTF8String(position).getBytes();
            ByteBuffer vb = art.get(value);
            int index;
            if (vb == null) {
                index = dictionaryIndex;
                art.insert(value, Bytes.toBytes(dictionaryIndex));
                dictionaryIndex++;
            } else {
                index = Bytes.toInt(vb.array(),vb.arrayOffset());
            }
            values.set(valueCount, index);
            valueCount++;

            if (!block.isNull(position)) {
                // todo min/max statistics only need to be updated if value was not already in the dictionary, but non-null count does
                statisticsBuilder.addValue(Slices.wrappedBuffer(value,0,value.length));
                rawBytes += value.length;
            }
        }
    }

    @Override
    public Map<Integer, ColumnStatistics> finishRowGroup()
    {
        checkState(!closed);
        checkState(inRowGroup);
        inRowGroup = false;

        if (directEncoded) {
            return directColumnWriter.finishRowGroup();
        }

        ColumnStatistics statistics = statisticsBuilder.buildColumnStatistics();
        rowGroups.add(new DictionaryRowGroup(values, valueCount, statistics));
        valueCount = 0;
        statisticsBuilder = new StringStatisticsBuilder();
        values = new IntBigArray();
        return ImmutableMap.of(column, statistics);
    }

    @Override
    public void close()
    {
        checkState(!closed);
        checkState(!inRowGroup);
        closed = true;
        if (directEncoded) {
            directColumnWriter.close();
        }
        else {
            bufferOutputData();
        }
    }

    @Override
    public Map<Integer, ColumnStatistics> getColumnStripeStatistics()
    {
        checkState(closed);
        if (directEncoded) {
            return directColumnWriter.getColumnStripeStatistics();
        }

        return ImmutableMap.of(column, ColumnStatistics.mergeColumnStatistics(rowGroups.stream()
                .map(DictionaryRowGroup::getColumnStatistics)
                .collect(toList())));
    }

    private void bufferOutputData()
    {
        checkState(closed);
        checkState(!directEncoded);

        int[] sortedDictionaryElements = new int[valueCount];
        Iterator<ByteBuffer[]> iterator = art.getRootIterator();
        int i = 0;
        while (iterator.hasNext()) {
            ByteBuffer[] kv = iterator.next();
            ByteBuffer key = kv[0];
            ByteBuffer value = kv[1];
            sortedDictionaryElements[Bytes.bytesToInt(value.array(),value.arrayOffset())] = i;
            dictionaryLengthStream.writeLong(key.array().length); // ?
            dictionaryDataStream.writeSlice(Slices.wrappedBuffer(key));
        }
        columnEncoding = new ColumnEncoding(isDwrf ? DICTIONARY : DICTIONARY_V2, dictionaryIndex - 1);

        if (!rowGroups.isEmpty()) {
            presentStream.recordCheckpoint();
            dataStream.recordCheckpoint();
        }
        for (DictionaryRowGroup rowGroup : rowGroups) {
            IntBigArray dictionaryIndexes = rowGroup.getDictionaryIndexes();
            for (int position = 0; position < rowGroup.getValueCount(); position++) {
                if (sortedDictionaryElements[position] != 0) {
                    presentStream.writeBoolean(true);
                    dataStream.writeLong(sortedDictionaryElements[position]);
                } else {
                    presentStream.writeBoolean(false);
                }

            }
            presentStream.recordCheckpoint();
            dataStream.recordCheckpoint();
        }

        // free the dictionary memory
        art.destroy();
        art = null;
        dictionaryDataStream.close();
        dictionaryLengthStream.close();

        dataStream.close();
        presentStream.close();
    }



    @Override
    public List<Stream> writeIndexStreams(SliceOutput outputStream, MetadataWriter metadataWriter)
            throws IOException
    {
        checkState(closed);

        if (directEncoded) {
            return directColumnWriter.writeIndexStreams(outputStream, metadataWriter);
        }

        ImmutableList.Builder<RowGroupIndex> rowGroupIndexes = ImmutableList.builder();

        List<LongStreamCheckpoint> dataCheckpoints = dataStream.getCheckpoints();
        Optional<List<BooleanStreamCheckpoint>> presentCheckpoints = presentStream.getCheckpoints();
        for (int i = 0; i < rowGroups.size(); i++) {
            int groupId = i;
            ColumnStatistics columnStatistics = rowGroups.get(groupId).getColumnStatistics();
            LongStreamCheckpoint dataCheckpoint = dataCheckpoints.get(groupId);
            Optional<BooleanStreamCheckpoint> presentCheckpoint = presentCheckpoints.map(checkpoints -> checkpoints.get(groupId));
            List<Integer> positions = createSliceColumnPositionList(compression != NONE, dataCheckpoint, presentCheckpoint);
            rowGroupIndexes.add(new RowGroupIndex(positions, columnStatistics));
        }

        int length = metadataWriter.writeRowIndexes(outputStream, rowGroupIndexes.build());
        return ImmutableList.of(new Stream(column, StreamKind.ROW_INDEX, length, false));
    }

    private static List<Integer> createSliceColumnPositionList(
            boolean compressed,
            LongStreamCheckpoint dataCheckpoint,
            Optional<BooleanStreamCheckpoint> presentCheckpoint)
    {
        ImmutableList.Builder<Integer> positionList = ImmutableList.builder();
        presentCheckpoint.ifPresent(booleanStreamCheckpoint -> positionList.addAll(booleanStreamCheckpoint.toPositionList(compressed)));
        positionList.addAll(dataCheckpoint.toPositionList(compressed));
        return positionList.build();
    }

    @Override
    public List<Stream> writeDataStreams(SliceOutput outputStream)
            throws IOException
    {
        checkState(closed);

        if (directEncoded) {
            return directColumnWriter.writeDataStreams(outputStream);
        }

        // actually write data
        ImmutableList.Builder<Stream> dataStreams = ImmutableList.builder();

        presentStream.writeDataStreams(column, outputStream).ifPresent(dataStreams::add);
        dataStream.writeDataStreams(column, outputStream).ifPresent(dataStreams::add);
        dictionaryLengthStream.writeDataStreams(column, outputStream).ifPresent(dataStreams::add);
        dictionaryDataStream.writeDataStreams(column, outputStream).ifPresent(dataStreams::add);
        return dataStreams.build();
    }

    @Override
    public long getBufferedBytes()
    {
        checkState(!closed);
        if (directEncoded) {
            return directColumnWriter.getBufferedBytes();
        }
        // for dictionary columns we report the data we expect to write to the output stream
        int indexBytes = estimateIndexBytesPerValue(dictionaryIndex) * getNonNullValueCount();
        return indexBytes + getDictionaryBytes();
    }

    @Override
    public long getRetainedBytes()
    {
        // NOTE: we do not include stats because they should be small and it would be annoying to calculate the size
        return INSTANCE_SIZE +
                values.sizeOf() +
                dataStream.getRetainedBytes() +
                presentStream.getRetainedBytes() +
                dictionaryDataStream.getRetainedBytes() +
                dictionaryLengthStream.getRetainedBytes() +
                bufferSize +
                (directColumnWriter == null ? 0 : directColumnWriter.getRetainedBytes());
    }

    @Override
    public void reset()
    {
        checkState(closed);
        closed = false;
        dataStream.reset();
        presentStream.reset();
        dictionaryDataStream.reset();
        dictionaryLengthStream.reset();
        rowGroups.clear();
        valueCount = 0;
        statisticsBuilder = new StringStatisticsBuilder();
        columnEncoding = null;
        if (art != null)
            art.destroy();
        art = new SimpleART();
        rawBytes = 0;
        dictionaryIndex = 0;
        if (directEncoded) {
            directEncoded = false;
            directColumnWriter.reset();
        }
    }

    private static class DictionaryRowGroup
    {
        private final IntBigArray dictionaryIndexes;
        private final int valueCount;
        private final ColumnStatistics columnStatistics;

        public DictionaryRowGroup(IntBigArray dictionaryIndexes, int valueCount, ColumnStatistics columnStatistics)
        {
            requireNonNull(dictionaryIndexes, "dictionaryIndexes is null");
            checkArgument(valueCount >= 0, "valueCount is negative");
            requireNonNull(columnStatistics, "columnStatistics is null");

            this.dictionaryIndexes = dictionaryIndexes;
            this.valueCount = valueCount;
            this.columnStatistics = columnStatistics;
        }

        public IntBigArray getDictionaryIndexes()
        {
            return dictionaryIndexes;
        }

        public int getValueCount()
        {
            return valueCount;
        }

        public ColumnStatistics getColumnStatistics()
        {
            return columnStatistics;
        }
    }
}

