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
package com.splicemachine.orc;

import com.splicemachine.orc.OrcWriteValidation.WriteChecksum;
import com.splicemachine.orc.OrcWriteValidation.WriteChecksumBuilder;
import com.splicemachine.orc.block.ColumnBlock;
import com.splicemachine.orc.block.LazyColumnBlock;
import com.splicemachine.orc.block.LazyIncludedColumnBlockLoaderImpl;
import com.splicemachine.orc.block.LazyPartitionColumnBlockLoaderImpl;
import com.splicemachine.orc.memory.AbstractAggregatedMemoryContext;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.ColumnEncoding;
import com.splicemachine.orc.metadata.MetadataReader;
import com.splicemachine.orc.metadata.OrcType;
import com.splicemachine.orc.metadata.OrcType.OrcTypeKind;
import com.splicemachine.orc.metadata.PostScript.HiveWriterVersion;
import com.splicemachine.orc.metadata.StripeInformation;
import com.splicemachine.orc.metadata.statistics.ColumnStatistics;
import com.splicemachine.orc.metadata.statistics.StripeStatistics;
import com.splicemachine.orc.reader.StreamReader;
import com.splicemachine.orc.reader.StreamReaders;
import com.splicemachine.orc.stream.StreamSources;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Predicate;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.airlift.units.DataSize;
import org.apache.spark.memory.MemoryMode;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.execution.vectorized.ColumnarBatch;
import org.apache.spark.sql.types.DataType;
import org.apache.spark.sql.types.StructField;
import org.apache.spark.sql.types.StructType;
import org.joda.time.DateTimeZone;

import java.io.Closeable;
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

import static com.splicemachine.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static com.splicemachine.orc.OrcReader.MAX_BATCH_SIZE;
import static com.splicemachine.orc.OrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static com.splicemachine.orc.OrcWriteValidation.WriteChecksumBuilder.createWriteChecksumBuilder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.*;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;

public class OrcRecordReader
        implements Closeable
{
    public static boolean allowCaching = true;
    private final OrcDataSource orcDataSource;

    private final StreamReader[] streamReaders;
    private final long[] maxBytesPerCell;
    private long maxCombinedBytesPerRow;

    private final long totalRowCount;
    private final long splitLength;
    private final Set<Integer> presentColumns;
    private final long maxBlockBytes;
    private final Map<Integer, DataType> includedColumns;
    private long currentPosition;
    private long currentStripePosition;
    private int currentBatchSize;
    private int maxBatchSize = MAX_BATCH_SIZE;

    private final List<StripeInformation> stripes;
    private final StripeReader stripeReader;
    private int currentStripe = -1;
    private AggregatedMemoryContext currentStripeSystemMemoryContext;

    private final long fileRowCount;
    private final List<Long> stripeFilePositions;
    private long filePosition;

    private Iterator<RowGroup> rowGroups = ImmutableList.<RowGroup>of().iterator();
    private long currentGroupRowCount;
    private long nextRowInGroup;

    private final Map<String, Slice> userMetadata;

    private final AbstractAggregatedMemoryContext systemMemoryUsage;

    private final Optional<OrcWriteValidation> writeValidation;
    private final Optional<WriteChecksumBuilder> writeChecksumBuilder;

    protected List<String> partitionValues;
    protected List<Integer> partitionIds;

    public OrcRecordReader(
            Map<Integer, DataType> includedColumns,
            OrcPredicate predicate,
            long numberOfRows,
            List<StripeInformation> fileStripes,
            List<ColumnStatistics> fileStats,
            List<StripeStatistics> stripeStats,
            OrcDataSource orcDataSource,
            long splitOffset,
            long splitLength,
            List<OrcType> types,
            Optional<OrcDecompressor> decompressor,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            DataSize maxMergeDistance,
            DataSize maxReadSize,
            DataSize maxBlockSize,
            Map<String, Slice> userMetadata,
            AbstractAggregatedMemoryContext systemMemoryUsage,
            Optional<OrcWriteValidation> writeValidation,
            List<Integer> partitionIds,
            List<String> partitionValues)
            throws IOException
    {
        requireNonNull(includedColumns, "includedColumns is null");
        requireNonNull(predicate, "predicate is null");
        requireNonNull(fileStripes, "fileStripes is null");
        requireNonNull(stripeStats, "stripeStats is null");
        requireNonNull(orcDataSource, "orcDataSource is null");
        requireNonNull(types, "types is null");
        requireNonNull(decompressor, "decompressor is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(userMetadata, "userMetadata is null");

        this.includedColumns = requireNonNull(includedColumns, "includedColumns is null");
        this.writeValidation = requireNonNull(writeValidation, "writeValidation is null");
        this.writeChecksumBuilder = writeValidation.map(validation -> createWriteChecksumBuilder(includedColumns));
        this.partitionValues = partitionValues;
        this.partitionIds = partitionIds;
        // reduce the included columns to the set that is also present
        ImmutableSet.Builder<Integer> presentColumns = ImmutableSet.builder();
        ImmutableMap.Builder<Integer, DataType> presentColumnsAndTypes = ImmutableMap.builder();
        OrcType root = types.get(0);
        int partitionDecrement = 0;
        for (Map.Entry<Integer, DataType> entry : includedColumns.entrySet()) {
            entry.getKey();
            // an old file can have less columns since columns can be added
            // after the file was written
            if (!partitionIds.contains(entry.getKey())) {
                presentColumns.add(entry.getKey()-partitionDecrement);
            }
            presentColumnsAndTypes.put(entry.getKey()-partitionDecrement, entry.getValue());

        }
        this.presentColumns = presentColumns.build();

        this.maxBlockBytes = requireNonNull(maxBlockSize, "maxBlockSize is null").toBytes();

        // it is possible that old versions of orc use 0 to mean there are no row groups
        checkArgument(rowsInRowGroup > 0, "rowsInRowGroup must be greater than zero");

        // sort stripes by file position
        List<StripeInfo> stripeInfos = new ArrayList<>();
        for (int i = 0; i < fileStripes.size(); i++) {
            Optional<StripeStatistics> stats = Optional.empty();
            // ignore all stripe stats if too few or too many
            if (stripeStats.size() == fileStripes.size()) {
                stats = Optional.of(stripeStats.get(i));
            }
            stripeInfos.add(new StripeInfo(fileStripes.get(i), stats));
        }
        Collections.sort(stripeInfos, comparingLong(info -> info.getStripe().getOffset()));

        long totalRowCount = 0;
        long fileRowCount = 0;
        ImmutableList.Builder<StripeInformation> stripes = ImmutableList.builder();
        ImmutableList.Builder<Long> stripeFilePositions = ImmutableList.builder();
        if (predicate.matches(numberOfRows, getStatisticsByColumnOrdinal(root, partitionIds,fileStats))) {
            // select stripes that start within the specified split
            for (StripeInfo info : stripeInfos) {
                StripeInformation stripe = info.getStripe();
                if (splitContainsStripe(splitOffset, splitLength, stripe) && isStripeIncluded(root, stripe, info.getStats(), predicate, partitionIds)) {
                    stripes.add(stripe);
                    stripeFilePositions.add(fileRowCount);
                    totalRowCount += stripe.getNumberOfRows();
                }
                fileRowCount += stripe.getNumberOfRows();
            }
        }
        this.totalRowCount = totalRowCount;
        this.stripes = stripes.build();
        this.stripeFilePositions = stripeFilePositions.build();

        orcDataSource = wrapWithCacheIfTinyStripes(orcDataSource, this.stripes, maxMergeDistance, maxReadSize);
        this.orcDataSource = orcDataSource;
        this.splitLength = splitLength;

        this.fileRowCount = stripeInfos.stream()
                .map(StripeInfo::getStripe)
                .mapToLong(StripeInformation::getNumberOfRows)
                .sum();

        this.userMetadata = ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));

        this.systemMemoryUsage = requireNonNull(systemMemoryUsage, "systemMemoryUsage is null").newAggregatedMemoryContext();
        this.currentStripeSystemMemoryContext = systemMemoryUsage.newAggregatedMemoryContext();

        stripeReader = new StripeReader(
                orcDataSource,
                decompressor,
                types,
                this.presentColumns,
                rowsInRowGroup,
                predicate,
                hiveWriterVersion,
                metadataReader,
<<<<<<< HEAD
                partitionIds);
=======
                writeValidation);
>>>>>>> 04f7add7c5... PAX, all commits combined into one.

        streamReaders = createStreamReaders(orcDataSource, types, hiveStorageTimeZone, presentColumnsAndTypes.build());
        maxBytesPerCell = new long[streamReaders.length];
    }

    private static boolean splitContainsStripe(long splitOffset, long splitLength, StripeInformation stripe)
    {
        long splitEndOffset = splitOffset + splitLength;
        return splitOffset <= stripe.getOffset() && stripe.getOffset() < splitEndOffset;
    }

    private static boolean isStripeIncluded(
            OrcType rootStructType,
            StripeInformation stripe,
            Optional<StripeStatistics> stripeStats,
            OrcPredicate predicate,
            List<Integer> partitionIds)
    {
        // if there are no stats, include the column
        if (!stripeStats.isPresent()) {
            return true;
        }
        return predicate.matches(stripe.getNumberOfRows(), getStatisticsByColumnOrdinal(rootStructType, partitionIds, stripeStats.get().getColumnStatistics()));
    }

    @VisibleForTesting
    static OrcDataSource wrapWithCacheIfTinyStripes(OrcDataSource dataSource, List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize maxReadSize)
    {
        if (!allowCaching)
            return dataSource;
        if (dataSource instanceof CachingOrcDataSource) {
            return dataSource;
        }
        for (StripeInformation stripe : stripes) {
            if (stripe.getTotalLength() > maxReadSize.toBytes()) {
                return dataSource;
            }
        }
        return new CachingOrcDataSource(dataSource, createTinyStripesRangeFinder(stripes, maxMergeDistance, maxReadSize));
    }

    /**
     * Return the row position relative to the start of the file.
     */
    public long getFilePosition()
    {
        return filePosition;
    }

    /**
     * Returns the total number of rows in the file. This count includes rows
     * for stripes that were completely excluded due to stripe statistics.
     */
    public long getFileRowCount()
    {
        return fileRowCount;
    }

    /**
     * Return the row position within the stripes being read by this reader.
     * This position will include rows that were never read due to row groups
     * that are excluded due to row group statistics. Thus, it will advance
     * faster than the number of rows actually read.
     */
    public long getReaderPosition()
    {
        return currentPosition;
    }

    /**
     * Returns the total number of rows that can possibly be read by this reader.
     * This count may be fewer than the number of rows in the file if some
     * stripes were excluded due to stripe statistics, but may be more than
     * the number of rows read if some row groups are excluded due to statistics.
     */
    public long getReaderRowCount()
    {
        return totalRowCount;
    }

    public float getProgress()
    {
        return ((float) currentPosition) / totalRowCount;
    }

    public long getSplitLength()
    {
        return splitLength;
    }

    /**
     * Returns the sum of the largest cells in size from each column
     */
    public long getMaxCombinedBytesPerRow()
    {
        return maxCombinedBytesPerRow;
    }

    @Override
    public void close()
            throws IOException
    {
        orcDataSource.close();

        if (writeChecksumBuilder.isPresent()) {
            WriteChecksum actualChecksum = writeChecksumBuilder.get().build();
            validateWrite(validation -> validation.getChecksum().getTotalRowCount() == actualChecksum.getTotalRowCount(), "Invalid row count");
            List<Long> columnHashes = actualChecksum.getColumnHashes();
            for (int i = 0; i < columnHashes.size(); i++) {
                int columnIndex = i;
                validateWrite(validation -> validation.getChecksum().getColumnHashes().get(columnIndex).equals(columnHashes.get(columnIndex)),
                        "Invalid checksum for column %s", columnIndex);
            }
            validateWrite(validation -> validation.getChecksum().getStripeHash() == actualChecksum.getStripeHash(), "Invalid stripes checksum");
        }
    }

    public boolean isColumnPresent(int hiveColumnIndex)
    {
        return presentColumns.contains(hiveColumnIndex);
    }

    public int nextBatch()
            throws IOException
    {
        // update position for current row group (advancing resets them)
        filePosition += currentBatchSize;
        currentPosition += currentBatchSize;

        // if next row is within the current group return
        if (nextRowInGroup >= currentGroupRowCount) {
            // attempt to advance to next row group
            if (!advanceToNextRowGroup()) {
                filePosition = fileRowCount;
                currentPosition = totalRowCount;
                return -1;
            }
        }

        currentBatchSize = toIntExact(min(maxBatchSize, currentGroupRowCount - nextRowInGroup));

        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.prepareNextRead(currentBatchSize);
            }
        }
        nextRowInGroup += currentBatchSize;
        validateWritePageChecksum();
        return currentBatchSize;
    }

    public ColumnVector readBlock(DataType type, int columnIndex)
            throws IOException
    {
        ColumnVector block = streamReaders[columnIndex].readBlock(type);
        if (block.getElementsAppended() > 0) {
            long bytesPerCell = 1000 / block.getElementsAppended(); // TODO JL
            if (maxBytesPerCell[columnIndex] < bytesPerCell) {
                maxCombinedBytesPerRow = maxCombinedBytesPerRow - maxBytesPerCell[columnIndex] + bytesPerCell;
                maxBytesPerCell[columnIndex] = bytesPerCell;
                maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / maxCombinedBytesPerRow)));
            }
        }
        return block;
    }

    public ColumnarBatch getColumnarBatch(StructType schema) throws IOException {
        ColumnarBatch columnarBatch = ColumnarBatch.allocate(schema, MemoryMode.ON_HEAP,currentBatchSize);

        // This is the place for predicate evaluation on the column level
        // ouch !!!

        // Count(*) optimization
        if (schema.fields().length == 0) {
            columnarBatch.setNumRows(currentBatchSize);
            return columnarBatch;
        }
        StructField[] fields = schema.fields();
        ColumnBlock[] columnBlocks = new ColumnBlock[fields.length];

        // information about partitioned column and non-partitioned columns are kept separate,
        // so we must put all columns back in the correct order in columnBlocks
        int fieldIndex = 0;
        for (int index = 0; index < streamReaders.length; index++){
            int j = 0;
            for (int partitionId : partitionIds){
                if (index == partitionId && includedColumns.containsKey(index)){
                    columnBlocks[fieldIndex] = new LazyColumnBlock(new LazyPartitionColumnBlockLoaderImpl(fields[fieldIndex].dataType(), currentBatchSize, partitionValues.get(j)));
                    fieldIndex++;
                    break;
                }
                j++;
            }
            if (streamReaders[index] != null){
                columnBlocks[fieldIndex] = new LazyColumnBlock(new LazyIncludedColumnBlockLoaderImpl(streamReaders[index],fields[fieldIndex].dataType()));
                fieldIndex++;
            }
        }

        // Populate Columnar Batch
        int l = 0;
        for (ColumnBlock columnBlock: columnBlocks) {
            columnarBatch.setColumn(l,columnBlock.getColumnVector());
            l++;
        }
        columnarBatch.setNumRows(currentBatchSize);
        return columnarBatch;
    }


    public StreamReader getStreamReader(int index)
    {
        checkArgument(index < streamReaders.length, "index does not exist");
        return streamReaders[index];
    }

    public Map<String, Slice> getUserMetadata()
    {
        return ImmutableMap.copyOf(Maps.transformValues(userMetadata, Slices::copyOf));
    }

    private boolean advanceToNextRowGroup()
            throws IOException
    {
        nextRowInGroup = 0;

        while (!rowGroups.hasNext() && currentStripe < stripes.size()) {
            advanceToNextStripe();
        }

        if (!rowGroups.hasNext()) {
            currentGroupRowCount = 0;
            return false;
        }

        RowGroup currentRowGroup = rowGroups.next();
        currentGroupRowCount = currentRowGroup.getRowCount();
        if (currentRowGroup.getMinAverageRowBytes() > 0) {
            maxBatchSize = toIntExact(min(maxBatchSize, max(1, maxBlockBytes / currentRowGroup.getMinAverageRowBytes())));
        }

        currentPosition = currentStripePosition + currentRowGroup.getRowOffset();
        filePosition = stripeFilePositions.get(currentStripe) + currentRowGroup.getRowOffset();

        // give reader data streams from row group
        StreamSources rowGroupStreamSources = currentRowGroup.getStreamSources();
        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.startRowGroup(rowGroupStreamSources);
            }
        }

        return true;
    }

    private void advanceToNextStripe()
            throws IOException
    {
        currentStripeSystemMemoryContext.close();
        currentStripeSystemMemoryContext = systemMemoryUsage.newAggregatedMemoryContext();
        rowGroups = ImmutableList.<RowGroup>of().iterator();

        currentStripe++;
        if (currentStripe >= stripes.size()) {
            return;
        }

        if (currentStripe > 0) {
            currentStripePosition += stripes.get(currentStripe - 1).getNumberOfRows();
        }

        StripeInformation stripeInformation = stripes.get(currentStripe);
        validateWriteStripe(stripeInformation.getNumberOfRows());

        Stripe stripe = stripeReader.readStripe(stripeInformation, currentStripeSystemMemoryContext);
        if (stripe != null) {
            // Give readers access to dictionary streams
            StreamSources dictionaryStreamSources = stripe.getDictionaryStreamSources();
            List<ColumnEncoding> columnEncodings = stripe.getColumnEncodings();
            for (StreamReader column : streamReaders) {
                if (column != null) {
                    column.startStripe(dictionaryStreamSources, columnEncodings);
                }
            }

            rowGroups = stripe.getRowGroups().iterator();
        }
    }

    private void validateWrite(Predicate<OrcWriteValidation> test, String messageFormat, Object... args)
            throws OrcCorruptionException
    {
        if (writeValidation.isPresent() && !test.apply(writeValidation.get())) {
            throw new OrcCorruptionException(orcDataSource.getId(), "Write validation failed: " + messageFormat, args);
        }
    }

    private void validateWriteStripe(int rowCount)
            throws IOException
    {
        if (writeChecksumBuilder.isPresent()) {
            writeChecksumBuilder.get().addStripe(rowCount);
        }
    }

    private void validateWritePageChecksum()
            throws IOException
    {
        if (writeChecksumBuilder.isPresent()) {
            ColumnVector[] blocks = new ColumnVector[streamReaders.length];
            for (int columnIndex = 0; columnIndex < streamReaders.length; columnIndex++) {
                blocks[columnIndex] = readBlock(includedColumns.get(columnIndex), columnIndex);
            }
//            writeChecksumBuilder.get().addPage(new Page(currentBatchSize, blocks)); TODO JL PAGE?
        }
    }

    private static StreamReader[] createStreamReaders(
            OrcDataSource orcDataSource,
            List<OrcType> types,
            DateTimeZone hiveStorageTimeZone,
            Map<Integer, DataType> includedColumns)
    {
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();

        OrcType rowType = types.get(0);
        StreamReader[] streamReaders = new StreamReader[rowType.getFieldCount()];
        for (int columnId = 0; columnId < rowType.getFieldCount(); columnId++) {
            if (includedColumns.containsKey(columnId)) {
                StreamDescriptor streamDescriptor = streamDescriptors.get(columnId);
                streamReaders[columnId] = StreamReaders.createStreamReader(streamDescriptor, hiveStorageTimeZone);
            }
        }
        return streamReaders;
    }

    private static StreamDescriptor createStreamDescriptor(String parentStreamName, String fieldName, int typeId, List<OrcType> types, OrcDataSource dataSource)
    {
        OrcType type = types.get(typeId);

        if (!fieldName.isEmpty()) {
            parentStreamName += "." + fieldName;
        }

        ImmutableList.Builder<StreamDescriptor> nestedStreams = ImmutableList.builder();
        if (type.getOrcTypeKind() == OrcTypeKind.STRUCT) {
            for (int i = 0; i < type.getFieldCount(); ++i) {
                nestedStreams.add(createStreamDescriptor(parentStreamName, type.getFieldName(i), type.getFieldTypeIndex(i), types, dataSource));
            }
        }
        else if (type.getOrcTypeKind() == OrcTypeKind.LIST) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "item", type.getFieldTypeIndex(0), types, dataSource));
        }
        else if (type.getOrcTypeKind() == OrcTypeKind.MAP) {
            nestedStreams.add(createStreamDescriptor(parentStreamName, "key", type.getFieldTypeIndex(0), types, dataSource));
            nestedStreams.add(createStreamDescriptor(parentStreamName, "value", type.getFieldTypeIndex(1), types, dataSource));
        }
        return new StreamDescriptor(parentStreamName, typeId, fieldName, type.getOrcTypeKind(), dataSource, nestedStreams.build());
    }

    private static Map<Integer, ColumnStatistics> getStatisticsByColumnOrdinal(OrcType rootStructType, List<Integer> partitionIds, List<ColumnStatistics> fileStats)
    {
        requireNonNull(rootStructType, "rootStructType is null");
        checkArgument(rootStructType.getOrcTypeKind() == OrcTypeKind.STRUCT);
        requireNonNull(fileStats, "fileStats is null");

        ImmutableMap.Builder<Integer, ColumnStatistics> statistics = ImmutableMap.builder();
        // fileStats and rootStructType does not include partition columns, need to take partition columns into consideration
        // when constructing the statistics map, so it is aligned with columnIds in predicate
        int numColumns = rootStructType.getFieldCount() + partitionIds.size();
        int ordinal = 0;
        for (int columnId = 0; columnId < numColumns; columnId++) {
            if (partitionIds.contains(columnId))
                continue;
            ColumnStatistics element = fileStats.get(rootStructType.getFieldTypeIndex(ordinal));
            if (element != null) {
                statistics.put(columnId, element);
            }
            ordinal ++;
        }
        return statistics.build();
    }


    private static class StripeInfo
    {
        private final StripeInformation stripe;
        private final Optional<StripeStatistics> stats;

        public StripeInfo(StripeInformation stripe, Optional<StripeStatistics> stats)
        {
            this.stripe = requireNonNull(stripe, "stripe is null");
            this.stats = requireNonNull(stats, "metadata is null");
        }

        public StripeInformation getStripe()
        {
            return stripe;
        }

        public Optional<StripeStatistics> getStats()
        {
            return stats;
        }
    }

    @VisibleForTesting
    static class LinearProbeRangeFinder
            implements CachingOrcDataSource.RegionFinder
    {
        private final List<DiskRange> diskRanges;
        private int index;

        public LinearProbeRangeFinder(List<DiskRange> diskRanges)
        {
            this.diskRanges = diskRanges;
        }

        @Override
        public DiskRange getRangeFor(long desiredOffset)
        {
            // Assumption: range are always read in order
            // Assumption: bytes that are not part of any range are never read
            for (; index < diskRanges.size(); index++) {
                DiskRange range = diskRanges.get(index);
                if (range.getEnd() > desiredOffset) {
                    checkArgument(range.getOffset() <= desiredOffset);
                    return range;
                }
            }
            throw new IllegalArgumentException("Invalid desiredOffset " + desiredOffset);
        }

        public static LinearProbeRangeFinder createTinyStripesRangeFinder(List<StripeInformation> stripes, DataSize maxMergeDistance, DataSize maxReadSize)
        {
            if (stripes.size() == 0) {
                return new LinearProbeRangeFinder(ImmutableList.of());
            }

            List<DiskRange> scratchDiskRanges = stripes.stream()
                    .map(stripe -> new DiskRange(stripe.getOffset(), toIntExact(stripe.getTotalLength())))
                    .collect(Collectors.toList());
            List<DiskRange> diskRanges = mergeAdjacentDiskRanges(scratchDiskRanges, maxMergeDistance, maxReadSize);

            return new LinearProbeRangeFinder(diskRanges);
        }
    }

    public int getCurrentBatchSize() {
        return currentBatchSize;
    }

}
