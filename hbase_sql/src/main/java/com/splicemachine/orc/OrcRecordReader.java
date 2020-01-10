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
package com.splicemachine.orc;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.orc.block.*;
import com.splicemachine.orc.memory.AbstractAggregatedMemoryContext;
import com.splicemachine.orc.memory.AggregatedMemoryContext;
import com.splicemachine.orc.metadata.*;
import com.splicemachine.orc.metadata.OrcType.OrcTypeKind;
import com.splicemachine.orc.metadata.PostScript.HiveWriterVersion;
import com.splicemachine.orc.reader.StreamReader;
import com.splicemachine.orc.reader.StreamReaders;
import com.splicemachine.orc.stream.StreamSources;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
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
import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;
import static com.splicemachine.orc.OrcDataSourceUtils.mergeAdjacentDiskRanges;
import static com.splicemachine.orc.OrcReader.MAX_BATCH_SIZE;
import static com.splicemachine.orc.OrcRecordReader.LinearProbeRangeFinder.createTinyStripesRangeFinder;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Math.toIntExact;
import static java.util.Comparator.comparingLong;
import static java.util.Objects.requireNonNull;
import static java.lang.Math.min;

public class OrcRecordReader
{
    private final OrcDataSource orcDataSource;

    private final StreamReader[] streamReaders;

    private final long totalRowCount;
    private final long splitLength;
    private final Set<Integer> presentColumns;
    private long currentPosition;
    private long currentStripePosition;
    private int currentBatchSize;

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
    protected Map<Integer, DataType> includedColumns;
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
            CompressionKind compressionKind,
            int bufferSize,
            int rowsInRowGroup,
            DateTimeZone hiveStorageTimeZone,
            HiveWriterVersion hiveWriterVersion,
            MetadataReader metadataReader,
            DataSize maxMergeDistance,
            DataSize maxReadSize,
            Map<String, Slice> userMetadata,
            AbstractAggregatedMemoryContext systemMemoryUsage,
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
        requireNonNull(compressionKind, "compressionKind is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(userMetadata, "userMetadata is null");

        // Place to add static values?

        // Adding All Included Columns
        this.includedColumns = includedColumns;

        this.partitionValues = partitionValues;
        this.partitionIds = partitionIds;

        // reduce the included columns to the set that is also present
        presentColumns = new TreeSet<>();
        Map<Integer, DataType> presentColumnsAndTypes = new TreeMap();

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
        if (predicate.matches(numberOfRows, getStatisticsByColumnOrdinal(root, partitionIds, fileStats))) {
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
                compressionKind,
                types,
                bufferSize,
                this.presentColumns,
                rowsInRowGroup,
                predicate,
                hiveWriterVersion,
                metadataReader,
                partitionIds);

        streamReaders = createStreamReaders(orcDataSource, types, hiveStorageTimeZone, presentColumnsAndTypes, partitionIds);
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

    public void close()
            throws IOException
    {
        orcDataSource.close();
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

        currentBatchSize = toIntExact(min(MAX_BATCH_SIZE, currentGroupRowCount - nextRowInGroup));

        for (StreamReader column : streamReaders) {
            if (column != null) {
                column.prepareNextRead(currentBatchSize);
            }
        }
        nextRowInGroup += currentBatchSize;
        return currentBatchSize;
    }

    public ColumnVector readBlock(DataType type, int columnIndex)
            throws IOException
    {
        return streamReaders[columnIndex].readBlock(type);
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

    private static StreamReader[] createStreamReaders(OrcDataSource orcDataSource,
            List<OrcType> types,
            DateTimeZone hiveStorageTimeZone,
            Map<Integer, DataType> includedColumns,
            List<Integer> partitionIds)
    {
        // create streamReaders with a size = table's total columns
        // only fill indexes which are selected, non-partitioned columns
        // doing this makes getColumnarBatch()'s job a lot easier
        int numPartitions = partitionIds.size();
        List<StreamDescriptor> streamDescriptors = createStreamDescriptor("", "", 0, types, orcDataSource).getNestedStreams();
        OrcType rowType = types.get(0);
        int totalColumns = rowType.getFieldCount() + numPartitions;
        StreamReader[] streamReaders = new StreamReader[totalColumns];

        int lastColumn = 0;
        for (Integer i : includedColumns.keySet()){
            if (i > lastColumn){
                lastColumn = i;
            }
        }
        int streamDescrIndex = 0;

        for (int col = 0; col <= lastColumn; col++) {
            if (includedColumns.containsKey(col)){
                if (partitionIds.contains(col)) {
                    // do nothing
                } else {
                    StreamDescriptor streamDescriptor = streamDescriptors.get(streamDescrIndex);
                    streamReaders[col] = StreamReaders.createStreamReader(streamDescriptor, hiveStorageTimeZone);
                    streamDescrIndex++;
                }
            } else {
                if (!partitionIds.contains(col)){
                    streamDescrIndex++;
                }
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
            if (stripes.isEmpty()) {
                return new LinearProbeRangeFinder(ImmutableList.of());
            }

            List<DiskRange> scratchDiskRanges = stripes.stream()
                    .map(stripe -> new DiskRange(stripe.getOffset(), toIntExact(stripe.getTotalLength())))
                    .collect(Collectors.toList());
            List<DiskRange> diskRanges = mergeAdjacentDiskRanges(scratchDiskRanges, maxMergeDistance, maxReadSize);

            return new LinearProbeRangeFinder(diskRanges);
        }
    }



    /**
     * Process the qualifier list on the row, return true if it qualifies.
     * <p>
     * A two dimensional array is to be used to pass around a AND's and OR's in
     * conjunctive normal form.  The top slot of the 2 dimensional array is
     * optimized for the more frequent where no OR's are present.  The first
     * array slot is always a list of AND's to be treated as described above
     * for single dimensional AND qualifier arrays.  The subsequent slots are
     * to be treated as AND'd arrays or OR's.  Thus the 2 dimensional array
     * qual[][] argument is to be treated as the following, note if
     * qual.length = 1 then only the first array is valid and it is and an
     * array of and clauses:
     *
     * (qual[0][0] and qual[0][0] ... and qual[0][qual[0].length - 1])
     * and
     * (qual[1][0] or  qual[1][1] ... or  qual[1][qual[1].length - 1])
     * and
     * (qual[2][0] or  qual[2][1] ... or  qual[2][qual[2].length - 1])
     * ...
     * and
     * (qual[qual.length - 1][0] or  qual[1][1] ... or  qual[1][2])
     *
     *
     * @return true if the row qualifies.
     *
     * @param row               The row being qualified.
     * @param qual_list         2 dimensional array representing conjunctive
     *                          normal form of simple qualifiers.
     *
     * @exception StandardException  Standard exception policy.
     **/
    /*
    public static BitSet qualifyBlocks(
            ColumnBlock[]        columnBlocks,
            Qualifier[][]   qual_list,
            int[] baseColumnMap,
            DataValueDescriptor probeValue)
            throws StandardException {
        assert columnBlocks!=null:"row passed in is null";
        assert qual_list!=null:"qualifier[][] passed in is null";
        boolean     row_qualifies = true;
        for (int i = 0; i < qual_list[0].length; i++) {
            // process each AND clause
            row_qualifies = false;
            // process each OR clause.
            Qualifier q = qual_list[0][i];
            q.clearOrderableCache();
            // Get the column from the possibly partial row, of the
            // q.getColumnId()'th column in the full row.
            DataValueDescriptor columnValue =
                    (DataValueDescriptor) row[baseColumnMap!=null?baseColumnMap[q.getStoragePosition()]:q.getStoragePosition()];
            if ( filterNull(q.getOperator(),columnValue,probeValue==null || i!=0?q.getOrderable():probeValue,q.getVariantType())) {
                return false;
            }
            row_qualifies =
                    columnValue.compare(
                            q.getOperator(),
                            probeValue==null || i!=0?q.getOrderable():probeValue,
                            q.getOrderedNulls(),
                            q.getUnknownRV());
            if (q.negateCompareResult())
                row_qualifies = !row_qualifies;
//            System.out.println(String.format("And Clause -> value={%s}, operator={%s}, orderable={%s}, " +
//                    "orderedNulls={%s}, unknownRV={%s}",
//                    columnValue, q.getOperator(),q.getOrderable(),q.getOrderedNulls(),q.getUnknownRV()));
            // Once an AND fails the whole Qualification fails - do a return!
            if (!row_qualifies)
                return(false);
        }

        // all the qual[0] and terms passed, now process the OR clauses
        for (int and_idx = 1; and_idx < qual_list.length; and_idx++) {
            // loop through each of the "and" clause.
            row_qualifies = false;
            for (int or_idx = 0; or_idx < qual_list[and_idx].length; or_idx++) {
                // Apply one qualifier to the row.
                Qualifier q      = qual_list[and_idx][or_idx];
                q.clearOrderableCache();
                // Get the column from the possibly partial row, of the
                // q.getColumnId()'th column in the full row.
                DataValueDescriptor columnValue =
                        (DataValueDescriptor) row[baseColumnMap!=null?baseColumnMap[q.getStoragePosition()]:q.getStoragePosition()];
                // do the compare between the column value and value in the
                // qualifier.
                if ( filterNull(q.getOperator(),columnValue,q.getOrderable(),q.getVariantType())) {
                    return false;
                }
                row_qualifies =
                        columnValue.compare(
                                q.getOperator(),
                                q.getOrderable(),
                                q.getOrderedNulls(),
                                q.getUnknownRV());

                if (q.negateCompareResult())
                    row_qualifies = !row_qualifies;
                // processing "OR" clauses, so as soon as one is true, break
                // to go and process next AND clause.
                if (row_qualifies)
                    break;

            }

            // The qualifier list represented a set of "AND'd"
            // qualifications so as soon as one is false processing is done.
            if (!row_qualifies)
                break;
        }
        return(row_qualifies);
    }

    public static boolean filterNull(int operator, DataValueDescriptor columnValue, DataValueDescriptor orderable, int variantType) {
        if (orderable==null||orderable.isNull()) {
            switch (operator) {
                case com.splicemachine.db.iapi.types.DataType.ORDER_OP_LESSTHAN:
                case com.splicemachine.db.iapi.types.DataType.ORDER_OP_LESSOREQUALS:
                case com.splicemachine.db.iapi.types.DataType.ORDER_OP_GREATERTHAN:
                case com.splicemachine.db.iapi.types.DataType.ORDER_OP_GREATEROREQUALS:
                    return true;
                case com.splicemachine.db.iapi.types.DataType.ORDER_OP_EQUALS:
                    if (variantType != 1)
                        return true;
//                    if (columnValue == null || columnValue.isNull())
//                            return true;
                    return false;
            }
        }
        return false;
    }*/
}
