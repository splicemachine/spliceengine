package com.splicemachine.derby.impl.sql.execute.operations.groupedaggregate;

import com.google.common.collect.Lists;
import com.splicemachine.constants.SpliceConstants;
import com.splicemachine.derby.iapi.sql.execute.SpliceRuntimeContext;
import com.splicemachine.derby.impl.sql.execute.operations.DistinctGroupedAggregateOperation;
import com.splicemachine.derby.impl.sql.execute.operations.SpliceBaseOperation;
import com.splicemachine.derby.impl.sql.execute.operations.framework.GroupedRow;
import com.splicemachine.derby.impl.sql.execute.operations.framework.SpliceGenericAggregator;
import com.splicemachine.derby.utils.StandardIterator;
import com.splicemachine.derby.utils.marshall.KeyEncoder;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.pipeline.exception.Exceptions;
import com.splicemachine.utils.SpliceLogUtils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.execute.ExecRow;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.util.Arrays;
import java.util.List;

/**
 * Aggregator for use with Sinking aggregates.
 *
 * Unlike {@link ScanGroupedAggregateIterator}, this implementation makes a distinction
 * between distinct aggregates and non-distinct aggregates.
 *
 * @author Scott Fines
 * Created on: 11/5/13
 */
public class SinkGroupedAggregateIterator extends GroupedAggregateIterator {
    private final DoubleBuffer buffer;
    private boolean completed = false;

    public SinkGroupedAggregateIterator(GroupedAggregateBuffer nonDistinctBuffer,
                                        GroupedAggregateBuffer distinctBuffer,
                                        StandardIterator<ExecRow> source,
                                        boolean rollup,
                                        int[] groupColumns,
                                        KeyEncoder groupKeyEncoder,
                                        KeyEncoder allKeyEncoder) {
        super(source, rollup, groupColumns);
        int maxEvictedSize = isRollup ? groupColumns.length : 1;
        evictedRows = Lists.newArrayListWithCapacity(maxEvictedSize);
        this.buffer = new DoubleBuffer(nonDistinctBuffer, distinctBuffer, groupKeyEncoder, allKeyEncoder, evictedRows);
    }

    public static SinkGroupedAggregateIterator newInstance(GroupedAggregateBuffer nonDistinctBuffer,
                                                           GroupedAggregateBuffer distinctBuffer,
                                                           StandardIterator<ExecRow> source,
                                                           boolean rollup,
                                                           int[] groupColumns,
                                                           boolean[] groupSortOrder,
                                                           int[] nonGroupedUniqueColumns,
                                                           DescriptorSerializer[] serializers) {
        int[] allKeyColumns = new int[groupColumns.length + nonGroupedUniqueColumns.length];
        System.arraycopy(groupColumns, 0, allKeyColumns, 0, groupColumns.length);
        System.arraycopy(nonGroupedUniqueColumns, 0, allKeyColumns, groupColumns.length, nonGroupedUniqueColumns.length);

        boolean[] allSortOrders = new boolean[groupColumns.length + nonGroupedUniqueColumns.length];
        System.arraycopy(groupSortOrder, 0, allSortOrders, 0, groupSortOrder.length);
        Arrays.fill(allSortOrders, groupSortOrder.length, allSortOrders.length, true);

        KeyEncoder groupKeyEncoder = KeyEncoder.bare(groupColumns, groupSortOrder, serializers);
        KeyEncoder allKeyEncoder = KeyEncoder.bare(allKeyColumns, allSortOrders, serializers);

        return new SinkGroupedAggregateIterator(nonDistinctBuffer, distinctBuffer, source, rollup, groupColumns, groupKeyEncoder, allKeyEncoder);
    }

    @Override
    public void open() throws StandardException, IOException {
        source.open();
    }

    @Override
    public GroupedRow next(SpliceRuntimeContext context) throws StandardException,
                                                                IOException {
        if (evictedRows.size() > 0)
            return evictedRows.remove(0);
        if (completed) {
            if (buffer.size() > 0) {
                return buffer.getFinalizedRow();
            } else return null;
        }

        boolean shouldContinue;
        GroupedRow toReturn = null;
        do {
            SpliceBaseOperation.checkInterrupt(rowsRead, SpliceConstants.interruptLoopCheck);
            ExecRow nextRow = source.next(context);
            shouldContinue = nextRow != null;
            if (!shouldContinue)
                break; //iterator exhausted, break from the loop

            toReturn = buffer(nextRow);
            shouldContinue = toReturn == null;
            rowsRead++;
        } while (shouldContinue);

        if (toReturn != null) {
            return toReturn;
        }
        /*
         * We can only get here if we exhaust the iterator without evicting a record, which
         * means that we have completed our steps.
         */
        completed = true;

        //we've exhausted the iterator, so return an entry from the buffer
        if (buffer.size() > 0) {
            return buffer.getFinalizedRow();
        }
        //return buffer.getFinalizedRow();

        //the buffer has nothing in it either, just return null
        return null;
    }

    @Override
    public void close() throws StandardException, IOException {
        source.close();
    }

    /*stats stuff*/
    @Override
    public long getRowsMerged() {
        return buffer.getRowsMerged();
    }

    @Override
    public double getMaxFillRatio() {
        return buffer.getMaxFillRatio();
    }

    @Override
    public long getRowsRead() {
        return rowsRead;
    }

    /**
     * ****************************************************************************************
     */
        /*private helper methods*/
    private GroupedRow buffer(ExecRow nextRow) throws StandardException {
        GroupedRow firstEvicted = null;
        if (!isRollup) {
            return buffer.buffer(nextRow);
        } else {
            rollupRows(nextRow);
            for (ExecRow rollup : rollupRows) {
                //we don't need to clone, cause rolling up rows does it for us
                GroupedRow groupedRow = buffer.buffer(rollup);

                if (groupedRow != null) {
                    if (firstEvicted == null)
                        firstEvicted = groupedRow;
                    else
                        evictedRows.add(groupedRow);
                }
            }
            return firstEvicted;
        }
    }

    private void rollupRows(ExecRow row) throws StandardException {
        if (rollupRows == null) {
            rollupRows = isRollup ? new ExecRow[groupColumns.length + 1] : new ExecRow[1];
        }
        if (!isRollup) {
            rollupRows[0] = row;
            return;
        }
        int rollUpPos = groupColumns.length;
        int pos = 0;
        ExecRow nextRow = row.getClone();
        do {
            rollupRows[pos] = nextRow;
            if (rollUpPos > 0) {
                nextRow = nextRow.getClone();
                DataValueDescriptor rollUpCol = nextRow.getColumn(groupColumns[rollUpPos - 1] + 1);
                rollUpCol.setToNull();
            }
            rollUpPos--;
            pos++;
        } while (rollUpPos >= 0);
    }

    private static interface Buffer {
        GroupedRow buffer(ExecRow row) throws StandardException;

        int size();

        GroupedRow getFinalizedRow() throws StandardException;

        long getRowsMerged();

        double getMaxFillRatio();
    }

    private static class DoubleBuffer implements Buffer {
        private final SingleBuffer nonDistinctBuffer;
        private final SingleBuffer distinctBuffer;
        private final List<GroupedRow> evictedRows;
        private final int[] nonDistinctInputCols;
        private final int[] nonDistinctAggCols;
        private final SpliceGenericAggregator[] nonDistinctAggs;
        private final int[] distinctInputCols;
        private final int[] distinctAggCols;
        private final SpliceGenericAggregator[] distinctAggs;
        private final boolean dontAggregateDistinct;
        private final boolean dontAggregateNonDistinct;


        private DoubleBuffer(GroupedAggregateBuffer nonDistinctBuffer,
                             GroupedAggregateBuffer distinctBuffer,
                             KeyEncoder groupKeyEncoder,
                             KeyEncoder allKeyEncoder,
                             List<GroupedRow> evictedRows) {
            dontAggregateDistinct = !distinctBuffer.hasAggregates() && nonDistinctBuffer.hasAggregates();
            dontAggregateNonDistinct = !nonDistinctBuffer.hasAggregates() && distinctBuffer.hasAggregates();

            nonDistinctInputCols = new int[nonDistinctBuffer.getAggregates().length];
            nonDistinctAggCols = new int[nonDistinctBuffer.getAggregates().length];
            nonDistinctAggs = nonDistinctBuffer.getAggregates();

            distinctInputCols = new int[distinctBuffer.getAggregates().length];
            distinctAggCols = new int[distinctBuffer.getAggregates().length];
            distinctAggs = distinctBuffer.getAggregates();

            for (int i = 0; i < nonDistinctInputCols.length; i++){
                nonDistinctInputCols[i] = nonDistinctBuffer.getAggregates()[i].getAggregatorInfo().getInputColNum();
            }
            for (int i = 0; i < nonDistinctInputCols.length; i++){
                nonDistinctAggCols[i] = nonDistinctBuffer.getAggregates()[i].getAggregatorInfo().getAggregatorColNum();
            }

            for (int i = 0; i < distinctInputCols.length; i++){
                distinctInputCols[i] =distinctBuffer.getAggregates()[i].getAggregatorInfo().getInputColNum();
            }

            for (int i = 0; i < distinctInputCols.length; i++){
                distinctAggCols[i] = distinctBuffer.getAggregates()[i].getAggregatorInfo().getAggregatorColNum();
            }

            this.nonDistinctBuffer = new SingleBuffer(nonDistinctBuffer, groupKeyEncoder, dontAggregateNonDistinct);
            this.distinctBuffer = new SingleBuffer(distinctBuffer, allKeyEncoder, dontAggregateDistinct);
            this.evictedRows = evictedRows;
        }

        private ExecRow maskedDistinctRow(ExecRow row) throws StandardException {
            if (!dontAggregateDistinct) {
                DataValueDescriptor[] cols = row.getRowArray();
                // null-out input columns for non-distinct aggs
                for (int idx : nonDistinctInputCols) {
                    cols[idx] = null;
                }
                // initialize aggregator objects for non-distinct aggs
                for (int idx = 0; idx < nonDistinctAggCols.length; idx++) {
                    cols[nonDistinctAggCols[idx]]
                        .setValue(nonDistinctAggs[idx].getAggregatorInstance());
                }
            }
            return row;
        }


        private ExecRow maskedNondistinctRow(ExecRow row) throws StandardException {
            // Clear values for distinct aggregation, to avoid accidentally involved in distinct aggregation calculation
            // for 2nd phase
            DataValueDescriptor[] cols = row.getRowArray();
            for (int idx : distinctInputCols) {
                row.getColumn(idx+1).setToNull();
            }
            // initialize aggregator objects for distinct aggs
            for (int idx = 0; idx < distinctAggCols.length; idx++) {
                cols[distinctAggCols[idx]]
                        .setValue(distinctAggs[idx].getAggregatorInstance());
            }
            return row;
        }
        @Override
        public GroupedRow buffer(ExecRow row) throws StandardException {
            GroupedRow firstEvicted = nonDistinctBuffer.buffer(row);
            GroupedRow distinct = distinctBuffer.buffer(maskedDistinctRow(row));
            GroupedRow toReturn = null;

            if (firstEvicted != null) {
                firstEvicted.setDistinct(false);
                toReturn = firstEvicted;
                if (distinct != null) {
                    distinct.setDistinct(true);
                    // TODO BUG: when are evicted rows ever returned?
                    evictedRows.add(distinct);
                }
            } else if (distinct != null) {
                distinct.setDistinct(true);
                //firstEvicted = distinct;
                toReturn = distinct;
            }
            //return firstEvicted;
            return toReturn;
        }

        @Override
        public int size() {
            return nonDistinctBuffer.size() + distinctBuffer.size();
        }

        @Override
        public GroupedRow getFinalizedRow() throws StandardException {
            if (nonDistinctBuffer.size() > 0) {
                GroupedRow groupedRow = nonDistinctBuffer.getFinalizedRow();
                maskedNondistinctRow(groupedRow.getRow());
                return groupedRow;
            }
            if (distinctBuffer.size() > 0) {
                GroupedRow finalizedRow = distinctBuffer.getFinalizedRow();
                finalizedRow.setDistinct(true);
                return finalizedRow;
            }
            return null;
        }

        @Override
        public long getRowsMerged() {
            long merged = distinctBuffer.getRowsMerged();
            if (merged == 0)
                merged = nonDistinctBuffer.getRowsMerged();
            return merged;
        }

        @Override
        public double getMaxFillRatio() {
            double distinctFill = distinctBuffer.getMaxFillRatio();
            double nonDistFill = nonDistinctBuffer.getMaxFillRatio();
            if (distinctFill == 0)
                return nonDistFill;
            else if (nonDistFill == 0)
                return distinctFill;
            else
                return Math.min(distinctFill, nonDistFill);
        }
    }

    private static class SingleBuffer implements Buffer {
        private final GroupedAggregateBuffer aggregateBuffer;
        private final KeyEncoder keyEncoder;
        private final boolean ignoreNonAggregates;

        private SingleBuffer(GroupedAggregateBuffer aggregateBuffer,
                             KeyEncoder keyEncoder,
                             boolean ignoreNonAggregates) {
            this.aggregateBuffer = aggregateBuffer;
            this.ignoreNonAggregates = ignoreNonAggregates;
            this.keyEncoder = keyEncoder;
        }

        @Override
        public GroupedRow buffer(ExecRow row) throws StandardException {
            //do nothing if we ignore the non-aggregates
            if (ignoreNonAggregates && !aggregateBuffer.hasAggregates()) return null;

            return aggregateBuffer.add(groupingKey(row), row);
        }

        @Override
        public int size() {
            return aggregateBuffer.size();
        }

        @Override
        public GroupedRow getFinalizedRow() throws StandardException {
            return aggregateBuffer.getFinalizedRow();
        }

        @Override
        public long getRowsMerged() {
            return aggregateBuffer.getRowsMerged();
        }

        @Override
        public double getMaxFillRatio() {
            return aggregateBuffer.getMaxFillRatio();
        }

        private byte[] groupingKey(ExecRow nextRow) throws StandardException {

            try {
                return keyEncoder.getKey(nextRow);
            } catch (IOException e) {
                throw Exceptions.parseException(e);
            }
        }
    }
}
