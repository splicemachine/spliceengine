/*
 * Copyright 2012-2021 Splice Machine Inc.  All rights reserved.
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.splicemachine.derby.hbase;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.storage.HScan;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hbase.thirdparty.com.google.protobuf.InvalidProtocolBufferException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static com.splicemachine.db.shared.common.reference.SQLState.LANG_INTERNAL_ERROR;

    /**
     * A custom {@link MultiRowRangeFilter} which implements {@link Filter#reset()}
     *
     * This HBase filter has the exact same behavior as MultiRowRangeFilter,
     * except that it may be reset and reused over and over again.
     * In the super class, once filterRowKey finds a rowkey passed the last
     * range in the filter, it sets private field {@code done} to true, and
     * all subsequent calls to {@link MultiRowRangeFilter#filterAllRemaining()}
     * return true, causing the scan to terminate.
     * The logic in MultiRowRangeFilter also sometimes returns true in
     * {@code hasFoundFirstRange()} when private field {@code range} has
     * not been set, so greater control over this logic was needed.
     * The logic of {@link MultiRowRangeFilter#filterRowKey(Cell firstRowCell)} and
     * {@code MultiRowRangeFilter#getNextRangeIndex(byte[] rowKey)} is copied
     * into this class to allow slight modification and so fields may be reset
     * back to their initial state.  The only main difference is that {@code filterRowkey}
     * returns true when the iteration through ranges is complete as a signal
     * to the caller that we are done.
     */

@SuppressFBWarnings(value="HE_EQUALS_NO_HASHCODE", justification="Intentional")
public class SpliceMultiRowRangeFilter extends MultiRowRangeFilter {

    private final RangeIteration ranges;
    private RowRange range;
    private int index;
    private boolean done = false;
    private ReturnCode currentReturnCode;
    private static final int ROW_BEFORE_FIRST_RANGE = -1;

  /**
   * @param list A list of <code>RowRange</code>
   */
    public SpliceMultiRowRangeFilter(List<RowRange> list) {
        super(list);
        this.ranges = new RangeIteration(super.getRowRanges());
    }

    @Override
    public void reset() throws IOException {
        done = false;
        ranges.setFoundFirstRange(false);
        currentReturnCode = ReturnCode.INCLUDE;
    }

    @Override
    public boolean filterAllRemaining() {
        return false;
    }

    @Override
    public ReturnCode filterCell(final Cell ignored) {
      return currentReturnCode;
    }

    @Override
    public Cell getNextCellHint(Cell currentKV) {
        // skip to the next range's start row
        // #getComparisonData lets us avoid the `if (reversed)` branch
        byte[] comparisonData = range.getComparisonData();
        return PrivateCellUtil.createFirstOnRow(comparisonData, 0, (short) comparisonData.length);
    }

    @Override
    public boolean filterRowKey(Cell firstRowCell) {
        if (filterAllRemaining()) return true;

        // N.b. We can only do this after we're iterating over records. If we try to do
        // it before, the Scan (and this filter) may not yet be fully initialized. This is a
        // wart on Filter and something that'd be nice to clean up (like CP's in HBase2.0)
        if (!ranges.isInitialized()) {
          ranges.initialize(isReversed());
        }

        // If it is the first time of running, calculate the current range index for
        // the row key. If index is out of bound which happens when the start row
        // user sets is after the largest stop row of the ranges, stop the scan.
        // If row key is after the current range, find the next range and update index.
        byte[] rowArr = firstRowCell.getRowArray();
        int length = firstRowCell.getRowLength();
        int offset = firstRowCell.getRowOffset();
        if (!ranges.hasFoundFirstRange() || !range.contains(rowArr, offset, length)) {
          byte[] rowkey = CellUtil.cloneRow(firstRowCell);
          index = ranges.getNextRangeIndex(rowkey);
          if (ranges.isIterationComplete(index)) {
            done = true;
            currentReturnCode = ReturnCode.NEXT_ROW;
            ranges.setFoundFirstRange(false);
            return true;
          }
          if(index != ROW_BEFORE_FIRST_RANGE) {
            range = ranges.get(index);
          } else {
            range = ranges.get(0);
          }
          if (ranges.isExclusive()) {
            ranges.resetExclusive();
            currentReturnCode = ReturnCode.NEXT_ROW;
            return false;
          }
          if (!ranges.hasFoundFirstRange()) {
            if(index != ROW_BEFORE_FIRST_RANGE) {
              currentReturnCode = ReturnCode.INCLUDE;
            } else {
              currentReturnCode = ReturnCode.SEEK_NEXT_USING_HINT;
            }
            ranges.setFoundFirstRange(true);
          } else {
            if (range.contains(rowArr, offset, length)) {
              currentReturnCode = ReturnCode.INCLUDE;
            } else {
              currentReturnCode = ReturnCode.SEEK_NEXT_USING_HINT;
            }
          }
        } else {
          currentReturnCode = ReturnCode.INCLUDE;
        }
        return false;
    }

    public static SpliceMultiRowRangeFilter parseFrom(final byte[] pbBytes)
        throws DeserializationException {
        FilterProtos.MultiRowRangeFilter proto;
        try {
          proto = FilterProtos.MultiRowRangeFilter.parseFrom(pbBytes);
        } catch (InvalidProtocolBufferException e) {
          throw new DeserializationException(e);
        }
        int length = proto.getRowRangeListCount();
        List<FilterProtos.RowRange> rangeProtos = proto.getRowRangeListList();
        List<RowRange> rangeList = new ArrayList<>(length);
        for (FilterProtos.RowRange rangeProto : rangeProtos) {
          RowRange range = new RowRange(rangeProto.hasStartRow() ? rangeProto.getStartRow()
              .toByteArray() : null, rangeProto.getStartRowInclusive(), rangeProto.hasStopRow() ?
                  rangeProto.getStopRow().toByteArray() : null, rangeProto.getStopRowInclusive());
          rangeList.add(range);
        }
        return new SpliceMultiRowRangeFilter(rangeList);
    }

    @Override
    public boolean equals(Object obj) {
        return obj instanceof SpliceMultiRowRangeFilter && super.equals((Filter) obj);
    }

    private static class RangeIteration {
        private boolean exclusive = false;
        private boolean initialized = false;
        private boolean foundFirstRange = false;
        private final List<RowRange> sortedAndMergedRanges;
        private List<? extends RowRange> ranges;

        public RangeIteration(List<RowRange> sortedAndMergedRanges) {
          this.sortedAndMergedRanges = sortedAndMergedRanges;
        }

        void initialize(boolean reversed) {
          // Avoid double initialization
          assert !this.initialized;
          if (reversed) {
            throw new RuntimeException(StandardException.newException(LANG_INTERNAL_ERROR,
                                       "SpliceMultiRowRangeFilter does not support reverse scan"));
          } else {
            this.ranges = sortedAndMergedRanges;
          }
          this.initialized = true;
        }

        /**
         * Calculates the position where the given rowkey fits in the ranges list.
         *
         * @param rowKey the row key to calculate
         * @return index the position of the row key
         */
        public int getNextRangeIndex(byte[] rowKey) {
          RowRange temp;
          temp = new RowRange(rowKey, true, null, true);
          // Because we make sure that `ranges` has the correct natural ordering (given it containing
          // RowRange or ReverseRowRange objects). This keeps us from having to have two different
          // implementations below.
          final int index = Collections.binarySearch(ranges, temp);
          if (index < 0) {
            int insertionPosition = -index - 1;
            // check if the row key in the range before the insertion position
            if (insertionPosition != 0 && ranges.get(insertionPosition - 1).contains(rowKey)) {
              return insertionPosition - 1;
            }
            // check if the row key is before the first range
            if (insertionPosition == 0 && !ranges.get(insertionPosition).contains(rowKey)) {
              return ROW_BEFORE_FIRST_RANGE;
            }
            if (!foundFirstRange) {
              foundFirstRange = true;
            }
            return insertionPosition;
          }
          // the row key equals one of the start keys, and the the range exclude the start key
          if(ranges.get(index).isSearchRowInclusive() == false) {
            exclusive = true;
          }
          return index;
        }

        /**
         * Sets {@link #foundFirstRange} to {@code true}, indicating that we found a matching row range.
         */
        public void setFoundFirstRange(boolean foundFirstRange) {
          this.foundFirstRange = foundFirstRange;
        }

        /**
         * Gets the RowRange at the given offset.
         */
        @SuppressWarnings("unchecked")
        public <T extends RowRange> T get(int i) {
          return (T) ranges.get(i);
        }

        /**
         * Returns true if the first matching row range was found.
         */
        public boolean hasFoundFirstRange() {
          return foundFirstRange;
        }

        /**
         * Returns true if the current range's key is exclusive
         */
        public boolean isExclusive() {
          return exclusive;
        }

        /**
         * Resets the exclusive flag.
         */
        public void resetExclusive() {
          exclusive = false;
        }

        /**
         * Returns true if this class has been initialized by calling {@link #initialize(boolean)}.
         */
        public boolean isInitialized() {
          return initialized;
        }

        /**
         * Returns true if we exhausted searching all row ranges.
         */
        public boolean isIterationComplete(int index) {
          return index >= ranges.size();
        }
    }
}
