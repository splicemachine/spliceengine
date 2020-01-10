/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */
package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.db.iapi.types.SQLChar;
import com.splicemachine.db.iapi.types.SQLDate;
import com.splicemachine.db.iapi.types.SQLDouble;
import com.splicemachine.db.iapi.types.SQLTime;
import com.splicemachine.db.iapi.types.SQLTimestamp;
import com.yahoo.memory.NativeMemory;
import com.yahoo.sketches.frequencies.ErrorType;
import com.yahoo.sketches.quantiles.ItemsSketch;
import com.yahoo.sketches.theta.Sketch;
import com.yahoo.sketches.theta.UpdateSketch;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static com.splicemachine.db.iapi.types.Orderable.*;

/**
 *
 * Statistics Container for a column wrapping quantiles, frequencies and theta sketches provided
 * via the Yahoo Sketches open source implementation.
 *
 * @see <a href="https://datasketches.github.io/">https://datasketches.github.io/</a>
 */
public class ColumnStatisticsImpl implements ItemStatistics<DataValueDescriptor>, Externalizable {
    private static Logger LOG=Logger.getLogger(ColumnStatisticsImpl.class);
    protected com.yahoo.sketches.quantiles.ItemsSketch<DataValueDescriptor> quantilesSketch;
    protected com.yahoo.sketches.frequencies.ItemsSketch<DataValueDescriptor> frequenciesSketch;
    protected Sketch thetaSketch;
    protected long nullCount;
    protected DataValueDescriptor dvd;
    private long rpv=-1; //rows per value excluding skewed values

    public ColumnStatisticsImpl() {

    }

    /**
     *
     * Generates the stats implementation based on defaults for the data type (DataValueDescriptor)
     *
     * @param dvd
     * @throws StandardException
     */
    public ColumnStatisticsImpl(DataValueDescriptor dvd) throws StandardException {
        this(dvd, dvd.getQuantilesSketch(),dvd.getFrequenciesSketch(),dvd.getThetaSketch(), 0L);
    }

    /**
     *
     *
     *
     * @param dvd
     * @param quantilesSketch
     * @param frequenciesSketch
     * @param thetaSketch
     * @param nullCount
     */
    public ColumnStatisticsImpl(DataValueDescriptor dvd,
                                com.yahoo.sketches.quantiles.ItemsSketch quantilesSketch,
                                com.yahoo.sketches.frequencies.ItemsSketch frequenciesSketch,
                                Sketch thetaSketch, long nullCount
                                         ) {
        this.dvd = dvd;
        this.quantilesSketch = quantilesSketch;
        this.frequenciesSketch = frequenciesSketch;
        this.thetaSketch = thetaSketch;
        this.nullCount = nullCount;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeLong(nullCount);
        out.writeObject(dvd);
        byte[] quantilesSketchBytes = quantilesSketch.toByteArray(new DVDArrayOfItemsSerDe(dvd));
        out.writeInt(quantilesSketchBytes.length);
        out.write(quantilesSketchBytes);
        byte[] frequenciesSketchBytes = frequenciesSketch.toByteArray(new DVDArrayOfItemsSerDe(dvd));
        out.writeInt(frequenciesSketchBytes.length);
        out.write(frequenciesSketchBytes);
        byte[] thetaSketchBytes = thetaSketch.toByteArray();
        out.writeInt(thetaSketchBytes.length);
        out.write(thetaSketchBytes);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        NativeMemory quantMem = null;
        NativeMemory freqMem = null;
        NativeMemory thetaMem = null;
        try {
            nullCount = in.readLong();
            dvd = (DataValueDescriptor) in.readObject();
            byte[] quantiles = new byte[in.readInt()];
            in.readFully(quantiles);
            quantMem = new NativeMemory(quantiles);
            quantilesSketch = com.yahoo.sketches.quantiles.ItemsSketch.getInstance(quantMem, dvd, new DVDArrayOfItemsSerDe(dvd));
            byte[] frequencies = new byte[in.readInt()];
            in.readFully(frequencies);
            freqMem = new NativeMemory(frequencies);
            frequenciesSketch = com.yahoo.sketches.frequencies.ItemsSketch.getInstance(freqMem, new DVDArrayOfItemsSerDe(dvd));
            byte[] thetaSketchBytes = new byte[in.readInt()];
            in.readFully(thetaSketchBytes);
            thetaMem = new NativeMemory(thetaSketchBytes);
            thetaSketch = Sketch.heapify(thetaMem);
        } finally {
            if (quantMem!=null)
                quantMem.freeMemory();
            if (freqMem!=null)
                freqMem.freeMemory();
            if (thetaMem!=null)
                thetaMem.freeMemory();

        }
    }

    /**
     *
     * Retrieves the minimum value from the quantile sketch
     *
     * @return
     */
    @Override
    public DataValueDescriptor minValue() {
        return quantilesSketch.getMinValue();
    }
    /**
     *
     * Retrieves the null count that is recorded during computation.
     *
     * @return
     */
    @Override
    public long nullCount() {
        return nullCount;
    }
    /**
     *
     * Retrieves the not null count from the quantiles sketch.
     *
     * @return
     */
    @Override
    public long notNullCount() {
        return quantilesSketch.getN();
    }
    /**
     *
     * Retrieves the number of unique records via the ThetaSketch.
     *
     * @return
     */
    @Override
    public long cardinality() {
        return (long)thetaSketch.getEstimate();
    }

    /**
     *
     * Retrieves the maximum value via the quantiles sketch.
     *
     * @return
     */
    @Override
    public DataValueDescriptor maxValue() {
        return quantilesSketch.getMaxValue();
    }
    /**
     *
     * Retrieves the total count as a summation of the not null count and the null count.
     *
     * @return
     */

    @Override
    public long totalCount() {
        return quantilesSketch.getN()+nullCount;
    }

    /**
     *
     * Number of Records Selected from this columns implementation.
     *
     * If
     *  element is null then nullCount
     * elseif
     *  frequent element then frequencies sketch estimate
     * else {
     *     quantilesSketch total count/thetaSketch estimate
     * }
     *
     * @param element the element to match
     * @return
     */
    @Override
    public long selectivity(DataValueDescriptor element) {
        // Use Null Data
        if (element == null || element.isNull())
            return nullCount;

        // Frequent Items Sketch?

        // When look up predicate value in quantilesSketch, the hashCodes of two different data types having different values may be same by coincident
        // We should follow this checking sequence
        // 1. Checking if predicate and column's data  types are same
        // 2. Checking if they are in the same data type family
        // 3. Checking if column typePrecedence >= predicate typePrecedence, then convert predicate's data type to column
        // 4. So far, we can't use quantilesSketch.ReverseRurgeItemHashMap to look up predicate, we need to compare the predicate with each elements in frequent item sketch iteratively.

        DataValueDescriptor lookUpElement = element;
        int typeFormatId = dvd.getTypeFormatId();
        int eTypeFormatId = element.getTypeFormatId();
        boolean isSameType = false;
        boolean isSameFamily = false;
        boolean isConverted = false;

        // Checking same datatype
        if (typeFormatId == eTypeFormatId)
            {isSameType = true;}
        // Checking datatype family
        else if ((eTypeFormatId == StoredFormatIds.SQL_INTEGER_ID || eTypeFormatId == StoredFormatIds.SQL_TINYINT_ID
                || eTypeFormatId == StoredFormatIds.SQL_SMALLINT_ID ||eTypeFormatId == StoredFormatIds.SQL_LONGINT_ID)
                &&
                (typeFormatId == StoredFormatIds.SQL_INTEGER_ID || typeFormatId == StoredFormatIds.SQL_TINYINT_ID
                || typeFormatId == StoredFormatIds.SQL_SMALLINT_ID || typeFormatId == StoredFormatIds.SQL_LONGINT_ID)) {
            isSameFamily = true;
        }
        else if ((eTypeFormatId == StoredFormatIds.SQL_REAL_ID || eTypeFormatId == StoredFormatIds.SQL_DOUBLE_ID || eTypeFormatId == StoredFormatIds.SQL_DECIMAL_ID) &&
                    (typeFormatId == StoredFormatIds.SQL_REAL_ID || typeFormatId == StoredFormatIds.SQL_DOUBLE_ID || typeFormatId == StoredFormatIds.SQL_DECIMAL_ID )) {
            isSameFamily = true;
        }
        else  if (lookUpElement instanceof SQLChar && dvd instanceof SQLChar) //Handle Char, Varchar, Long Varchar
        {
            isSameFamily = true;
        }
        // Checking if convertible
        else if (dvd.typePrecedence() >= lookUpElement.typePrecedence()){
            try {
                switch (typeFormatId) {
                    case StoredFormatIds.SQL_BOOLEAN_ID:
                        lookUpElement = new SQLBoolean(element.getBoolean());
                        break;
                    case StoredFormatIds.SQL_TIME_ID:
                        lookUpElement = new SQLTime(element.getTime(null));
                        break;
                    case StoredFormatIds.SQL_TIMESTAMP_ID:
                        lookUpElement = new SQLTimestamp(element.getTimestamp(SQLDate.GREGORIAN_CALENDAR.get()));
                        break;
                    case StoredFormatIds.SQL_DATE_ID:
                        lookUpElement = new SQLDate(element.getDate(SQLDate.GREGORIAN_CALENDAR.get()));
                        break;
                    case StoredFormatIds.SQL_DOUBLE_ID:
                    case StoredFormatIds.SQL_REAL_ID:
                    case StoredFormatIds.SQL_DECIMAL_ID:
                        lookUpElement = new SQLDouble(element.getDouble()); // These types are in the same family
                        break;
                    default:
                        throw  StandardException.newException(SQLState.TYPE_MISMATCH,"Failure convert from " + element.getTypeName() + " to " + dvd.getTypeName() );
                }
                isConverted = true;
            } catch (StandardException e) {
                LOG.warn("Data type conversion failure when looking up frequencies.ItemsSketch", e);
            }
        }

        long count = 0;
        if (isSameType || isSameFamily || isConverted) {
            count = frequenciesSketch.getEstimate(lookUpElement);
        } else {
        // Iterated comparing
            com.yahoo.sketches.frequencies.ItemsSketch.Row<DataValueDescriptor>[] items = frequenciesSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
            for (com.yahoo.sketches.frequencies.ItemsSketch.Row<DataValueDescriptor> row: items) {
                DataValueDescriptor skewedValue = row.getItem();
                try {
                    if (skewedValue != null && skewedValue.compare(ORDER_OP_EQUALS, lookUpElement, false, false)) {
                        count = row.getEstimate();
                        break;
                    }
                } catch (StandardException e) {
                    // this should not happen, but if it happens, cost estimation error does not need to fail the query
                    LOG.warn("Failure is not expected but we don't want to fail the query because of estimation error", e);
                }
            }
        }
        if (count > 0)
            return count;
        // Return Cardinality Based Estimate
        if (rpv == -1)
            rpv = getAvgRowsPerValueExcludingSkews();
        return rpv;
    }

    private long getAvgRowsPerValueExcludingSkews() {
        long skewCount = 0;
        long skewNum = 0;
        com.yahoo.sketches.frequencies.ItemsSketch.Row<DataValueDescriptor>[] items = frequenciesSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        for (com.yahoo.sketches.frequencies.ItemsSketch.Row<DataValueDescriptor> row: items) {
            skewCount += row.getEstimate();
            skewNum ++;
        }
        long nonSkewedNum = (long)(thetaSketch.getEstimate() - skewNum);
        if (nonSkewedNum <= 0)
            nonSkewedNum = 1;
        long nonSkewCount = quantilesSketch.getN() - skewCount;
        if (nonSkewCount < 0)
            nonSkewCount = 0;
        return (long) (((double)nonSkewCount)/nonSkewedNum);
    }

    private long getSkewedRowCountInRange(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
        long skewCount = 0;
        com.yahoo.sketches.frequencies.ItemsSketch.Row<DataValueDescriptor>[] items = frequenciesSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        for (com.yahoo.sketches.frequencies.ItemsSketch.Row<DataValueDescriptor> row: items) {
            DataValueDescriptor skewedValue = row.getItem();
            try {
                if (skewedValue != null &&
                        (start == null || start.isNull() || skewedValue.compare(includeStart ? ORDER_OP_GREATEROREQUALS : ORDER_OP_GREATERTHAN, start, false, false)) &&
                        (stop == null || stop.isNull() || skewedValue.compare(includeStop ? ORDER_OP_LESSOREQUALS : ORDER_OP_LESSTHAN, stop, false, false)))
                    skewCount += row.getEstimate();
            } catch (StandardException e) {
                // this should not happen, but if it happens, cost estimation error does not need to fail the query
                LOG.warn("Failure is not expected but we don't want to fail the query because of estimation error", e);
            }
        }

        return skewCount;
    }
    /**
     *
     * Using the Cumulative Distribution Function from the quantiles sketch to determine the
     * number of records that are in the range.
     *
     * @param start the start of the range to estimate. If {@code null}, then scan everything before {@code stop}.
     *              If {@code stop} is also {@code null}, then this will return an estimate to the number of entries
     *              in the entire data set.
     * @param stop the end of the range to estimate. If {@code null}, then scan everything after {@code start}.
     *             If {@code start} is also {@code null}, then this will return an estimate of the number of entries
     *             in the entire data set.
     * @param includeStart if {@code true}, then include entries which are equal to {@code start}
     * @param includeStop if {@code true}, then include entries which are <em>equal</em> to {@code stop}
     * @param useExtrapolation if {@code true}, then do extrapolation if the range falls beyond the min-max range recorded in stats
     * @return
     */
    @Override
    public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop, boolean useExtrapolation) {
        long qualifiedRows = 0;


        /** rule if extrapolation is enabled
         *  For point range: use average rows per value if the value falls beyond the min-max range.
         *  For regular range: compute the range that is in the min-max range, for the range fall outside the min-max range, linearly project based on the
         *  following formula::
         *         range_outside_min_max/(max-min)*total_rows
         */
        //check if range beyond [min,max]
        boolean outOfRange = false;
        DataValueDescriptor maxValue = quantilesSketch.getMaxValue();
        DataValueDescriptor minValue = quantilesSketch.getMinValue();

        try {
            if (maxValue != null &&
                    start != null && !start.isNull() && start.compare(includeStart ? ORDER_OP_GREATERTHAN : ORDER_OP_GREATEROREQUALS, maxValue, false, false))
                outOfRange = true;

            if (!outOfRange) {
                if (minValue != null &&
                        stop != null && !stop.isNull() && stop.compare(includeStop ? ORDER_OP_LESSTHAN : ORDER_OP_LESSOREQUALS, minValue, false, false))
                    outOfRange = true;
            }
        } catch (StandardException e) {
            // this should not happen, but if it happens, cost estimation error does not need to fail the query
            LOG.warn("Failure is not expected but we don't want to fail the query because of estimation error", e);
        }

        if (outOfRange && !useExtrapolation)
            return 0;

        //if point range, take the point range selectivity path
        boolean isPointRange = false;
        if ((start == null || start.isNull()) && (stop == null || stop.isNull()) && includeStart && includeStop)
            isPointRange = true;
        if (includeStart && includeStop && start != null && stop != null && start.equals(stop))
            isPointRange = true;

        if (isPointRange) {
            if (outOfRange && useExtrapolation) {
                if (rpv == -1)
                    rpv = getAvgRowsPerValueExcludingSkews();
                return rpv;
            }
            else /* not out of range */
                return selectivity(start);
        }

        if (!useExtrapolation) {
            qualifiedRows = computeRangeSelectivity(start, stop, includeStart, includeStop);
        } else {
            // compute the range that falls in the min-max range
            DataValueDescriptor newStart = start, newStop = stop;
            double length = 0;

            try {
                // is start < min?
                if (minValue != null &&
                      start != null && !start.isNull() && start.compare(ORDER_OP_LESSTHAN, minValue, false, false)) {
                    // check if even stop < min
                    if (stop != null && !stop.isNull() && stop.compare(ORDER_OP_LESSTHAN, minValue, false, false))
                        length = computeRange(start, stop);
                    else
                        length = computeRange(start, minValue);
                    newStart = minValue;
                    includeStart = true;
                }

                // is stop > max?
                if (maxValue != null &&
                      stop != null && !stop.isNull() && stop.compare(ORDER_OP_GREATERTHAN, maxValue, false, false)) {
                    // check if even start > max
                    if (start != null && !start.isNull() && start.compare(ORDER_OP_GREATERTHAN, maxValue, false, false))
                        length += computeRange(start, stop);
                    else
                        length += computeRange(maxValue, stop);
                    newStop = maxValue;
                    includeStop = true;
                }

                qualifiedRows = computeRangeSelectivity(newStart, newStop, includeStart, includeStop);

                //do the linear projection for the range that falls out of the min-max range
                if (length > 0.0)
                    qualifiedRows += length/computeRange(minValue, maxValue) * quantilesSketch.getN();
            } catch (StandardException e) {
                // this should not happen, but if it happens, cost estimation error does not need to fail the query
                LOG.warn("Failure is not expected but we don't want to fail the query because of estimation error", e);
            }
        }

        /* Use average selectivity as a lower bound */
        if (rpv == -1)
            rpv = getAvgRowsPerValueExcludingSkews();

        if (qualifiedRows < rpv)
            qualifiedRows = rpv;

        /* with extrapolation, there is a possibility that the qualifiedRows is bigger than the total row, bound it with total row */
        if (qualifiedRows > quantilesSketch.getN())
            qualifiedRows = quantilesSketch.getN();

        return qualifiedRows;
    }

    private long computeRangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
        /** range selectivity path:
         * we want to be a bit conservative to avoid extreme under-estimation. So we compare the following 2 points:
         * 1. range selectivity returned by CDF
         * 2. selectivity of skewed values fall in the current range
         * And take the maximum among 2. Coming out of this function, at the caller function rangeSelectivity(), we need to further bound
         * it with the lower bound of rpv and upper bound of total not-null rows.
         */

        long qualifiedRows = 0;
        /* 1. range selectivity returned by CDF */
        if (!includeStart && start != null && !start.isNull())
            start = new StatsExcludeStartDVD(start);
        double startSelectivity = start == null || start.isNull() ? 0.0d : quantilesSketch.getCDF(new DataValueDescriptor[]{start})[0];
        if (includeStop && stop != null && !stop.isNull())
            stop = new StatsIncludeEndDVD(stop);
        double stopSelectivity = stop == null || stop.isNull() ? 1.0d : quantilesSketch.getCDF(new DataValueDescriptor[]{stop})[0];
        double totalSelectivity = stopSelectivity - startSelectivity;
        double count = (double) quantilesSketch.getN();
        if (totalSelectivity == Double.NaN || count == 0)
            qualifiedRows = 0;
        else
            qualifiedRows = Math.round(totalSelectivity * count);

        /* 2. selectivity of skewed values fall in the current range */
        long skewedRowCountInRange = getSkewedRowCountInRange(start, stop, includeStart, includeStop);
        if (qualifiedRows < skewedRowCountInRange)
            qualifiedRows = skewedRowCountInRange;

        return qualifiedRows;
    }

    private double computeRange(DataValueDescriptor low, DataValueDescriptor high) throws StandardException {
        int typeFormatId = dvd.getTypeFormatId();
        switch (typeFormatId) {
            case StoredFormatIds.SQL_TINYINT_ID:
            case StoredFormatIds.SQL_SMALLINT_ID:
            case StoredFormatIds.SQL_INTEGER_ID:
            case StoredFormatIds.SQL_LONGINT_ID:
            case StoredFormatIds.SQL_REAL_ID:
            case StoredFormatIds.SQL_DOUBLE_ID:
            case StoredFormatIds.SQL_DECIMAL_ID:
                return high.getDouble() - low.getDouble();
            case StoredFormatIds.SQL_DATE_ID:
                SQLDate newLowDate, newHighDate;
                if (low.getTypeFormatId() != typeFormatId) {
                    try {
                        newLowDate = new SQLDate(low.getDate(SQLDate.GREGORIAN_CALENDAR.get()));
                    } catch (StandardException e) {
                        LOG.warn("Failure conversion from " + low.getTypeName() + " to " + dvd.getTypeName() + " in stats estimation.", e);
                        throw StandardException.newException(SQLState.TYPE_MISMATCH, "Failure conversion from " + low.getTypeName() + " to " + dvd.getTypeName() + " in stats estimation.");
                    }
                } else {
                    newLowDate = (SQLDate) low;
                }

                if (high.getTypeFormatId() != typeFormatId) {
                    try {
                        newHighDate = new SQLDate(high.getDate(SQLDate.GREGORIAN_CALENDAR.get()));
                    } catch (StandardException e) {
                        LOG.warn("Failure conversion from " + high.getTypeName() + " to " + dvd.getTypeName() + " in stats estimation.", e);
                        throw StandardException.newException(SQLState.TYPE_MISMATCH, "Failure conversion from " + high.getTypeName() + " to " + dvd.getTypeName() + " in stats estimation.");
                    }
                } else {
                    newHighDate = (SQLDate) high;
                }
                return newHighDate.minus(newHighDate, newLowDate, null).getDouble();
            case StoredFormatIds.SQL_TIMESTAMP_ID:
                SQLTimestamp newLowTimestamp, newHighTimeStamp;
                if (low.getTypeFormatId() != typeFormatId) {
                    try {
                        newLowTimestamp = new SQLTimestamp(low.getTimestamp(SQLDate.GREGORIAN_CALENDAR.get()));
                    } catch (StandardException e) {
                        LOG.warn("Failure conversion from " + low.getTypeName() + " to " + dvd.getTypeName() + " in stats estimation.", e);
                        throw StandardException.newException(SQLState.TYPE_MISMATCH, "Failure conversion from " + low.getTypeName() + " to " + dvd.getTypeName() + " in stats estimation.");
                    }
                } else {
                    newLowTimestamp = (SQLTimestamp)low;
                }
                if (high.getTypeFormatId() != typeFormatId) {
                    try {
                        newHighTimeStamp = new SQLTimestamp(high.getTimestamp(SQLDate.GREGORIAN_CALENDAR.get()));
                    } catch (StandardException e) {
                        LOG.warn("Failure conversion from " + high.getTypeName() + " to " + dvd.getTypeName() + " in stats estimation.", e);
                        throw StandardException.newException(SQLState.TYPE_MISMATCH, "Failure conversion from " + high.getTypeName() + " to " + dvd.getTypeName() + " in stats estimation.");
                    }
                } else {
                    newHighTimeStamp = (SQLTimestamp)high;
                }
                return newHighTimeStamp.minus(newHighTimeStamp, newLowTimestamp, null).getDouble();
            default:
                break;
        }
        return 1.0;
    }

    @Override
    public long rangeSelectivity(DataValueDescriptor start, DataValueDescriptor stop, boolean includeStart, boolean includeStop) {
        return rangeSelectivity(start, stop, includeStart, includeStop, false);
    }

        /**
         *
         * Updating the column's value for the three sketches and the null count.
         *
         * @param dvd
         */
    @Override
    public void update(DataValueDescriptor dvd) {
        if (dvd.isNull()) {
            nullCount++;
        } else {
            frequenciesSketch.update(dvd);
            quantilesSketch.update(dvd);
            dvd.updateThetaSketch((UpdateSketch) thetaSketch);
        }
    }

    @Override
    public String toString() {
        return String.format("Statistics{nullCount=%d, frequencies=%s, quantiles=%s, theta=%s}",nullCount,frequenciesSketch,quantilesSketch.toString(true,false),thetaSketch.toString());
    }

    /**
     *
     * Shallow copy of the statistics.
     *
     * @return
     */
    @Override
    public ItemStatistics<DataValueDescriptor> getClone() {
        return new ColumnStatisticsImpl(dvd.cloneValue(false),
                quantilesSketch,
                frequenciesSketch,
                thetaSketch,
                nullCount);
    }

    /**
     *
     * COLUMN
     *
     * @return
     */
    @Override
    public Type getType() {
        return Type.COLUMN;
    }

    /**
     *
     * Column DataValueDescriptor (type information)
     *
     * @return
     */
    public DataValueDescriptor getColumnDescriptor() {
        return dvd;
    }

    /**
     *
     * Retrieve the quantiles sketch directly.
     *
     * @return
     */
    public ItemsSketch<DataValueDescriptor> getQuantilesSketch() {
        return quantilesSketch;
    }
    /**
     *
     * Retrieve the frequencies sketch directly.
     *
     * @return
     */
    public com.yahoo.sketches.frequencies.ItemsSketch<DataValueDescriptor> getFrequenciesSketch() {
        return frequenciesSketch;
    }
    /**
     *
     * Retrieve the theta sketch directly.
     *
     * @return
     */
    public Sketch getThetaSketch() {
        return thetaSketch;
    }

    @Override
    public long selectivityExcludingValueIfSkewed(DataValueDescriptor value) {
        long skewCount = 0;
        long skewNum = 0;
        com.yahoo.sketches.frequencies.ItemsSketch.Row<DataValueDescriptor>[] items = frequenciesSketch.getFrequentItems(ErrorType.NO_FALSE_POSITIVES);
        for (com.yahoo.sketches.frequencies.ItemsSketch.Row<DataValueDescriptor> row: items) {
            DataValueDescriptor skewedValue = row.getItem();
            try {
                if (skewedValue != null && skewedValue.compare(ORDER_OP_EQUALS, value, false, false)) {
                    skewCount = row.getEstimate();
                    skewNum++;
                    break;
                }
            } catch (StandardException e) {
                // this should not happen, but if it happens, cost estimation error does not need to fail the query
                LOG.warn("Failure is not expected but we don't want to fail the query because of estimation error", e);
            }
        }
        long nonSkewedNum = (long)(thetaSketch.getEstimate() - skewNum);
        if (nonSkewedNum <= 0)
            nonSkewedNum = 1;
        long nonSkewCount = quantilesSketch.getN() - skewCount;
        if (nonSkewCount < 0)
            nonSkewCount = 0;
        return (long) (((double)nonSkewCount)/nonSkewedNum);
    }
}
