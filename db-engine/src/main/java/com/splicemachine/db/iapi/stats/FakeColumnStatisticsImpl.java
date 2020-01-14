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

package com.splicemachine.db.iapi.stats;

import com.splicemachine.db.iapi.types.DataValueDescriptor;
import org.apache.log4j.Logger;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import static com.splicemachine.db.impl.sql.compile.SelectivityUtil.DEFAULT_RANGE_SELECTIVITY;

/**
 * Created by yxia on 11/7/19.
 */
public class FakeColumnStatisticsImpl extends ColumnStatisticsImpl implements Externalizable {
    private static Logger LOG=Logger.getLogger(FakeColumnStatisticsImpl.class);
    private long nullCount;
    private long totalCount;
    private long cardinality;
    private long rpv;

    public FakeColumnStatisticsImpl() {
    }

    public FakeColumnStatisticsImpl(DataValueDescriptor dvd,
                             long nullCount,
                             long totalCount,
                             long cardinality) {
        this.dvd = dvd;
        this.nullCount = nullCount;
        this.totalCount = totalCount;
        if (totalCount < nullCount)
            this.totalCount = nullCount;
        this.cardinality = cardinality;
        if (cardinality > this.totalCount - nullCount + 1)
            this.cardinality = this.totalCount - nullCount + 1;
        rpv = (long) (((double)(this.totalCount - nullCount))/cardinality);

    }

    @Override
    public Type getType() {
        return Type.FAKE;
    }

    @Override
    public DataValueDescriptor minValue() {
        return null;
    }

    @Override
    public DataValueDescriptor maxValue() {
        return null;
    }

    @Override
    public long totalCount() {
        return totalCount;
    }

    @Override
    public long nullCount() {
        return nullCount;
    }

    @Override
    public long notNullCount() {
        long notNullCount = totalCount - nullCount;
        if (notNullCount < 0)
            notNullCount = 0;
        return notNullCount;
    }

    @Override
    public long cardinality() {
        return cardinality;
    }

    @Override
    public long selectivity(DataValueDescriptor element) {
        // Use Null Data
        if (element == null || element.isNull())
            return nullCount;

        if (rpv == -1)
            rpv = (long) (((double)notNullCount())/cardinality);

        return rpv;
    }

    @Override
    public long rangeSelectivity(DataValueDescriptor start,DataValueDescriptor stop, boolean includeStart,boolean includeStop,boolean useExtrapolation) {
        if ((start == null || start.isNull()) && (stop == null || stop.isNull()))
            return nullCount;

        if (start.equals(stop))
            return rpv;

        return (long)(DEFAULT_RANGE_SELECTIVITY * totalCount);
    }

    @Override
    public long rangeSelectivity(DataValueDescriptor start,DataValueDescriptor stop, boolean includeStart,boolean includeStop) {
        return rangeSelectivity(start, stop, includeStart, includeStop, false);
    }

    public void update(DataValueDescriptor item) {

    }

    @Override
    public ItemStatistics<DataValueDescriptor> getClone() {
        return new FakeColumnStatisticsImpl(dvd.cloneValue(false), nullCount, totalCount, cardinality);
    }

    @Override
    public long selectivityExcludingValueIfSkewed(DataValueDescriptor value) {
        return totalCount;
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(dvd);
        out.writeLong(totalCount);
        out.writeLong(nullCount);
        out.writeLong(cardinality);
        out.writeLong(rpv);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        dvd = (DataValueDescriptor)in.readObject();
        totalCount = in.readLong();
        nullCount = in.readLong();
        cardinality = in.readLong();
        rpv = in.readLong();
    }

    @Override
    public String toString() {
        return "totalCount:" + totalCount +
                ", nullCount:" + nullCount +
                ", cardinality:" + cardinality +
                ", rpv:" + rpv;
    }

}
