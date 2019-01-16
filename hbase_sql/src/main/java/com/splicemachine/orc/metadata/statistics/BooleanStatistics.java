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
package com.splicemachine.orc.metadata.statistics;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.types.SQLBoolean;
import com.splicemachine.orc.input.SpliceOrcNewInputFormat;

import java.io.IOException;
import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class BooleanStatistics
{
    // 1 byte to denote if null + 1 byte for the value
    public static final long BOOLEAN_VALUE_BYTES = Byte.BYTES + Byte.BYTES;

    private final long trueValueCount;

    public BooleanStatistics(long trueValueCount)
    {
        this.trueValueCount = trueValueCount;
    }

    public long getTrueValueCount()
    {
        return trueValueCount;
    }
/* msirek-temp->
    public static ColumnStatistics getPartitionColumnStatistics(String value) throws IOException {
        BooleanStatistics booleanStatistics = null;
        if(value != null) {
            try {
                SQLBoolean sqlBoolean = new SQLBoolean();
                sqlBoolean.setValue(value);
                booleanStatistics = new BooleanStatistics(sqlBoolean.getBoolean()? SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE:0l);
            } catch (StandardException se) {
                throw new IOException(se);
            }

        }
        return new ColumnStatistics(SpliceOrcNewInputFormat.DEFAULT_PARTITION_SIZE,booleanStatistics,null,null,null,null,null,null);
    }
*/
    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        BooleanStatistics that = (BooleanStatistics) o;
        return trueValueCount == that.trueValueCount;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(trueValueCount);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("trueValueCount", trueValueCount)
                .toString();
    }
}

