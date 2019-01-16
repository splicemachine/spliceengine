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

import static com.google.common.base.MoreObjects.toStringHelper;

public class BinaryStatistics
{
    // 1 byte to denote if null + 4 bytes to denote offset
    public static final long BINARY_VALUE_BYTES_OVERHEAD = Byte.BYTES + Integer.BYTES;

    private final long sum;

    public BinaryStatistics(long sum)
    {
        this.sum = sum;
    }

    public long getSum()
    {
        return sum;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("sum", sum)
                .toString();
    }
}

