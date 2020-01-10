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
package com.splicemachine.orc.metadata;

import static com.google.common.base.MoreObjects.toStringHelper;

public class StripeInformation
{
    private final int numberOfRows;
    private final long offset;
    private final long indexLength;
    private final long dataLength;
    private final long footerLength;

    public StripeInformation(int numberOfRows, long offset, long indexLength, long dataLength, long footerLength)
    {
        this.numberOfRows = numberOfRows;
        this.offset = offset;
        this.indexLength = indexLength;
        this.dataLength = dataLength;
        this.footerLength = footerLength;
    }

    public int getNumberOfRows()
    {
        return numberOfRows;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getIndexLength()
    {
        return indexLength;
    }

    public long getDataLength()
    {
        return dataLength;
    }

    public long getFooterLength()
    {
        return footerLength;
    }

    public long getTotalLength()
    {
        return indexLength + dataLength + footerLength;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRows", numberOfRows)
                .add("offset", offset)
                .add("indexLength", indexLength)
                .add("dataLength", dataLength)
                .add("footerLength", footerLength)
                .toString();
    }
}
