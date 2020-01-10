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

import org.apache.spark.sql.types.DataType;

import static java.util.Objects.requireNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.MoreObjects.toStringHelper;

/**
 *
 */
public class ColumnReference<C>
{
    private final C column;
    private final int ordinal;
    private final DataType type;

    public ColumnReference(C column, int ordinal, DataType type)
    {
        this.column = requireNonNull(column, "column is null");
        checkArgument(ordinal >= 0, "ordinal is negative");
        this.ordinal = ordinal;
        this.type = requireNonNull(type, "type is null");
    }

    public C getColumn()
    {
        return column;
    }

    public int getOrdinal()
    {
        return ordinal;
    }

    public DataType getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("column", column)
                .add("ordinal", ordinal)
                .add("type", type)
                .toString();
    }
}

