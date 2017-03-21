package com.splicemachine.orc;

import org.apache.spark.sql.types.DataType;

import static java.util.Objects.requireNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.MoreObjects.toStringHelper;

/**
 * Created by jleach on 3/16/17.
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

