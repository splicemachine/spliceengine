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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class OrcType
{
    public enum OrcTypeKind
    {
        BOOLEAN,

        BYTE,
        SHORT,
        INT,
        LONG,
        DECIMAL,

        FLOAT,
        DOUBLE,

        STRING,
        VARCHAR,
        CHAR,

        BINARY,

        DATE,
        TIMESTAMP,

        LIST,
        MAP,
        STRUCT,
        UNION,
    }

    private final OrcTypeKind orcTypeKind;
    private final List<Integer> fieldTypeIndexes;
    private final List<String> fieldNames;
    private final Optional<Integer> precision;
    private final Optional<Integer> scale;

    public OrcType(OrcTypeKind orcTypeKind, List<Integer> fieldTypeIndexes, List<String> fieldNames, Optional<Integer> precision, Optional<Integer> scale)
    {
        this.orcTypeKind = requireNonNull(orcTypeKind, "typeKind is null");
        this.fieldTypeIndexes = ImmutableList.copyOf(requireNonNull(fieldTypeIndexes, "fieldTypeIndexes is null"));
        if (fieldNames == null || (fieldNames.isEmpty() && !fieldTypeIndexes.isEmpty())) {
            this.fieldNames = null;
        }
        else {
            this.fieldNames = ImmutableList.copyOf(requireNonNull(fieldNames, "fieldNames is null"));
            checkArgument(fieldNames.size() == fieldTypeIndexes.size(), "fieldNames and fieldTypeIndexes have different sizes");
        }
        this.precision = requireNonNull(precision, "precision is null");
        this.scale = requireNonNull(scale, "scale can not be null");
    }

    public OrcTypeKind getOrcTypeKind()
    {
        return orcTypeKind;
    }

    public int getFieldCount()
    {
        return fieldTypeIndexes.size();
    }

    public int getFieldTypeIndex(int field)
    {
        return fieldTypeIndexes.get(field);
    }

    public String getFieldName(int field)
    {
        return fieldNames.get(field);
    }

    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    public Optional<Integer> getPrecision()
    {
        return precision;
    }

    public Optional<Integer> getScale()
    {
        return scale;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("orcTypeKind", orcTypeKind)
                .add("fieldTypeIndexes", fieldTypeIndexes)
                .add("fieldNames", fieldNames)
                .toString();
    }
}
