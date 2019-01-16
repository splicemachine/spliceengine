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
package com.splicemachine.orc.metadata;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import org.apache.spark.sql.types.*;
import java.util.ArrayList;
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
    private final Optional<Integer> length;
    private final Optional<Integer> precision;
    private final Optional<Integer> scale;

    private OrcType(OrcTypeKind orcTypeKind)
    {
        this(orcTypeKind, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.empty(), Optional.empty());
    }

    private OrcType(OrcTypeKind orcTypeKind, int length)
    {
        this(orcTypeKind, ImmutableList.of(), ImmutableList.of(), Optional.of(length), Optional.empty(), Optional.empty());
    }

    private OrcType(OrcTypeKind orcTypeKind, int precision, int scale)
    {
        this(orcTypeKind, ImmutableList.of(), ImmutableList.of(), Optional.empty(), Optional.of(precision), Optional.of(scale));
    }

    private OrcType(OrcTypeKind orcTypeKind, List<Integer> fieldTypeIndexes, List<String> fieldNames)
    {
        this(orcTypeKind, fieldTypeIndexes, fieldNames, Optional.empty(), Optional.empty(), Optional.empty());
    }

    public OrcType(OrcTypeKind orcTypeKind, List<Integer> fieldTypeIndexes, List<String> fieldNames, Optional<Integer> length, Optional<Integer> precision, Optional<Integer> scale)
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
        this.length = requireNonNull(length, "length is null");
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

    public List<Integer> getFieldTypeIndexes()
    {
        return fieldTypeIndexes;
    }

    public String getFieldName(int field)
    {
        return fieldNames.get(field);
    }

    public List<String> getFieldNames()
    {
        return fieldNames;
    }

    public Optional<Integer> getLength()
    {
        return length;
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

    private static List<OrcType> toOrcType(int nextFieldTypeIndex, DataType type)
    {
        if (type instanceof DoubleType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.BOOLEAN));
        }
        if (type instanceof ByteType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.BYTE));
        }
        if (type instanceof ShortType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.SHORT));
        }
        if (type instanceof IntegerType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.INT));
        }
        if (type instanceof LongType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.LONG));
        }
        if (type instanceof DoubleType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.DOUBLE));
        }
        if (type instanceof FloatType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.FLOAT));
        }
        if (type instanceof StringType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.STRING));
        }
        if (type instanceof CharType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.CHAR, ((CharType) type).length()));
        }
        if (type instanceof VarcharType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.VARCHAR, ((VarcharType) type).length()));
        }
        if (type instanceof BinaryType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.BINARY));
        }
        if (type instanceof DateType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.DATE));
        }
        if (type instanceof TimestampType) {
            return ImmutableList.of(new OrcType(OrcTypeKind.TIMESTAMP));
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return ImmutableList.of(new OrcType(OrcTypeKind.DECIMAL, decimalType.precision(), decimalType.scale()));
        }
        if (type instanceof ArrayType) {
            return createOrcArrayType(nextFieldTypeIndex, ((ArrayType) type).elementType());
        }
        if (type instanceof MapType) {
            return createOrcMapType(nextFieldTypeIndex, ((MapType) type).keyType(), ((MapType) type).valueType());
        }
        if (type instanceof StructType) {
            List<String> fieldNames = Lists.newArrayList(((StructType) type).fieldNames());
            List<DataType> fieldTypes = new ArrayList<DataType>(fieldNames.size());
            for (StructField sf: ((StructType) type).fields()) {
                fieldTypes.add(sf.dataType());
            }
            return createOrcRowType(nextFieldTypeIndex, fieldNames, fieldTypes);
        }
        throw new RuntimeException("unsupported hive type");
    }

    private static List<OrcType> createOrcArrayType(int nextFieldTypeIndex, DataType itemType)
    {
        nextFieldTypeIndex++;
        List<OrcType> itemTypes = toOrcType(nextFieldTypeIndex, itemType);

        List<OrcType> orcTypes = new ArrayList<>();
        orcTypes.add(new OrcType(OrcTypeKind.LIST, ImmutableList.of(nextFieldTypeIndex), ImmutableList.of("item")));
        orcTypes.addAll(itemTypes);
        return orcTypes;
    }

    private static List<OrcType> createOrcMapType(int nextFieldTypeIndex, DataType keyType, DataType valueType)
    {
        nextFieldTypeIndex++;
        List<OrcType> keyTypes = toOrcType(nextFieldTypeIndex, keyType);
        List<OrcType> valueTypes = toOrcType(nextFieldTypeIndex + keyTypes.size(), valueType);

        List<OrcType> orcTypes = new ArrayList<>();
        orcTypes.add(new OrcType(OrcTypeKind.MAP, ImmutableList.of(nextFieldTypeIndex, nextFieldTypeIndex + keyTypes.size()), ImmutableList.of("key", "value")));
        orcTypes.addAll(keyTypes);
        orcTypes.addAll(valueTypes);
        return orcTypes;
    }

    public static List<OrcType> createOrcRowType(int nextFieldTypeIndex, List<String> fieldNames, List<DataType> fieldTypes)
    {
        nextFieldTypeIndex++;
        List<Integer> fieldTypeIndexes = new ArrayList<>();
        List<List<OrcType>> fieldTypesList = new ArrayList<>();
        for (DataType fieldType : fieldTypes) {
            fieldTypeIndexes.add(nextFieldTypeIndex);
            List<OrcType> fieldOrcTypes = toOrcType(nextFieldTypeIndex, fieldType);
            fieldTypesList.add(fieldOrcTypes);
            nextFieldTypeIndex += fieldOrcTypes.size();
        }

        List<OrcType> orcTypes = new ArrayList<>();
        orcTypes.add(new OrcType(
                OrcTypeKind.STRUCT,
                fieldTypeIndexes,
                fieldNames));
        fieldTypesList.forEach(orcTypes::addAll);

        return orcTypes;
    }
}

