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
package com.splicemachine.orc.writer;

import com.splicemachine.orc.metadata.CompressionKind;
import com.splicemachine.orc.metadata.OrcType;
import com.splicemachine.orc.metadata.statistics.BinaryStatisticsBuilder;
import com.splicemachine.orc.metadata.statistics.DateStatisticsBuilder;
import com.splicemachine.orc.metadata.statistics.IntegerStatisticsBuilder;
import com.google.common.collect.ImmutableList;
import org.apache.spark.sql.types.*;
import org.joda.time.DateTimeZone;
import shapeless.DataT;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static java.util.Objects.requireNonNull;
import static com.google.common.base.Preconditions.checkArgument;

public final class ColumnWriters
{
    private ColumnWriters() {}

    public static ColumnWriter createColumnWriter(
            int columnIndex,
            List<OrcType> orcTypes,
            DataType type,
            CompressionKind compression,
            int bufferSize,
            boolean isDwrf,
            DateTimeZone hiveStorageTimeZone)
    {
        requireNonNull(type, "type is null");
        OrcType orcType = orcTypes.get(columnIndex);
        switch (orcType.getOrcTypeKind()) {
            case BOOLEAN:
                return new BooleanColumnWriter(columnIndex, type, compression, bufferSize);

            case FLOAT:
                return new FloatColumnWriter(columnIndex, type, compression, bufferSize);

            case DOUBLE:
                return new DoubleColumnWriter(columnIndex, type, compression, bufferSize);

            case BYTE:
                return new ByteColumnWriter(columnIndex, type, compression, bufferSize);

            case DATE:
                checkArgument(!isDwrf, "DWRF does not support %s type", type);
                return new LongColumnWriter(columnIndex, type, compression, bufferSize, false, DateStatisticsBuilder::new);

            case SHORT:
            case INT:
            case LONG:
                return new LongColumnWriter(columnIndex, type, compression, bufferSize, isDwrf, IntegerStatisticsBuilder::new);

            case DECIMAL:
                checkArgument(!isDwrf, "DWRF does not support %s type", type);
                return new DecimalColumnWriter(columnIndex, type, compression, bufferSize, false);

            case TIMESTAMP:
                return new TimestampColumnWriter(columnIndex, type, compression, bufferSize, isDwrf, hiveStorageTimeZone);

            case BINARY:
                return new SliceDirectColumnWriter(columnIndex, type, compression, bufferSize, isDwrf, BinaryStatisticsBuilder::new);

            case CHAR:
                checkArgument(!isDwrf, "DWRF does not support %s type", type);
                // fall through
            case VARCHAR:
            case STRING:
                return new SliceDictionaryColumnWriter(columnIndex, type, compression, bufferSize, isDwrf);

            case LIST: {
                int fieldColumnIndex = orcType.getFieldTypeIndex(0);
                DataType fieldType = ((ArrayType)type).elementType();
                ColumnWriter elementWriter = createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, bufferSize, isDwrf, hiveStorageTimeZone);
                return new ListColumnWriter(columnIndex, compression, bufferSize, isDwrf, elementWriter);
            }

            case MAP: {
                ColumnWriter keyWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(0),
                        orcTypes,
                        ((MapType)type).keyType(),
                        compression,
                        bufferSize,
                        isDwrf,
                        hiveStorageTimeZone);
                ColumnWriter valueWriter = createColumnWriter(
                        orcType.getFieldTypeIndex(1),
                        orcTypes,
                        ((MapType)type).valueType(),
                        compression,
                        bufferSize,
                        isDwrf,
                        hiveStorageTimeZone);
                return new MapColumnWriter(columnIndex, compression, bufferSize, isDwrf, keyWriter, valueWriter);
            }

            case STRUCT: {
                ImmutableList.Builder<ColumnWriter> fieldWriters = ImmutableList.builder();
                for (int fieldId = 0; fieldId < orcType.getFieldCount(); fieldId++) {
                    int fieldColumnIndex = orcType.getFieldTypeIndex(fieldId);
                    DataType fieldType = ((StructType) type).fields()[fieldId].dataType();
                    fieldWriters.add(createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, bufferSize, isDwrf, hiveStorageTimeZone));
                }
                return new StructColumnWriter(columnIndex, compression, bufferSize, fieldWriters.build());
            }
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }


    public static ColumnWriter createColumnWriterFromSparkType(
            int columnIndex,
            List<DataType> sparkTypes,
            DataType type,
            CompressionKind compression,
            int bufferSize,
            boolean isDwrf,
            DateTimeZone hiveStorageTimeZone) {
        requireNonNull(type, "type is null");
        DataType dataType = sparkTypes.get(columnIndex);
        if (dataType instanceof BooleanType)
                return new BooleanColumnWriter(columnIndex, type, compression, bufferSize);
        if (dataType instanceof FloatType)
                return new FloatColumnWriter(columnIndex, type, compression, bufferSize);
        if (dataType instanceof DoubleType)
                return new DoubleColumnWriter(columnIndex, type, compression, bufferSize);
        if (dataType instanceof ByteType)
                return new ByteColumnWriter(columnIndex, type, compression, bufferSize);
        if (dataType instanceof DateType) {
            checkArgument(!isDwrf, "DWRF does not support %s type", type);
            return new LongColumnWriter(columnIndex, type, compression, bufferSize, false, DateStatisticsBuilder::new);
        }
        if (dataType instanceof ShortType || dataType instanceof IntegerType || dataType instanceof LongType)
                return new LongColumnWriter(columnIndex, type, compression, bufferSize, isDwrf, IntegerStatisticsBuilder::new);
        if (dataType instanceof DecimalType) {
            checkArgument(!isDwrf, "DWRF does not support %s type", type);
            return new DecimalColumnWriter(columnIndex, type, compression, bufferSize, false);
        }
        if (dataType instanceof TimestampType)
                return new TimestampColumnWriter(columnIndex, type, compression, bufferSize, isDwrf, hiveStorageTimeZone);
        if (dataType instanceof BinaryType)
                return new SliceDirectColumnWriter(columnIndex, type, compression, bufferSize, isDwrf, BinaryStatisticsBuilder::new);
        if (dataType instanceof CharType)
            checkArgument(!isDwrf, "DWRF does not support %s type", type);
        if (dataType instanceof CharType || dataType instanceof StringType)
                return new SliceDictionaryColumnWriter(columnIndex, type, compression, bufferSize, isDwrf);
        if (dataType instanceof ArrayType) {
            throw new UnsupportedOperationException("Not Supported yet");
/*            int fieldColumnIndex = .getFieldTypeIndex(0);
            DataType fieldType = ((ArrayType) type).elementType();
            ColumnWriter elementWriter = createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, bufferSize, isDwrf, hiveStorageTimeZone);
            return new ListColumnWriter(columnIndex, compression, bufferSize, isDwrf, elementWriter);
            */
        }
        if (dataType instanceof MapType) {
                List list = new ArrayList<>();
                list.add(((MapType)dataType).keyType());
                list.add(((MapType)dataType).valueType());
                ColumnWriter keyWriter = createColumnWriterFromSparkType(
                        0,
                        list,
                        ((MapType)type).keyType(),
                        compression,
                        bufferSize,
                        isDwrf,
                        hiveStorageTimeZone);
                ColumnWriter valueWriter = createColumnWriterFromSparkType(
                        1,
                        list,
                        ((MapType)type).valueType(),
                        compression,
                        bufferSize,
                        isDwrf,
                        hiveStorageTimeZone);
                return new MapColumnWriter(columnIndex, compression, bufferSize, isDwrf, keyWriter, valueWriter);
            }
        if (dataType instanceof StructType) {
                throw new UnsupportedOperationException("Not Supported");
                /*
                ImmutableList.Builder<ColumnWriter> fieldWriters = ImmutableList.builder();
                for (int fieldId = 0; fieldId < orcType.getFieldCount(); fieldId++) {
                    int fieldColumnIndex = orcType.getFieldTypeIndex(fieldId);
                    DataType fieldType = ((StructType) type).fields()[fieldId].dataType();
                    fieldWriters.add(createColumnWriter(fieldColumnIndex, orcTypes, fieldType, compression, bufferSize, isDwrf, hiveStorageTimeZone));
                }
                return new StructColumnWriter(columnIndex, compression, bufferSize, fieldWriters.build());
                */
        }

        throw new IllegalArgumentException("Unsupported type: " + type);
    }

}

