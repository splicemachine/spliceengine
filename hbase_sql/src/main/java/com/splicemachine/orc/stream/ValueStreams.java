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
package com.splicemachine.orc.stream;

import com.splicemachine.orc.StreamId;
import com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.splicemachine.orc.metadata.OrcType.OrcTypeKind;
import static com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind.*;
import static com.splicemachine.orc.metadata.OrcType.OrcTypeKind.*;
import static com.splicemachine.orc.metadata.Stream.StreamKind.*;
import static java.lang.String.format;

public final class ValueStreams
{
    private ValueStreams()
    {
    }

    public static ValueStream<?> createValueStreams(
            StreamId streamId,
            OrcInputStream inputStream,
            OrcTypeKind type,
            ColumnEncodingKind encoding,
            boolean usesVInt)
    {
        if (streamId.getStreamKind() == PRESENT) {
            return new BooleanStream(inputStream);
        }

        // dictionary length and data streams are unsigned int streams
        if ((encoding == DICTIONARY || encoding == DICTIONARY_V2) && (streamId.getStreamKind() == LENGTH || streamId.getStreamKind() == DATA)) {
            return createLongStream(inputStream, encoding, INT, false, usesVInt);
        }

        if (streamId.getStreamKind() == DATA) {
            switch (type) {
                case BOOLEAN:
                    return new BooleanStream(inputStream);
                case BYTE:
                    return new ByteStream(inputStream);
                case SHORT:
                case INT:
                case LONG:
                case DATE:
                    return createLongStream(inputStream, encoding, type, true, usesVInt);
                case FLOAT:
                    return new FloatStream(inputStream);
                case DOUBLE:
                    return new DoubleStream(inputStream);
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    return new ByteArrayStream(inputStream);
                case TIMESTAMP:
                    return createLongStream(inputStream, encoding, type, true, usesVInt);
                case DECIMAL:
                    return new DecimalStream(inputStream);
            }
        }

        // length stream of a direct encoded string or binary column
        if (streamId.getStreamKind() == LENGTH) {
            switch (type) {
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                case MAP:
                case LIST:
                    return createLongStream(inputStream, encoding, type, false, usesVInt);
            }
        }

        // length stream of a the row group dictionary
        if (streamId.getStreamKind() == ROW_GROUP_DICTIONARY_LENGTH) {
            switch (type) {
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    return new RowGroupDictionaryLengthStream(inputStream, false);
            }
        }

        // row group dictionary
        if (streamId.getStreamKind() == ROW_GROUP_DICTIONARY) {
            switch (type) {
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    return new ByteArrayStream(inputStream);
            }
        }

        // row group dictionary
        if (streamId.getStreamKind() == IN_DICTIONARY) {
            return new BooleanStream(inputStream);
        }

        // length (nanos) of a timestamp column
        if (type == TIMESTAMP && streamId.getStreamKind() == SECONDARY) {
            return createLongStream(inputStream, encoding, type, false, usesVInt);
        }

        // scale of a decimal column
        if (type == DECIMAL && streamId.getStreamKind() == SECONDARY) {
            // specification (https://orc.apache.org/docs/encodings.html) says scale stream is unsigned,
            // however Hive writer stores scale as signed integer (org.apache.hadoop.hive.ql.io.orc.WriterImpl.DecimalTreeWriter)
            // BUG link: https://issues.apache.org/jira/browse/HIVE-13229
            return createLongStream(inputStream, encoding, type, true, usesVInt);
        }

        if (streamId.getStreamKind() == DICTIONARY_DATA) {
            switch (type) {
                case SHORT:
                case INT:
                case LONG:
                    return createLongStream(inputStream, DWRF_DIRECT, INT, true, usesVInt);
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    return new ByteArrayStream(inputStream);
            }
        }

        throw new IllegalArgumentException(format("Unsupported column type %s for stream %s with encoding %s", type, streamId, encoding));
    }

    private static ValueStream<?> createLongStream(
            OrcInputStream inputStream,
            ColumnEncodingKind encoding,
            OrcTypeKind type,
            boolean signed,
            boolean usesVInt)
    {
        if (encoding == DIRECT_V2 || encoding == DICTIONARY_V2) {
            return new LongStreamV2(inputStream, signed, false);
        }
        else if (encoding == DIRECT || encoding == DICTIONARY) {
            return new LongStreamV1(inputStream, signed);
        }
        else if (encoding == DWRF_DIRECT) {
            return new LongStreamDwrf(inputStream, type, signed, usesVInt);
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding for long stream: " + encoding);
        }
    }
}
