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
package com.splicemachine.orc.reader;

import com.splicemachine.orc.StreamDescriptor;
import org.joda.time.DateTimeZone;

public final class StreamReaders
{
    private StreamReaders()
    {
    }

    public static StreamReader createStreamReader(StreamDescriptor streamDescriptor, DateTimeZone hiveStorageTimeZone)
    {
        switch (streamDescriptor.getStreamType()) {
            case BOOLEAN:
                return new BooleanStreamReader(streamDescriptor);
            case BYTE:
                return new ByteStreamReader(streamDescriptor);
            case INT:
                return new IntStreamReader(streamDescriptor);
            case SHORT:
                return new ShortStreamReader(streamDescriptor);
            case DATE:
            case LONG:
                return new LongStreamReader(streamDescriptor);
            case FLOAT:
                return new FloatStreamReader(streamDescriptor);
            case DOUBLE:
                return new DoubleStreamReader(streamDescriptor);
            case BINARY:
            case STRING:
            case VARCHAR:
            case CHAR:
                return new SliceStreamReader(streamDescriptor);
            case TIMESTAMP:
                return new TimestampStreamReader(streamDescriptor, hiveStorageTimeZone);
            case LIST:
                return new ListStreamReader(streamDescriptor, hiveStorageTimeZone);
            case STRUCT:
                return new StructStreamReader(streamDescriptor, hiveStorageTimeZone);
            case MAP:
                return new MapStreamReader(streamDescriptor, hiveStorageTimeZone);
            case DECIMAL:
                return new DecimalStreamReader(streamDescriptor);
            case UNION:
            default:
                throw new IllegalArgumentException("Unsupported type: " + streamDescriptor.getStreamType());
        }
    }
}
