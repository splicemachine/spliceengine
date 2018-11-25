/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.splicemachine.orc.checkpoint;

import com.splicemachine.orc.StreamId;
import com.splicemachine.orc.metadata.ColumnEncoding;
import com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind;
import com.splicemachine.orc.metadata.OrcType;
import com.splicemachine.orc.metadata.OrcType.OrcTypeKind;
import com.splicemachine.orc.metadata.RowGroupIndex;
import com.splicemachine.orc.metadata.Stream;
import com.splicemachine.orc.metadata.Stream.StreamKind;
import com.google.common.collect.*;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.splicemachine.orc.checkpoint.InputStreamCheckpoint.createInputStreamCheckpoint;
import static com.splicemachine.orc.metadata.ColumnEncoding.ColumnEncodingKind.*;
import static com.splicemachine.orc.metadata.Stream.StreamKind.*;
import static com.google.common.base.Predicates.equalTo;
import static java.util.Objects.requireNonNull;
import static java.lang.String.format;

public final class Checkpoints
{
    private Checkpoints()
    {
    }

    public static Map<StreamId, StreamCheckpoint> getStreamCheckpoints(
            Set<Integer> columns,
            List<OrcType> columnTypes,
            boolean compressed,
            int rowGroupId,
            List<ColumnEncoding> columnEncodings,
            Map<StreamId, Stream> streams,
            Map<Integer, List<RowGroupIndex>> columnIndexes)
            throws InvalidCheckpointException
    {
        ImmutableSetMultimap.Builder<Integer, StreamKind> streamKindsBuilder = ImmutableSetMultimap.builder();
        for (Stream stream : streams.values()) {
            streamKindsBuilder.put(stream.getColumn(), stream.getStreamKind());
        }
        SetMultimap<Integer, StreamKind> streamKinds = streamKindsBuilder.build();

        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();
        for (int column : columns) {
            List<Integer> positionsList = columnIndexes.get(column).get(rowGroupId).getPositions();

            ColumnEncodingKind columnEncoding = columnEncodings.get(column).getColumnEncodingKind();
            OrcTypeKind columnType = columnTypes.get(column).getOrcTypeKind();
            Set<StreamKind> availableStreams = streamKinds.get(column);

            ColumnPositionsList columnPositionsList = new ColumnPositionsList(column, columnType, positionsList);
            switch (columnType) {
                case BOOLEAN:
                    checkpoints.putAll(getBooleanColumnCheckpoints(column, compressed, availableStreams, columnPositionsList));
                    break;
                case BYTE:
                    checkpoints.putAll(getByteColumnCheckpoints(column, compressed, availableStreams, columnPositionsList));
                    break;
                case SHORT:
                case INT:
                case LONG:
                case DATE:
                    checkpoints.putAll(getLongColumnCheckpoints(column, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                case FLOAT:
                    checkpoints.putAll(getFloatColumnCheckpoints(column, compressed, availableStreams, columnPositionsList));
                    break;
                case DOUBLE:
                    checkpoints.putAll(getDoubleColumnCheckpoints(column, compressed, availableStreams, columnPositionsList));
                    break;
                case TIMESTAMP:
                    checkpoints.putAll(getTimestampColumnCheckpoints(column, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                case BINARY:
                case STRING:
                case VARCHAR:
                case CHAR:
                    checkpoints.putAll(getSliceColumnCheckpoints(column, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                case LIST:
                case MAP:
                    checkpoints.putAll(getListOrMapColumnCheckpoints(column, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                case STRUCT:
                    checkpoints.putAll(getStructColumnCheckpoints(column, compressed, availableStreams, columnPositionsList));
                    break;
                case DECIMAL:
                    checkpoints.putAll(getDecimalColumnCheckpoints(column, columnEncoding, compressed, availableStreams, columnPositionsList));
                    break;
                case UNION:
                    throw new IllegalArgumentException("Unsupported column type " + columnType);
            }

            // The DWRF code is not meticulous in the handling of checkpoints.  It appears that for the first row group
            // it will write checkpoints for all streams, but in other cases it will write only the streams that exist.
            // We detect this case by checking that all offsets in the initial position list are zero, and if so, we
            // clear the extra offsets
            if (columnPositionsList.hasNextPosition() && !Iterables.all(positionsList, equalTo(0))) {
                throw new InvalidCheckpointException(format("Column %s, of type %s, contains %s offset positions, but only %s positions were consumed",
                        column,
                        columnType,
                        positionsList.size(),
                        columnPositionsList.getIndex()));
            }
        }
        return checkpoints.build();
    }

    public static StreamCheckpoint getDictionaryStreamCheckpoint(StreamId streamId, OrcTypeKind columnType, ColumnEncodingKind columnEncoding)
    {
        if (streamId.getStreamKind() == DICTIONARY_DATA) {
            switch (columnType) {
                case SHORT:
                case INT:
                case LONG:
                    return new LongStreamDwrfCheckpoint(createInputStreamCheckpoint(0, 0));
                case STRING:
                case VARCHAR:
                case CHAR:
                case BINARY:
                    return new ByteArrayStreamCheckpoint(createInputStreamCheckpoint(0, 0));
            }
        }

        // dictionary length and data streams are unsigned long streams
        if (streamId.getStreamKind() == LENGTH || streamId.getStreamKind() == DATA) {
            if (columnEncoding == DICTIONARY_V2) {
                return new LongStreamV2Checkpoint(0, createInputStreamCheckpoint(0, 0));
            }
            else if (columnEncoding == DICTIONARY) {
                return new LongStreamV1Checkpoint(0, createInputStreamCheckpoint(0, 0));
            }
        }
        throw new IllegalArgumentException("Unsupported column type " + columnType + " for dictionary stream " + streamId);
    }

    private static Map<StreamId, StreamCheckpoint> getBooleanColumnCheckpoints(
            int column,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getByteColumnCheckpoints(
            int column,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new ByteStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getLongColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(IN_DICTIONARY)) {
            checkpoints.put(new StreamId(column, IN_DICTIONARY), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getFloatColumnCheckpoints(
            int column,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new FloatStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getDoubleColumnCheckpoints(
            int column,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new DoubleStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getTimestampColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        if (availableStreams.contains(SECONDARY)) {
            checkpoints.put(new StreamId(column, SECONDARY), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getSliceColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (encoding == DIRECT || encoding == DIRECT_V2) {
            if (availableStreams.contains(DATA)) {
                checkpoints.put(new StreamId(column, DATA), new ByteArrayStreamCheckpoint(compressed, positionsList));
            }

            if (availableStreams.contains(LENGTH)) {
                checkpoints.put(new StreamId(column, LENGTH), createLongStreamCheckpoint(encoding, compressed, positionsList));
            }
        }
        else if (encoding == DICTIONARY || encoding == DICTIONARY_V2) {
            // DWRF has rules inconsistent with the ORC style
            if (availableStreams.contains(IN_DICTIONARY)) {
                if (availableStreams.contains(ROW_GROUP_DICTIONARY)) {
                    checkpoints.put(new StreamId(column, ROW_GROUP_DICTIONARY), new ByteArrayStreamCheckpoint(compressed, positionsList));
                }

                checkpoints.put(new StreamId(column, ROW_GROUP_DICTIONARY_LENGTH), new RowGroupDictionaryLengthStreamCheckpoint(compressed, positionsList));

                if (availableStreams.contains(DATA)) {
                    checkpoints.put(new StreamId(column, DATA), createLongStreamCheckpoint(encoding, compressed, positionsList));
                }

                checkpoints.put(new StreamId(column, IN_DICTIONARY), new BooleanStreamCheckpoint(compressed, positionsList));
            }
            else {
                if (availableStreams.contains(DATA)) {
                    checkpoints.put(new StreamId(column, DATA), createLongStreamCheckpoint(encoding, compressed, positionsList));
                }
            }
        }
        else {
            throw new IllegalArgumentException("Unsupported encoding for slice column: " + encoding);
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getListOrMapColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(LENGTH)) {
            checkpoints.put(new StreamId(column, LENGTH), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getStructColumnCheckpoints(
            int column,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        return checkpoints.build();
    }

    private static Map<StreamId, StreamCheckpoint> getDecimalColumnCheckpoints(
            int column,
            ColumnEncodingKind encoding,
            boolean compressed,
            Set<StreamKind> availableStreams,
            ColumnPositionsList positionsList)
    {
        ImmutableMap.Builder<StreamId, StreamCheckpoint> checkpoints = ImmutableMap.builder();

        if (availableStreams.contains(PRESENT)) {
            checkpoints.put(new StreamId(column, PRESENT), new BooleanStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(DATA)) {
            checkpoints.put(new StreamId(column, DATA), new DecimalStreamCheckpoint(compressed, positionsList));
        }

        if (availableStreams.contains(SECONDARY)) {
            checkpoints.put(new StreamId(column, SECONDARY), createLongStreamCheckpoint(encoding, compressed, positionsList));
        }

        return checkpoints.build();
    }

    private static StreamCheckpoint createLongStreamCheckpoint(ColumnEncodingKind encoding, boolean compressed, ColumnPositionsList positionsList)
    {
        if (encoding == DIRECT_V2 || encoding == DICTIONARY_V2) {
            return new LongStreamV2Checkpoint(compressed, positionsList);
        }

        if (encoding == DIRECT || encoding == DICTIONARY) {
            return new LongStreamV1Checkpoint(compressed, positionsList);
        }

        if (encoding == DWRF_DIRECT) {
            return new LongStreamDwrfCheckpoint(compressed, positionsList);
        }

        throw new IllegalArgumentException("Unsupported encoding for long stream: " + encoding);
    }

    public static class ColumnPositionsList
    {
        private final int column;
        private final OrcTypeKind columnType;
        private final List<Integer> positionsList;
        private int index;

        private ColumnPositionsList(int column, OrcTypeKind columnType, List<Integer> positionsList)
        {
            this.column = column;
            this.columnType = requireNonNull(columnType, "columnType is null");
            this.positionsList = ImmutableList.copyOf(requireNonNull(positionsList, "positionsList is null"));
        }

        public int getIndex()
        {
            return index;
        }

        public boolean hasNextPosition()
        {
            return index < positionsList.size();
        }

        public int nextPosition()
        {
            if (!hasNextPosition()) {
                throw new InvalidCheckpointException("Not enough positions for column %s, of type %s, checkpoints",
                        column,
                        columnType);
            }

            return positionsList.get(index++);
        }
    }
}
