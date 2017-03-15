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
package com.splicemachine.orc.reader;

import com.splicemachine.orc.OrcCorruptionException;
import com.splicemachine.orc.StreamDescriptor;
import com.splicemachine.orc.metadata.ColumnEncoding;
import com.splicemachine.orc.stream.*;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.spark.sql.execution.vectorized.ColumnVector;
import org.apache.spark.sql.types.DataType;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import static com.splicemachine.orc.metadata.Stream.StreamKind.*;
import static com.splicemachine.orc.stream.MissingStreamSource.missingStreamSource;
import static com.google.common.base.MoreObjects.toStringHelper;

public class SliceDictionaryStreamReader {
//        extends AbstractStreamReader {

    /*
    private final StreamDescriptor streamDescriptor;

    @Nonnull
    private StreamSource<BooleanStream> presentStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream presentStream;
    private boolean[] isNullVector = new boolean[0];

    @Nonnull
    private StreamSource<ByteArrayStream> stripeDictionaryDataStreamSource = missingStreamSource(ByteArrayStream.class);
    private boolean stripeDictionaryOpen;
    private int stripeDictionarySize;
    @Nonnull
    private Slice[] stripeDictionary = new Slice[1];

    private SliceArrayBlock dictionaryBlock = new SliceArrayBlock(stripeDictionary.length, stripeDictionary, true);

    @Nonnull
    private StreamSource<LongStream> stripeDictionaryLengthStreamSource = missingStreamSource(LongStream.class);

    @Nonnull
    private StreamSource<BooleanStream> inDictionaryStreamSource = missingStreamSource(BooleanStream.class);
    @Nullable
    private BooleanStream inDictionaryStream;
    private boolean[] inDictionary = new boolean[0];

    @Nonnull
    private StreamSource<ByteArrayStream> rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayStream.class);
    @Nonnull
    private Slice[] rowGroupDictionary = new Slice[0];

    @Nonnull
    private StreamSource<RowGroupDictionaryLengthStream> rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthStream.class);
    @Nonnull
    private int[] rowGroupDictionaryLength = new int[0];

    @Nonnull
    private StreamSource<LongStream> dataStreamSource = missingStreamSource(LongStream.class);
    @Nullable
    private LongStream dataStream;

    private boolean rowGroupOpen;

    public SliceDictionaryStreamReader(StreamDescriptor streamDescriptor)
    {
        this.streamDescriptor = requireNonNull(streamDescriptor, "stream is null");
    }

    @Override
    public void prepareNextRead(int batchSize)
    {
        readOffset += nextBatchSize;
        nextBatchSize = batchSize;
    }

    @Override
    public ColumnVector readBlock(DataType type, ColumnVector vector)
            throws IOException
    {
        if (!rowGroupOpen) {
            openRowGroup(type);
        }

        if (readOffset > 0) {
            if (presentStream != null) {
                // skip ahead the present bit reader, but count the set bits
                // and use this as the skip size for the length reader
                readOffset = presentStream.countBitsSet(readOffset);
            }
            if (readOffset > 0) {
                if (dataStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                if (inDictionaryStream != null) {
                    inDictionaryStream.skip(readOffset);
                }
                dataStream.skip(readOffset);
            }
        }

        if (isNullVector.length < nextBatchSize) {
            isNullVector = new boolean[nextBatchSize];
        }

        int[] dataVector = new int[nextBatchSize];
        if (presentStream == null) {
            if (dataStream == null) {
                throw new OrcCorruptionException("Value is not null but data stream is not present");
            }
            Arrays.fill(isNullVector, false);
            dataStream.nextIntVector(nextBatchSize, dataVector);
        }
        else {
            int nullValues = presentStream.getUnsetBits(nextBatchSize, isNullVector);
            if (nullValues != nextBatchSize) {
                if (dataStream == null) {
                    throw new OrcCorruptionException("Value is not null but data stream is not present");
                }
                dataStream.nextIntVector(nextBatchSize, dataVector, isNullVector);
            }
        }

        if (inDictionary.length < nextBatchSize) {
            inDictionary = new boolean[nextBatchSize];
        }
        if (inDictionaryStream == null) {
            Arrays.fill(inDictionary, true);
        }
        else {
            inDictionaryStream.getSetBits(nextBatchSize, inDictionary, isNullVector);
        }

        // create the dictionary ids
        for (int i = 0; i < nextBatchSize; i++) {
            if (isNullVector[i]) {
                // null is the last entry in the slice dictionary
                dataVector[i] = dictionaryBlock.getPositionCount() - 1;
            }
            else if (inDictionary[i]) {
                // stripe dictionary elements have the same dictionary id
            }
            else {
                // row group dictionary elements are after the main dictionary
                dataVector[i] += stripeDictionarySize;
            }
        }

        // copy ids into a private array for this block since data vector is reused
        Block block = new DictionaryBlock(nextBatchSize, dictionaryBlock, dataVector);

        readOffset = 0;
        nextBatchSize = 0;
        return block;
    }

    private void setDictionaryBlockData(Slice[] dictionary)
    {
        // only update the block if the array changed to prevent creation of new Block objects, since
        // the engine currently uses identity equality to test if dictionaries are the same
        if (dictionaryBlock.getValues() != dictionary) {
            dictionaryBlock = new SliceArrayBlock(dictionary.length, dictionary, true);
        }
    }

    private void openRowGroup(DataType type)
            throws IOException
    {
        // read the dictionary
        if (!stripeDictionaryOpen) {
            // We must always create a new dictionary array because the previous dictionary may still be referenced
            // add one extra entry for null
            stripeDictionary = new Slice[stripeDictionarySize + 1];
            if (stripeDictionarySize > 0) {
                int[] dictionaryLength = new int[stripeDictionarySize];

                // read the lengths
                LongStream lengthStream = stripeDictionaryLengthStreamSource.openStream();
                if (lengthStream == null) {
                    throw new OrcCorruptionException("Dictionary is not empty but dictionary length stream is not present");
                }
                lengthStream.nextIntVector(stripeDictionarySize, dictionaryLength);

                // read dictionary values
                ByteArrayStream dictionaryDataStream = stripeDictionaryDataStreamSource.openStream();
                readDictionary(dictionaryDataStream, stripeDictionarySize, dictionaryLength, 0, stripeDictionary, type);
            }
        }
        stripeDictionaryOpen = true;

        // read row group dictionary
        RowGroupDictionaryLengthStream dictionaryLengthStream = rowGroupDictionaryLengthStreamSource.openStream();
        if (dictionaryLengthStream != null) {
            int rowGroupDictionarySize = dictionaryLengthStream.getEntryCount();

            // We must always create a new dictionary array because the previous dictionary may still be referenced
            // The first elements of the dictionary are from the stripe dictionary, then the row group dictionary elements, and then a null
            rowGroupDictionary = Arrays.copyOf(stripeDictionary, stripeDictionarySize + rowGroupDictionarySize + 1);
            setDictionaryBlockData(rowGroupDictionary);

            // resize the dictionary lengths array if necessary
            if (rowGroupDictionaryLength.length < rowGroupDictionarySize) {
                rowGroupDictionaryLength = new int[rowGroupDictionarySize];
            }

            // read the lengths
            dictionaryLengthStream.nextIntVector(rowGroupDictionarySize, rowGroupDictionaryLength);

            // read dictionary values
            ByteArrayStream dictionaryDataStream = rowGroupDictionaryDataStreamSource.openStream();
            readDictionary(dictionaryDataStream, rowGroupDictionarySize, rowGroupDictionaryLength, stripeDictionarySize, rowGroupDictionary, type);
        }
        else {
            // there is no row group dictionary so use the stripe dictionary
            setDictionaryBlockData(stripeDictionary);
        }

        presentStream = presentStreamSource.openStream();
        inDictionaryStream = inDictionaryStreamSource.openStream();
        dataStream = dataStreamSource.openStream();

        rowGroupOpen = true;
    }

    private static void readDictionary(
            @Nullable ByteArrayStream dictionaryDataStream,
            int dictionarySize,
            int[] dictionaryLength,
            int dictionaryOutputOffset,
            Slice[] dictionary,
            DataType type)
            throws IOException
    {
        // build dictionary slices
        for (int i = 0; i < dictionarySize; i++) {
            int length = dictionaryLength[i];
            if (length == 0) {
                dictionary[dictionaryOutputOffset + i] = Slices.EMPTY_SLICE;
            }
            else {
                Slice value = Slices.wrappedBuffer(dictionaryDataStream.next(length));
                if (isVarcharType(type)) {
                    value = truncateToLength(value, type);
                }
                if (isCharType(type)) {
                    value = trimSpacesAndTruncateToLength(value, type);
                }
                dictionary[dictionaryOutputOffset + i] = value;
            }
        }
    }

    @Override
    public void startStripe(StreamSources dictionaryStreamSources, List<ColumnEncoding> encoding)
            throws IOException
    {
        stripeDictionaryDataStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, DICTIONARY_DATA, ByteArrayStream.class);
        stripeDictionaryLengthStreamSource = dictionaryStreamSources.getStreamSource(streamDescriptor, LENGTH, LongStream.class);
        stripeDictionarySize = encoding.get(streamDescriptor.getStreamId()).getDictionarySize();
        stripeDictionaryOpen = false;

        presentStreamSource = missingStreamSource(BooleanStream.class);
        dataStreamSource = missingStreamSource(LongStream.class);

        inDictionaryStreamSource = missingStreamSource(BooleanStream.class);
        rowGroupDictionaryLengthStreamSource = missingStreamSource(RowGroupDictionaryLengthStream.class);
        rowGroupDictionaryDataStreamSource = missingStreamSource(ByteArrayStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public void startRowGroup(StreamSources dataStreamSources)
            throws IOException
    {
        presentStreamSource = dataStreamSources.getStreamSource(streamDescriptor, PRESENT, BooleanStream.class);
        dataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, DATA, LongStream.class);

        // the "in dictionary" stream signals if the value is in the stripe or row group dictionary
        inDictionaryStreamSource = dataStreamSources.getStreamSource(streamDescriptor, IN_DICTIONARY, BooleanStream.class);
        rowGroupDictionaryLengthStreamSource = dataStreamSources.getStreamSource(streamDescriptor, ROW_GROUP_DICTIONARY_LENGTH, RowGroupDictionaryLengthStream.class);
        rowGroupDictionaryDataStreamSource = dataStreamSources.getStreamSource(streamDescriptor, ROW_GROUP_DICTIONARY, ByteArrayStream.class);

        readOffset = 0;
        nextBatchSize = 0;

        presentStream = null;
        inDictionaryStream = null;
        dataStream = null;

        rowGroupOpen = false;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .addValue(streamDescriptor)
                .toString();
    }
    */
}
