/*
 * Copyright (c) 2012 - 2021 Splice Machine, Inc.
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

package com.splicemachine.derby.hbase;

import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.splicemachine.access.client.SpliceKVComparator;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.encoding.Encoding;
import com.splicemachine.encoding.MultiFieldDecoder;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import static com.splicemachine.encoding.MultiFieldDecoder.create;
import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.INCLUDE;
import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.SEEK_NEXT_USING_HINT;

    /**
     * A custom HBase {@link Filter} which finds all encoded first column
     * values in the present rowkeys of a splice internal table, and applies a
     * secondary {@link Filter} on the rowkeys, ignoring rowkey bytes belonging
     * to the first column.
     *
     * In order to correctly skip over the first column's bytes in a rowkey,
     * the class must be told the type of the first column, whether it is
     * scalar, float, double, or other, as indicated in the
     * {@link Encoding.SpliceEncodingKind firstKeyColumnEncodingKind} field
     * of the constructor.  The only {@code secondaryFilter} currently
     * supported is any instanceof {@link MultiRowRangeFilter}.
     * The {@link KeyPrefixProbingFilter#currentReturnCode} value is only
     * set to {@link ReturnCode.INCLUDE} when the first column bytes of
     * the current rowkey match the first column value currently being
     * probed.  In this case we use the {@code secondaryFilter} to determine
     * the final return code which {@code filterCell()} should indicate
     * back to HBase.  The only two return codes the KeyPrefixProbingFilter
     * uses are:
     *     {@link ReturnCode.INCLUDE} : indicates secondaryFilter should
     *                                  determine the return code and cell hint
     *     {@link ReturnCode.SEEK_NEXT_USING_HINT} : indicates the secondaryFilter
     *                                               will not be used any further for
     *                                               the current first column value.
     *                                               The current first column bytes,
     *                                               suffixed with a 0x01 byte in place
     *                                               of the 0x00 separator byte will be
     *                                               used to seek the next rowkey.
     *
     * @notes The usefulness of this filter comes from its use of the
     *        SEEK_NEXT_USING_HINT return code, which signals to HBase
     *        to seek forward in the filesystem (or memstore) for the
     *        next Cell with rowkey greater than or equal to the rowkey
     *        returned via {@link Filter#getNextCellHint}.
     */

public class KeyPrefixProbingFilter extends FilterBase implements Writable{
    @SuppressWarnings("unused")

    private static final long serialVersionUID=1l;
    private byte[] currentPrefix = new byte[0];
    private int currentPrefixLength = 0;
    private byte[] nextRowkeyHint = new byte[0];
    private int nextRowkeyHintLength = 0;
    private Encoding.SpliceEncodingKind firstKeyColumnEncodingKind;
    private ReturnCode currentReturnCode = ReturnCode.INCLUDE;
    private MultiFieldDecoder decoder = create();
    protected Filter secondaryFilter;

    public KeyPrefixProbingFilter(){
        super();
    }

    public KeyPrefixProbingFilter(Encoding.SpliceEncodingKind firstKeyColumnEncodingKind, MultiRowRangeFilter secondaryFilter){
        this();
        this.firstKeyColumnEncodingKind = firstKeyColumnEncodingKind;

        if (secondaryFilter instanceof SpliceMultiRowRangeFilter)
            this.secondaryFilter   = secondaryFilter;
        else
            this.secondaryFilter = new SpliceMultiRowRangeFilter(new ArrayList<>(secondaryFilter.getRowRanges()));
    }

    public Encoding.SpliceEncodingKind getFirstKeyColumnEncodingKind() {
        return firstKeyColumnEncodingKind;
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeLong(serialVersionUID);
        out.writeInt(firstKeyColumnEncodingKind.ordinal());
        byte [] filterAsBytes = secondaryFilter.toByteArray();
        out.writeInt(filterAsBytes.length);
        out.write(filterAsBytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        in.readLong();
        firstKeyColumnEncodingKind = Encoding.SpliceEncodingKind.values()[in.readInt()];
        byte [] filterAsBytes = new byte[in.readInt()];
        in.readFully(filterAsBytes);
        try {
            secondaryFilter = SpliceMultiRowRangeFilter.parseFrom(filterAsBytes);
        }
        catch (DeserializationException de) {
            throw new IOException(de);
        }
    }

    @Override
    public ReturnCode filterCell(final Cell ignored) throws IOException {
        if (currentReturnCode == INCLUDE)
            return secondaryFilter.filterCell(ignored);
        return currentReturnCode;
    }

    @Override
    public boolean filterRowKey(Cell firstRowCell) throws IOException {
        byte[] rowArr = firstRowCell.getRowArray();
        int length = firstRowCell.getRowLength();
        int offset = firstRowCell.getRowOffset();

        decoder.set(rowArr, offset, length);
        int prefixLength = keyColumnByteLength();

        if (prefixLength <= 0 || SpliceKVComparator.isSpecialTimestamp(firstRowCell)) {
            currentReturnCode = INCLUDE;
            return false;
        }

        int compareResult;
        boolean savePrefix = false;
        if (currentPrefix.length == 0) {
            compareResult = 0;
            savePrefix = true;
        }
        else {
            compareResult = Bytes.compareTo(rowArr, offset, prefixLength, currentPrefix, 0, currentPrefixLength);
            if (compareResult > 0)
                savePrefix = true;
        }
        if (savePrefix) {
            currentReturnCode = INCLUDE;
            if (currentPrefix.length < prefixLength)
                currentPrefix = new byte[prefixLength*2];
            if (nextRowkeyHint.length < prefixLength)
                nextRowkeyHint = new byte[prefixLength*2];

            currentPrefixLength = prefixLength;
            System.arraycopy(rowArr, offset, currentPrefix, 0, prefixLength);
            System.arraycopy(rowArr, offset, nextRowkeyHint, 0, prefixLength);
            // Add the delimiter
            nextRowkeyHint[prefixLength-1] = 0x01;
            nextRowkeyHintLength = prefixLength;
        }
        if (compareResult < 0)
            currentReturnCode = SEEK_NEXT_USING_HINT;
        else {
            if (currentReturnCode == SEEK_NEXT_USING_HINT)
                return false;

            Cell partialCell =
                PrivateCellUtil.createFirstOnRow(rowArr, offset + prefixLength,
                                                 (short) (length - prefixLength));
            boolean filterAllRemaining = secondaryFilter.filterRowKey(partialCell);

            if (filterAllRemaining) {
                currentReturnCode = SEEK_NEXT_USING_HINT;
                secondaryFilter.reset();
            }
        }

        return false;
    }

    // decoder.set must be called before calling this function
    private int keyColumnByteLength() {
        int length;
        if (firstKeyColumnEncodingKind == Encoding.SpliceEncodingKind.SCALAR)
            length = decoder.skipLong();
        else if (firstKeyColumnEncodingKind == Encoding.SpliceEncodingKind.FLOAT)
            length = decoder.skipFloat();
        else if (firstKeyColumnEncodingKind == Encoding.SpliceEncodingKind.DOUBLE)
            length = decoder.skipDouble();
        else
            length = decoder.skip();

        return length;
    }

    @Override
    public Cell getNextCellHint(Cell currentKV) throws IOException {
        if (currentReturnCode == INCLUDE) {

            Cell secondaryHint = secondaryFilter.getNextCellHint(currentKV);
            byte[] rowArr = secondaryHint.getRowArray();
            int length = secondaryHint.getRowLength();
            int offset = secondaryHint.getRowOffset();

            int newRowKeyLength = currentPrefixLength + length;
            byte[] rowHint = new byte[newRowKeyLength];
            System.arraycopy(currentPrefix, 0, rowHint, 0, currentPrefixLength);
            System.arraycopy(rowArr, offset, rowHint, currentPrefixLength, length);
            return PrivateCellUtil.createFirstOnRow(rowHint, 0, (short) rowHint.length);
        }
        else
            return PrivateCellUtil.createFirstOnRow(nextRowkeyHint, 0, (short) nextRowkeyHintLength);
    }

    /**
     * @return The filter serialized using pb
     */
    @Override
    public byte[] toByteArray() throws IOException {
        SpliceMessage.KeyPrefixProbingFilterMessage.Builder builder = SpliceMessage.KeyPrefixProbingFilterMessage.newBuilder();
		builder.setFirstKeyColumnEncodingKind(firstKeyColumnEncodingKind.ordinal());
		ByteString filterAsBytes = ByteString.copyFrom(secondaryFilter.toByteArray());
        builder.setSecondaryFilter(filterAsBytes);
        return builder.build().toByteArray();
    }

    /**
     * @param data A pb serialized {@code KeyPrefixProbingFilter} instance
     * @return An instance of {@code KeyPrefixProbingFilter} made from <code>bytes</code>
     * @throws org.apache.hadoop.hbase.exceptions.DeserializationException
     * @see #toByteArray
     */
    @SuppressWarnings("unused") //Deserialization method-- REQUIRED
    public static KeyPrefixProbingFilter parseFrom(final byte [] data) throws DeserializationException{
        SpliceMessage.KeyPrefixProbingFilterMessage proto;
        try{
            proto=SpliceMessage.KeyPrefixProbingFilterMessage.parseFrom(data);
        }catch(InvalidProtocolBufferException e){
            throw new DeserializationException(e);
        }

        try {
            Encoding.SpliceEncodingKind firstKeyColumnEncodingKind =
                     Encoding.SpliceEncodingKind.values()[proto.getFirstKeyColumnEncodingKind()];
            MultiRowRangeFilter secondaryFilter =
                 MultiRowRangeFilter.parseFrom(proto.getSecondaryFilter().toByteArray());

            return new KeyPrefixProbingFilter(firstKeyColumnEncodingKind, secondaryFilter);
        }
        catch (Exception e) {
            throw new DeserializationException(e);
        }
    }

    boolean areSerializedFieldsEqual(Filter o) {
        if (o == this) return true;
        if (!(o instanceof KeyPrefixProbingFilter)) return false;

        KeyPrefixProbingFilter other = (KeyPrefixProbingFilter)o;
        return this.getFirstKeyColumnEncodingKind() ==
               other.getFirstKeyColumnEncodingKind()  &&
               this.secondaryFilter.equals(other.secondaryFilter);
    }

    @Override
    public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
    }

    @Override
    public int hashCode() {
        Object[] objectsToHash = { firstKeyColumnEncodingKind.ordinal(), secondaryFilter };
        return Arrays.deepHashCode(objectsToHash);
    }
}
