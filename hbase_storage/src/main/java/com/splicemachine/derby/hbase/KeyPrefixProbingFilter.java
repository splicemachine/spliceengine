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
import com.google.protobuf.ZeroCopyLiteralByteString;
import com.splicemachine.access.client.SpliceKVComparator;
import com.splicemachine.coprocessor.SpliceMessage;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.TypeProvider;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.exceptions.DeserializationException;
import org.apache.hadoop.hbase.filter.Filter;
import org.apache.hadoop.hbase.filter.FilterBase;
import org.apache.hadoop.hbase.filter.MultiRowRangeFilter;
import org.apache.hadoop.hbase.shaded.protobuf.generated.FilterProtos;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Writable;

import java.io.*;
import java.util.ArrayList;
import java.util.Arrays;

import static com.splicemachine.encoding.MultiFieldDecoder.create;
import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.INCLUDE;
import static org.apache.hadoop.hbase.filter.Filter.ReturnCode.SEEK_NEXT_USING_HINT;

public class KeyPrefixProbingFilter extends FilterBase implements Writable{
    @SuppressWarnings("unused")
    private static final long serialVersionUID=1l;
    private byte[] currentPrefix = new byte[0];
    private int currentPrefixLength = 0;
    private byte[] nextRowkeyHint = new byte[0];
    private int nextRowkeyHintLength = 0;

    private DataValueDescriptor firstKeyColumnDVD;
    private int typeFormatId;
    private byte[] firstKeyColumnDVDAsByteArray;
    private String tableVersion;
    private TypeProvider typeProvider;
    private DescriptorSerializer[] serializers;
    private DescriptorSerializer   serializer;
    private ReturnCode currentReturnCode = ReturnCode.INCLUDE;
    private MultiFieldDecoder decoder = create();
    protected Filter secondaryFilter;

    public KeyPrefixProbingFilter(){
        super();
    }

    public KeyPrefixProbingFilter(DataValueDescriptor firstKeyColumnDVD, String tableVersion, Filter secondaryFilter){
        this();
        if (firstKeyColumnDVD == null)
            throw new RuntimeException();
        if (tableVersion == null)
            throw new RuntimeException();
        if (!(secondaryFilter instanceof MultiRowRangeFilter))
            throw new RuntimeException();
        MultiRowRangeFilter multiRowRangeFilter = (MultiRowRangeFilter)secondaryFilter;

        this.firstKeyColumnDVD = firstKeyColumnDVD.cloneValue(true);
        this.typeFormatId      = this.firstKeyColumnDVD.getTypeFormatId();
        this.tableVersion      = tableVersion;
        if (secondaryFilter instanceof SpliceMultiRowRangeFilter)
            this.secondaryFilter   = secondaryFilter;
        else
            this.secondaryFilter = new SpliceMultiRowRangeFilter(new ArrayList<>(multiRowRangeFilter.getRowRanges()));

        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        try {
            ObjectOutputStream oos = new ObjectOutputStream(bos);
            oos.writeObject(firstKeyColumnDVD);
            oos.flush();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
        firstKeyColumnDVDAsByteArray = bos.toByteArray();
        populateSerializers();
    }

    public byte [] getFirstKeyColumnDVDAsByteArray() {
        return firstKeyColumnDVDAsByteArray;
    }

    public String getTableVersion() {
        return tableVersion;
    }

    @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "intentional")
    private void populateSerializers() {
        typeProvider = VersionedSerializers.typesForVersion(tableVersion);
        DataValueDescriptor [] dvdArray = new DataValueDescriptor[1];
        dvdArray[0] = firstKeyColumnDVD;
        serializers = VersionedSerializers.forVersion(tableVersion,true).getSerializers(dvdArray);
        serializer = serializers[0];
    }

    @Override
    public void write(DataOutput out) throws IOException{
        out.writeInt(firstKeyColumnDVDAsByteArray.length);
        out.write(firstKeyColumnDVDAsByteArray);
        out.writeUTF(tableVersion);
        byte [] filterAsBytes = secondaryFilter.toByteArray();
        out.writeInt(filterAsBytes.length);
        out.write(filterAsBytes);
    }

    @Override
    public void readFields(DataInput in) throws IOException{
        firstKeyColumnDVDAsByteArray=new byte[in.readInt()];
        in.readFully(firstKeyColumnDVDAsByteArray);
        try {
            ByteArrayInputStream bis = new ByteArrayInputStream(firstKeyColumnDVDAsByteArray);
            ObjectInput ois = new ObjectInputStream(bis);
            firstKeyColumnDVD = (DataValueDescriptor) ois.readObject();
            typeFormatId = firstKeyColumnDVD.getTypeFormatId();
        }
        catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
        tableVersion = in.readUTF();
        byte [] filterAsBytes = new byte[in.readInt()];
        in.readFully(filterAsBytes);
        try {
            secondaryFilter = SpliceMultiRowRangeFilter.parseFrom(filterAsBytes);
        }
        catch (DeserializationException de) {
            throw new IOException(de);
        }
        populateSerializers();
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
        if(typeProvider.isScalar(typeFormatId))
            length = decoder.skipLong();
        else if(typeProvider.isFloat(typeFormatId))
            length = decoder.skipFloat();
        else if(typeProvider.isDouble(typeFormatId))
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
		builder.setFirstKeyColumnDVDAsByteArray(ZeroCopyLiteralByteString.wrap(firstKeyColumnDVDAsByteArray));
		builder.setTableVersion(tableVersion);
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
            ByteArrayInputStream bis = new ByteArrayInputStream(proto.getFirstKeyColumnDVDAsByteArray().toByteArray());
            ObjectInput in = new ObjectInputStream(bis);
            DataValueDescriptor dvd = (DataValueDescriptor) in.readObject();

            MultiRowRangeFilter secondaryFilter =
                 MultiRowRangeFilter.parseFrom(proto.getSecondaryFilter().toByteArray());

            return new KeyPrefixProbingFilter(dvd, proto.getTableVersion(), secondaryFilter);
        }
        catch (Exception e) {
            throw new DeserializationException(e);
        }
    }

    boolean areSerializedFieldsEqual(Filter o) {
        if (o == this) return true;
        if (!(o instanceof KeyPrefixProbingFilter)) return false;

        KeyPrefixProbingFilter other = (KeyPrefixProbingFilter)o;
        return this.getTableVersion().equals(other.getTableVersion()) &&
               Bytes.equals(this.getFirstKeyColumnDVDAsByteArray(),
                            other.getFirstKeyColumnDVDAsByteArray())  &&
               this.secondaryFilter.equals(other.secondaryFilter);
    }

    @Override
    public boolean equals(Object obj) {
    return obj instanceof Filter && areSerializedFieldsEqual((Filter) obj);
    }

    @Override
    public int hashCode() {
        Object[] objectsToHash = { firstKeyColumnDVDAsByteArray, tableVersion, secondaryFilter };
        return Arrays.deepHashCode(objectsToHash);
    }
}
