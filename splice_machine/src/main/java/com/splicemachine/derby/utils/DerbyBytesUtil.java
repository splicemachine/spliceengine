/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.utils;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.services.io.StoredFormatIds;
import com.splicemachine.db.iapi.store.access.ScanController;
import com.splicemachine.db.iapi.types.DataValueDescriptor;
import com.splicemachine.derby.utils.marshall.dvd.DescriptorSerializer;
import com.splicemachine.derby.utils.marshall.dvd.VersionedSerializers;
import com.splicemachine.encoding.MultiFieldDecoder;
import com.splicemachine.encoding.MultiFieldEncoder;
import com.splicemachine.pipeline.Exceptions;
import com.splicemachine.utils.ByteDataInput;
import com.splicemachine.utils.ByteDataOutput;
import com.splicemachine.utils.SpliceLogUtils;
import org.apache.log4j.Logger;

import java.io.IOException;


public class DerbyBytesUtil {
    private static Logger LOG = Logger.getLogger(DerbyBytesUtil.class);

    @SuppressWarnings("unchecked")
    public static <T> T fromBytes(byte[] bytes) throws StandardException {
        try {
            return fromBytesUnsafe(bytes);
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "fromBytes Exception", Exceptions.parseException(e));
            return null; //can't happen
        }
    }

    @SuppressWarnings("unchecked")
    public static <T> T fromBytesUnsafe(byte[] bytes) throws IOException, ClassNotFoundException {
        try(ByteDataInput bdi = new ByteDataInput(bytes)){
            return (T) bdi.readObject();
        }
    }

    public static byte[] toBytes(Object object) throws StandardException {
        try(ByteDataOutput bdo = new ByteDataOutput()){
            bdo.writeObject(object);
            return bdo.toByteArray();
        } catch (Exception e) {
            SpliceLogUtils.logAndThrow(LOG, "fromBytes Exception", Exceptions.parseException(e));
            return null;
        }
    }


    public static byte[] generateIndexKey(DataValueDescriptor[] descriptors, boolean[] sortOrder, String tableVersion, boolean rowIdKey) throws IOException, StandardException {
        MultiFieldEncoder encoder = MultiFieldEncoder.create(descriptors.length);
        DescriptorSerializer[] serializers = VersionedSerializers.forVersion(tableVersion, false).getSerializers(descriptors);
        DescriptorSerializer rowLocSerializer = VersionedSerializers.forVersion(tableVersion, false).getSerializer(StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID);
        try {
            if (rowIdKey) {
                return descriptors[0].getBytes();
            }
            /*
             * The last entry is a RowLocation (for indices). They must be sortable, but the default encoding
             * for RowLocations is unsorted. Thus, we have to be careful to encode any RowLocation values differently
             */
            for (int i = 0; i < descriptors.length; i++) {
                DataValueDescriptor dvd = descriptors[i];
                boolean desc = sortOrder != null && !sortOrder[i];
                if (dvd.getTypeFormatId() == StoredFormatIds.ACCESS_HEAP_ROW_LOCATION_V1_ID) {
                    rowLocSerializer.encode(encoder, dvd, desc);
                    // encoder = encoder.encodeNext(dvd.getBytes(),desc);
                } else
                    serializers[i].encode(encoder, dvd, desc);
                    // encodeInto(encoder,dvd,desc);
            }
            return encoder.build();
        } finally {
            for (DescriptorSerializer serializer : serializers) {
                serializer.close();
            }
        }
    }

    public static byte[] generateScanKeyForIndex(DataValueDescriptor[] startKeyValue,
                                                 int startSearchOperator,
                                                 boolean[] sortOrder,
                                                 String tableVersion,
                                                 boolean rowIdKey) throws IOException, StandardException {
        if (startKeyValue == null) return null;
        switch (startSearchOperator) { // public static final int GT = -1;
            case ScanController.NA:
            case ScanController.GE:
                return generateIndexKey(startKeyValue, sortOrder, tableVersion, rowIdKey);
            case ScanController.GT:
                byte[] indexKey = generateIndexKey(startKeyValue, sortOrder, tableVersion, rowIdKey);
                /*
                 * For a GT operation we want the next row in sorted order, and that's the row plus a
                 * trailing 0x0 byte
                 * The problem is sometimes we have composed keys such as:
                 * 0xFF 0xFF 0xFF 0x00 0xEE 0xEE
                 * 0xFF 0xFF 0xFF 0x00 0xEE 0xFF
                 *
                 * When we search for 0xFF 0xFF 0xFF we want both rows returned.
                 *
                 * In this case, the first row greater than anything of the form
                 * 0xFF 0xFF 0xFF 0x00 0x?? 0x??
                 *
                 * Is 0xFF 0xFF 0xFF 0x01
                 *
                 * Here we append a 0x01 byte to the end of the key
                 */
                byte[] b = new byte[indexKey.length+1];
                System.arraycopy(indexKey,0,b,0,indexKey.length);
                b[b.length-1] = 0x01;
                return b;
            default:
                throw new RuntimeException("Error with Key Generation");
        }
    }

    public static void skip(MultiFieldDecoder rowDecoder, DataValueDescriptor dvd) {
        dvd.setToNull();
        skipField(rowDecoder, dvd);
    }

    public static void skipField(MultiFieldDecoder rowDecoder, DataValueDescriptor dvd) {
        if (isDoubleType(dvd))
            rowDecoder.skipDouble();
        else if (isFloatType(dvd))
            rowDecoder.skipFloat();
        else if (isScalarType(dvd, null))
            rowDecoder.skipLong();
        else
            rowDecoder.skip();
    }


    public static boolean isScalarType(DataValueDescriptor dvd, String tableVersion) {
        return dvd != null && VersionedSerializers.typesForVersion(tableVersion).isScalar(dvd.getTypeFormatId());
    }

    public static boolean isFloatType(DataValueDescriptor dvd) {
        return dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_REAL_ID;
    }

    public static boolean isDoubleType(DataValueDescriptor dvd) {
        return dvd != null && dvd.getTypeFormatId() == StoredFormatIds.SQL_DOUBLE_ID;
    }

    public static byte[] slice(MultiFieldDecoder fieldDecoder, int[] keyColumns, DataValueDescriptor[] rowArray) {
        int offset = fieldDecoder.offset();
        int size = skip(fieldDecoder, keyColumns, rowArray);
        //return to the original position
        fieldDecoder.seek(offset);
        return fieldDecoder.slice(size);
    }

    public static int skip(MultiFieldDecoder fieldDecoder, int[] keyColumns, DataValueDescriptor[] rowArray) {
        int size = 0;
        for (int keyColumn : keyColumns) {
            DataValueDescriptor dvd = rowArray[keyColumn];
            if (DerbyBytesUtil.isFloatType(dvd))
                size += fieldDecoder.skipFloat();
            else if (DerbyBytesUtil.isDoubleType(dvd))
                size += fieldDecoder.skipDouble();
            else
                size += fieldDecoder.skip();
        }
        return size;
    }


}
