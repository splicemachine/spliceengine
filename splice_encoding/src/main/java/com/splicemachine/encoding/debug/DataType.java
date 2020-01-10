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

package com.splicemachine.encoding.debug;

import com.splicemachine.encoding.Encoding;
import com.splicemachine.primitives.Bytes;
import java.math.BigDecimal;

/**
 * @author Scott Fines
 *         Created on: 9/3/13
 */
public enum DataType{
    BOOLEAN("b"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(Boolean.parseBoolean(text));
        }

        @Override
        public String decode(byte[] bytes, boolean printRawHex) {
            return Boolean.toString(Encoding.decodeBoolean(bytes));
        }
    },

    SCALAR("l"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(Long.parseLong(text));
        }

        @Override
        public String decode(byte[] bytes, boolean printRawHex) {
            return Long.toString(Encoding.decodeLong(bytes));
        }
    },

    FLOAT("f"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(Float.parseFloat(text));
        }

        @Override
        public String decode(byte[] bytes, boolean printRawHex) {
            return Float.toString(Encoding.decodeFloat(bytes));
        }
    },

    DOUBLE("d"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(Double.parseDouble(text));
        }

        @Override
        public String decode(byte[] bytes, boolean printRawHex) {
            return Double.toString(Encoding.decodeDouble(bytes));
        }
    },

    BIG_DECIMAL("B"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(new BigDecimal(text));
        }

        @Override
        public String decode(byte[] bytes, boolean printRawHex) {
            return Encoding.decodeBigDecimal(bytes).toString();
        }
    },

    STRING("s"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(text);
        }

        @Override
        public String decode(byte[] bytes, boolean printRawHex) {
            return Encoding.decodeString(bytes);
        }
    },

    SORTED_BYTES("a"){
        @Override
        public byte[] encode(String text) {
            //two possible forms--HBase hex, or raw hex. Both can be treated using toBytesBinary
            return Encoding.encode(Bytes.toBytesBinary(text));
        }

        @Override
        public String decode(byte[] bytes, boolean printRawHex) {
            byte[] decoded = Encoding.decodeBytes(bytes);
            if(printRawHex)
                return Bytes.toHex(decoded);
            return Bytes.toStringBinary(decoded,0,decoded.length);
        }
    },
    UNSORTED_BYTES("u"){
        @Override
        public byte[] encode(String text) {
            //two possible forms--HBase hex, or raw hex. Both can be treated using toBytesBinary
            return Encoding.encodeBytesUnsorted(Bytes.toBytesBinary(text));
        }

        @Override
        public String decode(byte[] bytes, boolean printRawHex) {
            byte[] decoded = Encoding.decodeBytesUnsortd(bytes,0,bytes.length);
            if(printRawHex)
                return Bytes.toHex(decoded);

            return Bytes.toStringBinary(decoded, 0, decoded.length);
        }
    } ;

    private final String typeCode;

    private DataType(String typeCode) {
        this.typeCode = typeCode;
    }

    public static DataType fromCode(String typeCode){
        for(DataType dt: values()){
            if(dt.typeCode.equals(typeCode))
                return dt;
        }

        throw new IllegalArgumentException("Unable to parse typeCode "+ typeCode);
    }

    public byte[] encode(String text){
        throw new UnsupportedOperationException();
    }

    public String decode(byte[] bytes){
        return decode(bytes,false);
    }

    public String decode(byte[] bytes, boolean printRawHex){
        throw new UnsupportedOperationException();
    }

    private static final String helpfulFormat = "%5s\t%-20s%n";

    public static void printHelpfulMessage() {
        System.out.println("Data Types:");
        for(DataType type:values()){
            String typeName = type.name();
            typeName = typeName.replaceAll("_"," ").toLowerCase();
            String typeCode = type.typeCode;
            System.out.printf(helpfulFormat,typeCode,typeName);
        }
    }
}
