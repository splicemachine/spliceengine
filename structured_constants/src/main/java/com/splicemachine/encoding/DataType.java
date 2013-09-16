package com.splicemachine.encoding;

import org.apache.hadoop.hbase.util.Bytes;

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
        public String decode(byte[] bytes) {
            return Boolean.toString(Encoding.decodeBoolean(bytes));
        }
    },

    SCALAR("l"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(Long.parseLong(text));
        }

        @Override
        public String decode(byte[] bytes) {
            return Long.toString(Encoding.decodeLong(bytes));
        }
    },

    FLOAT("f"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(Float.parseFloat(text));
        }

        @Override
        public String decode(byte[] bytes) {
            return Float.toString(Encoding.decodeFloat(bytes));
        }
    },

    DOUBLE("d"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(Double.parseDouble(text));
        }

        @Override
        public String decode(byte[] bytes) {
            return Double.toString(Encoding.decodeDouble(bytes));
        }
    },

    BIG_DECIMAL("B"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(new BigDecimal(text));
        }

        @Override
        public String decode(byte[] bytes) {
            return Encoding.decodeBigDecimal(bytes).toString();
        }
    },

    STRING("s"){
        @Override
        public byte[] encode(String text) {
            return Encoding.encode(text);
        }

        @Override
        public String decode(byte[] bytes) {
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
        public String decode(byte[] bytes) {
            return Bytes.toStringBinary(Encoding.decodeBytes(bytes),0,bytes.length);
        }
    },
    UNSORTED_BYTES("u"){
        @Override
        public byte[] encode(String text) {
            //two possible forms--HBase hex, or raw hex. Both can be treated using toBytesBinary
            return Encoding.encodeBytesUnsorted(Bytes.toBytesBinary(text));
        }

        @Override
        public String decode(byte[] bytes) {
            byte[] decoded = Encoding.decodeBytesUnsortd(bytes,0,bytes.length);
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
