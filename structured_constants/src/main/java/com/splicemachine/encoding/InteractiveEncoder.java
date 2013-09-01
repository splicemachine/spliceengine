package com.splicemachine.encoding;

import org.apache.hadoop.hbase.util.Bytes;

import java.io.Console;
import java.math.BigDecimal;

/**
 * @author Scott Fines
 * Created on: 9/1/13
 */
public class InteractiveEncoder {

    public static void main(String...args) throws Exception{
        Console console = System.console();
        while(true){
            console.format(">");
            String command = console.readLine();
            getCommand(command).execute(command,console);
        }
    }

    private static Command getCommand(String command) {
        if(command.startsWith("exit"))
            return exitCommand;
        if(command.startsWith("encode"))
            return encodeCommand;
        else if(command.startsWith("decode"))
            return decodeCommand;
        else
            return errorCommand;
    }

    private static interface Command{
        void execute(String context,Console response);
    }

    private static final Command errorCommand = new Command(){
        @Override
        public void execute(String context, Console response) {
            response.printf("I don't understand. Please explain more clearly: \"%s\"%n",context);
        }
    };

    private static final Command decodeCommand = new Command() {
        @Override
        public void execute(String context, Console response) {
            String[] command = context.split(" ");
            /*
             * First entry is "encode", throw it away
             * Second entry is the text to encode.
             * Third entry is the type of the text.
             */
            if(command.length<3){
                response.printf("Incorrect decode command(must be of the form \"encode\" <text to encode> <type of encoding>: \"%s\" %n",context);
                return;
            }

            String toEncode = command[1];
            String type = command[2];
            DataType dt;
            try{
                dt = DataType.fromCode(type);
            }catch(IllegalArgumentException iae){
                response.printf("Unable to determine data type for code \"%s\"%n",type);
                return;
            }
            response.printf("%s%n",dt.decode(Bytes.toBytesBinary(toEncode)));
        }
    };

    private static final Command exitCommand = new Command() {
        @Override
        public void execute(String context, Console response) {
            System.exit(0);
        }
    };

    private static final Command encodeCommand = new Command() {
        @Override
        public void execute(String context,Console response) {
            String[] command = context.split(" ");
            /*
             * First entry is "encode", throw it away
             * Second entry is the text to encode.
             * Third entry is the type of the text.
             */
            if(command.length<3){
                response.printf("Incorrect encode command(must be of the form \"encode\" <text to encode> <type of encoding>: \"%s\" %n",context);
                return;
            }

            String toEncode = command[1];
            String type = command[2];
            DataType dt;
            try{
                dt = DataType.fromCode(type);
            }catch(IllegalArgumentException iae){
                response.printf("Unable to determine data type for code \"%s\"%n",type);
                return;
            }

            byte[] data = dt.encode(toEncode);
            String hexDump = Bytes.toStringBinary(data);
            response.printf("%s%n", hexDump);
        }
    };

    private enum DataType{
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
                return Bytes.toStringBinary(Encoding.decodeBytes(bytes));
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
                return Bytes.toStringBinary(Encoding.decodeBytesUnsortd(bytes, 0, bytes.length));
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
    }
}
