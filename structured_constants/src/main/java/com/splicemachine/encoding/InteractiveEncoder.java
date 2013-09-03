package com.splicemachine.encoding;

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
            response.printf("%s%n",dt.decode(toBytesBinary(toEncode)));
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
            String hexDump = toStringBinary(data,0,data.length);
            response.printf("%s%n", hexDump);
        }
    };



    public static byte [] toBytesBinary(String in) {
        // this may be bigger than we need, but let's be safe.
        byte [] b = new byte[in.length()];
        int size = 0;
        for (int i = 0; i < in.length(); ++i) {
            char ch = in.charAt(i);
            if (ch == '\\' && in.length() > i+1 && in.charAt(i+1) == 'x') {
                // ok, take next 2 hex digits.
                char hd1 = in.charAt(i+2);
                char hd2 = in.charAt(i+3);

                // they need to be A-F0-9:
                if (!isHexDigit(hd1) ||
                        !isHexDigit(hd2)) {
                    // bogus escape code, ignore:
                    continue;
                }
                // turn hex ASCII digit -> number
                byte d = (byte) ((toBinaryFromHex((byte)hd1) << 4) + toBinaryFromHex((byte)hd2));

                b[size++] = d;
                i += 3; // skip 3
            } else {
                b[size++] = (byte) ch;
            }
        }
        // resize:
        byte [] b2 = new byte[size];
        System.arraycopy(b, 0, b2, 0, size);
        return b2;
    }
    public static String toStringBinary(final byte [] b, int off, int len) {
        StringBuilder result = new StringBuilder();
        for (int i = off; i < off + len ; ++i ) {
            int ch = b[i] & 0xFF;
            if ( (ch >= '0' && ch <= '9')
                    || (ch >= 'A' && ch <= 'Z')
                    || (ch >= 'a' && ch <= 'z')
                    || " `~!@#$%^&*()-_=+[]{}|;:'\",.<>/?".indexOf(ch) >= 0 ) {
                result.append((char)ch);
            } else {
                result.append(String.format("\\x%02X", ch));
            }
        }
        return result.toString();
    }

    private static boolean isHexDigit(char c) {
        return
                (c >= 'A' && c <= 'F') ||
                        (c >= '0' && c <= '9');
    }

    /**
     * Takes a ASCII digit in the range A-F0-9 and returns
     * the corresponding integer/ordinal value.
     * @param ch  The hex digit.
     * @return The converted hex value as a byte.
     */
    public static byte toBinaryFromHex(byte ch) {
        if ( ch >= 'A' && ch <= 'F' )
            return (byte) ((byte)10 + (byte) (ch - 'A'));
        // else
        return (byte) (ch - '0');
    }
}
