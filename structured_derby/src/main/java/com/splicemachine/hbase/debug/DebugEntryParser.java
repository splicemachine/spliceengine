package com.splicemachine.hbase.debug;

import com.splicemachine.encoding.DataType;
import org.apache.hadoop.hbase.util.Bytes;

/**
 * @author Scott Fines
 * Created on: 9/16/13
 */
public class DebugEntryParser {

    public static final void main(String...args) throws Exception{
        if(args==null||args.length<4){
            printUsage();
            System.exit(65);
        }

        String dataDelim = ",";
        String dataStr = null;
        String typesStr = null;
        for(int i=0;i<args.length;i++){
            if("-t".equals(args[i])){
                i++;
                typesStr = args[i];
            }else if("-b".equals(args[i])){
                i++;
                dataStr = args[i];
            }else if("-d".equals(args[i])){
                i++;
                dataDelim = args[i];
            }else{
                System.err.println("Unknown argument: "+ args[i]);
                System.exit(65);
            }
        }

        String[] types = typesStr.split(",");
        String[] data = dataStr.split(",");
        if(types.length<data.length){
            System.err.println("Insufficient types to read line");
            System.exit(4);
        }
        DataType[] dataTypes = new DataType[types.length];
        for(int i=0;i<types.length;i++){
            dataTypes[i] = DataType.fromCode(types[i]);
        }

        String[] decodedOutput = new String[data.length];
        StringBuilder output = new StringBuilder();
        boolean isFirst=true;
        for(int i=0;i<decodedOutput.length;i++){
            byte[] unhexlified = fromHex(data[i]);
            String decoded = dataTypes[i].decode(unhexlified);
            if(isFirst)
                isFirst=false;
            else
                output = output.append(",");

            output.append(decoded);
        }
        output.append("\n");
        System.out.println(output);
    }

    private static byte[] fromHex(String rawHex){
        //take two digits at a time
        int length = rawHex.length()/2;
        if(rawHex.length()%2!=0)
            length++;
        byte[] raw = new byte[length];
        char[] rawChars = rawHex.toCharArray();
        for(int i=0,pos=0;i<rawHex.length()-1;i+=2,pos++){
            char n = rawChars[i];
            char m = rawChars[i+1];

            byte d = (byte)(Bytes.toBinaryFromHex((byte)n)<<4);
            d |= Bytes.toBinaryFromHex((byte)m);
            raw[pos] =d;
        }

        return raw;
    }

    private static void printUsage() {
        System.out.println("Usage: DebugEntryParser [-d] -t {column types} -b {column data}");
        System.out.println("Options:");
        System.out.printf("%-20s\t%100s%n","-d","The Delimiter separating column entries. Default is ,");
        System.out.printf("%-20s\t%100s%n","column type", "A comma-separated list of column types IN ORDER");
        System.out.printf("%-20s\t%100s%n","column data", "A delimiter-separated list of column values in Hexadecimal. Correlated with types to generate an output row");
    }
}
