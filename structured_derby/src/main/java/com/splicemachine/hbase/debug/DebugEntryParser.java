package com.splicemachine.hbase.debug;

import com.splicemachine.encoding.debug.DataType;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.BufferedReader;
import java.io.InputStreamReader;

/**
 * @author Scott Fines
 * Created on: 9/16/13
 */
public class DebugEntryParser {

    public static void main(String...args) throws Exception{
        if(args==null||args.length<2){
            printUsage();
            System.exit(65);
        }

        String dataDelim = ",";
        String typesStr = null;
        boolean outputRawHex = false;
        for(int i=0;i<args.length;i++){
            if("-t".equals(args[i])){
                i++;
                typesStr = args[i];
            }else if("-d".equals(args[i])){
                i++;
                dataDelim = args[i];
            }else if("-h".equals(args[i])){
                i++;
                outputRawHex = true;
            }else{
                System.err.println("Unknown argument: "+ args[i]);
                System.exit(65);
            }
        }
        if(typesStr==null){
            printUsage();
            System.exit(65);
        }

        String[] types = typesStr.split(",");
        DataType[] dataTypes = new DataType[types.length];
        for(int i=0;i<types.length;i++){
            dataTypes[i] = DataType.fromCode(types[i]);
        }

        BufferedReader inputReader = new BufferedReader(new InputStreamReader(System.in));
        String dataStr;
        while((dataStr = inputReader.readLine())!=null){
            if(dataStr.length()==0)
                continue;
            String[] data = dataStr.split(dataDelim);
            StringBuilder output = new StringBuilder();
            boolean isFirst=true;
            for(int i=0;i<dataTypes.length;i++){
                byte[] unhexlified = fromHex(data[i]);
                String decoded = dataTypes[i].decode(unhexlified,outputRawHex);
                if(isFirst)
                    isFirst=false;
                else
                    output = output.append(",");

                output.append(decoded);
            }
            System.out.println(output);
        }
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
