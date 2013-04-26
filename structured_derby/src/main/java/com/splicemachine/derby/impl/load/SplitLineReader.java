package com.splicemachine.derby.impl.load;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

public class SplitLineReader extends Reader {

    private InputStreamReader isr = null;
    private int totalBytesToRead;
    private boolean foundEOL = false;

    public SplitLineReader(FSDataInputStream fis, long beginAt, int totalBytesToRead) throws IOException {
        fis.seek(beginAt);
        isr = new InputStreamReader(fis);
        this.totalBytesToRead = totalBytesToRead;

    }

    @Override
    public int read(char[] cbuf) throws IOException {
        return super.read(cbuf);    //To change body of overridden methods use File | Settings | File Templates.
    }

    private int readUntilEOL(char[] chars, int offset, int maxBytesToRead) throws IOException {
        int result = 0;

        for(int i=offset; i < maxBytesToRead && !foundEOL; i++){
            char nextChar = (char) isr.read();

            if(nextChar == '\n'){
                foundEOL = true;
            } else {
                chars[i]=nextChar;
                result++;
            }
        }

        return result;
    }

    @Override
    public int read(char[] chars, int offset, int length) throws IOException {

        int bytesToRead = length-offset;
        int result;

        if(totalBytesToRead < 0 && foundEOL){
            result = -1;
        } else if(bytesToRead < totalBytesToRead){
            result = isr.read(chars, offset, length);
            totalBytesToRead -= bytesToRead;
        }else{
            if(totalBytesToRead > 0){
                result = isr.read(chars, offset, totalBytesToRead);
                result += readUntilEOL(chars, offset + totalBytesToRead, bytesToRead - totalBytesToRead);
                totalBytesToRead = -1;
            }else{
                result = readUntilEOL(chars, offset, bytesToRead);
            }

        }

        return result;
    }

    @Override
    public void close() throws IOException {
        isr.close();
    }
}
