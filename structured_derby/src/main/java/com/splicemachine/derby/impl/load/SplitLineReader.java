package com.splicemachine.derby.impl.load;

import org.apache.hadoop.fs.FSDataInputStream;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;

/**
 * Reader that recognizes byte boundries.  This class is useful for splitting a file
 * starting at an arbitrary byte of the file and consuming up to some total amount of bytes.
 * It's line aware and will consume bytes beyond totalBytesToRead, until it has found the next
 * newline.
 */
public class SplitLineReader extends Reader {

    private InputStreamReader isr = null;
    private int totalBytesToRead;
    private boolean foundEOL = false;

    public SplitLineReader(FSDataInputStream fis, long beginAt, int totalBytesToRead) throws IOException {
        fis.seek(beginAt);
        isr = new InputStreamReader(fis);
        this.totalBytesToRead = totalBytesToRead;

    }

    /**
     * Consumes either maxBytesToRead or up to the newline character from the input stream reader, whichever comes first.
     *
     * @param chars
     * @param offset
     * @param maxBytesToRead
     * @return
     * @throws IOException
     */
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

    /**
     * Returns a BufferedReader wrapping a SplitLineReader.  There's nothing special about the BufferedReader
     * other than the first line is always discarded, unless the first bytes from the stream are being read.
     * It's assumed that the previous chunk read from the file will have consumed the first bytes up to the newline.
     *
     * @param fis
     * @param beginAt
     * @param totalBytesToRead
     * @return
     * @throws IOException
     */
    public static BufferedReader createBufferedLineReader(FSDataInputStream fis, long beginAt, int totalBytesToRead) throws IOException{
        BufferedReader buff = new BufferedReader(new SplitLineReader(fis, beginAt, totalBytesToRead));

        if(beginAt != 0){
            buff.readLine();
        }

        return buff;
    }
}
