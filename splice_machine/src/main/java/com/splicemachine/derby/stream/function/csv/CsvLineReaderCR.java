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

package com.splicemachine.derby.stream.function.csv;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

import org.supercsv.io.ITokenizer;
import org.supercsv.io.Tokenizer;
import org.supercsv.prefs.CsvPreference;

/**
 * Defines the standard behaviour of a Tokenizer. Extend this class if you want the line-reading functionality of the
 * default {@link Tokenizer}, but want to define your own implementation of {@link #readColumns(List)}.
 *
 * @author James Bassett
 * @since 2.0.0
 */
public class CsvLineReaderCR {
    private final Reader reader;
    private boolean skipCarriageReturn;
    private int lineNumber = 0;
    private int configInitialStringBufferSize = 80;
    private int configBufferSize = 8192;

    // buffer
    private char bufferBytes[];
    private int bufferSize, bufferPos;

    public CsvLineReaderCR(final Reader reader, boolean skipCarriageReturn) {
         if( reader == null ) {
            throw new NullPointerException("reader should not be null");
        }
        this.reader = reader;
        this.skipCarriageReturn = skipCarriageReturn;
    }

    public CsvLineReaderCR(final Reader reader, boolean skipCarriageReturn, int configInitialStringBufferSize,
                           int configBufferSize) {
        if( reader == null ) {
            throw new NullPointerException("reader should not be null");
        }
        this.reader = reader;
        this.skipCarriageReturn = skipCarriageReturn;
        this.configInitialStringBufferSize = configInitialStringBufferSize;
        this.configBufferSize = configBufferSize;
    }

    /**
     * Closes the underlying reader.
     */
    public void close() throws IOException {
        reader.close();
    }

    /**
     * {@inheritDoc}
     */
    public int getLineNumber() {
        return lineNumber;
    }

    private boolean readBuffer() throws IOException {
        bufferPos = 0;
        bufferSize = reader.read(bufferBytes, 0, bufferBytes.length);
        if(bufferSize < 0) bufferSize = 0;
        return bufferSize != 0;
    }

    StringBuffer res = null;
    boolean eof = false;

    String getRes(int start, int pos)
    {
        lineNumber++;
        if (res == null) {
            return new String(bufferBytes, start, pos - start);
        } else {
            if(start != pos) res.append(bufferBytes, start, pos - start);
            String s = res.toString();
            res = null;
            return s;
        }
    }
    void appendToRes(int start, int pos)
    {
        if(start == pos) return;
        if (res == null)
            res = new StringBuffer(configInitialStringBufferSize);
        res.append(bufferBytes, start, pos - start);
    }

    public boolean eof()
    {
        return eof;
    }

    enum lineending {
        ending0D0A,
        ending0D,
        ending0A,
        endingNone
    };

    lineending currentLineEnding;


    public String readLine() throws IOException {
        if( eof ) return null;
        if( bufferBytes == null ) {
            bufferBytes = new char[configBufferSize];
            if(readBuffer() == false)
            {
                eof = true;
                return null;
            }
        }
        boolean crBefore = false;
        if(bufferPos >= bufferSize) {
            if(readBuffer() == false) {
                eof = true;
                return null;
            }
        }
        boolean crNewline = true;
        while(true) {

            char c = 0;
            int start = bufferPos;
            for ( ;bufferPos < bufferSize; bufferPos++) {
                c = bufferBytes[bufferPos];
                if ((c == '\n') || (skipCarriageReturn && c == '\r')) break;
                else crBefore = false;
            }
            // todo: maybe avoid using StringBuffer here totally by
            // filling the bufferBytes up so we can return new String(bufferBytes, start, pos - start); as much as possible
            // todo: if lines are longer than expected, increase buffer
            if(c == '\n') {
                // CASE 1
                if(crBefore)
                    currentLineEnding = lineending.ending0D0A;
                else
                    currentLineEnding = lineending.ending0A;
                return getRes(start, bufferPos++);
            }
            else if(bufferPos >= bufferSize) {
                // CASE 2
                appendToRes(start, bufferPos);
                currentLineEnding = lineending.endingNone;
                if(readBuffer() == false) {
                    eof = true;
                    return getRes(0, 0);
                }
            }
            else // only c==\r
            {
                if( bufferPos+1 < bufferSize ) {
                    boolean is0A = bufferBytes[bufferPos+1] == '\n';
                    if( is0A || crNewline) {
                        // case 3
                        currentLineEnding = is0A ? lineending.ending0D0A :
                                lineending.ending0D;
                        String res = getRes(start, bufferPos);
                        bufferPos += is0A ? 2 : 1;
                        return res;
                    }
                    else {
                        if(crNewline) {
                            // case 4
                            // \r in the middle like "AAA\rBBB\r\n"
                            crBefore = true;
                            appendToRes(start, bufferPos + 1);
                            bufferPos++;
                        }
                    }
                }
                else { // \r was last sign in buffer
                    appendToRes(start, bufferPos);
                    if(readBuffer() == false) {
                        // case 5
                        // \r then EOF
                        eof = true;
                        currentLineEnding = lineending.ending0D;
                        return getRes(0, 0);
                    }
                    boolean is0A = bufferBytes[0] == '\n';
                    if( is0A || crNewline) {
                        currentLineEnding = is0A ? lineending.ending0D0A :
                                lineending.ending0D;
                        // case 6
                        // \r | \n
                        if( is0A ) bufferPos = 1;
                        currentLineEnding = lineending.ending0D0A;
                        return getRes(0, 0);
                    }
                    else {
                        currentLineEnding = lineending.ending0A;
                        //res.append('\r');
                        return getRes(0, 0);
                        // case 7
                        // \r | A
                        // continue;
                    }
                }
            }
        }
    }
}
