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

import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.IOException;
import java.io.Reader;

/**
 * A buffered CSV line reader.
 * With the option skipCarriageReturn we can define if carriage return (\r = 0x0D)
 * is meant to be skipped or considered part of the returned string.
 * e.g. input "Hello\r\nWorld!\n"
 * will return lines "Hello" and "World!" with skipCarriageReturn = true,
 * but will return "Hello\r" and "World!" with skipCarriageReturn = false.
 * This is meant for preserving line endings.
 */
public class CsvLineReaderCR {
    private final Reader reader;
    private final boolean skipCarriageReturn;
    private final boolean crNewline;    ;
    private final int configInitialStringBufferSize;
    private final int configBufferSize;

    private char bufferBytes[];
    private int bufferSize, bufferPos;
    private int lineNumber = 0;

    private StringBuffer res = null;
    private boolean eof = false;

    public enum LineEndingType {
        ending0D0A,
        ending0D,
        ending0A,
        endingNone
    };

    @SuppressFBWarnings("URF_UNREAD_FIELD")
    LineEndingType currentLineEnding = LineEndingType.endingNone;

    public CsvLineReaderCR(final Reader reader, boolean skipCarriageReturn, boolean crNewline) {
        this(reader, skipCarriageReturn, crNewline, 80, 8192);
    }

    public CsvLineReaderCR(final Reader reader, boolean skipCarriageReturn, boolean crNewline,
                           int configInitialStringBufferSize, int configBufferSize) {
        if( reader == null ) {
            throw new NullPointerException("reader should not be null");
        }
        this.reader = reader;
        this.skipCarriageReturn = skipCarriageReturn;
        this.crNewline = crNewline;
        this.configInitialStringBufferSize = configInitialStringBufferSize;
        this.configBufferSize = configBufferSize;
    }

    public void close() throws IOException {
        reader.close();
    }

    public int getLineNumber() {
        return lineNumber;
    }

    private boolean readBuffer() throws IOException {
        bufferPos = 0;
        bufferSize = reader.read(bufferBytes, 0, bufferBytes.length);
        if(bufferSize < 0) bufferSize = 0;
        return bufferSize != 0;
    }

    private String getRes(int start, int pos)
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
    private void appendToRes(int start, int pos)
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
                    currentLineEnding = LineEndingType.ending0D0A;
                else
                    currentLineEnding = LineEndingType.ending0A;
                return getRes(start, bufferPos++);
            }
            else if(bufferPos >= bufferSize) {
                // CASE 2
                appendToRes(start, bufferPos);
                currentLineEnding = LineEndingType.endingNone;
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
                        currentLineEnding = is0A ? LineEndingType.ending0D0A :
                                LineEndingType.ending0D;
                        String res = getRes(start, bufferPos);
                        bufferPos += is0A ? 2 : 1;
                        return res;
                    }
                    else {
                        if(!crNewline) {
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
                        currentLineEnding = LineEndingType.ending0D;
                        return getRes(0, 0);
                    }
                    boolean is0A = bufferBytes[0] == '\n';
                    if( is0A || crNewline) {
                        currentLineEnding = is0A ? LineEndingType.ending0D0A :
                                LineEndingType.ending0D;
                        // case 6
                        // \r | \n
                        if( is0A ) bufferPos = 1;
                        currentLineEnding = LineEndingType.ending0D0A;
                        return getRes(0, 0);
                    }
                    else {
                        currentLineEnding = LineEndingType.ending0A;
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
