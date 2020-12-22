/*
 * Copyright 2007 Kasper B. Graversen
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
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
public abstract class AbstractTokenizerCR implements ITokenizer {

    private final CsvPreference preferences;

    private CsvLineReaderCR clr;

    /**
     * Constructs a new <tt>AbstractTokenizer</tt>, which reads the CSV file, line by line.
     *
     * @param reader
     *            the reader
     * @param preferences
     *            the CSV preferences
     * @throws NullPointerException
     *             if reader or preferences is null
     */
    public AbstractTokenizerCR(final Reader reader, final CsvPreference preferences, boolean skipCarriageReturnIn0D0A) {
        if( reader == null ) {
            throw new NullPointerException("reader should not be null");
        }
        if( preferences == null ) {
            throw new NullPointerException("preferences should not be null");
        }
        this.preferences = preferences;
        clr = new CsvLineReaderCR(reader, skipCarriageReturnIn0D0A, true);
    }

    /**
     * Closes the underlying reader.
     */
    public void close() throws IOException {
        clr.close();
    }

    /**
     * {@inheritDoc}
     */
    public int getLineNumber() {
        return clr.getLineNumber();
    }

    /**
     * Reads a line of text. Whenever a line terminator is read the current line number is incremented.
     *
     * @return A String containing the contents of the line, not including any line termination characters, or
     *         <tt>null</tt> if the end of the stream has been reached
     * @throws IOException
     *             If an I/O error occurs
     */
    protected String readLine() throws IOException {
        return clr.readLine();
    }

    /**
     * Gets the CSV preferences.
     *
     * @return the preferences
     */
    protected CsvPreference getPreferences() {
        return preferences;
    }
}
