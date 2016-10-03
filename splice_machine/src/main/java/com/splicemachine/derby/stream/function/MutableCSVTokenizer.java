/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 */

package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.stream.utils.BooleanList;
import org.supercsv.io.Tokenizer;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;

/**
 *
 * Scaled back tokenizer that allows you to swap in a string during imports.
 *
 * This class allows us to not have to re-instantiate the CSV parser each time and
 * to bypass the synchronized lineNumber implementation in SuperCSV.
 *
 */
public class MutableCSVTokenizer extends QuoteTrackingTokenizer {

    private String line;
    private final List<String> columns =new ArrayList<>();
    private final BooleanList quotedColumns = new BooleanList();
    public MutableCSVTokenizer(Reader reader, CsvPreference preferences) {
        super(reader, preferences);
    }

    /**
     *
     * Reads the line that is set and then sets it to null
     *
     * @return
     * @throws IOException
     */
    @Override
    protected String readLine() throws IOException {
        try {
            return line;
        } finally {
            line = null;
        }
    }
    /**
     *
     * Set the line to be tokenized
     *
     * @return
     * @throws IOException
     */
    public void setLine(String line) {
        this.line = line;
    }

    /**
     *
     * Perform read, main way to take the string passed to setLine and return a
     * list of strings
     *
     * @return
     * @throws IOException
     */
    public List<String> read() throws IOException {
        if( readRow() ) {
            return new ArrayList<>(getColumns()); // Do we need to array copy here?
        }
        return null; // EOF
    }

    public BooleanList getQuotedColumns(){
        return new BooleanList(quotedColumns); //do we need this array copy?
    }

    /**
     * Reads the row
     *
     * @return
     * @throws IOException
     */
    protected boolean readRow() throws IOException {
        if( readColumns(columns,quotedColumns) ) {
            return true;
        }
        return false;
    }

    /**
     * Gets the tokenized columns.
     *
     * @return the tokenized columns
     */
    protected List<String> getColumns() {
        return columns;
    }

}

