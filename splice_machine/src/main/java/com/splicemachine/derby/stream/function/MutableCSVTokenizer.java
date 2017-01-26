/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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

package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.stream.utils.BooleanList;
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

