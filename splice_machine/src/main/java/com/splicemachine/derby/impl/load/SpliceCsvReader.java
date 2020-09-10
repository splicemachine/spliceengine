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

package com.splicemachine.derby.impl.load;

import java.io.IOException;
import java.io.Reader;
import java.util.*;

import com.splicemachine.derby.stream.function.QuoteTrackingTokenizer;
import com.splicemachine.derby.stream.utils.BooleanList;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;
import org.supercsv.prefs.CsvPreference;

/**
 * SpliceCsvReader is a simple reader that reads a row from a CSV file into an array of Strings.
 *
 * @author dwinters
 */
@SuppressFBWarnings(value = "IT_NO_SUCH_ELEMENT", justification = "this is by design, the caller is responsible for " +
		"calling hasNext first to check whether a new element is there.")
public class SpliceCsvReader implements Iterator<List<String>> {

	private QuoteTrackingTokenizer tokenizer;
	private List<String> columns = new ArrayList<>();
	private BooleanList quotedColumns= new BooleanList();

	private boolean firstRead = true;
	/**
	 * Constructs a new <tt>SpliceCsvReader</tt> with the supplied Reader and CSV preferences. Note that the
	 * <tt>reader</tt> will be wrapped in a <tt>BufferedReader</tt> before accessed.
	 * 
	 * @param reader
	 *            the reader
	 * @param preferences
	 *            the CSV preferences
	 * @throws NullPointerException
	 *             if reader or preferences are null
	 */
	public SpliceCsvReader(Reader reader, CsvPreference preferences) {
		this.tokenizer = new QuoteTrackingTokenizer(reader,preferences, false);
	}

	public SpliceCsvReader(Reader reader, CsvPreference preferences, long scanThreshold, List<Integer> valueSizesHint) {
		this.tokenizer = new QuoteTrackingTokenizer(reader,preferences,false, scanThreshold, valueSizesHint);
	}

    @Override
    public boolean hasNext() {
        try {
			boolean read = tokenizer.readColumns(columns,quotedColumns);
			if(firstRead){
				((ArrayList)columns).trimToSize();
				quotedColumns.trimToSize();
				firstRead =false;
			}
            return read;
        } catch (IOException ioe) {
            throw new RuntimeException(ioe);
        }
    }

    @Override
    public List<String> next() {
		return columns;
    }

	public BooleanList nextQuotedColumns(){
		return quotedColumns;
	}

    @Override
    public void remove() {
        throw new UnsupportedOperationException();
    }

}
