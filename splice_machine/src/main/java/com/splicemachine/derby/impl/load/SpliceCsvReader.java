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

package com.splicemachine.derby.impl.load;

import java.io.IOException;
import java.io.Reader;
import java.util.*;
import java.util.List;

import com.splicemachine.derby.stream.function.QuoteTrackingTokenizer;
import com.splicemachine.derby.stream.utils.BooleanList;
import org.supercsv.prefs.CsvPreference;

/**
 * SpliceCsvReader is a simple reader that reads a row from a CSV file into an array of Strings.
 *
 * @author dwinters
 */
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
		this.tokenizer = new QuoteTrackingTokenizer(reader,preferences);
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
