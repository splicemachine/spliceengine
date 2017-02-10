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

package com.splicemachine.derby.impl.load;

import com.splicemachine.derby.stream.function.QuoteTrackingTokenizer;
import org.supercsv.prefs.CsvPreference;

import java.io.Reader;

/**
 * @author Scott Fines
 *         Date: 9/29/16
 */
public class SpliceCsvTokenizer extends QuoteTrackingTokenizer{

    /**
     * Constructs a new <tt>Tokenizer</tt>, which reads the CSV file, line by line.
     *
     * @param reader      the reader
     * @param preferences the CSV preferences
     * @throws NullPointerException if reader or preferences is null
     */
    public SpliceCsvTokenizer(Reader reader,CsvPreference preferences){
        super(reader,preferences);
    }



}
