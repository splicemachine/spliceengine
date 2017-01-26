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
import org.junit.Assert;
import org.junit.Test;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.StringReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * @author Scott Fines
 *         Date: 9/29/16
 */
public class QuoteTrackingTokenizerTest{

    @Test
    public void ignoresUnquotedColumns() throws Exception{
        String column = "hello,goodbye,parseThis!,boots\n";
        List<String> correctCols = Arrays.asList("hello","goodbye","parseThis!","boots");
        BooleanList correctQuotes = BooleanList.wrap(new boolean[4]);

        checkResults(column,correctCols,correctQuotes,4);
    }

    @Test
    public void catchesQuotedFirstColumn() throws Exception{
        String column = "\"hello\",goodbye,parseThis!,boots\n";
        List<String> correctCols = Arrays.asList("hello","goodbye","parseThis!","boots");
        BooleanList correctQuotes = BooleanList.wrap(true,false,false,false);

        checkResults(column,correctCols,correctQuotes,4);
    }

    @Test
    public void recordsQuotesAcrossLineBreaks() throws Exception{
        String column = "\"hello\",goodbye,parseThis!,\"boots\nmagoo\"";
        List<String> correctCols = Arrays.asList("hello","goodbye","parseThis!","boots\nmagoo");
        BooleanList correctQuotes = BooleanList.wrap(true,false,false,true);

        checkResults(column,correctCols,correctQuotes,4);
    }

    /* ****************************************************************************************************************/
    /*private helper methods*/
    private void checkResults(String column,List<String> correctCols,BooleanList correctQuotes,int size) throws IOException{
        QuoteTrackingTokenizer qtt = new QuoteTrackingTokenizer(new StringReader(column),CsvPreference.STANDARD_PREFERENCE);
        List<String> cols = new ArrayList<>(size);
        BooleanList quoteCols = new BooleanList(size);
        Assert.assertTrue("Did not properly read the columns!",qtt.readColumns(cols,quoteCols));
        Assert.assertEquals("Did not return correct column information",correctCols, cols);
        Assert.assertEquals("Did not return correct quoted column information",correctQuotes,quoteCols);
    }
}