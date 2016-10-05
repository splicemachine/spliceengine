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
 *
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