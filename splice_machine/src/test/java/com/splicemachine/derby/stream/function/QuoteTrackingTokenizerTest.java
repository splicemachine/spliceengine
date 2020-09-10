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

package com.splicemachine.derby.stream.function;

import com.splicemachine.derby.stream.utils.BooleanList;
import org.junit.Assert;
import org.junit.Test;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.StringReader;
import java.util.*;

/**
 * @author Scott Fines
 * Date: 9/29/16
 */
public class QuoteTrackingTokenizerTest {

    @Test
    public void ignoresUnquotedColumns() throws Exception {
        String row = "hello,goodbye,parseThis!,boots\n";
        List<String> correctCols = Arrays.asList("hello", "goodbye", "parseThis!", "boots");
        BooleanList correctQuotes = BooleanList.wrap(new boolean[4]);

        checkResults(row, correctCols, correctQuotes, 4);
    }

    @Test
    public void catchesQuotedFirstColumn() throws Exception {
        String row = "\"hello\",goodbye,parseThis!,boots\n";
        List<String> correctCols = Arrays.asList("hello", "goodbye", "parseThis!", "boots");
        BooleanList correctQuotes = BooleanList.wrap(true, false, false, false);

        checkResults(row, correctCols, correctQuotes, 4);
    }

    @Test
    public void recordsQuotesAcrossLineBreaks() throws Exception {
        String row = "\"hello\",goodbye,parseThis!,\"boots\nmagoo\"";
        List<String> correctCols = Arrays.asList("hello", "goodbye", "parseThis!", "boots\nmagoo");
        BooleanList correctQuotes = BooleanList.wrap(true, false, false, true);

        checkResults(row, correctCols, correctQuotes, 4);
    }

    @Test
    public void recordsQuotesAcrossLineManyBreaks() throws Exception {
        String row = "\"hello\",goodbye,parseThis!,\"boots\nma\n\n\ngoo\"";
        List<String> correctCols = Arrays.asList("hello", "goodbye", "parseThis!", "boots\nma\n\n\ngoo");
        BooleanList correctQuotes = BooleanList.wrap(true, false, false, true);
        checkResults(row, correctCols, correctQuotes, 3);
    }

    @Test
    public void exactSizeCalculationMultipleRowsIsCorrect() throws Exception {
        CSVBuilder csvBuilder = new CSVBuilder(false);
        int colCount = 100, rowCount = 100;
        csvBuilder.initCsv(colCount, rowCount);
        checkResults(csvBuilder.csv, csvBuilder.expectedStrings, csvBuilder.expectedQuotes,
                csvBuilder.expectedActualSizes, 2, Collections.nCopies(colCount, 10), false);
    }

    @Test
    public void extraColumnsAreTolerated() throws Exception {
        String invalidRow = "\"hello\",,,goodbye,parseThis!,\"boots\nma\n\n\ngoo\"";
        List<String> correctCols = Arrays.asList("hello", null, null, "goodbye", "parseThis!", "boots\nma\n\n\ngoo");
        BooleanList correctQuotes = BooleanList.wrap(true, false, false, false, false, true);
        checkResults(invalidRow, correctCols, correctQuotes, 3, false);
    }

    @Test
    public void boundaryMissingQuoteErrorDuringScanIsReportedCorrectly() throws Exception {
        String invalidRow = "\"hello,goodbye,parseThis!,boots\n";
        List<String> correctCols = Arrays.asList("hello", "goodbye", "parseThis!", "boots");
        BooleanList correctQuotes = BooleanList.wrap(true, false, false, false);
        try {
            checkResults(invalidRow, correctCols, correctQuotes, 4, true);
            Assert.fail("expected exception to be thrown, but no exception was thrown");
        } catch(Exception e) {
            Assert.assertTrue(e instanceof SuperCsvException);
            Assert.assertEquals("partial record found while scanning row at line 1 and ending on line 1", e.getMessage());
            return;
        }
        Assert.fail("expected exception to be thrown, but no exception was thrown");
    }

    @Test
    public void boundaryMissingQuoteErrorDuringReadIsReportedCorrectly() throws Exception {
        String invalidRow = "\"hello,goodbye,parseThis!,boots\n";
        List<String> correctCols = Arrays.asList("hello", "goodbye", "parseThis!", "boots");
        BooleanList correctQuotes = BooleanList.wrap(true, false, false, false);
        try {
            checkResults(invalidRow, correctCols, correctQuotes, 4);
            Assert.fail("expected exception to be thrown, but no exception was thrown");
        } catch(Exception e) {
            Assert.assertTrue(e instanceof SuperCsvException);
            Assert.assertEquals("partial record found [hello,goodbye,parseThis!,boots\n" +
                    "] while reading quoted column beginning on line 1 and ending on line 1", e.getMessage());
            return;
        }
        Assert.fail("expected exception to be thrown, but no exception was thrown");
    }

    @Test
    public void exactSizeCalculationMultipleOneLineRecordsIsCorrect() throws Exception {
        CSVBuilder csvBuilder = new CSVBuilder(true);
        int colCount = 200, rowCount = 100;
        csvBuilder.initCsv(colCount, rowCount);
        checkResults(csvBuilder.csv, csvBuilder.expectedStrings, csvBuilder.expectedQuotes, csvBuilder.expectedActualSizes,
                2, Collections.nCopies(colCount, 10), true);
    }

    @Test
    public void quotedMultiRowRecordWithOneLineRecordSetThrowsProperly() throws Exception {
        String invalidRow = "\"hello\",\"wrong\ncell\",goodbye\n";
        try {
            QuoteTrackingTokenizer qtt = new QuoteTrackingTokenizer(new StringReader(invalidRow), CsvPreference.STANDARD_PREFERENCE, true);
            List<String> columns = new ArrayList<>();
            qtt.readColumns(columns);
            Assert.fail("expected exception to be thrown, but no exception was thrown");
        } catch(Exception e) {
            Assert.assertTrue(e instanceof SuperCsvException);
            Assert.assertEquals("one-line record CSV has a record that spans over multiple lines at line 1", e.getMessage());
            return;
        }
        Assert.fail("expected exception to be thrown, but no exception was thrown");
    }


    private static void checkResults(String row, List<String> expectedColumns, BooleanList expectedQuotes, int size) throws IOException {
        checkResults(row, expectedColumns, expectedQuotes, size, false);
    }

    private static void checkResults(String row, List<String> expectedColumns, BooleanList expectedQuotes, int size, boolean triggerScan) throws IOException {
        QuoteTrackingTokenizer qtt = null;
        if(triggerScan) {
            qtt = new QuoteTrackingTokenizer(new StringReader(row), CsvPreference.STANDARD_PREFERENCE, false, 1, Collections.nCopies(expectedColumns.size(), 10));
        } else {
            qtt = new QuoteTrackingTokenizer(new StringReader(row), CsvPreference.STANDARD_PREFERENCE, false);
        }
        List<String> actualColumns = new ArrayList<>(size);
        BooleanList actualQuotes = new BooleanList(size);
        Assert.assertTrue("Did not properly read the columns!", qtt.readColumns(actualColumns, actualQuotes));
        Assert.assertEquals("Did not return correct column information", expectedColumns, actualColumns);
        Assert.assertEquals("Did not return correct quoted column information", expectedQuotes, actualQuotes);
    }

    private static void checkResults(String column, List<List<String>> expectedColumns, List<BooleanList> expectedQuotes,
                                     List<List<Integer>> expectedValueSizes, int size, List<Integer> valueSizes, boolean oneLineRecord) throws IOException {
        QuoteTrackingTokenizer qtt = new QuoteTrackingTokenizer(new StringReader(column), CsvPreference.STANDARD_PREFERENCE, oneLineRecord, 1, valueSizes);

        List<String> cols = new ArrayList<>(size);
        BooleanList quoteCols = new BooleanList(size);
        for(int i = 0; i < expectedColumns.size(); ++i) {
            Assert.assertTrue("Did not properly read the columns!", qtt.readColumns(cols, quoteCols));
            Assert.assertEquals("Did not return correct column information", expectedColumns.get(i), cols);
            Assert.assertEquals("Did not return correct quoted column information", expectedQuotes.get(i), quoteCols);
            if(!oneLineRecord) {
                Assert.assertArrayEquals("Did not return correct column sizes", expectedValueSizes.get(i).stream().mapToInt(k->k).toArray(), qtt.getExactColumnSizes());
            }
        }
    }

    private static class CSVBuilder {
        private final boolean oneLineRecord;
        private StringBuilder sb = new StringBuilder();

        private Random random = new Random();

        CSVBuilder(boolean oneLineRecord) {
            this.oneLineRecord = oneLineRecord;
        }

        int nextInt() {
            return random.nextInt(20) + 23;
        }

        List<BooleanList> expectedQuotes;
        List<List<Integer>> expectedActualSizes;
        List<List<String>> expectedStrings;
        String csv;

        void initCsv(int colCount, int rowCount) {
            csv = "";
            expectedQuotes = new ArrayList<>(rowCount);
            expectedActualSizes = new ArrayList<>(rowCount);
            expectedStrings = new ArrayList<>(rowCount);
            for(int row = 0; row < rowCount; ++row) {
                BooleanList bl = new BooleanList(colCount);
                List<Integer> expectedActualSize = new ArrayList<>(colCount);
                List<String> expectedStringsRow = new ArrayList<>(colCount);
                for(int col = 0; col < colCount; ++col) {
                    StringBuilder expectedString = new StringBuilder(), csvString = new StringBuilder();
                    boolean quoted = false;
                    int size = 0;
                    String value = String.join("", Collections.nCopies(nextInt(), "A"));
                    expectedString.append(value);
                    csvString.append(value);
                    quoted = false;
                    if(nextInt() % 2 == 0) {
                        quoted = true;

                        if(!oneLineRecord) {
                            // add some new lines.
                            int numNewLines = nextInt();
                            String newLines = String.join("", Collections.nCopies(numNewLines, "\n"));
                            csvString.append(newLines);
                            expectedString.append(newLines);
                        }

                        // also add some quotes, escape them in the CSV input.
                        int numQuotes = nextInt();
                        String quotes = String.join("", Collections.nCopies(numQuotes, "\""));
                        csvString.append(quotes).append(quotes); // for escaping
                        expectedString.append(quotes);
                        csv += "\"" + csvString.toString() + "\"";
                    }
                    else{
                        csv += csvString.toString();
                    }
                    size = expectedString.length();
                    if(col < colCount - 1) {
                        csv += ",";
                    }
                    bl.add(quoted);
                    expectedActualSize.add(size);
                    expectedStringsRow.add(expectedString.toString());
                }
                expectedQuotes.add(bl);
                expectedActualSizes.add(expectedActualSize);
                expectedStrings.add(expectedStringsRow);
                csv += "\n";
            }
        }

    }
}
