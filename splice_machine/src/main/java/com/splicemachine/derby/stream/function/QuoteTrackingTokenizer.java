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

import com.splicemachine.EngineDriver;
import com.splicemachine.derby.stream.utils.BooleanList;
import org.apache.log4j.Logger;
import org.supercsv.comment.CommentMatcher;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.AbstractTokenizer;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * A Tokenizer which keeps track of which columns were quoted and which were not.
 * This allows us to make a distinction between columns with value {@code Foo}
 * versus columns with values {@code "Foo"}.
 *
 * @author Scott Fines
 * Date: 9/29/16
 */
public class QuoteTrackingTokenizer extends AbstractTokenizer {

    private static final Logger LOG = Logger.getLogger(QuoteTrackingTokenizer.class);
    private static final char NEWLINE = '\n';
    private static final char SPACE = ' ';
    private final List<Integer> valueSizeHints;
    // the line number where a potential multi-line cell starts
    private int potentialSpaces;
    private int charIndex;
    private boolean wasQuoted;
    // the line number where a potential multi-line cell starts
    private int quoteScopeStartingLine;
    private TokenizerState state;
    private int colIdx;
    private boolean firstRun;
    private boolean checkExactSizeFirst = false;
    private int rowIdx = 0;
    private int[] exactColumnSizes = null;
    private StringBuilder currentColumn = new StringBuilder();
    /* the raw, untokenized CSV row (may span multiple lines) */
    private final LazyStringBuilder currentRow = new LazyStringBuilder();
    private final int quoteChar;
    private final int delimeterChar;
    private final boolean surroundingSpacesNeedQuotes;
    private final boolean ignoreEmptyLines;
    private final CommentMatcher commentMatcher;
    private final int maxLinesPerRow;
    private long scanThresold;
    private final boolean oneLineRecord;
    private int oneLineRecordLength = 0;

    private static class LazyStringBuilder {

        final List<String> bufferList;
        int size = 0;
        boolean sizeCached = false;

        public LazyStringBuilder() {
            this.bufferList = new ArrayList<String>(1000);
        }

        public int length() {
            if (sizeCached) {
                return size;
            }
            size = 0;
            for (String s : bufferList) {
                size += s.length();
            }
            sizeCached = true;
            return size;
        }

        public void clear() {
            bufferList.clear();
        }

        public String last() {
            assert bufferList.size() > 0;
            return bufferList.get(bufferList.size() - 1);
        }

        public LazyStringBuilder append(String str) {
            sizeCached = false;
            bufferList.add(str);
            return this;
        }

        public String at(int index) {
            assert index >= 0 && index < bufferList.size();
            return bufferList.get(index);
        }

        public String toString() {
            StringBuilder sb = new StringBuilder(length());
            for (String cb : bufferList) {
                sb.append(cb);
            }
            return sb.toString();
        }
    }

    /**
     * Enumeration of tokenizer states. QUOTE_MODE is activated between quotes.
     */
    private enum TokenizerState {
        NORMAL, QUOTE_MODE, FINISHED
    }

    /**
     * Constructs a new <tt>Tokenizer</tt>, which reads the CSV file, line by line.
     *
     * @param reader      the reader
     * @param preferences the CSV preferences
     * @throws NullPointerException if reader or preferences is null
     */
    public QuoteTrackingTokenizer(final Reader reader, final CsvPreference preferences, final boolean oneLineRecord){
        this(reader, preferences, oneLineRecord, -1, null);
    }

    /**
     * Constructs a new <tt>Tokenizer</tt>, which reads the CSV file, line by line.
     *
     * @param reader         the reader
     * @param preferences    the CSV preferences
     * @param valueSizeHints the maximum size of each column. this is used to optimize memory footprint of the reader.
     * @throws NullPointerException if reader or preferences is null
     */
    public QuoteTrackingTokenizer(final Reader reader, final CsvPreference preferences, final boolean oneLineRecord,
                                  final long scanThresold, final List<Integer> valueSizeHints) {
        super(reader, preferences);
        this.quoteChar = preferences.getQuoteChar();
        this.delimeterChar = preferences.getDelimiterChar();
        this.surroundingSpacesNeedQuotes = preferences.isSurroundingSpacesNeedQuotes();
        this.ignoreEmptyLines = preferences.isIgnoreEmptyLines();
        this.commentMatcher = preferences.getCommentMatcher();
        this.maxLinesPerRow = preferences.getMaxLinesPerRow();
        this.valueSizeHints = valueSizeHints;
        this.scanThresold = scanThresold;
        this.oneLineRecord = oneLineRecord;
    }

    @Override
    public boolean readColumns(final List<String> columns) throws IOException {
        return readColumns(columns, null);
    }

    private void reset() {
        // clear the reusable List and StringBuilders
        colIdx = 0;
        currentColumn.setLength(0);
        // process each character in the line, catering for surrounding quotes (QUOTE_MODE)
        state = TokenizerState.NORMAL;
        quoteScopeStartingLine = -1;
        potentialSpaces = 0;
        charIndex = 0;
        wasQuoted = false;
    }

    /**
     * Parses the columns from current CSV row.
     *
     * @param columns       output parameter, the resulting set of parsed columns
     * @param quotedColumns output parameter, for each one of the resulting columns: true if the column was quoted, otherwise, false
     * @return true if more rows exists, otherwise false.
     */
    public boolean readColumns(final List<String> columns, final BooleanList quotedColumns) throws IOException {
        if (columns == null) {
            throw new NullPointerException("columns should not be null");
        }
        columns.clear();
        if (quotedColumns != null) quotedColumns.clear();

        reset();
        currentRow.clear();

        // read a line (ignoring empty lines/comments if necessary)
        if (!readFirstLine()) return false; // EOF

        checkExactSizeFirst = false;
        if(oneLineRecord) {
            oneLineRecordLength = getCurrentLine().length();
            currentColumn = new StringBuilder(oneLineRecordLength);
        } else {
            // now, if some column value is larger than scanThreshold we perform the dry run to avoid extra allocation of StringBuilder.
            if (valueSizeHints != null) {
                if (valueSizeHints.stream().max(Integer::compare).get() > scanThresold) {
                    checkExactSizeFirst = true;
                    exactColumnSizes = new int[valueSizeHints.size()];
                }
            }
            if (checkExactSizeFirst) {
                runStateMachine(new ArrayList<>(), null); // will only calculate exact size of each column
                reset();
                checkExactSizeFirst = false; // allow setting the column in the next state machine run instead just counting the characters.
                allocateColumnMemory(); // allocate memory for the first column
                rowIdx = 0; // rollback to start reading the data (again) from currentRow.
            }
        }
        runStateMachine(columns, quotedColumns);
        clearRow();
        return true;
    }

    public void clearRow() {
        currentRow.clear();
    }

    int[] getExactColumnSizes() {
        return exactColumnSizes;
    }

    private void runStateMachine(final List<String> columns, final BooleanList quotedColumns) throws IOException {
        while (true) {
            switch (state) {
                case NORMAL:
                    handleNormalState(columns, quotedColumns);
                    break;
                case QUOTE_MODE:
                    handleQuoteState();
                    break;
                case FINISHED:
                    return;
            }
        }
    }

    private void appendToRowNewLine() {
        if (checkExactSizeFirst) {
            currentRow.append(String.valueOf(NEWLINE));
        } else if (exactColumnSizes != null && !oneLineRecord) {
            rowIdx++; // simulate adding a newline by moving a sequence by one
        } else {
            currentRow.append(String.valueOf(NEWLINE));
        }
    }

    private void appendToRowFromFile() throws IOException {
        if (checkExactSizeFirst) {
            currentRow.append(readLine());
        } else if (exactColumnSizes != null && !oneLineRecord) {
            rowIdx++; // simulate adding a newline by moving a sequence by one
        } else {
            currentRow.append(readLine());
        }
    }

    private void addToColumn(char c) {
        if (checkExactSizeFirst) {
            if (colIdx >= exactColumnSizes.length) {
                // it seems this could happen, still we want to tolerate it and not throw. (see expected behavior in HdfsImportIT::testNullDatesWithMixedCaseAccuracy)
                return;
            }
            exactColumnSizes[colIdx]++;
        } else {
            currentColumn.append(c);
        }
    }

    private void allocateColumnMemory() {
        if (!checkExactSizeFirst) {
            if(oneLineRecord) {
                currentColumn.delete(0, currentColumn.length());
            } else if (exactColumnSizes != null) { // we set the column sizes before, use it to set the SB size correctly
                if(colIdx >= exactColumnSizes.length) {
                    // it seems this could happen, still we want to tolerate it and not throw. (see expected behavior in HdfsImportIT::testNullDatesWithMixedCaseAccuracy)
                    currentColumn.setLength(0);
                } else {
                    if(LOG.isDebugEnabled()) {
                        LOG.debug(String.format("reinit column StringBuilder to exact size of %d", exactColumnSizes[colIdx]));
                    }
                    int maxExactColumnSize = Arrays.stream(exactColumnSizes).max().getAsInt();
                    if(maxExactColumnSize > currentColumn.capacity()) {
                        currentColumn = new StringBuilder(maxExactColumnSize);
                    } else {
                        currentColumn.setLength(0);
                    }
                }
            } else { // we didn't because the column size is too small. Just reset the size and let SB grow exponentially.
                currentColumn.setLength(0);
            }
        }  // else is a no-op, we don't care at this moment about the columns
    }

    private String getCurrentLine() {
        if (checkExactSizeFirst) {
            return currentRow.last();
        } else if (exactColumnSizes != null) {
            return currentRow.at(rowIdx);
        } else {
            return currentRow.last();
        }
    }

    private boolean readFirstLine() throws IOException {
        String line;
        do {
            line = readLine();
            if (line == null) {
                return false;
            }
        }
        while (ignoreEmptyLines && line.isEmpty() || (commentMatcher != null && commentMatcher.isComment(line)));
        currentRow.append(line);
        return true;
    }

    private void handleNormalState(final List<String> columns, final BooleanList quotedColumns) {
        if (charIndex == getCurrentLine().length()) { // Newline. Add any required spaces (if surrounding spaces don't need quotes) and return (we've read a line!).
            finishCol(columns, quotedColumns);
            state = TokenizerState.FINISHED;
        } else {
            final char c = getCurrentLine().charAt(charIndex);
            if (c == delimeterChar) { // Delimiter. Save the column (trim trailing space if required) then continue to next character.
                finishCol(columns, quotedColumns);
                potentialSpaces = 0;
                allocateColumnMemory();
            } else if (c == SPACE) { // Space. Remember it, then continue to next character.
                potentialSpaces++;
            } else if (c == quoteChar) { // A single quote ("). Update to QUOTESCOPE (but don't save quote), then continue to next character.
                state = TokenizerState.QUOTE_MODE;
                wasQuoted = true;
                quoteScopeStartingLine = getLineNumber();
                // cater for spaces before a quoted section (be lenient!)
                fillSpaces();
            } else { // Just a normal character. Add any required spaces (but trim any leading spaces if surrounding
                // spaces need quotes), add the character, then continue to next character.
                fillSpaces();
                addToColumn(c);
            }
        }
        charIndex++; // read next char of the line
    }

    private void handleQuoteState() throws IOException {
        if (charIndex == getCurrentLine().length()) { // Newline. Doesn't count as newline while in QUOTESCOPE. Add the newline char, reset the charIndex
            // (will update to 0 for next iteration), read in the next line, then continue to next character)
            addToColumn(NEWLINE);
            appendToRowNewLine(); // specific line terminator lost, \n will have to suffice
            charIndex = 0;
            checkLineErrors(quoteScopeStartingLine);
            appendToRowFromFile();
            if (getCurrentLine() == null) {
                String errorMessage = checkExactSizeFirst
                        ? String.format("partial record found while scanning row at line %d and ending on line %d", quoteScopeStartingLine, getLineNumber())
                        : String.format("partial record found [%s] while reading quoted column beginning on line %d and ending on line %d",
                        this.currentColumn, quoteScopeStartingLine, getLineNumber());
                throw new SuperCsvException(errorMessage);
            }
            if (oneLineRecord) {
                String errorMessage = String.format("one-line record CSV has a record that spans over multiple lines at line %d", quoteScopeStartingLine);
                throw new SuperCsvException(errorMessage);
            }
        } else { // QUOTE_MODE (within quotes).
            final char c = getCurrentLine().charAt(charIndex);
            if (c == quoteChar) {
                int nextCharIndex = charIndex + 1;
                boolean availableCharacters = nextCharIndex < getCurrentLine().length();
                boolean nextCharIsQuote = availableCharacters && getCurrentLine().charAt(nextCharIndex) == quoteChar;
                if (nextCharIsQuote) { // An escaped quote (""). Add a single quote, then move the cursor so the next iteration of the
                    // loop will read the character following the escaped quote.
                    addToColumn(c);
                    charIndex++;
                } else { // A single quote ("). Update to NORMAL (but don't save quote), then continue to next character.
                    state = TokenizerState.NORMAL;
                    quoteScopeStartingLine = -1; // reset ready for next multi-line cell
                }
            } else { // Just a normal character, delimiter (they don't count in QUOTESCOPE) or space. Add the character,
                // then continue to next character.
                addToColumn(c);
            }
            charIndex++; // read next char of the line
        }
    }

    private void fillSpaces() {
        if (!surroundingSpacesNeedQuotes || currentColumn.length() > 0) {
            for (int i = 0; i < potentialSpaces; i++) {
                currentColumn.append(SPACE);
            }
        }
        potentialSpaces = 0;
    }

    private void finishCol(List<String> columns, BooleanList quotedColumns) {
        if (!surroundingSpacesNeedQuotes) {
            for (int i = 0; i < potentialSpaces; i++) {
                currentColumn.append(SPACE);
            }
        }
        assert columns != null;
        columns.add(currentColumn.length() > 0 ? currentColumn.toString() : null); // "" -> null
        if (quotedColumns != null) {
            quotedColumns.add(wasQuoted);
        }
        colIdx++;
        wasQuoted = false;
    }

    private void checkLineErrors(int quoteScopeStartingLine) {
        if (maxLinesPerRow > 0 && getLineNumber() - quoteScopeStartingLine + 1 >= maxLinesPerRow) {
            /*
             * The quoted section that is being parsed spans too many lines, so to avoid excessive memory
             * usage parsing something that is probably human error anyways, throw an exception. If each
             * row is supposed to be a single line and this has been exceeded, throw a more descriptive
             * exception.
             */
            String msg = maxLinesPerRow == 1 ?
                    String.format("unexpected end of line while reading quoted column on line %d", getLineNumber()) :
                    String.format("max number of lines to read exceeded while reading quoted column" +
                            " beginning on line %d and ending on line %d", quoteScopeStartingLine, getLineNumber());
            throw new SuperCsvException(msg);
        }
    }

    void setScanThreshold(long scanThreshold) {
        this.scanThresold = scanThreshold;
    }

    /**
     * {@inheritDoc}
     */
    public String getUntokenizedRow() {
        return currentRow.toString();
    }

}
