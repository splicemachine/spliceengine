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
import org.supercsv.comment.CommentMatcher;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.AbstractTokenizer;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.Reader;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collections;
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

    private final List<Integer> valueSizeHints;
    private int potentialSpaces;
    private int charIndex;
    private boolean wasQuoted;
    private int quoteScopeStartingLine;

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
            return size;
        }

        public void clear() {
            bufferList.clear();
        }

        public LazyStringBuilder append(String str) {
            sizeCached = false;
            bufferList.add(str);
            return this;
        }

        public String toString() {
            String result = new String(new char[length()]);
            try {
                Field valueField = String.class.getDeclaredField("value");
                valueField.setAccessible(true);
                char[] value = (char[]) valueField.get(result);
                int needle = 0;
                for (String cb : bufferList) {
                    System.arraycopy(cb.toCharArray(), 0, value, needle, cb.length());
                    needle += cb.length();
                }
            } catch (NoSuchFieldException | SecurityException | IllegalArgumentException | IllegalAccessException e) {
                e.printStackTrace(); // improve on this.
            }
            return result;
        }
    }

    private static final char NEWLINE = '\n';

    private static final char SPACE = ' ';

    private StringBuilder currentColumn = new StringBuilder();

    /* the raw, untokenized CSV row (may span multiple lines) */
    private final LazyStringBuilder currentRow = new LazyStringBuilder();

    private final int quoteChar;

    private final int delimeterChar;

    private final boolean surroundingSpacesNeedQuotes;

    private final boolean ignoreEmptyLines;

    private final CommentMatcher commentMatcher;

    private final int maxLinesPerRow;

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
    public QuoteTrackingTokenizer(final Reader reader, final CsvPreference preferences) {
        super(reader, preferences);
        this.quoteChar = preferences.getQuoteChar();
        this.delimeterChar = preferences.getDelimiterChar();
        this.surroundingSpacesNeedQuotes = preferences.isSurroundingSpacesNeedQuotes();
        this.ignoreEmptyLines = preferences.isIgnoreEmptyLines();
        this.commentMatcher = preferences.getCommentMatcher();
        this.maxLinesPerRow = preferences.getMaxLinesPerRow();
        valueSizeHints = null;
    }

    public QuoteTrackingTokenizer(final Reader reader, final CsvPreference preferences, final List<Integer> valueSizeHints) {
        super(reader, preferences);
        this.quoteChar = preferences.getQuoteChar();
        this.delimeterChar = preferences.getDelimiterChar();
        this.surroundingSpacesNeedQuotes = preferences.isSurroundingSpacesNeedQuotes();
        this.ignoreEmptyLines = preferences.isIgnoreEmptyLines();
        this.commentMatcher = preferences.getCommentMatcher();
        this.maxLinesPerRow = preferences.getMaxLinesPerRow();
        this.valueSizeHints = valueSizeHints;
    }

    private void init() {

    }

    @Override
    public boolean readColumns(final List<String> columns) throws IOException {
        return readColumns(columns, null);
    }

    private void addSpacesToCol(int count) {
        if (!surroundingSpacesNeedQuotes) {
            appendSpaces(currentColumn, count);
        }
    }

    private void saveCol(final List<String> outColumns) {
        assert outColumns != null;
        outColumns.add(currentColumn.length() > 0 ? currentColumn.toString() : null); // "" -> null
    }

    public boolean readColumns(final List<String> columns, final BooleanList quotedColumns) throws IOException {
        if (columns == null) {
            throw new NullPointerException("columns should not be null");
        }
        boolean recordQuotes = quotedColumns != null;

        // clear the reusable List and StringBuilders
        columns.clear();
        int colIdx = 0;
        if (recordQuotes) quotedColumns.clear();
        currentColumn.setLength(0);
        currentRow.clear();

        // read a line (ignoring empty lines/comments if necessary)
        String line;
        do {
            line = readLine();
            if (line == null) {
                return false; // EOF
            }
        }
        while (ignoreEmptyLines && line.isEmpty() || (commentMatcher != null && commentMatcher.isComment(line)));

        // update the untokenized CSV row
        currentRow.append(line);

        // process each character in the line, catering for surrounding quotes (QUOTE_MODE)
        TokenizerState state = TokenizerState.NORMAL;
        // the line number where a potential multi-line cell starts
        quoteScopeStartingLine = -1;
        // keep track of spaces (so leading/trailing space can be removed if required)
        potentialSpaces = 0;
        charIndex = 0;
        wasQuoted = false;
        while (true) {
            boolean endOfLineReached = charIndex == line.length();
            switch (state) {
                case NORMAL:
                    if (endOfLineReached) { // Newline. Add any required spaces (if surrounding spaces don't need quotes) and return (we've read
                        finishCol(columns, quotedColumns, colIdx);
                        return true;
                    } else {
                        final char c = line.charAt(charIndex);
                        if (c == delimeterChar) { // Delimiter. Save the column (trim trailing space if required) then continue to next character.
                            finishCol(columns, quotedColumns, colIdx);
                            potentialSpaces = 0;
                            currentColumn.setLength(0);
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
                            currentColumn.append(String.valueOf(c));
                        }
                    }
                    break;
                case QUOTE_MODE:
                    if (endOfLineReached) { // Newline. Doesn't count as newline while in QUOTESCOPE. Add the newline char, reset the charIndex
                                            // (will update to 0 for next iteration), read in the next line, then then continue to next character)
                        currentColumn.append(String.valueOf(NEWLINE));
                        currentRow.append(String.valueOf(NEWLINE)); // specific line terminator lost, \n will have to suffice
                        charIndex = 0;
                        checkLineErrors(quoteScopeStartingLine);
                        if ((line = readLine()) == null) {
                            throw new SuperCsvException(
                                    String.format(
                                            "partial record found [%s] while reading quoted column beginning on line %d and ending on line %d",
                                            this.currentColumn, quoteScopeStartingLine, getLineNumber()));
                        }
                        currentRow.append(line); // update untokenized CSV row
                        if (line.isEmpty()) {
                            continue; // consecutive empty line
                        }
                    } else {
                        final char c = line.charAt(charIndex);
                        if (c == quoteChar) {
                            int nextCharIndex = charIndex + 1;
                            boolean availableCharacters = nextCharIndex < line.length();
                            boolean nextCharIsQuote = availableCharacters && line.charAt(nextCharIndex) == quoteChar;
                            if (nextCharIsQuote) { // An escaped quote (""). Add a single quote, then move the cursor so the next iteration of the
                                                   // loop will read the character following the escaped quote.
                                currentColumn.append(String.valueOf(c));
                                charIndex++;

                            } else { // A single quote ("). Update to NORMAL (but don't save quote), then continue to next character.
                                state = TokenizerState.NORMAL;
                                quoteScopeStartingLine = -1; // reset ready for next multi-line cell
                            }
                        } else { // Just a normal character, delimiter (they don't count in QUOTESCOPE) or space. Add the character,
                                 // then continue to next character.
                            currentColumn.append(String.valueOf(c));
                        }
                    }
                case FINISHED:
                    return true;
            }
            charIndex++; // read next char of the line
        }
    }

    private void fillSpaces() {
        if (!surroundingSpacesNeedQuotes || currentColumn.length() > 0) {
            appendSpaces(currentColumn, potentialSpaces);
        }
        potentialSpaces = 0;
    }

    private void finishCol(List<String> columns, BooleanList quotedColumns, int colIdx) {
        addSpacesToCol(potentialSpaces);
        saveCol(columns);
        recordQuotes(wasQuoted, quotedColumns);
        colIdx++;
        wasQuoted = false;
    }

    private void checkLineErrors(int quoteScopeStartingLine) {
        if (maxLinesPerRow > 0 && getLineNumber() - quoteScopeStartingLine + 1 >= maxLinesPerRow) {
            /*
             * The quoted section that is being parsed spans too many lines, so to avoid excessive memory
             * usage parsing something that is probably human error anyways, throw an exception. If each
             * row is suppose to be a single line and this has been exceeded, throw a more descriptive
             * exception
             */
            String msg = maxLinesPerRow == 1 ?
                    String.format("unexpected end of line while reading quoted column on line %d",
                            getLineNumber()) :
                    String.format("max number of lines to read exceeded while reading quoted column" +
                                    " beginning on line %d and ending on line %d",
                            quoteScopeStartingLine, getLineNumber());
            throw new SuperCsvException(msg);
        }
    }

    private void recordQuotes(boolean wasQuoted, BooleanList quotedColumns) {
        if (quotedColumns != null) {
            quotedColumns.add(wasQuoted);
        }
    }

    /**
     * Appends the required number of spaces to the StringBuilder.
     *
     * @param sb    the StringBuilder
     * @param count the required number of spaces to append
     */
    private static void appendSpaces(final LazyStringBuilder sb, final int count) {
        String spaces = String.join("", Collections.nCopies(count, " "));
        sb.append(spaces);
    }

    private static void appendSpaces(final StringBuilder sb, final int spaces) {
        for (int i = 0; i < spaces; i++) {
            sb.append(SPACE);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String getUntokenizedRow() {
        return currentRow.toString();
    }

}
