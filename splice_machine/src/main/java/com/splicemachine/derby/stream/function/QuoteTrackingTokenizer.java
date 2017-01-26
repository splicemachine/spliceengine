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
import org.supercsv.comment.CommentMatcher;
import org.supercsv.exception.SuperCsvException;
import org.supercsv.io.AbstractTokenizer;
import org.supercsv.prefs.CsvPreference;

import java.io.IOException;
import java.io.Reader;
import java.util.List;

/**
 * A Tokenizer which keeps track of which columns were quoted and which were not.
 * This allows us to make a distinction between columns with value {@code Foo}
 * versus columns with values {@code "Foo"}.
 *
 * @author Scott Fines
 *         Date: 9/29/16
 */
public class QuoteTrackingTokenizer extends AbstractTokenizer{

    private static final char NEWLINE='\n';

    private static final char SPACE=' ';

    private final StringBuilder currentColumn=new StringBuilder();

    /* the raw, untokenized CSV row (may span multiple lines) */
    private final StringBuilder currentRow=new StringBuilder();

    private final int quoteChar;

    private final int delimeterChar;

    private final boolean surroundingSpacesNeedQuotes;

    private final boolean ignoreEmptyLines;

    private final CommentMatcher commentMatcher;

    private final int maxLinesPerRow;

    /**
     * Enumeration of tokenizer states. QUOTE_MODE is activated between quotes.
     */
    private enum TokenizerState{
        NORMAL,QUOTE_MODE;
    }

    /**
     * Constructs a new <tt>Tokenizer</tt>, which reads the CSV file, line by line.
     *
     * @param reader      the reader
     * @param preferences the CSV preferences
     * @throws NullPointerException if reader or preferences is null
     */
    public QuoteTrackingTokenizer(final Reader reader,final CsvPreference preferences){
        super(reader,preferences);
        this.quoteChar=preferences.getQuoteChar();
        this.delimeterChar=preferences.getDelimiterChar();
        this.surroundingSpacesNeedQuotes=preferences.isSurroundingSpacesNeedQuotes();
        this.ignoreEmptyLines=preferences.isIgnoreEmptyLines();
        this.commentMatcher=preferences.getCommentMatcher();
        this.maxLinesPerRow=preferences.getMaxLinesPerRow();
    }

    @Override
    public boolean readColumns(final List<String> columns) throws IOException{
        return readColumns(columns,null);
    }

    public boolean readColumns(final List<String> columns,final BooleanList quotedColumns) throws IOException{
        if(columns==null){
            throw new NullPointerException("columns should not be null");
        }
        boolean recordQuotes = quotedColumns!=null;

        // clear the reusable List and StringBuilders
        columns.clear();
        if(recordQuotes) quotedColumns.clear();
        currentColumn.setLength(0);
        currentRow.setLength(0);

        // read a line (ignoring empty lines/comments if necessary)
        String line;
        do{
            line=readLine();
            if(line==null){
                return false; // EOF
            }
        }
        while(ignoreEmptyLines && line.length()==0 || (commentMatcher!=null && commentMatcher.isComment(line)));

        // update the untokenized CSV row
        currentRow.append(line);

        // process each character in the line, catering for surrounding quotes (QUOTE_MODE)
        TokenizerState state=TokenizerState.NORMAL;
        int quoteScopeStartingLine=-1; // the line number where a potential multi-line cell starts
        int potentialSpaces=0; // keep track of spaces (so leading/trailing space can be removed if required)
        int charIndex=0;
        boolean wasQuoted = false;
        while(true){
            boolean endOfLineReached=charIndex==line.length();

            if(endOfLineReached){
                if(TokenizerState.NORMAL.equals(state)){
                    /*
					 * Newline. Add any required spaces (if surrounding spaces don't need quotes) and return (we've read
					 * a line!).
					 */
                    if(!surroundingSpacesNeedQuotes){
                        appendSpaces(currentColumn,potentialSpaces);
                    }
                    columns.add(currentColumn.length()>0?currentColumn.toString():null); // "" -> null
                    if(recordQuotes) quotedColumns.add(wasQuoted);
                    wasQuoted = false;
                    return true;
                }else{
					/*
					 * Newline. Doesn't count as newline while in QUOTESCOPE. Add the newline char, reset the charIndex
					 * (will update to 0 for next iteration), read in the next line, then then continue to next
					 * character.
					 */
                    currentColumn.append(NEWLINE);
                    currentRow.append(NEWLINE); // specific line terminator lost, \n will have to suffice

                    charIndex=0;

                    if(maxLinesPerRow>0 && getLineNumber()-quoteScopeStartingLine+1>=maxLinesPerRow){
						/*
						 * The quoted section that is being parsed spans too many lines, so to avoid excessive memory
						 * usage parsing something that is probably human error anyways, throw an exception. If each
						 * row is suppose to be a single line and this has been exceeded, throw a more descriptive
						 * exception
						 */
                        String msg=maxLinesPerRow==1?
                                String.format("unexpected end of line while reading quoted column on line %d",
                                        getLineNumber()):
                                String.format("max number of lines to read exceeded while reading quoted column"+
                                                " beginning on line %d and ending on line %d",
                                        quoteScopeStartingLine,getLineNumber());
                        throw new SuperCsvException(msg);
                    }else if((line=readLine())==null){
                        throw new SuperCsvException(
                                String.format(
                                                "unexpected end of file while reading quoted column beginning on line %d and ending on line %d",
                                                quoteScopeStartingLine,getLineNumber()));
                    }

                    currentRow.append(line); // update untokenized CSV row

                    if(line.length()==0){
                        // consecutive newlines
                        continue;
                    }
                }
            }

            final char c=line.charAt(charIndex);

            if(TokenizerState.NORMAL.equals(state)){
                if(c==delimeterChar){
					/*
					 * Delimiter. Save the column (trim trailing space if required) then continue to next character.
					 */
                    if(!surroundingSpacesNeedQuotes){
                        appendSpaces(currentColumn,potentialSpaces);
                    }
                    columns.add(currentColumn.length()>0?currentColumn.toString():null); // "" -> null
                    potentialSpaces=0;
                    currentColumn.setLength(0);
                    if(recordQuotes){
                        quotedColumns.append(wasQuoted);
                    }
                    wasQuoted = false;
                }else if(c==SPACE){
					/*
					 * Space. Remember it, then continue to next character.
					 */
                    potentialSpaces++;

                }else if(c==quoteChar){
					/*
					 * A single quote ("). Update to QUOTESCOPE (but don't save quote), then continue to next character.
					 */
                    state=TokenizerState.QUOTE_MODE;
                    wasQuoted = true;
                    quoteScopeStartingLine=getLineNumber();

                    // cater for spaces before a quoted section (be lenient!)
                    if(!surroundingSpacesNeedQuotes || currentColumn.length()>0){
                        appendSpaces(currentColumn,potentialSpaces);
                    }
                    potentialSpaces=0;

                }else{
					/*
					 * Just a normal character. Add any required spaces (but trim any leading spaces if surrounding
					 * spaces need quotes), add the character, then continue to next character.
					 */
                    if(!surroundingSpacesNeedQuotes || currentColumn.length()>0){
                        appendSpaces(currentColumn,potentialSpaces);
                    }

                    potentialSpaces=0;
                    currentColumn.append(c);
                }

            }else{
				/*
				 * QUOTE_MODE (within quotes).
				 */
                if(c==quoteChar){
                    int nextCharIndex=charIndex+1;
                    boolean availableCharacters=nextCharIndex<line.length();
                    boolean nextCharIsQuote=availableCharacters && line.charAt(nextCharIndex)==quoteChar;
                    if(nextCharIsQuote){
						/*
						 * An escaped quote (""). Add a single quote, then move the cursor so the next iteration of the
						 * loop will read the character following the escaped quote.
						 */
                        currentColumn.append(c);
                        charIndex++;

                    }else{
						/*
						 * A single quote ("). Update to NORMAL (but don't save quote), then continue to next character.
						 */
                        state=TokenizerState.NORMAL;
                        quoteScopeStartingLine=-1; // reset ready for next multi-line cell
                    }
                }else{
					/*
					 * Just a normal character, delimiter (they don't count in QUOTESCOPE) or space. Add the character,
					 * then continue to next character.
					 */
                    currentColumn.append(c);
                }
            }

            charIndex++; // read next char of the line
        }
    }

    /**
     * Appends the required number of spaces to the StringBuilder.
     *
     * @param sb     the StringBuilder
     * @param spaces the required number of spaces to append
     */
    private static void appendSpaces(final StringBuilder sb,final int spaces){
        for(int i=0;i<spaces;i++){
            sb.append(SPACE);
        }
    }

    /**
     * {@inheritDoc}
     */
    public String getUntokenizedRow(){
        return currentRow.toString();
    }

}
