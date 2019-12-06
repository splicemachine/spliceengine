/*
 * This file is part of Splice Machine.
 * Splice Machine is free software: you can redistribute it and/or modify it under the terms of the
 * GNU Affero General Public License as published by the Free Software Foundation, either
 * version 3, or (at your option) any later version.
 * Splice Machine is distributed in the hope that it will be useful, but WITHOUT ANY WARRANTY;
 * without even the implied warranty of MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.
 * See the GNU Affero General Public License for more details.
 * You should have received a copy of the GNU Affero General Public License along with Splice Machine.
 * If not, see <http://www.gnu.org/licenses/>.
 *
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
 * Apache Derby is a subproject of the Apache DB project, and is licensed under
 * the Apache License, Version 2.0 (the "License"); you may not use these files
 * except in compliance with the License. You may obtain a copy of the License at:
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2019 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.tools.ij;

import com.splicemachine.db.iapi.tools.i18n.LocalizedOutput;
import com.splicemachine.db.iapi.tools.i18n.LocalizedInput;

import java.io.IOException;
import java.io.Reader;

/**
	StatementGrabber looks through an input stream for
	the next JSQL statement.  A statement is considered to
	be any tokens up to the next semicolon or EOF.
	<p>
	Semicolons inside comments, strings, and delimited identifiers
	are not considered to be statement terminators but to be
	part of those tokens.
	<p>
    Comments currently recognized include the SQL comment,
    which begins with "--" and ends at the next EOL, and nested
    bracketed comments.
	<p>
	Strings and delimited identifiers are permitted to contain
	newlines; the actual IJ or JSQL parsers will report errors when
	those cases occur.
	<p>
	There are no escaped characters, i.e. "\n" is considered to
	be two characters, '\' and 'n'.

 */

public class StatementFinder {

	private Reader source; 
	private StringBuffer statement = new StringBuffer();
	private int state;
	private boolean atEOF = false;
	private boolean peekEOF = false;
	private char peekChar;
	private boolean peeked = false;
	private LocalizedOutput promptwriter;
	private boolean doPrompt;
	private boolean continuedStatement;

	// state variables
	private static final int IN_STATEMENT = 0;
	private static final int IN_STRING = 1;
	private static final int IN_SQLCOMMENT = 2;
	private static final int END_OF_STATEMENT = 3;
	private static final int END_OF_INPUT = 4;

	// special state-changing characters
	private static final char MINUS = '-';
	private static final char SINGLEQUOTE = '\'';
	private static final char DOUBLEQUOTE = '\"';
	private static final char SEMICOLON = ';';
	private static final char NEWLINE = '\n';
	private static final char RETURN = '\r';
	private static final char SPACE = ' ';
	private static final char TAB = '\t';
	private static final char FORMFEED = '\f';
	private static final char SLASH = '/';
	private static final char ASTERISK = '*';


	private static final char LITTLE_A = 'a';
	private static final char LITTLE_T = 't';
	private static final char LITTLE_O = 'o';
	private static final char LITTLE_M = 'm';
	private static final char LITTLE_I = 'i';
	private static final char LITTLE_C = 'c';

	private static final char LITTLE_E = 'e';
	private static final char LITTLE_N = 'n';
	private static final char LITTLE_D = 'd';

	private static final char BIG_A = 'A';
	private static final char BIG_T = 'T';
	private static final char BIG_O = 'O';
	private static final char BIG_M = 'M';
	private static final char BIG_I = 'I';
	private static final char BIG_C = 'C';

	private static final char BIG_E = 'E';
	private static final char BIG_N = 'N';
	private static final char BIG_D = 'D';


	// state variables
	private static final int NOT_IN_BEGIN_BLOCK = 0;
	private static final int IN_BEGIN_BLOCK = 1;
	private static final int IN_BEGIN_BLOCK_SEMICOLON_SEEN = 2;

	// trigger parsing state
        private static final int WHITESPACE_SEEN = 0;
        private static final int E_SEEN = 1;
        private static final int EN_SEEN = 2;

        private static final int A_SEEN = 3;
        private static final int AT_SEEN = 4;
	private static final int ATO_SEEN = 5;
	private static final int ATOM_SEEN = 6;
	private static final int ATOMI_SEEN = 7;
        private static final int NOTHING_SEEN = 8;


	/**
		The constructor does not assume the stream is data input
		or buffered, so it will wrap it appropriately.

		If the StatementFinder's input stream is connected to
		System.in, a LocalizedOutput stream may be given to print
		line continuation prompts when StatementFinder reads a newline.

		@param s the input stream for reading statements from.
		@param promptDest LocalizedOutput stream to write line
						continuation prompts ("> ") to. If null,
						no such prompts will be written.
	 */
	public StatementFinder(LocalizedInput s, LocalizedOutput promptDest) {
		source = s;
		if(promptDest != null && s.isStandardInput()) {
			promptwriter = promptDest;
			doPrompt = true;
		} else {
			doPrompt = false;
		}
	}

	/**
		Reinit is used to redirect the finder to another stream.
		The previous stream should not have been in a PEEK state.

		If an output stream was given when constructing this 
		StatementFinder and the input is standard input, 
		continuation prompting will be enabled.

		@param s the input stream for reading statements from.
	 */
	public void ReInit(LocalizedInput s) {
	    try {
			source.close();
		} catch (IOException ioe) {
			// just be quiet if it is already gone
		}
		source = s;
		state = IN_STATEMENT;
		atEOF = false;
		peekEOF = false;
		peeked = false;
		doPrompt = s.isStandardInput() && promptwriter != null;
	}

	public void close() throws IOException {
		source.close();
	}

	/**
		get the next statement in the input stream. Returns it,
		dropping its closing semicolon if it has one. If there is
		no next statement, return a null.

		@return the next statement in the input stream.
	 */
	public String nextStatement() {
		boolean haveSemi = false;
		char nextChar;
		int triggerState = NOT_IN_BEGIN_BLOCK;
		int triggerParseState = WHITESPACE_SEEN;

		// initialize fields for getting the next statement
		statement.setLength(0);
		if (state == END_OF_INPUT) return null;

		state = IN_STATEMENT;

		// skip leading whitespace
		nextChar = peekChar();
		if (peekEOF()) {
			state = END_OF_INPUT;
			return null;
		}
		if (whiteSpace(nextChar)) {
			while (whiteSpace(peekChar()) && ! peekEOF());
			if (peekEOF()) {
				state = END_OF_INPUT;
				return null;
			}
		}

		while (state != END_OF_STATEMENT && state != END_OF_INPUT) {

			// get the next character from the input
			nextChar = readChar();
			if (atEOF()) {
				state = END_OF_INPUT;
				break;
			}
			
			if (!(nextChar == MINUS))
				continuedStatement=true;

			switch(nextChar) {
				case SPACE:
				case TAB:
				case FORMFEED:
					triggerParseState = WHITESPACE_SEEN;
					break;
				case LITTLE_A:
				case BIG_A:
					if (triggerParseState == WHITESPACE_SEEN)
						triggerParseState = A_SEEN;
					else
						triggerParseState = NOTHING_SEEN;
					break;
				case LITTLE_T:
				case BIG_T:
					if (triggerParseState == A_SEEN)
						triggerParseState = AT_SEEN;
					else
						triggerParseState = NOTHING_SEEN;
					break;
				case LITTLE_O:
				case BIG_O:
					if (triggerParseState == AT_SEEN)
						triggerParseState = ATO_SEEN;
					else
						triggerParseState = NOTHING_SEEN;
					break;
				case LITTLE_M:
				case BIG_M:
					if (triggerParseState == ATO_SEEN)
						triggerParseState = ATOM_SEEN;
					else
						triggerParseState = NOTHING_SEEN;
					break;
				case LITTLE_I:
				case BIG_I:
					if (triggerParseState == ATOM_SEEN)
						triggerParseState = ATOMI_SEEN;
					else
						triggerParseState = NOTHING_SEEN;
					break;
				case LITTLE_C:
				case BIG_C:
					if (triggerParseState == ATOMI_SEEN)
						triggerState = IN_BEGIN_BLOCK;
					triggerParseState = NOTHING_SEEN;
					break;
				case LITTLE_E:
				case BIG_E:
					if (triggerState == IN_BEGIN_BLOCK_SEMICOLON_SEEN && triggerParseState == WHITESPACE_SEEN)
						triggerParseState = E_SEEN;
					else
						triggerParseState = NOTHING_SEEN;
					break;
				case LITTLE_N:
				case BIG_N:
					if (triggerState == IN_BEGIN_BLOCK_SEMICOLON_SEEN && triggerParseState == E_SEEN)
						triggerParseState = EN_SEEN;
					else
						triggerParseState = NOTHING_SEEN;
					break;
				case LITTLE_D:
				case BIG_D:
					if (triggerState == IN_BEGIN_BLOCK_SEMICOLON_SEEN && triggerParseState == EN_SEEN) {
						triggerState = NOT_IN_BEGIN_BLOCK;
					}
					triggerParseState = NOTHING_SEEN;
					break;
				case MINUS:
					readSingleLineComment(nextChar);
					triggerParseState = WHITESPACE_SEEN;
					break;
				case SLASH:
					readBracketedComment();
					triggerParseState = WHITESPACE_SEEN;
					break;
				case SINGLEQUOTE:
				case DOUBLEQUOTE:
					readString(nextChar);
					triggerParseState = NOTHING_SEEN;
					break;
				case SEMICOLON:
					if (triggerState == NOT_IN_BEGIN_BLOCK || peekChar() == SEMICOLON) {
						haveSemi = true;
						state = END_OF_STATEMENT;
						continuedStatement = false;
					}
					if (triggerState == IN_BEGIN_BLOCK)
						triggerState = IN_BEGIN_BLOCK_SEMICOLON_SEEN;
					triggerParseState = WHITESPACE_SEEN;
					break;
				case NEWLINE:
				case RETURN:
					if(doPrompt) {
						utilMain.doPrompt(false, promptwriter, "");
						/* If the next character is a newline as well,
						   we swallow it to avoid double prompting on
						   Windows. */
						if(nextChar == RETURN && peekChar() == NEWLINE) {
							readChar();
						}
					}
					triggerParseState = WHITESPACE_SEEN;
					break;
				default:
					triggerParseState = NOTHING_SEEN;
					// keep going, just a normal character
					break;
			}
		}

		if (haveSemi)
			statement.setLength(statement.length()-1);
		return statement.toString();
	}

	/**
		Determine if the given character is considered whitespace

		@param c the character to consider
		@return true if the character is whitespace
	 */
	private boolean whiteSpace(char c) {
		return (c == SPACE ||
		    	c == TAB ||
		    	c == RETURN ||
		    	c == NEWLINE ||
		    	c == FORMFEED);
	}

	/**
	 	* Advance the source stream to the end of a comment
		* if it is on one, assuming the first character of
		* a potential bracketed comment has been found.
		* If it is not a comment, do not advance the stream.
	 */
	private void readBracketedComment() {
		char nextChar = peekChar();

		// if next char is EOF, we are done.
		if (peekEOF()) return;

		// if nextChar is not an asterisk, then not a comment.
		if (nextChar != ASTERISK)
		{
			continuedStatement = true;
			return;
		}

		// we are really in a comment
		readChar(); // grab the asterisk for real.

		int nestingLevel = 1;

		while (true) {
			nextChar = readChar();

			if (atEOF()) {
				// let the caller process the EOF, don't read it
				state = IN_STATEMENT;
				return;
			}

			char peek = peekChar();

			if (nextChar == SLASH && peek == ASTERISK) {
				readChar();
				nestingLevel++;
			} else if (nextChar == ASTERISK && peek == SLASH) {
				readChar();
				nestingLevel--;
				if (nestingLevel == 0) {
					state = IN_STATEMENT;
					return;
				}
			} else if (nextChar == NEWLINE || nextChar == RETURN) {
				if (doPrompt) {
					utilMain.doPrompt(false, promptwriter, "");
					// If the next character is a NEWLINE, we process
					// it as well to account for Windows CRLFs.
					if (nextChar == RETURN && peek == NEWLINE) {
						readChar();
					}
				}
			}
		}
	}

	/**
		Advance the source stream to the end of a comment if it
		is on one, assuming the first character of
		a potential single line comment has been found.
		If it is not a comment, do not advance the stream.
		<p>
		The form of a single line comment is, in regexp, XX.*$,
		where XX is two instances of commentChar.

		@param commentChar the character whose duplication signifies
			the start of the comment.
	 */
	private void readSingleLineComment(char commentChar) {
		char nextChar;

		nextChar = peekChar();
		// if next char is EOF, we are done.
		if (peekEOF()) return;

		// if nextChar is not a minus, it was just a normal minus,
		// nothing special to do
		if (nextChar != commentChar)
		{
			continuedStatement=true;
			return;
		}

		// we are really in a comment
		readChar(); // grab the minus for real.

		state = IN_SQLCOMMENT;
		do {
			nextChar = peekChar();
			if (peekEOF()) {
				// let the caller process the EOF, don't read it
				state = IN_STATEMENT;
				return;
			}
			switch (nextChar) {
				case NEWLINE:
				case RETURN:
					readChar(); // okay to process the character
					state = IN_STATEMENT;
					if (doPrompt){
						// If we had previously already started a statement,
						// add the prompt.
						// Otherwise, consider this a single line comment,
						// and the next line should not get a prompt
						if (continuedStatement)
							utilMain.doPrompt(false, promptwriter, "");
                        else
                            utilMain.doPrompt(true, promptwriter, "");
					    
						/* If the next character is a NEWLINE, we process
						 *  it as well to account for Windows CRLFs. */
						if(nextChar == RETURN && peekChar() == NEWLINE) {
							readChar();
						}
					}
				return;
				default:
					readChar(); // process the character, still in comment
					break;
			}
		} while (state == IN_SQLCOMMENT); // could be while true...
	}

	/**
		Advance the stream to the end of the string.
		Assumes the opening delimiter of the string has been read.
		This handles the SQL ability to put the delimiter within
		the string by doubling it, by reading those as two strings
		sitting next to one another.  I.e, 'Mary''s lamb' is read
		by this class as two strings, 'Mary' and 's lamb'.
		<p>
		The delimiter of the string is expected to be repeated at
		its other end. If the other flavor of delimiter occurs within
		the string, it is just a normal character within it.
		<p>
		All characters except the delimiter are permitted within the
		string. If EOF is hit before the closing delimiter is found,
		the end of the string is assumed. Parsers using this parser
		will detect the error in that case and return appropriate messages.

		@param stringDelimiter the starting and ending character
			for the string being read.
	 */
	private void readString(char stringDelimiter) {
		state = IN_STRING;
		do {
			char nextChar = readChar();

			if (atEOF()) {
				state = END_OF_INPUT;
				return;
			}

			if (nextChar == stringDelimiter) {
				// we've reached the end of the string
				state = IN_STATEMENT;
				return;
			}

			// still in string
		} while (state == IN_STRING); // could be while true...
	}

	private boolean atEOF() {
		return atEOF;
	}

	private boolean peekEOF() {
		return peekEOF;
	}

	/**
		return the next character in the source stream and
		append it to the statement buffer.

		@return the next character in the source stream.
	 */
	private char readChar() {
		if (!peeked) peekChar();

		peeked = false;
		atEOF = peekEOF;

		if (!atEOF) statement.append(peekChar);

		return peekChar;
	}

	/**
		return the next character in the source stream, without
		advancing.

		@return the next character in the source stream.
	 */
	private char peekChar() {
		peeked = true;
		char c = '\00';

		try {
		    int cInt;

			// REMIND: this is assuming a flat ascii source file.
			// will need to beef it up at some future point to
			// understand whether the stream is ascii or something else.
			cInt = source.read();
			peekEOF = (cInt == -1);
			if (!peekEOF) c = (char)cInt;
		} catch (IOException ie) {
			throw ijException.iOException(ie);
		}

		peekChar = c;
		return c;
	}
}
