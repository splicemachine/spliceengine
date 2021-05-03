/*
 * Copyright (c) 2021 Splice Machine, Inc.
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

package com.splicemachine.qpt;

import java.io.BufferedReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

public class SQLTokenizer {

    private static final char EOF               = (char)-1;
    private static final char NEWLINE           = '\n';

    private static final int TOKEN_KEYWORD = 0x10000;
    private static final int TOKEN_DELIMITER    = 0x20000;

    public static final int TOKEN_SELECT        = TOKEN_KEYWORD | 1;
    public static final int TOKEN_INSERT        = TOKEN_KEYWORD | 2;
    public static final int TOKEN_DELETE        = TOKEN_KEYWORD | 3;
    public static final int TOKEN_UPDATE        = TOKEN_KEYWORD | 4;
    public static final int TOKEN_WHERE         = TOKEN_KEYWORD | 5;
    public static final int TOKEN_VALUES        = TOKEN_KEYWORD | 6;
    public static final int TOKEN_GROUP         = TOKEN_KEYWORD | 7;
    public static final int TOKEN_ORDER         = TOKEN_KEYWORD | 8;
    public static final int TOKEN_IN            = TOKEN_KEYWORD | 9;
    public static final int TOKEN_IS            = TOKEN_KEYWORD | 10;
    public static final int TOKEN_BY            = TOKEN_KEYWORD | 11;
    public static final int TOKEN_NOT           = TOKEN_KEYWORD | 12;
    public static final int TOKEN_OPTIMIZE      = TOKEN_KEYWORD | 13;
    public static final int TOKEN_FETCH         = TOKEN_KEYWORD | 14;
    public static final int TOKEN_CAST          = TOKEN_KEYWORD | 15;
    public static final int TOKEN_SET           = TOKEN_KEYWORD | 16;
    public static final int TOKEN_CHAR          = TOKEN_KEYWORD | 17;
    public static final int TOKEN_CONCAT        = TOKEN_KEYWORD | 18;
    public static final int TOKEN_EXPLAIN       = TOKEN_KEYWORD | 19;

    public static final int TOKEN_SLASH         = TOKEN_DELIMITER | 2;
    public static final int TOKEN_LPAR          = TOKEN_DELIMITER | 3;
    public static final int TOKEN_RPAR          = TOKEN_DELIMITER | 4;
    public static final int TOKEN_PIPE          = TOKEN_DELIMITER | 5;
    public static final int TOKEN_PIPED         = TOKEN_DELIMITER | 6;
    public static final int TOKEN_OTHER         = TOKEN_DELIMITER | 7;    /* everything else */

    public static final int TOKEN_NULL          = 1;
    public static final int TOKEN_NAME          = 2;
    public static final int TOKEN_SPLICEPROP    = 3;
    public static final int TOKEN_STRINGS       = 4;    /* single quoted string literal */
    public static final int TOKEN_STRINGD       = 5;    /* double quoted string literal */
    public static final int TOKEN_PARAMETER     = 6;    /* ? */
    public static final int TOKEN_NUMBER        = 7;
    public static final int TOKEN_STAR          = 8;    /* DB2 doesn't accept SELECT*FROM... */
    public static final int TOKEN_MINUS         = 9;    /* DB2 doesn't accept SELECT-SUM()... */

    public static class Token {
        public int      value;
        public String   string;

        Token(int v, String s) {
            this.value = v;
            this.string = s;
        }

        public boolean isDelimiter() {
            return (value & TOKEN_DELIMITER) != 0;
        }

        @Override
        public String toString() {
            if (value == TOKEN_STRINGS) {
                StringBuilder sb = new StringBuilder();
                sb.append("'");
                for (int i = 0; i < string.length(); ++i) {
                    char ch = string.charAt(i);
                    if (ch == '\'') sb.append(ch);
                    sb.append(ch);
                }
                sb.append("'");
                return sb.toString();
            }
            else if (value == TOKEN_STRINGD) {
                return '"' + string + '"';
            }
            else {
                return string;
            }
        }
    }

    private static HashMap<String, Token> tokenMap;
    static {
        tokenMap = new HashMap<>();
        addToMap(new Token(TOKEN_SELECT, "SELECT"));
        addToMap(new Token(TOKEN_INSERT, "INSERT"));
        addToMap(new Token(TOKEN_DELETE, "DELETE"));
        addToMap(new Token(TOKEN_UPDATE, "UPDATE"));
        addToMap(new Token(TOKEN_WHERE, "WHERE"));
        addToMap(new Token(TOKEN_GROUP, "GROUP"));
        addToMap(new Token(TOKEN_ORDER, "ORDER"));
        addToMap(new Token(TOKEN_VALUES, "VALUES"));
        addToMap(new Token(TOKEN_IN, "IN"));
        addToMap(new Token(TOKEN_IS, "IS"));
        addToMap(new Token(TOKEN_BY, "BY"));
        addToMap(new Token(TOKEN_NOT, "NOT"));
        addToMap(new Token(TOKEN_OPTIMIZE, "OPTIMIZE"));
        addToMap(new Token(TOKEN_FETCH, "FETCH"));
        addToMap(new Token(TOKEN_CAST, "CAST"));
        addToMap(new Token(TOKEN_SET, "SET"));
        addToMap(new Token(TOKEN_CHAR, "CHAR"));
        addToMap(new Token(TOKEN_CONCAT, "CONCAT"));
        addToMap(new Token(TOKEN_EXPLAIN, "EXPLAIN"));
        addToMap(new Token(TOKEN_NULL, "NULL"));
        addToMap(new Token(TOKEN_MINUS, "-"));
        addToMap(new Token(TOKEN_SLASH, "/"));
        addToMap(new Token(TOKEN_LPAR, "("));
        addToMap(new Token(TOKEN_RPAR, ")"));
        addToMap(new Token(TOKEN_PIPE, "|"));
        addToMap(new Token(TOKEN_PIPED, "||"));
        addToMap(new Token(TOKEN_PARAMETER, "?"));
        addToMap(new Token(TOKEN_STAR, "*"));
    }

    private static void addToMap(Token token) {
        tokenMap.put(token.string, token);
    }

    private BufferedReader reader;
    private boolean endOfFile;
    private String curLine;
    private int curIdx;

    private char nextCh() throws IOException {
        if (endOfFile) return EOF;
        int len = curLine.length();
        if (curIdx == len && len > 0) {
            ++curIdx;
            return NEWLINE;
        }
        if (curIdx >= len) {
            curIdx = 0;
            curLine = reader.readLine();
            if (curLine == null) {
                endOfFile = true;
                return EOF;
            }
            if (curLine.length() == 0) {
                return NEWLINE;
            }
        }
        return curLine.charAt(curIdx++);
    }

    public SQLTokenizer(BufferedReader reader) {
        this.reader = reader;
        endOfFile = false;
        curLine = "";
        curIdx = 0;
    }

    public Token[] tokenize() throws IOException {
        ArrayList<Token> tokens = new ArrayList<>();
        StringBuilder sb = new StringBuilder();
        boolean inComment = false;

        for (char ch = nextCh();;) {

            if (ch == EOF) {
                break;
            } else if (inComment) {
                if (ch == '*') {
                    ch = nextCh();
                    if (ch == '/') {
                        inComment = false;
                    }
                    else {
                        continue;
                    }
                }
            } else if (ch == '"' || ch == '\'') {
                char quote = ch;
                sb.setLength(0);
                for (;;) {
                    ch = nextCh();
                    if (ch == quote) {
                        ch = nextCh();
                        if (ch == quote) {
                            sb.append(ch);
                        }
                        else {
                            tokens.add(new Token(quote == '"' ? TOKEN_STRINGD : TOKEN_STRINGS, sb.toString()));
                            break;
                        }
                    }
                    else if (ch == EOF) {
                        throw new IOException("unclosed quote");
                    }
                    else {
                        sb.append(ch);
                    }
                }
                continue;
            } else if (ch == ';') {
                if (tokens.size() > 0) break;
            } else if (ch == '-') {
                ch = nextCh();
                if (ch == '-') {
                    String comment = curLine.substring(curIdx);
                    if (comment.startsWith("splice-properties")) {
                        sb.setLength(0);
                        sb.append("--").append(comment);
                        tokens.add(new Token(TOKEN_SPLICEPROP, sb.toString()));
                    }
                    curIdx = curLine.length() + 1;
                } else if (Character.isDigit(ch)) {
                    sb.setLength(0);
                    sb.append("-");
                    sb.append(ch);
                    for (;;) {
                        ch = nextCh();
                        if (!Character.isDigit(ch) && !Character.isAlphabetic(ch) && ch != '.' && ch != '-') break;
                        sb.append(ch);
                    }
                    tokens.add(new Token(TOKEN_NUMBER, sb.toString()));
                    continue;
                } else {
                    tokens.add(tokenMap.get("-"));
                    continue;
                }
            } else if (ch == '/') {
                ch = nextCh();
                if (ch == '*') {
                    inComment = true;
                } else {
                    tokens.add(tokenMap.get("/"));
                    continue;
                }
            } else if (ch == '|') {
                ch = nextCh();
                if (ch == '|') {
                    tokens.add(tokenMap.get("||"));
                }
                else {
                    tokens.add(tokenMap.get("|"));
                    continue;
                }
            } else if (Character.isAlphabetic(ch) || ch == '_' || ch == '@' || ch == '#') {
                sb.setLength(0);
                sb.append(ch);
                char ch0 = ch;
                ch = nextCh();
                if (ch == '\'' && (ch0 == 'b' || ch0 == 'B' || ch0 == 'x' || ch0 == 'X')) {
                    sb.append(ch);
                    for (;;) {
                        ch = nextCh();
                        if (ch == '\n' || ch == EOF) {
                            throw new IOException("unclosed quote");
                        }
                        sb.append(ch);
                        if (ch == '\'') {
                            break;
                        }
                    }
                    tokens.add(new Token(TOKEN_NUMBER, sb.toString()));
                    ch = nextCh();
                    continue;
                }
                for (;;ch = nextCh()) {
                    if (!Character.isAlphabetic(ch) && !Character.isDigit(ch) && ch != '_' && ch != '@' && ch != '#' && ch != '$') break;
                    sb.append(ch);
                }
                String s = sb.toString().toUpperCase();
                Token token = tokenMap.get(s);
                if (token == null) {
                    token = new Token(TOKEN_NAME, s);
                }
                tokens.add(token);
                continue;
            } else if (Character.isDigit(ch)) {
                sb.setLength(0);
                sb.append(ch);
                for (;;) {
                    ch = nextCh();
                    if (!Character.isDigit(ch) && !Character.isAlphabetic(ch) && ch != '.' && ch != '-') break;
                    sb.append(ch);
                }
                tokens.add(new Token(TOKEN_NUMBER, sb.toString()));
                continue;
            } else if (!Character.isWhitespace(ch)) {
                String key = Character.toString(ch);
                Token token = tokenMap.get(key);
                if (token == null) {
                    token = new Token(TOKEN_OTHER, key);
                }
                tokens.add(token);
            }
            ch = nextCh();
        }

        if (endOfFile && tokens.size() == 0) return new Token[0];
        return tokens.toArray(new Token[tokens.size()]);
    }
}
