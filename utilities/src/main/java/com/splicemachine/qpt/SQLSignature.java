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

import com.splicemachine.qpt.SQLTokenizer.Token;

import java.util.ArrayList;
import java.util.concurrent.ConcurrentHashMap;

public class SQLSignature {

    private static ConcurrentHashMap<Long,SQLSignature> signatures = new ConcurrentHashMap<>();

    private String      id;
    private Token[]     tokens;
    private long        hashCode;

    private SQLSignature(Token[] tokens) {
        this.tokens = parameterize(tokens);
        hashCode = computeHash(tokens);
        String prefix;
        if(tokens.length == 0)
            prefix = "#";
        else {
            switch (tokens[0].value) {
                case SQLTokenizer.TOKEN_DELETE:
                case SQLTokenizer.TOKEN_INSERT:
                case SQLTokenizer.TOKEN_SELECT:
                case SQLTokenizer.TOKEN_UPDATE:
                    prefix = tokens[0].string.substring(0, 1);
                    break;
                default:
                    prefix = "#";
                    break;
            }
        }
        id = Encoding.makeId(prefix, hashCode);
    }

    private static Token[] parameterize(Token[] orig) {
        ArrayList<Token> tlist = new ArrayList<>();
        Token parameter = new Token(SQLTokenizer.TOKEN_PARAMETER, "?");
        boolean allowParameters = false;
        int curLevel = 0;
        boolean[] savedAllow = new boolean[64];
        savedAllow[curLevel] = allowParameters;
        for (int i = 0; i < orig.length; ++i) {
            Token token = orig[i];
            int prevToken = i > 0 ? orig[i - 1].value : -1;
            int nextToken = i < orig.length - 1 ? orig[i + 1].value : -1;

            switch (token.value) {
                case SQLTokenizer.TOKEN_LPAR:
                    savedAllow[curLevel++] = allowParameters;
                    if (prevToken == SQLTokenizer.TOKEN_CHAR) {
                        allowParameters = false;
                    }
                    break;
                case SQLTokenizer.TOKEN_RPAR:
                    allowParameters = savedAllow[--curLevel];
                    break;
                case SQLTokenizer.TOKEN_WHERE:
                case SQLTokenizer.TOKEN_VALUES:
                case SQLTokenizer.TOKEN_SET:
                    allowParameters = true;
                    break;
                case SQLTokenizer.TOKEN_SELECT:
                case SQLTokenizer.TOKEN_UPDATE:
                case SQLTokenizer.TOKEN_INSERT:
                case SQLTokenizer.TOKEN_DELETE:
                    allowParameters = false;
                    break;
                case SQLTokenizer.TOKEN_BY:
                    if (prevToken == SQLTokenizer.TOKEN_GROUP || prevToken == SQLTokenizer.TOKEN_ORDER) {
                        allowParameters = false;
                    }
                    break;
                case SQLTokenizer.TOKEN_OPTIMIZE:
                case SQLTokenizer.TOKEN_FETCH:
                    allowParameters = false;
                    break;
                default:
                    // fallthrough
            }

            if (isExcluded(token) && allowParameters) {
                if (token.value == SQLTokenizer.TOKEN_NULL) {
                    if (prevToken != SQLTokenizer.TOKEN_IS && prevToken != SQLTokenizer.TOKEN_NOT) {
                        token = parameter;
                    }
                }
                else if (token.value == SQLTokenizer.TOKEN_STRINGS) {
                    if (prevToken != SQLTokenizer.TOKEN_PIPED && nextToken != SQLTokenizer.TOKEN_PIPED &&
                            prevToken != SQLTokenizer.TOKEN_CONCAT && nextToken != SQLTokenizer.TOKEN_CONCAT) {
                        token = parameter;
                    }
                }
                else {
                    token = parameter;
                }
            }
            tlist.add(token);
        }
        return tlist.toArray(new Token[tlist.size()]);
    }

    private static boolean isExcluded(Token token) {
        switch (token.value) {
            case SQLTokenizer.TOKEN_NULL:
            case SQLTokenizer.TOKEN_STRINGS:
            case SQLTokenizer.TOKEN_NUMBER:
                return true;
        }
        return false;
    }

    /* An attempt to automate parameterization based on changing values
    private synchronized void apply(Token[] newTokens) {
        if (tokens.length != newTokens.length) throw new RuntimeException("SQLSignature mismatch (possible hash collision)");
        for (int i = 0; i < tokens.length; ++i) {
            if (parameter[i]) continue;
            Token token = tokens[i];
            if (isExcluded(token) && !token.string.equals(newTokens[i].string)) {
                parameter[i] = true;
            }
        }
    }
    */

    public static SQLSignature getSignature(Token[] tokens) {
        SQLSignature signature = signatures.computeIfAbsent(computeHash(tokens), (key) -> new SQLSignature(tokens));
//        signature.apply(tokens);
        return signature;
    }

    public String getId() {
        return id;
    }

    public String getSQL() {
        return getSQL(null);
    }

    public String getSQL(Token[] orig) {
        StringBuilder sb = new StringBuilder();
        boolean addSpace = false;
        for (int i = 0; i < tokens.length; ++i) {
            Token token = tokens[i];
            if (token.isDelimiter()) {
                addSpace = false;
            }
            else {
                if (addSpace) {
                    sb.append(" ");
                }
                addSpace = true;
            }
            if (token.value == SQLTokenizer.TOKEN_PARAMETER && orig != null) {
                sb.append(orig[i].toString());
            }
            else {
                sb.append(token.toString());
            }
        }
        return sb.toString();
    }

    public Token[] getParams(Token[] orig) {
        ArrayList<Token> params = new ArrayList<>();
        for (int i = 0; i < tokens.length; ++i) {
            if (tokens[i].value == SQLTokenizer.TOKEN_PARAMETER) {
                params.add(orig[i]);
            }
        }
        return params.toArray(new Token[params.size()]);
    }

    private static long computeHash(Token[] tokens) {
        long hash = 0;
        for (Token token : tokens) {
            hash = 31 * hash + (isExcluded(token) ? 1 : token.string.hashCode());
        }
        return hash;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj) return true;
        if (!(obj instanceof SQLSignature)) return false;
        SQLSignature other = (SQLSignature)obj;
        if (hashCode != other.hashCode) return false;
        if (tokens.length != other.tokens.length) return false;
        for (int i = 0; i < tokens.length; ++i) {
            if (tokens[i] != other.tokens[i]) {
                if (tokens[i].value != other.tokens[i].value) return false;
                if (!tokens[i].string.equals(other.tokens[i].string)) return false;
            }
        }
        return true;
    }

    @Override
    public int hashCode() {
        return (int)hashCode;
    }
}
