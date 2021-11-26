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
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

import java.io.*;
import java.nio.charset.Charset;

@SuppressFBWarnings("EI_EXPOSE_REP2")
public class SQLStatement {

    private SQLSignature signature;
    private Token[]         tokens;

    enum PrepMode {
        NONE,    // don't prepare
        WHOLE,   // prepare the entire statement as is
        AUTO,    // prepare with auto-parameterization
        SUBST;   // prepare with parameter substitution
    };
    static PrepMode prepareMode = PrepMode.NONE;

    public SQLStatement(Token[] tokens) {
        this.tokens = tokens;
        signature = SQLSignature.getSignature(tokens);
    }

    public SQLSignature getSignature() {
        return signature;
    }

    public String getSQL() {
        switch (prepareMode) {
            case NONE:
            case WHOLE:
                return signature.getSQL(tokens);
            default:
                return signature.getSQL();
        }
    }

    public Token[] getParams() {
        switch (prepareMode) {
            case NONE:
            case WHOLE:
                return null;
            default:
                return signature.getParams(tokens);
        }
    }

    public String getId() {
        return signature.getId();
    }

    public boolean isQuery() {
        return tokens[0].string.equals("SELECT");
    }

    @Override
    public String toString() {
        return getId() + " " + getSQL();
    }

    public static SQLStatement getSqlStatement(String statement) throws IOException {
        if(statement == null) statement = "";
        String charset = Charset.defaultCharset().name();
        InputStream is = new ByteArrayInputStream(statement.getBytes(charset));
        try (BufferedReader in = new BufferedReader(new InputStreamReader(is, charset))) {
            SQLTokenizer lexer = new SQLTokenizer(in);
            return new SQLStatement(lexer.tokenize());
        }
    }
}
