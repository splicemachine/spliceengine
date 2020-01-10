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

package com.splicemachine.db.impl.sql.misc;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.impl.sql.compile.TokenMgrError;
/**
 * Created by yxia on 6/27/18.
 */
public class CommentStripperImpl implements CommentStripper {
    /*
    ** We will use the following constant to pass in to
    ** our CharStream.  It is the size of the internal
    ** buffers that are used to buffer tokens.  It
    ** should be set to what is typically around the
    ** largest token that is likely to be hit.  Note
    ** that if the size is exceeded, the buffer will
    ** automatically be expanded by 2048, so it is ok
    ** to choose something that is smaller than the
    ** max token supported.
    **
    ** Since, JavaCC generates parser and tokenmanagers classes
    ** tightly connected, to use another parser or tokenmanager
    ** inherit this class, override the following methods
    ** to use specific instances:<ul>
    ** <li>getTokenManager()</li>
    ** <li>getParser()</li>
    ** <li>parseGoalProduction(...)</li>
    ** </ul>
    **
    */
    static final int LARGE_TOKEN_SIZE = 128;

    private SQLCommentStripper cachedCommentStripper;
    protected Object cachedTokenManager;

    protected CharStream charStream;
    protected String SQLtext;

    public CommentStripperImpl() {
    }
    /**
     * Returns a initialized (clean) TokenManager, paired w. the Parser in getParser,
     * Appropriate for this ParserImpl object.
     */
    protected Object getTokenManager()
    {
	    /* returned a cached tokenmanager if already exists, otherwise create */
        SQLCommentStripperTokenManager tm = (SQLCommentStripperTokenManager) cachedTokenManager;
        if (tm == null) {
            tm = new SQLCommentStripperTokenManager(charStream);
            cachedTokenManager = tm;
        } else {
            tm.ReInit(charStream);
        }
        return tm;
    }

    public String stripStatement(String statementSQLText)
            throws StandardException {

        java.io.Reader sqlText = new java.io.StringReader(statementSQLText);

		/* Get a char stream if we don't have one already */
        if (charStream == null)
        {
            charStream = new UCode_CharStream(sqlText, 1, 1, LARGE_TOKEN_SIZE);
        }
        else
        {
            charStream.ReInit(sqlText, 1, 1, LARGE_TOKEN_SIZE);
        }

		/* remember the string that we're parsing */
        SQLtext = statementSQLText;

		/* Parse the statement, and return the QueryTree */
        try
        {
            return getStripper().strip();
        }
        catch (ParseException e)
        {
            throw StandardException.newException(SQLState.LANG_SYNTAX_ERROR, e.getMessage());
        }
        catch (TokenMgrError e)
        {
            cachedCommentStripper = null;
            throw StandardException.newException(SQLState.LANG_LEXICAL_ERROR, e.getMessage());
        }
    }

    /**
     * new parser, appropriate for the ParserImpl object.
     */
    private SQLCommentStripper getStripper()
    {
        SQLCommentStripperTokenManager tm = (SQLCommentStripperTokenManager) getTokenManager();
	    /* returned a cached comment stripper if already exists, otherwise create */
        SQLCommentStripper p = (SQLCommentStripper) cachedCommentStripper;
        if (p == null) {
            p = new SQLCommentStripper(tm);
            cachedCommentStripper = p;
        } else {
            p.ReInit(tm);
        }
        return p;
    }
    /**
     * Returns the current SQL text string that is being parsed.
     *
     * @return	Current SQL text string.
     *
     */
    public	String		getSQLtext() {
        return SQLtext;
    }

}
