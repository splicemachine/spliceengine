/*
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
 * Splice Machine, Inc. has modified this file.
 *
 * All Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the License; you may not use this file except in
 * compliance with the License.
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.am;

import java.io.UnsupportedEncodingException;
import java.sql.DataTruncation;
import java.util.ArrayList;
import java.util.List;

import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.db.client.net.Typdef;
import edu.umd.cs.findbugs.annotations.SuppressFBWarnings;

public abstract class Sqlca {
    transient protected ClientConnection connection_;
    SqlException exceptionThrownOnStoredProcInvocation_;
    @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "messageTextRetrievedContainsTokensOnly_ is read in ExceptionFormatter.printTrace")
    boolean messageTextRetrievedContainsTokensOnly_ = true;

    // data corresponding to SQLCA fields
    protected int sqlCode_;        // SQLCODE
    /** A string representation of <code>sqlErrmcBytes_</code>. */
    private String sqlErrmc_;
    /** Array of errmc strings for each message in the chain. */
    protected String[] sqlErrmcMessages_;
    /** SQL states for all the messages in the exception chain. */
    private String[] sqlStates_;
    // contain an error token
    protected String sqlErrp_;        // function name issuing error
    protected int[] sqlErrd_;        // 6 diagnostic Information
    protected char[] sqlWarn_;        // 11 warning Flags
    protected String sqlState_;       // SQLSTATE

    // raw sqlca data fields before unicode conversion
    protected byte[] sqlErrmcBytes_;
    protected byte[] sqlErrpBytes_;
    protected byte[] sqlWarnBytes_;
    
    protected boolean containsSqlcax_ = true;
    protected long rowsetRowCount_;

    /**
     * Character sequence that separates the different messages in the errmc.
     * @see com.splicemachine.db.catalog.SystemProcedures#SQLERRMC_MESSAGE_DELIMITER
     */
    private static final String sqlErrmcDelimiter__ = "\u0014\u0014\u0014";

    /** Token delimiter for SQLERRMC. */
    private final static String SQLERRMC_TOKEN_DELIMITER = "\u0014";

    // JDK stack trace calls e.getMessage(), so we must set some state on the sqlca that says return tokens only.
    private boolean returnTokensOnlyInMessageText_ = false;

    transient private final Agent agent_;

    /** Cached error messages (to prevent multiple invocations of the stored
     * procedure to get the same message). */
    private String[] cachedMessages;

    protected Sqlca(ClientConnection connection) {
        connection_ = connection;
        agent_ = connection_ != null ? connection_.agent_ : null;
    }

    void returnTokensOnlyInMessageText(boolean returnTokensOnlyInMessageText) {
        returnTokensOnlyInMessageText_ = returnTokensOnlyInMessageText;
    }

    /**
     * Returns the number of messages this SQLCA contains.
     *
     * @return number of messages
     */
    synchronized int numberOfMessages() {
        initSqlErrmcMessages();
        if (sqlErrmcMessages_ != null) {
            return sqlErrmcMessages_.length;
        }
        // even if we don't have an array of errmc messages, we are able to get
        // one message out of this sqlca (although it's not very readable)
        return 1;
    }

    synchronized public int getSqlCode() {
        return sqlCode_;
    }

    synchronized public String getSqlErrmc() {
        if (sqlErrmc_ != null) {
            return sqlErrmc_;
        }

        // sqlErrmc string is dependent on sqlErrmcMessages_ array having
        // been built
        initSqlErrmcMessages();

        // sqlErrmc will be built only if sqlErrmcMessages_ has been built.
        // Otherwise, a null string will be returned.
        if (sqlErrmcMessages_ == null) {
            return null;
        }

        // create 0-length String if no tokens
        if (sqlErrmcMessages_.length == 0) {
            sqlErrmc_ = "";
            return sqlErrmc_;
        }

        // concatenate tokens with sqlErrmcDelimiter delimiters into one String
        StringBuilder buffer = new StringBuilder();
        int indx;
        for (indx = 0; indx < sqlErrmcMessages_.length - 1; indx++) {
            buffer.append(sqlErrmcMessages_[indx]);
            buffer.append(sqlErrmcDelimiter__);
            // all but the first message should be preceded by the SQL state
            // and a colon (see DRDAConnThread.buildTokenizedSqlerrmc() on the
            // server)
            buffer.append(sqlStates_[indx+1]);
            buffer.append(":");
        }
        // add the last token
        buffer.append(sqlErrmcMessages_[indx]);

        // save as a string
        sqlErrmc_ = buffer.toString();
        return sqlErrmc_;
    }

    /**
     * Initialize and build the arrays <code>sqlErrmcMessages_</code> and
     * <code>sqlStates_</code>.
     */
    @SuppressFBWarnings(value = "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD", justification = "the value is set in NetSqlcla.setSqlerrmcBytes")
    private void initSqlErrmcMessages() {
        if (sqlErrmcMessages_ == null || sqlStates_ == null) {
            // processSqlErrmcTokens handles null sqlErrmcBytes_ case
            processSqlErrmcTokens(sqlErrmcBytes_);
        }
    }

    synchronized public String getSqlErrp() {
        if (sqlErrp_ != null) {
            return sqlErrp_;
        }

        if (sqlErrpBytes_ == null) {
            return null;
        }

        try {
            sqlErrp_ = bytes2String(sqlErrpBytes_,
                    0,
                    sqlErrpBytes_.length);
            return sqlErrp_;
        } catch (java.io.UnsupportedEncodingException e) {
            // leave sqlErrp as null.
            return null;
        }
    }

    @SuppressFBWarnings(value = "EI_EXPOSE_REP", justification = "DB-9811")
    public int[] getSqlErrd() {
        if (sqlErrd_ != null) {
            return sqlErrd_;
        }

        sqlErrd_ = new int[6]; // create an int array.
        return sqlErrd_;
    }

    @SuppressFBWarnings(value = {"EI_EXPOSE_REP", "UWF_UNWRITTEN_PUBLIC_OR_PROTECTED_FIELD"}, justification = "DB-9811, sqlWarn_ is set in NetSqlca.setSqlwarnBytes")
    synchronized public char[] getSqlWarn() {
        if (sqlWarn_ != null) {
            return sqlWarn_;
        }

        try {
            if (sqlWarnBytes_ == null) {
                sqlWarn_ = new char[]{' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '}; // 11 blank.
            } else {
                sqlWarn_ = bytes2String(sqlWarnBytes_, 0, sqlWarnBytes_.length).toCharArray();
            }
            return sqlWarn_;
        } catch (java.io.UnsupportedEncodingException e) {
            sqlWarn_ = new char[]{' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' ', ' '}; // 11 blank.
            return sqlWarn_;
        }
    }

    synchronized public String getSqlState() {
        return sqlState_;
    }

    /**
     * Get the SQL state for a given error.
     *
     * @param messageNumber the error to retrieve SQL state for
     * @return SQL state for the error
     */
    synchronized String getSqlState(int messageNumber) {
        initSqlErrmcMessages();
        if (sqlStates_ != null) {
            return sqlStates_[messageNumber];
        }
        return getSqlState();
    }

    // Gets the formatted message, can throw an exception.
    private String getMessage(int messageNumber) throws SqlException {
        // should this be traced to see if we are calling a stored proc?
        if (cachedMessages != null && cachedMessages[messageNumber] != null) {
            return cachedMessages[messageNumber];
        }

        if (connection_ == null || connection_.isClosedX() || returnTokensOnlyInMessageText_) {
            return getUnformattedMessage(messageNumber);
        }

        CallableStatement cs = null;
        synchronized (connection_) {
            try {
                cs = connection_.prepareMessageProc("call SYSIBM.SQLCAMESSAGE(?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

                String errmc = null;
                String sqlState = null;

                if (sqlErrmcMessages_ != null) {
                    errmc = sqlErrmcMessages_[messageNumber];
                    sqlState = sqlStates_[messageNumber];
                }

                // SQLCode: SQL return code.
                cs.setIntX(1, (messageNumber == 0) ? getSqlCode() : 0);
                // SQLErrml: Length of SQL error message tokens.
                cs.setShortX(2, (short) ((errmc == null) ? 0 : errmc.length()));
                // SQLErrmc: SQL error message tokens as a String
                cs.setStringX(3, errmc);
                // SQLErrp: Product signature.
                cs.setStringX(4, getSqlErrp());
                // SQLErrd: SQL internal error code.
                cs.setIntX(5, getSqlErrd()[0]);
                cs.setIntX(6, getSqlErrd()[1]);
                cs.setIntX(7, getSqlErrd()[2]);
                cs.setIntX(8, getSqlErrd()[3]);
                cs.setIntX(9, getSqlErrd()[4]);
                cs.setIntX(10, getSqlErrd()[5]);
                // SQLWarn: SQL warning flags.
                cs.setStringX(11, new String(getSqlWarn()));
                // SQLState: standard SQL state.
                cs.setStringX(12, sqlState);
                // MessageFileName: Not used by our driver, so set to null.
                cs.setStringX(13, null);
                // Locale: language preference requested for the return error message.
                cs.setStringX(14, java.util.Locale.getDefault().toString());
                // server could return a locale different from what we requested
                cs.registerOutParameterX(14, java.sql.Types.VARCHAR);
                // Message: error message returned from SQLCAMessage stored procedure.
                cs.registerOutParameterX(15, java.sql.Types.LONGVARCHAR);
                // RCode: return code from SQLCAMessage stored procedure.
                cs.registerOutParameterX(16, java.sql.Types.INTEGER);
                cs.executeX();

                if (cs.getIntX(16) == 0) {
                    // Return the message text.
                    messageTextRetrievedContainsTokensOnly_ = false;
                    String message = cs.getStringX(15);
                    if (cachedMessages == null) {
                        cachedMessages = new String[numberOfMessages()];
                    }
                    cachedMessages[messageNumber] = message;
                    return message;
                } else {
                    // Stored procedure can't return a valid message text, so we return
                    // unformated exception
                    return getUnformattedMessage(messageNumber);
                }
            } finally {
                if (cs != null) {
                    try {
                        cs.closeX();
                    } catch (SqlException doNothing) {
                    }
                }
            }
        }
    }

    // May or may not get the formatted message depending upon datasource directives.  cannot throw exeption.
    @SuppressFBWarnings(value = "URF_UNREAD_FIELD", justification = "exceptionThrownOnStoredProcInvocation is read in ExceptionFormatter.printTrace")
    synchronized String getJDBCMessage(int messageNumber) {
        // The transient connection_ member will only be null if the Sqlca has been deserialized
        if (connection_ != null && connection_.retrieveMessageText_) {
            try {
                return getMessage(messageNumber);
            } catch (SqlException e) {
                // Invocation of stored procedure fails, so we return error message tokens directly.
                exceptionThrownOnStoredProcInvocation_ = e;
                chainDeferredExceptionsToAgentOrAsConnectionWarnings((SqlException) e);
                return getUnformattedMessage(messageNumber);
            }
        } else {
            return getUnformattedMessage(messageNumber);
        }
    }

    /**
     * Get the unformatted message text (in case we cannot ask the server).
     *
     * @param messageNumber which message number to get the text for
     * @return string with details about the error
     */
    private String getUnformattedMessage(int messageNumber) {
        int sqlCode;
        String sqlState;
        String sqlErrmc;
        if (messageNumber == 0) {
            // if the first exception in the chain is requested, return all the
            // information we have
            sqlCode = getSqlCode();
            sqlState = getSqlState();
            sqlErrmc = getSqlErrmc();
        } else {
            // otherwise, return information about the specified error only
            sqlCode = 0;
            sqlState = sqlStates_[messageNumber];
            sqlErrmc = sqlErrmcMessages_[messageNumber];
        }
        return "DERBY SQL error: SQLCODE: " + sqlCode + ", SQLSTATE: " +
            sqlState + ", SQLERRMC: " + sqlErrmc;
    }

    private void chainDeferredExceptionsToAgentOrAsConnectionWarnings(SqlException e) {
        SqlException current = e;
        while (current != null) {
            SqlException next = (SqlException) current.getNextException();
            current = current.copyAsUnchainedSQLException(agent_.logWriter_);
            if (current.getErrorCode() == -440) {
                SqlWarning warningForStoredProcFailure = new SqlWarning(agent_.logWriter_,
                    new ClientMessageId(SQLState.UNABLE_TO_OBTAIN_MESSAGE_TEXT_FROM_SERVER));
                warningForStoredProcFailure.setNextException(current.getSQLException());
                connection_.accumulate440WarningForMessageProcFailure(warningForStoredProcFailure);
            } else if (current.getErrorCode() == -444) {
                SqlWarning warningForStoredProcFailure = new SqlWarning(agent_.logWriter_,
                    new ClientMessageId(SQLState.UNABLE_TO_OBTAIN_MESSAGE_TEXT_FROM_SERVER));
                warningForStoredProcFailure.setNextException(current.getSQLException());
                connection_.accumulate444WarningForMessageProcFailure(warningForStoredProcFailure);
            } else {
                agent_.accumulateDeferredException(current);
            }
            current = next;
        }
    }

    public boolean includesSqlCode(int[] codes) {
        for (int i = 0; i < codes.length; i++) {
            if (codes[i] == getSqlCode()) {
                return true;
            }
        }
        return false;
    }

    /**
     * Get a {@code java.sql.DataTruncation} warning based on the information
     * in this SQLCA.
     *
     * @return a {@code java.sql.DataTruncation} instance
     */
    DataTruncation getDataTruncation() {
        // The network server has serialized all the parameters needed by
        // the constructor in the SQLERRMC field.
        String[] tokens = getSqlErrmc().split(SQLERRMC_TOKEN_DELIMITER);
        return new DataTruncation(
                Integer.parseInt(tokens[0]),                // index
                Boolean.valueOf(tokens[1]).booleanValue(),  // parameter
                Boolean.valueOf(tokens[2]).booleanValue(),  // read
                Integer.parseInt(tokens[3]),                // dataSize
                Integer.parseInt(tokens[4]));               // transferSize
    }

    // ------------------- helper methods ----------------------------------------

    private void processSqlErrmcTokens(byte[] tokenBytes) {
        if (tokenBytes == null) {
            return;
        }

        // create 0-length String tokens array if tokenBytes is 0-length
        int length = tokenBytes.length;
        if (length == 0) {
            sqlStates_ = sqlErrmcMessages_ = new String[0];
            return;
        }

        try {
            // tokenize and convert tokenBytes
            String fullString = bytes2String(tokenBytes, 0, length);
            String[] tokens = fullString.split("\\u0014{3}");
            String[] states = new String[tokens.length];
            states[0] = getSqlState();
            for (int i = 1; i < tokens.length; i++) {
                // All but the first message are preceded by the SQL state
                // (five characters) and a colon. Extract the SQL state and
                // clean up the token. See
                // DRDAConnThread.buildTokenizedSqlerrmc() for more details.
                int colonpos = tokens[i].indexOf(":");
                states[i] = tokens[i].substring(0, colonpos);
                tokens[i] = tokens[i].substring(colonpos + 1);
            }
            sqlStates_ = states;
            sqlErrmcMessages_ = tokens;
        } catch (java.io.UnsupportedEncodingException e) {
            /* do nothing, the arrays continue to be null */
        }
    }

    protected String bytes2String(byte[] bytes, int offset, int length)
            throws java.io.UnsupportedEncodingException {
        // Network server uses utf8 encoding
        return new String(bytes, offset, length, Typdef.UTF8ENCODING);
    }

    public int getUpdateCount() {
        if (sqlErrd_ == null) {
            return 0;
        }
        return sqlErrd_[2];
    }

    public long getRowCount() {
        return ((long) sqlErrd_[0] << 32) + sqlErrd_[1];
    }

    public void setContainsSqlcax(boolean containsSqlcax) {
        containsSqlcax_ = containsSqlcax;
    }

    public boolean containsSqlcax() {
        return containsSqlcax_;
    }

    @SuppressFBWarnings(value = {"EI_EXPOSE_REP2", "IS2_INCONSISTENT_SYNC"}, justification = "DB-9811, DB-9812")
    public void resetRowsetSqlca(ClientConnection connection,
                                 int sqlCode,
                                 String sqlState,
                                 byte[] sqlErrpBytes) {
        connection_ = connection;
        sqlCode_ = sqlCode;
        sqlState_ = sqlState;
        sqlErrpBytes_ = sqlErrpBytes;
    }

    public void setRowsetRowCount(long rowCount) {
        rowsetRowCount_ = rowCount;
    }

    public long getRowsetRowCount() {
        return rowsetRowCount_;
    }

    public String [] getArgs() {
        if (sqlErrmcBytes_ == null || sqlErrmcBytes_.length == 0)
            return null;
        int beginOffset = 0;
        List<String> argList = new ArrayList<>();
        char delimiter = Sqlca.SQLERRMC_TOKEN_DELIMITER.charAt(0);
        try {
            for (int i = 0; i < sqlErrmcBytes_.length; i++) {
                if (sqlErrmcBytes_[i] == delimiter) {
                    argList.add(bytes2String(sqlErrmcBytes_, beginOffset, i - beginOffset));
                    beginOffset = i + 1;
                }
            }
            return argList.toArray(new String[0]);
        }
        catch (UnsupportedEncodingException e) {
            return null;
        }
    }
}

