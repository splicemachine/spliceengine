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

package com.splicemachine.pipeline;

import com.splicemachine.db.client.am.SqlException;
import com.splicemachine.db.client.am.Sqlca;
import com.splicemachine.db.iapi.reference.SQLState;
import org.apache.log4j.Logger;
import splice.com.google.common.base.Throwables;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.pipeline.api.PipelineExceptionFactory;
import com.splicemachine.si.api.data.ExceptionFactory;
import com.splicemachine.si.impl.driver.SIDriver;

import java.io.IOException;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Created on: 3/5/13
 */
public class Exceptions {

    private static final Logger LOG = Logger.getLogger(Exceptions.class);

    private Exceptions(){} //can't make me

    public static StandardException parseException(Throwable e) {
        LOG.warn("Parsing exception, original stacktrace:", e);
        try {
            return parseException(e,PipelineDriver.driver().exceptionFactory(),SIDriver.driver().getExceptionFactory());
        } catch (Throwable t) {
            LOG.error("Unexpected error while parsing exception, root:", e);
            return StandardException.newException(SQLState.ERROR_PARSING_EXCEPTION, e, t.getMessage());
        }
    }

    public static StandardException parseException(Throwable e,PipelineExceptionFactory pef, ExceptionFactory baseEf) {
        try {
            if (e instanceof StandardException)
                return (StandardException) e;
            Throwable rootCause = Throwables.getRootCause(e);
            if (rootCause instanceof StandardException) {
                return (StandardException) rootCause;
            } else if (rootCause instanceof SqlException) {
                SqlException sqlException = (SqlException) rootCause;
                String state = sqlException.getSQLState();
                String messageID = sqlException.getMessage();
                Throwable next = sqlException.getNextException();
                Sqlca sca = sqlException.getSqlca();
                String [] args = sca.getArgs();
                if (args != null)
                    return StandardException.newException(state, next, args);
                else
                    return StandardException.newPreLocalizedException(state, next, messageID);
            } else if (rootCause instanceof SQLException) {
                return StandardException.plainWrapException(rootCause);
            }
            if (pef != null) {
                e = pef.processPipelineException(rootCause);
            } else {
                assert baseEf != null : "Cannot parse exception without an Exception Factory";
                e = baseEf.processRemoteException(rootCause);
            }

            ErrorState state = ErrorState.stateFor(e);

            return state.newException(e);
        } catch (Throwable t) {
            LOG.error("Unexpected error while parsing exception, root:", e);
            return StandardException.newException(SQLState.ERROR_PARSING_EXCEPTION, e, t.getMessage());
        }
    }

    /*
     * Wraps the underlying exception in an IOException, WITHOUT regard to retryability.
     */
    public static IOException rawIOException(Throwable t){
        t = Throwables.getRootCause(t);
        if(t instanceof IOException) return (IOException)t;
        else return new IOException(t);
    }

    public static IOException getIOException(Throwable t,PipelineExceptionFactory ef){
        t = Throwables.getRootCause(t);
        if(t instanceof StandardException){
            return ef.doNotRetry(t);
        }
        else if(t instanceof IOException) return (IOException)t;
        else return new IOException(t);
    }

    public static IOException getIOException(Throwable t){
        if (PipelineDriver.driver() != null) {
            return getIOException(t, PipelineDriver.driver().exceptionFactory());
        } else if (t instanceof IOException) {
            return (IOException) t;
        } else {
            return new IOException(t);
        }
    }

    public static RuntimeException getRuntimeException(Throwable t){
        return t instanceof RuntimeException ? (RuntimeException) t : new RuntimeException(t);
    }

    public static RuntimeException throwAsRuntime(Throwable t) {
        Exceptions.<RuntimeException> doThrow(t);
        return new RuntimeException();
    }

    @SuppressWarnings("unchecked")
    static <T extends Throwable> void doThrow(Throwable t) throws T {
        throw (T) t;
    }

}
