/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
 *
 */

package com.splicemachine.db.client.cluster;

import com.splicemachine.db.iapi.reference.SQLState;

import java.net.ConnectException;
import java.net.SocketException;
import java.net.UnknownHostException;
import java.sql.SQLException;

/**
 * @author Scott Fines
 *         Date: 9/1/16
 */
public class ClientErrors{
    private ClientErrors(){} //ha-hah! you can't make me!

    static boolean willDisconnect(SQLException se){
        //TODO -sf- this is almost certainly not perfectly correct
        return ClientErrors.isNetworkError(se);
    }

    /**
     * Determine if the client error is in fact a transient error--that is, if
     * retrying the command on the same server stands a reasonable chance of working.
     *
     * @param se the error
     * @return true if the error is something that can be considered retryable
     */
    public static boolean isNetworkError(SQLException se){
        String sqlState=se.getSQLState();
        if(sqlState==null) return false; //we don't really know
        switch(sqlState){
            //the following are errors that allow connection retries
            case SQLState.DRDA_CONNECTION_TERMINATED:
            case SQLState.NO_CURRENT_CONNECTION:
            case SQLState.PHYSICAL_CONNECTION_ALREADY_CLOSED:
            case SQLState.CONNECT_SOCKET_EXCEPTION:
            case SQLState.SOCKET_EXCEPTION:
            case SQLState.CONNECT_UNABLE_TO_CONNECT_TO_SERVER:
            case SQLState.CONNECT_UNABLE_TO_OPEN_SOCKET_STREAM:
            case SQLState.AUTH_DATABASE_CONNECTION_REFUSED:
                return true;
            case "08001":
                /*
                 * This is a generic connection error, that derby exceptionfactory didn't parse; this
                 * means that we are wrapping an underlying exception, so we need to parse that
                 */
                @SuppressWarnings("ThrowableResultOfMethodCallIgnored") Throwable t = ErrorUtils.getRootCause(se);
                //put specific return exceptions here
                if(t instanceof ConnectException
                        || t instanceof UnknownHostException
                        || t instanceof SocketException) return true;
            default:
                //Derby tells us this isn't a network problem
                return false;
        }

    }
}
