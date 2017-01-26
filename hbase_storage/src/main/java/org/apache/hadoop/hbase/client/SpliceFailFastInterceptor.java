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
package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.DoNotRetryIOException;
import org.apache.hadoop.ipc.RemoteException;

import java.io.IOException;
import java.lang.reflect.UndeclaredThrowableException;

/**
 * Created by jleach on 8/17/16.
 */
public class SpliceFailFastInterceptor extends RetryingCallerInterceptor {
    private static final RetryingCallerInterceptorContext NO_OP_CONTEXT =
            new NoOpRetryingInterceptorContext();
    protected SpliceFailFastInterceptor() {
        super();
    }

    @Override
    public RetryingCallerInterceptorContext createEmptyContext() {
        return NO_OP_CONTEXT;
    }

    @Override
    public void handleFailure(RetryingCallerInterceptorContext context, Throwable t) throws IOException {

        if (t instanceof UndeclaredThrowableException) {
            t = t.getCause();
        }
        if (t instanceof RemoteException) {
            RemoteException re = (RemoteException)t;
            t = re.unwrapRemoteException();
        }
        if (t instanceof DoNotRetryIOException) {
            throw (DoNotRetryIOException)t;
        }
        if (t instanceof IOException) {
            throw (IOException) t;
        }
        throw new IOException(t);
    }

    @Override
    public void intercept(RetryingCallerInterceptorContext abstractRetryingCallerInterceptorContext) throws IOException {

    }

    @Override
    public void updateFailureInfo(RetryingCallerInterceptorContext context) {
    }

    @Override
    public String toString() {
        return null;
    }
}
