/*
 * Copyright (c) 2012 - 2019 Splice Machine, Inc.
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

package org.apache.hadoop.hbase.ipc;

import java.io.IOException;
import java.io.InterruptedIOException;

/**
 * Created by jyuan on 4/9/19.
 */
public class BlockingRpcCallback<R> implements org.apache.hbase.thirdparty.com.google.protobuf.RpcCallback<R>,
                                               com.google.protobuf.RpcCallback<R> {
    private R result;
    private boolean resultSet = false;

    public BlockingRpcCallback() {
    }

    public void run(R parameter) {
        synchronized(this) {
            this.result = parameter;
            this.resultSet = true;
            this.notifyAll();
        }
    }

    public synchronized R get() throws IOException {
        while(!this.resultSet) {
            try {
                this.wait();
            } catch (InterruptedException var3) {
                InterruptedIOException exception = new InterruptedIOException(var3.getMessage());
                exception.initCause(var3);
                throw exception;
            }
        }

        return this.result;
    }
}
