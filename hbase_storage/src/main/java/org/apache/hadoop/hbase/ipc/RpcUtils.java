/*
 * Copyright (c) 2012 - 2018 Splice Machine, Inc.
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
 *
 */

package org.apache.hadoop.hbase.ipc;

public class RpcUtils {

    public static RootEnv getRootEnv() {
        return new RootEnv();
    }

    public static class RootEnv implements AutoCloseable {
        RpcServer.Call stored;

        RootEnv() {
            stored = (RpcServer.Call) RpcServer.getCurrentCall();
            RpcServer.CurCall.set(null);
        }

        @Override
        public void close() {
            RpcServer.CurCall.set(stored);
        }
    }
}

