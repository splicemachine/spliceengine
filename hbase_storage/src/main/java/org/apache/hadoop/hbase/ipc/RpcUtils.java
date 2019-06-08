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
 *
 */

package org.apache.hadoop.hbase.ipc;

import com.splicemachine.access.HConfiguration;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.security.UserProvider;
import org.apache.log4j.Logger;

import java.io.IOException;
import java.lang.reflect.Field;

public class RpcUtils {
    private static final Logger LOG = Logger.getLogger(RpcUtils.class);

    public static ThreadLocal<Boolean> accessAllowed = new ThreadLocal<>();

    public static RootEnv getRootEnv() throws IOException {
        return new RootEnv();
    }

    public static boolean isAccessAllowed() {
        Boolean allowed = accessAllowed.get();
        return allowed != null && allowed;
    }

    static Field userField;
    static UserProvider userProvider;

    static {
        try {
            userField = RpcServer.Call.class.getDeclaredField("user");
            userField.setAccessible(true);
            userProvider = UserProvider.instantiate(HConfiguration.unwrapDelegate());
        } catch (Exception e) {
            LOG.warn("Couldn't initialize userField/Provider");
        }
    }

    public static class RootEnv implements AutoCloseable {
        Boolean stored;
        User oldUser;

        RootEnv() throws IOException {
            stored = accessAllowed.get();
            accessAllowed.set(true);

            if (userField != null && userProvider != null) {
                RpcServer.Call call = RpcServer.CurCall.get();
                User newUser = userProvider.getCurrent();
                oldUser = call.getRequestUser();
                try {
                    userField.set(call, newUser);
                } catch (IllegalAccessException e) {
                    LOG.warn("Couldn't update user field");
                }
            }
        }

        @Override
        public void close() {
            accessAllowed.set(stored);
            RpcServer.Call call = RpcServer.CurCall.get();
            if (userField != null && userProvider != null) {
                try {
                    userField.set(call, oldUser);
                } catch (IllegalAccessException e) {
                    LOG.warn("Couldn't update user field");
                }
            }
        }
    }
}
