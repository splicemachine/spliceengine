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
 *
 */

package com.splicemachine.derby.impl;

import com.splicemachine.db.impl.drda.User;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.security.PrivilegedAction;
import java.security.PrivilegedExceptionAction;

public class UGIUser implements User {

    private final UserGroupInformation ugi;

    public UGIUser(UserGroupInformation ugi) {
        this.ugi = ugi;
    }

    @Override
    public <T> T doAs(PrivilegedAction<T> action) {
        return ugi.doAs(action);
    }

    @Override
    public String getUserName() {
        return ugi.getUserName();
    }

    @Override
    public <T> T doAs(PrivilegedExceptionAction<T> action) throws IOException, InterruptedException {
        return ugi.doAs(action);
    }
}
