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

package com.splicemachine.db.impl.drda;

import com.splicemachine.db.iapi.reference.Property;
import com.splicemachine.db.iapi.reference.SQLState;
import com.splicemachine.db.iapi.services.property.PropertyUtil;
import com.sun.security.jgss.GSSUtil;
import org.ietf.jgss.GSSContext;
import org.ietf.jgss.GSSCredential;
import org.ietf.jgss.GSSName;

import javax.security.auth.Subject;
import java.security.PrivilegedExceptionAction;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

public class RemoteUserKerberos implements RemoteUser {
    private GSSContext context;

    public RemoteUserKerberos(GSSContext context) {
        this.context = context;
    }

    public Connection getRemoteConnection(String hostname) throws SQLException {
        try {
            GSSCredential clientCr = context.getDelegCred();
            GSSName clientName = clientCr.getName(); 
            Subject subject = GSSUtil.createSubject(clientName, clientCr);
            String sslMode = PropertyUtil.getSystemProperty(Property.DRDA_PROP_SSL_MODE);
            String databaseURL = sslMode==null?
                    String.format("jdbc:splice://%s/splicedb;principal=%s",hostname, clientName.toString()):
                    String.format("jdbc:splice://%s/splicedb;principal=%s;ssl=%s",hostname, clientName.toString(), sslMode);
            return UserManagerService.loadPropertyManager().getUserFromSubject(subject).doAs(
                    (PrivilegedExceptionAction<Connection>) () -> DriverManager.getConnection(databaseURL));
        } catch (Exception e) {
            throw new SQLException(SQLState.AUTH_ERROR_KERBEROS_CLIENT,
                    SQLState.AUTH_ERROR_KERBEROS_CLIENT.substring(0,5), e);
        }
    }
}
