/*
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
 * Some parts of this source code are based on Apache Derby, and the following notices apply to
 * Apache Derby:
 *
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
 * Splice Machine, Inc. has modified the Apache Derby code in this file.
 *
 * All such Splice Machine modifications are Copyright 2012 - 2020 Splice Machine, Inc.,
 * and are licensed to you under the GNU Affero General Public License.
 */

package com.splicemachine.db.impl.jdbc.authentication;

import com.splicemachine.db.authentication.AccessTokenVerifier;
import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.reference.Attribute;
import com.splicemachine.db.iapi.services.monitor.Monitor;
import com.splicemachine.db.iapi.sql.dictionary.DataDictionary;
import com.splicemachine.db.iapi.sql.dictionary.UserDescriptor;
import com.splicemachine.db.iapi.util.IdUtil;
import com.splicemachine.db.impl.jdbc.Util;

import java.lang.reflect.Method;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

/**
 * <p>
 * This authentication service supports Derby NATIVE and TOKEN authentication.
 * </p>
 *
 * <p>
 * To activate this service, set the db.authentication.provider database
 * or system property to a value beginning with the token "TOKEN:".
 * </p>
 *
 * <p>
 * This service instantiates and calls the basic User authentication scheme at runtime.
 * </p>
 *
 * <p>
 * User credentials are defined in the SYSUSERS table.
 * </p>
 */
public final class TokenAuthenticationServiceImpl extends NativeAuthenticationServiceImpl {

    public String authenticateUser (String userName, String userPassword, String databaseName, Properties info) throws SQLException {
        if (userPassword != null) {
            return super.authenticateUser(userName, userPassword, databaseName, info);
        } else {
            return authenticateUserWithToken(userName, info);
        }
    }

    private String authenticateUserWithToken (String userName, Properties info) throws SQLException {
        try {
            String jwt = info.getProperty(Attribute.USER_TOKEN);
            if (jwt == null) {
                return null;
            }

            String tokenUserName = getTokenVerifier(info.getProperty(Attribute.USER_TOKEN_AUTHENTICATOR)).decodeUsername(jwt);

            if (!tokenUserName.equals(IdUtil.getUserAuthorizationId(userName))) {
                return null;
            }

            if (userName == null) {
                return null;
            }

            if (!authenticateLocally(userName)) {
                return null;
            }

            // check group mapping
            List<String> groupList = new ArrayList<>();
            groupList.add(userName);
            return mapUserGroups(groupList);
        } catch (StandardException se) {
            throw Util.generateCsSQLException(se);
        }
    }


    /**
     * Authenticate the passed-in credentials against the local database.
     *
     * @param userName     The user's name used to connect to JBMS system
     */
    private boolean authenticateLocally(String userName) throws StandardException {
        userName = IdUtil.getUserAuthorizationId(userName);

        //
        // we expect to find a data dictionary
        //
        DataDictionary dd = (DataDictionary) Monitor.getServiceModule(this, DataDictionary.MODULE);

        UserDescriptor userDescriptor = dd.getUser(userName);

        return userDescriptor != null;
    }

    private AccessTokenVerifier getTokenVerifier(String authenticatorName) throws StandardException {
        try {
            Class<?> verifierFactoryClass = Class.forName("com.splicemachine.db.impl.token.AccessTokenVerifierFactory");
            Method m = verifierFactoryClass.getDeclaredMethod("createVerifier", String.class);
            return (AccessTokenVerifier) m.invoke(null, authenticatorName);
        } catch (Exception e) {
            throw StandardException.plainWrapException(e);
        }

    }
}
