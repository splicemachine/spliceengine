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
package com.splicemachine.db.iapi.services.authorization;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.iapi.sql.conn.Authorizer;
import com.splicemachine.db.iapi.sql.conn.LanguageConnectionContext;
import com.splicemachine.db.impl.sql.conn.GenericAuthorizer;

/**
 * Created by jleach on 3/19/18.
 */
public class GenericAuthorizationFactory implements AuthorizationFactory {
    @Override
    public Authorizer getAuthorizer(LanguageConnectionContext lcc) throws StandardException {
        return new GenericAuthorizer(lcc);
    }

    @Override
    public int getPriority() {
        return 1;
    }
}
