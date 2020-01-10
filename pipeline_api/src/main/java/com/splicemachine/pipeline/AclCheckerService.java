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

package com.splicemachine.pipeline;

import com.splicemachine.db.iapi.error.StandardException;
import com.splicemachine.db.shared.common.reference.SQLState;
import com.splicemachine.pipeline.security.AclChecker;

import java.util.Iterator;
import java.util.ServiceLoader;

public class AclCheckerService {
    private static volatile AclChecker service;

    public static AclChecker getService() throws StandardException {
        AclChecker result = service;
        if (result == null) {
            result = loadService();
        }
        return result;
    }

    private static synchronized AclChecker loadService() throws StandardException {
        if (service != null)
            return service;

        ServiceLoader<AclChecker> serviceLoader = ServiceLoader.load(AclChecker.class);
        Iterator<AclChecker> it = serviceLoader.iterator();
        if (!it.hasNext()) {
            throw StandardException.newException(SQLState.MANAGER_DISABLED);
        }
        service = it.next();
        return service;
    }
}
