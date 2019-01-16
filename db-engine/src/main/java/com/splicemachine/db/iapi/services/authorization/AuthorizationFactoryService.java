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

import java.util.Iterator;
import java.util.ServiceLoader;

public class AuthorizationFactoryService {

    public static AuthorizationFactory newAuthorizationFactory(){
        ServiceLoader<AuthorizationFactory> factoryService = ServiceLoader.load(AuthorizationFactory.class);
        Iterator<AuthorizationFactory> iter = factoryService.iterator();
        if(!iter.hasNext())
            throw new IllegalStateException("No AuthorizationFactory service found!");
        AuthorizationFactory af = null;
        AuthorizationFactory currentAF = null;
        while (iter.hasNext()) {
            currentAF = (AuthorizationFactory) iter.next();
            if (af == null || af.getPriority() < currentAF.getPriority())
                af = currentAF;
        }
        return af;
    }
}
