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

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class UserManagerService {
    private static volatile UserManager userManager;

    public static UserManager loadPropertyManager(){
        UserManager pm = userManager;
        if(pm==null){
            pm = loadPropertyManagerSync();
        }
        return pm;
    }

    private static synchronized UserManager loadPropertyManagerSync(){
        UserManager pm = userManager;
        if(pm==null){
            ServiceLoader<UserManager> load=ServiceLoader.load(UserManager.class);
            Iterator<UserManager> iter=load.iterator();
            if(!iter.hasNext())
                throw new IllegalStateException("No UserManager service found, make sure Enterprise Edition is enabled!");
            pm = userManager = iter.next();
            if(iter.hasNext())
                throw new IllegalStateException("Only one UserManager service is allowed!");
        }
        return pm;
    }
}
