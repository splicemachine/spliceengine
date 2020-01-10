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
 */

package com.splicemachine.derby.iapi.sql;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 1/11/16
 */
public class PropertyManagerService{
    private static volatile PropertyManager propertyManager;

    public static PropertyManager loadPropertyManager(){
        PropertyManager pm = propertyManager;
        if(pm==null){
            pm = loadPropertyManagerSync();
        }
        return pm;
    }

    private static synchronized PropertyManager loadPropertyManagerSync(){
        PropertyManager pm = propertyManager;
        if(pm==null){
            ServiceLoader<PropertyManager> load=ServiceLoader.load(PropertyManager.class);
            Iterator<PropertyManager> iter=load.iterator();
            if(!iter.hasNext())
                throw new IllegalStateException("No PropertyManager service found!");
            pm = propertyManager = iter.next();
            //TODO -sf- initialize if needed
            if(iter.hasNext())
                throw new IllegalStateException("Only one PropertyManager service is allowed!");
        }
        return pm;
    }
}
