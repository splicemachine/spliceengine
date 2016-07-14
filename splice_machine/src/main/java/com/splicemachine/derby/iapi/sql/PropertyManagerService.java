/*
 * Copyright 2012 - 2016 Splice Machine, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use
 * this file except in compliance with the License. You may obtain a copy of the
 * License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed
 * under the License is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR
 * CONDITIONS OF ANY KIND, either express or implied. See the License for the
 * specific language governing permissions and limitations under the License.
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
