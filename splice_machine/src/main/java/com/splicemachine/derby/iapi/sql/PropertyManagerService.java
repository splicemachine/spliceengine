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
