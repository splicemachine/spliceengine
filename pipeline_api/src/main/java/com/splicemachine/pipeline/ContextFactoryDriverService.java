package com.splicemachine.pipeline;

import com.splicemachine.pipeline.contextfactory.ContextFactoryDriver;

import java.util.Iterator;
import java.util.ServiceLoader;

/**
 * @author Scott Fines
 *         Date: 1/6/16
 */
public class ContextFactoryDriverService{
    private static volatile ContextFactoryDriver driver;

    public static ContextFactoryDriver loadDriver(){
        ContextFactoryDriver cfd = driver;
        if(cfd==null){
            cfd = loadDriverInternal();
        }
        return cfd;
    }

    public static void setDriver(ContextFactoryDriver cfDriver){
        assert cfDriver!=null: "Cannot set a null Driver!";
        driver = cfDriver;
    }

    private static ContextFactoryDriver loadDriverInternal(){
        synchronized(ContextFactoryDriverService.class){
            if(driver==null){
                ServiceLoader<ContextFactoryDriver> load=ServiceLoader.load(ContextFactoryDriver.class);
                Iterator<ContextFactoryDriver> iter=load.iterator();
                if(!iter.hasNext())
                    throw new IllegalStateException("No ContextFactoryDriver found!");
                driver = iter.next();
                if(iter.hasNext())
                    throw new IllegalStateException("Only one ContextFactoryDriver is allowed!");
            }
        }
        return driver;
    }
}
