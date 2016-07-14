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
