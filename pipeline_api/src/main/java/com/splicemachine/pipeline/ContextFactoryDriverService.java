/*
 * Copyright (c) 2012 - 2017 Splice Machine, Inc.
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
